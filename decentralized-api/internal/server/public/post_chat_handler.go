package public

import (
	"bytes"
	"context"
	"decentralized-api/apiconfig"
	"decentralized-api/completionapi"
	"decentralized-api/logging"
	"decentralized-api/utils"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/labstack/echo/v4"
	"github.com/productscience/inference/cmd/inferenced/cmd"
	"github.com/productscience/inference/x/inference/calculations"
	"github.com/productscience/inference/x/inference/types"
	"go.opentelemetry.io/otel/attribute"
)

// AuthKeyContext represents the context in which an AuthKey was used
type AuthKeyContext int

const (
	// TransferContext indicates the AuthKey was used for a transfer request
	TransferContext AuthKeyContext = 1
	// ExecutorContext indicates the AuthKey was used for an executor request
	ExecutorContext AuthKeyContext = 2
	// BothContexts indicates the AuthKey was used for both transfer and executor requests
	BothContexts = TransferContext | ExecutorContext

	// MaxRequestBodySize is the maximum allowed size for request bodies (10 MiB)
	MaxRequestBodySize = 10 * 1024 * 1024
	// MaxRequestBodyLimit is the Echo body-limit middleware value that matches MaxRequestBodySize exactly.
	MaxRequestBodyLimit = "10485760"

	chatCompletionsPath = "/v1/chat/completions"
)

const executorCompletionsUnsupportedMsg = "selected executor does not support /v1/completions; upgrade required"

// Package-level variables for AuthKey reuse prevention
var (
	// Map for O(1) lookup of existing AuthKeys and their contexts
	usedAuthKeys = make(map[string]AuthKeyContext)

	// Map for O(1) lookup of what to remove, organized by block height
	authKeysByBlock = make(map[int64][]string)

	// Track the oldest block height we're storing
	oldestBlockHeight int64

	// Mutex for thread safety
	authKeysMutex sync.RWMutex

	// Reference to the config manager for accessing validation parameters
	configManagerRef *apiconfig.ConfigManager
)

func NewNoRedirectClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// emptyButParseableResponsePayload returns a deterministic "empty" response payload that:
// - is valid JSON parseable by older validators
// - yields no logits (so validator re-execution cannot meaningfully compare)
// - produces a stable response hash (hash is over these exact bytes)
//
// IMPORTANT: This payload is committed via `ResponseHash` on-chain and served to validators.
func emptyButParseableResponsePayload(inferenceId, model string, promptTokens uint64) *completionapi.JsonCompletionResponse {
	choice := completionapi.Choice{
		Index:        0,
		Message:      &completionapi.Message{Role: "assistant", Content: ""},
		FinishReason: "error",
		StopReason:   "",
	}
	// Provide a minimal synthetic logprob entry so older validators won't end up with:
	// - EnforcedTokens.Tokens == nil (marshals to {"tokens":null})
	// - or an error due to missing enforced tokens
	//
	// This must have TopLogprobs != nil AND len(TopLogprobs) > 0 to pass GetEnforcedTokens().
	choice.Logprobs.Content = []completionapi.Logprob{
		{
			Token:   "<EMPTY>",
			Logprob: 0,
			Bytes:   []int{},
			TopLogprobs: []completionapi.TopLogprobs{
				{Token: "<EMPTY>", Logprob: 0, Bytes: []int{}},
			},
		},
	}

	resp := completionapi.Response{
		ID:      inferenceId,
		Object:  "chat.completion",
		Created: 0,
		Model:   model,
		Choices: []completionapi.Choice{choice},
		Usage: completionapi.Usage{
			// Must be non-zero so `completionapi.JsonCompletionResponse.GetUsage()` won't error.
			// We set it to the best-effort prompt token count so MsgFinishInference can still charge.
			PromptTokens:     promptTokens,
			CompletionTokens: 0,
		},
	}

	b, err := json.Marshal(resp)
	if err != nil {
		// If marshaling fails, return error instead of generating a fallback response
		return nil
	}
	return &completionapi.JsonCompletionResponse{Bytes: b, Resp: resp}
}

// checkAndRecordAuthKey checks if an AuthKey has been used before and records it if not
// Returns true if the key has been used before in the specified context, false otherwise
func checkAndRecordAuthKey(authKey string, currentBlockHeight int64, context AuthKeyContext) bool {
	authKeysMutex.Lock()
	defer authKeysMutex.Unlock()

	existingContext, exists := usedAuthKeys[authKey]
	if exists {
		// If the key exists, check if it's been used in the current context
		if existingContext&context != 0 {
			return true // Key was used before in this context
		}

		// Key exists but hasn't been used in this context, update the context
		usedAuthKeys[authKey] = existingContext | context
		return false // Key wasn't used before in this context
	}

	// Key doesn't exist, add it with the current context
	usedAuthKeys[authKey] = context
	authKeysByBlock[currentBlockHeight] = append(authKeysByBlock[currentBlockHeight], authKey)

	if oldestBlockHeight == 0 {
		oldestBlockHeight = currentBlockHeight
	}

	cleanupExpiredAuthKeys(currentBlockHeight)

	return false // Key wasn't used before
}

// cleanupExpiredAuthKeys removes auth keys from block heights based on timestamp_expiration parameter
func cleanupExpiredAuthKeys(currentBlockHeight int64) {
	// Default expiration is 4 blocks if configManager is not set
	expirationBlocks := int64(4)

	// If configManager is available, use twice the timestamp_expiration value
	if configManagerRef != nil {
		validationParams := configManagerRef.GetValidationParams()
		timestampExpiration := validationParams.TimestampExpiration

		// Use default value if parameter is not set
		if timestampExpiration == 0 {
			timestampExpiration = 10 // Default 10 seconds
		}

		// Use twice the timestamp_expiration value (converted to blocks)
		// Assuming average block time of 5 seconds
		expirationBlocks = (timestampExpiration * 2) / 4

		// Ensure we keep at least 4 blocks for safety
		if expirationBlocks < 4 {
			expirationBlocks = 4
		}

		logging.Debug("Auth key expiration", types.Inferences,
			"timestampExpiration", timestampExpiration,
			"expirationBlocks", expirationBlocks)
	}

	expirationHeight := currentBlockHeight - expirationBlocks

	for height := oldestBlockHeight; height < expirationHeight; height++ {
		keys, exists := authKeysByBlock[height]
		if !exists {
			continue
		}

		for _, key := range keys {
			delete(usedAuthKeys, key)
		}

		delete(authKeysByBlock, height)
	}

	if oldestBlockHeight < expirationHeight {
		oldestBlockHeight = expirationHeight
	}
}

// enforceTransferAgentAccess checks if the given TA address is in the whitelist.
// Returns nil if allowed, or a Forbidden error if not allowed.
func (s *Server) enforceTransferAgentAccess(taAddress string) error {
	cache := s.configManager.GetTransferAgentAccessCache()
	if !cache.IsEnabled {
		return nil // no restriction
	}
	if _, ok := cache.AllowedAddresses[taAddress]; ok {
		return nil
	}
	logging.Warn("Transfer Agent not in whitelist", types.Inferences, "address", taAddress)
	return echo.NewHTTPError(http.StatusForbidden, "Transfer Agent not allowed")
}

func validateRequest(request *ChatRequest, status *coretypes.ResultStatus, configManager *apiconfig.ConfigManager) error {
	lastHeightTime := status.SyncInfo.LatestBlockTime.UnixNano()
	currentBlockHeight := status.SyncInfo.LatestBlockHeight

	// Get validation parameters from config
	validationParams := configManager.GetValidationParams()
	logging.Info("Validating timestamp", types.Inferences,
		"timestampExpiration", validationParams.TimestampExpiration,
		"timestampAdvance", validationParams.TimestampAdvance,
		"lastHeightTime", lastHeightTime,
		"requestTimestamp", request.Timestamp)
	err := calculations.ValidateTimestamp(request.Timestamp, lastHeightTime, validationParams.TimestampExpiration, validationParams.TimestampAdvance, 0)

	if err != nil {
		logging.Warn("Invalid timestamp", types.Inferences,
			"inferenceId", request.InferenceId,
			"status", status,
			"error", err)
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	// Check if AuthKey has been used before for a transfer request
	if checkAndRecordAuthKey(request.AuthKey, currentBlockHeight, TransferContext) {
		logging.Warn("AuthKey reuse detected for transfer request", types.Inferences, "authKey", request.AuthKey)
		return echo.NewHTTPError(http.StatusBadRequest, "AuthKey has already been used for a transfer request")
	}

	return nil
}

func (s *Server) getAllowedPubKeys(ctx echo.Context, granterAddress string) ([]string, error) {
	return s.authzCache.GetPubKeys(ctx.Request().Context(), granterAddress, "/inference.inference.MsgStartInference")
}

// calculateSignature calculates a signature for the given components and agent type
func (s *Server) calculateSignature(payload string, timestamp int64, transferAddress string, executorAddress string, agentType calculations.SignatureType) (string, error) {
	components := calculations.SignatureComponents{
		Payload:         payload,
		Timestamp:       timestamp,
		TransferAddress: transferAddress,
		ExecutorAddress: executorAddress,
	}

	signerAddressStr := s.recorder.GetSignerAddress()
	signerAddress, err := sdk.AccAddressFromBech32(signerAddressStr)
	if err != nil {
		logging.Error("Failed to parse address", types.Inferences, "address", signerAddressStr, "error", err)
		return "", err
	}
	accountSigner := &cmd.AccountSigner{
		Addr:    signerAddress,
		Keyring: s.recorder.GetKeyring(),
	}

	signature, err := calculations.Sign(accountSigner, components, agentType)
	if err != nil {
		logging.Error("Failed to sign signature", types.Inferences, "error", err, "agentType", agentType)
		return "", err
	}

	return signature, nil
}

// attributeStatus formats an HTTP status code as the OTel `http.status_code`
// attribute used on outbound HTTP client spans.
func attributeStatus(code int) attribute.KeyValue {
	return attribute.Int("http.status_code", code)
}

func (s *Server) storePayloadsToStorage(ctx context.Context, inferenceId string, promptPayload, responsePayload []byte) {
	if s.payloadStorage == nil {
		logging.Warn("Cannot store payload: payloadStorage is nil", types.Inferences, "inferenceId", inferenceId)
		return
	}
	if s.phaseTracker == nil {
		logging.Warn("Cannot store payload: phaseTracker is nil", types.Inferences, "inferenceId", inferenceId)
		return
	}

	epochState := s.phaseTracker.GetCurrentEpochState()
	if epochState == nil {
		logging.Warn("Cannot store payload: epoch state is nil", types.Inferences, "inferenceId", inferenceId)
		return
	}
	epochId := epochState.LatestEpoch.EpochIndex

	err := s.payloadStorage.Store(ctx, inferenceId, epochId, promptPayload, responsePayload)
	if err != nil {
		logging.Error("Failed to store payloads locally", types.Inferences, "inferenceId", inferenceId, "epochId", epochId, "error", err)
		return
	}
	logging.Debug("Stored payloads locally", types.Inferences, "inferenceId", inferenceId, "epochId", epochId)
}

func readRequest(request *http.Request, transferAddress string, body []byte, signBodyHash string, forwardPath string, forwardBody []byte) (*ChatRequest, error) {
	if forwardPath == "" {
		forwardPath = chatCompletionsPath
	}

	openAiRequest := OpenAiRequest{}
	if err := json.Unmarshal(body, &openAiRequest); err != nil {
		logging.Warn("Invalid chat completion request body", types.Inferences, "error", err)
		return nil, echo.NewHTTPError(http.StatusBadRequest, "invalid chat completion request: "+err.Error())
	}
	if len(openAiRequest.Messages) == 0 && forwardPath == completionsPath {
		if completionsRequest, ok := tryBuildOpenAiRequestFromCompletionsBody(body); ok {
			openAiRequest = completionsRequest
		}
	}
	if forwardPath == chatCompletionsPath && len(openAiRequest.Messages) == 0 {
		logging.Warn("Chat completion request without messages", types.Inferences)
		return nil, echo.NewHTTPError(http.StatusBadRequest, "messages is required")
	}

	timestamp, err := strconv.ParseInt(request.Header.Get(utils.XTimestampHeader), 10, 64)
	if err != nil {
		timestamp = 0
	}
	if request.Header.Get(utils.XTransferAddressHeader) != "" {
		transferAddress = request.Header.Get(utils.XTransferAddressHeader)
	}
	if len(forwardBody) == 0 {
		forwardBody = body
	}

	return &ChatRequest{
		Body:              body,
		ForwardPath:       forwardPath,
		ForwardBody:       append([]byte(nil), forwardBody...),
		Request:           request,
		OpenAiRequest:     openAiRequest,
		AuthKey:           request.Header.Get(utils.AuthorizationHeader),
		Seed:              request.Header.Get(utils.XSeedHeader),
		InferenceId:       request.Header.Get(utils.XInferenceIdHeader),
		RequesterAddress:  request.Header.Get(utils.XRequesterAddressHeader),
		Timestamp:         timestamp,
		TransferAddress:   transferAddress,
		TransferSignature: request.Header.Get(utils.XTASignatureHeader),
		PromptHash:        request.Header.Get(utils.XPromptHashHeader),
		SignBodyHash:      signBodyHash,
	}, nil
}

func readRequestBody(r *http.Request, writer http.ResponseWriter) ([]byte, error) {
	// Limit request body size to prevent memory exhaustion attacks
	r.Body = http.MaxBytesReader(writer, r.Body, MaxRequestBodySize)
	defer r.Body.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// mapRequestBodyReadError converts low-level body read failures into stable, safe HTTP responses.
func mapRequestBodyReadError(err error) error {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		return echo.NewHTTPError(http.StatusRequestEntityTooLarge, "request body too large")
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return echo.NewHTTPError(http.StatusBadRequest, "malformed request body")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return echo.NewHTTPError(http.StatusRequestTimeout, "request body read timeout")
	}
	if errors.Is(err, context.Canceled) {
		return echo.NewHTTPError(http.StatusBadRequest, "request body read cancelled")
	}
	return echo.NewHTTPError(http.StatusBadRequest, "failed to read request body")
}

func mapExecutorCompletionsUnsupportedError(forwardPath string, statusCode int) error {
	if forwardPath != completionsPath {
		return nil
	}
	switch statusCode {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return echo.NewHTTPError(http.StatusServiceUnavailable, executorCompletionsUnsupportedMsg)
	default:
		return nil
	}
}

func (s *Server) validateModelSupported(model string) error {
	if model == "" {
		return ErrNoModelSpecified
	}
	if s.phaseTracker == nil || s.epochGroupDataCache == nil {
		return nil
	}
	epochState := s.phaseTracker.GetCurrentEpochState()
	if epochState == nil {
		return nil
	}
	epochGroupData, err := s.epochGroupDataCache.GetCurrentEpochGroupData(epochState.LatestEpoch.EpochIndex)
	if err != nil {
		logging.Warn("Failed to fetch current epoch group data for model validation", types.Inferences, "error", err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "unable to fetch current epoch group data")
	}
	for _, m := range epochGroupData.SubGroupModels {
		if m == model {
			return nil
		}
	}
	return echo.NewHTTPError(http.StatusNotFound, "model not found")
}

// validateRequester validates requester with dynamic pricing fallback to legacy
func (s *Server) validateRequester(ctx context.Context, request *ChatRequest, requester *types.QueryAccountByAddressResponse, promptTokenCount int) error {
	if requester == nil {
		logging.Error("Account not found", types.Inferences, "address", request.RequesterAddress)
		return ErrAccountNotFound
	}

	err := validateTransferRequest(request, requester.Pubkey)
	if err != nil {
		logging.Error("Unable to validate request against PubKey", types.Inferences, "error", err)
		return echo.NewHTTPError(http.StatusUnauthorized, "Unable to validate request against PubKey:"+err.Error())
	}

	if err := s.validateModelSupported(request.OpenAiRequest.Model); err != nil {
		return err
	}

	if request.OpenAiRequest.MaxTokens == 0 {
		request.OpenAiRequest.MaxTokens = calculations.DefaultMaxTokens
	}

	var escrowNeeded uint64
	var perTokenPrice uint64

	// Try to get dynamic pricing first
	queryClient := s.recorder.NewInferenceQueryClient()
	priceResponse, err := queryClient.GetModelPerTokenPrice(ctx, &types.QueryGetModelPerTokenPriceRequest{
		ModelId: request.OpenAiRequest.Model,
	})

	if err == nil && priceResponse.Found {
		// Use dynamic pricing
		perTokenPrice = priceResponse.Price

		logging.Debug("Using dynamic pricing", types.Inferences,
			"perTokenPrice", perTokenPrice,
			"model", request.OpenAiRequest.Model)
	} else {
		// Fall back to legacy pricing
		logging.Warn("Failed to get dynamic pricing, falling back to legacy calculation", types.Inferences, "error", err)
		perTokenPrice = uint64(calculations.PerTokenCost)

		logging.Debug("Using legacy pricing", types.Inferences,
			"perTokenPrice", perTokenPrice)
	}

	// Calculate escrow using consistent formula: (PromptTokens + MaxTokens) × PerTokenPrice
	totalTokens := uint64(promptTokenCount) + uint64(request.OpenAiRequest.MaxTokens)
	escrowNeeded = totalTokens * perTokenPrice

	logging.Debug("Escrow calculation", types.Inferences,
		"escrowNeeded", escrowNeeded,
		"perTokenPrice", perTokenPrice,
		"promptTokens", promptTokenCount,
		"maxTokens", request.OpenAiRequest.MaxTokens,
		"totalTokens", totalTokens)

	logging.Debug("Client balance", types.Inferences, "balance", requester.Balance)
	if requester.Balance < int64(escrowNeeded) {
		return ErrInsufficientBalance
	}
	return nil
}
