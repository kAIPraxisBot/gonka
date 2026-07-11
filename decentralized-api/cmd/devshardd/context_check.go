package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const preflightTokenizeTimeout = 2 * time.Second

func contextLimitExceeded(inputTokens, outputTokens, maxModelLen int64) bool {
	return maxModelLen > 0 && inputTokens+outputTokens > maxModelLen
}

func requestedOutputTokens(maxTokens, maxCompletionTokens *int64) int64 {
	out := int64(0)
	if maxTokens != nil && *maxTokens > out {
		out = *maxTokens
	}
	if maxCompletionTokens != nil && *maxCompletionTokens > out {
		out = *maxCompletionTokens
	}
	return out
}

func contextOverflowMessage(maxModelLen, inputTokens, outputTokens int64) string {
	return fmt.Sprintf(
		"This model's maximum context length is %d tokens. However, you requested %d output tokens and your prompt contains %d input tokens, for a total of %d tokens. Please reduce the length of the input prompt or the number of requested output tokens.",
		maxModelLen, outputTokens, inputTokens, inputTokens+outputTokens,
	)
}

func contextOverflowResponse(maxModelLen, inputTokens, outputTokens int64) *http.Response {
	payload, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"message": contextOverflowMessage(maxModelLen, inputTokens, outputTokens),
			"type":    "invalid_request_error",
			"param":   "messages",
			"code":    "context_length_exceeded",
		},
	})
	return &http.Response{
		StatusCode:    http.StatusBadRequest,
		Status:        "400 Bad Request",
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"Content-Type": []string{"application/json"}},
		Body:          io.NopCloser(bytes.NewReader(payload)),
		ContentLength: int64(len(payload)),
	}
}

// Rejects (400) a request whose input + requested output tokens exceed the model
// context window, via vLLM /tokenize (which stays off the generation queue).
// Fail-open: any error returns nil so normal inference proceeds.
func preflightContextCheck(ctx context.Context, httpClient *http.Client, endpoint, model string, body []byte) *http.Response {
	var req struct {
		Model               string          `json:"model"`
		Messages            json.RawMessage `json:"messages"`
		MaxTokens           *int64          `json:"max_tokens"`
		MaxCompletionTokens *int64          `json:"max_completion_tokens"`
	}
	if err := json.Unmarshal(body, &req); err != nil || len(req.Messages) == 0 {
		return nil
	}
	reqModel := req.Model
	if reqModel == "" {
		reqModel = model
	}
	tokBody, err := json.Marshal(map[string]any{
		"model":                 reqModel,
		"messages":              req.Messages,
		"add_generation_prompt": true,
	})
	if err != nil {
		return nil
	}

	tokCtx, cancel := context.WithTimeout(ctx, preflightTokenizeTimeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(tokCtx, http.MethodPost, endpoint+"/tokenize", bytes.NewReader(tokBody))
	if err != nil {
		return nil
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var tok struct {
		Count       int64 `json:"count"`
		MaxModelLen int64 `json:"max_model_len"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		return nil
	}

	outputTokens := requestedOutputTokens(req.MaxTokens, req.MaxCompletionTokens)
	if !contextLimitExceeded(tok.Count, outputTokens, tok.MaxModelLen) {
		return nil
	}
	return contextOverflowResponse(tok.MaxModelLen, tok.Count, outputTokens)
}
