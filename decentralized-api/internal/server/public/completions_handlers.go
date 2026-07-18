package public

import (
	"encoding/json"
	"strings"
)

const completionsPath = "/v1/completions"

func tryBuildOpenAiRequestFromCompletionsBody(body []byte) (OpenAiRequest, bool) {
	var completionsReq CompletionsRequest
	if err := json.Unmarshal(body, &completionsReq); err != nil {
		return OpenAiRequest{}, false
	}
	if strings.TrimSpace(completionsReq.Model) == "" || len(completionsReq.Prompt) != 1 {
		return OpenAiRequest{}, false
	}

	rawPrompt := completionsReq.Prompt.First()
	if strings.TrimSpace(rawPrompt) == "" {
		return OpenAiRequest{}, false
	}

	var maxTokens int32
	if completionsReq.MaxTokens != nil {
		maxTokens = *completionsReq.MaxTokens
	}
	var seed int32
	if completionsReq.Seed != nil {
		seed = *completionsReq.Seed
	}
	promptText := rawPrompt

	return OpenAiRequest{
		Model:               completionsReq.Model,
		Seed:                seed,
		MaxTokens:           maxTokens,
		MaxCompletionTokens: maxTokens,
		Messages: []Message{{
			Role:    "user",
			Content: MessageContent{Text: &promptText},
		}},
	}, true
}
