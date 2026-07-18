package public

import (
	"encoding/json"
	"errors"
)

type StringOrArray []string

func (s *StringOrArray) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*s = nil
		return nil
	}

	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		*s = []string{str}
		return nil
	}

	var arr []string
	if err := json.Unmarshal(data, &arr); err == nil {
		*s = arr
		return nil
	}

	return errors.New("expected string or array of strings")
}

func (s StringOrArray) First() string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}

type ModelDescriptor struct {
	Object           string   `json:"object,omitempty"`
	ID               string   `json:"id"`
	HuggingFaceID    string   `json:"hugging_face_id,omitempty"`
	Name             string   `json:"name"`
	Created          int64    `json:"created"`
	Description      string   `json:"description,omitempty"`
	InputModalities  []string `json:"input_modalities"`
	OutputModalities []string `json:"output_modalities"`
	Quantization     string   `json:"quantization,omitempty"`
	ContextLength    uint64   `json:"context_length"`
	MaxOutputLength  uint64   `json:"max_output_length"`
}

type ModelsListResponse struct {
	Object string            `json:"object"`
	Data   []ModelDescriptor `json:"data"`
}
