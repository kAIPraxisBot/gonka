package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func i64(v int64) *int64 { return &v }

func TestContextLimitExceeded(t *testing.T) {
	cases := []struct {
		in, out, max int64
		want         bool
	}{
		{100, 50, 120000, false},
		{103617, 16384, 120000, true}, // the reported Kimi-K2.6 overflow (total 120001)
		{120000, 1, 120000, true},
		{119999, 1, 120000, false},
		{100, 50, 0, false},  // unknown limit -> fail open
		{100, 50, -1, false}, // negative limit -> fail open
	}
	for _, c := range cases {
		if got := contextLimitExceeded(c.in, c.out, c.max); got != c.want {
			t.Errorf("contextLimitExceeded(%d,%d,%d)=%v want %v", c.in, c.out, c.max, got, c.want)
		}
	}
}

func TestRequestedOutputTokens(t *testing.T) {
	if got := requestedOutputTokens(nil, nil); got != 0 {
		t.Errorf("nil,nil => %d want 0", got)
	}
	if got := requestedOutputTokens(i64(16384), nil); got != 16384 {
		t.Errorf("max_tokens => %d want 16384", got)
	}
	if got := requestedOutputTokens(i64(100), i64(500)); got != 500 {
		t.Errorf("max of the two => %d want 500", got)
	}
}

func TestContextOverflowMessage(t *testing.T) {
	msg := contextOverflowMessage(120000, 103617, 16384)
	for _, want := range []string{
		"maximum context length is 120000",
		"103617 input tokens",
		"16384 output tokens",
		"total of 120001",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q missing %q", msg, want)
		}
	}
}

func fakeTokenizeServer(t *testing.T, count, maxModelLen int64, status int, hit *bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tokenize" {
			t.Errorf("unexpected path %q (only /tokenize expected)", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if hit != nil {
			*hit = true
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]int64{"count": count, "max_model_len": maxModelLen})
	}))
}

func chatBody(maxTokens int64) []byte {
	b, _ := json.Marshal(map[string]any{
		"model":      "Kimi-K2.6",
		"messages":   []map[string]string{{"role": "user", "content": "hi"}},
		"max_tokens": maxTokens,
	})
	return b
}

func TestPreflightContextCheck_WithinLimit(t *testing.T) {
	srv := fakeTokenizeServer(t, 100, 120000, http.StatusOK, nil)
	defer srv.Close()
	if resp := preflightContextCheck(context.Background(), srv.Client(), srv.URL, "Kimi-K2.6", chatBody(16384)); resp != nil {
		t.Fatalf("within limit should not reject, got status %d", resp.StatusCode)
	}
}

func TestPreflightContextCheck_Overflow(t *testing.T) {
	srv := fakeTokenizeServer(t, 103617, 120000, http.StatusOK, nil)
	defer srv.Close()
	resp := preflightContextCheck(context.Background(), srv.Client(), srv.URL, "Kimi-K2.6", chatBody(16384))
	if resp == nil {
		t.Fatal("overflow should reject with a 400, got nil")
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d want 400", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "maximum context length is 120000") {
		t.Errorf("body missing vLLM-style message: %s", b)
	}
}

func TestPreflightContextCheck_FailOpenOnTokenizeError(t *testing.T) {
	srv := fakeTokenizeServer(t, 0, 0, http.StatusInternalServerError, nil)
	defer srv.Close()
	if resp := preflightContextCheck(context.Background(), srv.Client(), srv.URL, "Kimi-K2.6", chatBody(16384)); resp != nil {
		t.Fatalf("tokenize error should fail open (nil), got status %d", resp.StatusCode)
	}
}

func TestPreflightContextCheck_NoMessagesSkipsTokenize(t *testing.T) {
	hit := false
	srv := fakeTokenizeServer(t, 0, 0, http.StatusOK, &hit)
	defer srv.Close()
	body := []byte(`{"model":"Kimi-K2.6"}`) // no messages
	if resp := preflightContextCheck(context.Background(), srv.Client(), srv.URL, "Kimi-K2.6", body); resp != nil {
		t.Fatalf("no messages should skip and return nil, got %d", resp.StatusCode)
	}
	if hit {
		t.Error("should not have called /tokenize when there are no messages")
	}
}
