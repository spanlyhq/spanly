package spanly

import (
	"encoding/json"
	"strings"
	"testing"
)

// truncatedTail appends a huge, unterminated JSON string so the input mimics a
// frame whose real body was cut off at the inspect cap.
func truncatedTail() string {
	return strings.Repeat("a", 4096)
}

func TestBuildOversizedStubToolCallRequest(t *testing.T) {
	prefix := `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"bigtool","arguments":{"pad":"` + truncatedTail()

	stub := BuildOversizedStub([]byte(prefix))
	if stub == nil {
		t.Fatal("expected a stub, got nil")
	}
	var got struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Params  struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	if err := json.Unmarshal(stub, &got); err != nil {
		t.Fatalf("stub is not valid JSON: %v (%s)", err, stub)
	}
	if got.JSONRPC != "2.0" {
		t.Errorf("jsonrpc = %q", got.JSONRPC)
	}
	if string(got.ID) != "7" {
		t.Errorf("id = %s, want 7", got.ID)
	}
	if got.Method != "tools/call" {
		t.Errorf("method = %q", got.Method)
	}
	if got.Params.Name != "bigtool" {
		t.Errorf("tool name = %q, want bigtool", got.Params.Name)
	}
}

func TestBuildOversizedStubResponse(t *testing.T) {
	// A response has an id and a result but no method; the tool name lives on
	// the paired request half, not here.
	prefix := `{"jsonrpc":"2.0","id":"abc","result":{"content":[{"type":"text","text":"` + truncatedTail()

	stub := BuildOversizedStub([]byte(prefix))
	if stub == nil {
		t.Fatal("expected a stub, got nil")
	}
	var got struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Result  json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(stub, &got); err != nil {
		t.Fatalf("stub is not valid JSON: %v (%s)", err, stub)
	}
	if string(got.ID) != `"abc"` {
		t.Errorf("id = %s, want \"abc\"", got.ID)
	}
	if got.Method != "" {
		t.Errorf("method = %q, want empty for a response", got.Method)
	}
	if string(got.Result) != "{}" {
		t.Errorf("result = %s, want {}", got.Result)
	}
}

func TestBuildOversizedStubResourceRead(t *testing.T) {
	prefix := `{"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"file:///big","extra":"` + truncatedTail()

	stub := BuildOversizedStub([]byte(prefix))
	if stub == nil {
		t.Fatal("expected a stub, got nil")
	}
	var got struct {
		Method string `json:"method"`
		Params struct {
			URI string `json:"uri"`
		} `json:"params"`
	}
	if err := json.Unmarshal(stub, &got); err != nil {
		t.Fatalf("stub is not valid JSON: %v", err)
	}
	if got.Method != "resources/read" {
		t.Errorf("method = %q", got.Method)
	}
	if got.Params.URI != "file:///big" {
		t.Errorf("uri = %q, want file:///big", got.Params.URI)
	}
}

func TestBuildOversizedStubNotification(t *testing.T) {
	// No id, has method: routes to the notifications table downstream.
	prefix := `{"jsonrpc":"2.0","method":"notifications/message","params":{"data":"` + truncatedTail()

	stub := BuildOversizedStub([]byte(prefix))
	if stub == nil {
		t.Fatal("expected a stub, got nil")
	}
	var got struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if err := json.Unmarshal(stub, &got); err != nil {
		t.Fatalf("stub is not valid JSON: %v", err)
	}
	if got.ID != nil {
		t.Errorf("id = %s, want none for a notification", got.ID)
	}
	if got.Method != "notifications/message" {
		t.Errorf("method = %q", got.Method)
	}
}

func TestBuildOversizedStubArgumentsBeforeName(t *testing.T) {
	// Best-effort: when the huge field precedes the tool name we lose the name
	// but still recover method + id, so the span stays visible.
	prefix := `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"arguments":{"pad":"` + truncatedTail()

	stub := BuildOversizedStub([]byte(prefix))
	if stub == nil {
		t.Fatal("expected a stub, got nil")
	}
	var got struct {
		Method string `json:"method"`
		Params *struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	if err := json.Unmarshal(stub, &got); err != nil {
		t.Fatalf("stub is not valid JSON: %v", err)
	}
	if got.Method != "tools/call" {
		t.Errorf("method = %q", got.Method)
	}
	if got.Params != nil && got.Params.Name != "" {
		t.Errorf("did not expect a recovered name, got %q", got.Params.Name)
	}
}

func TestBuildOversizedStubRejectsNonJSONRPC(t *testing.T) {
	cases := map[string]string{
		"plain text":      "hello world " + truncatedTail(),
		"json no jsonrpc": `{"foo":"bar","pad":"` + truncatedTail(),
		"array":           `[{"jsonrpc":"2.0","pad":"` + truncatedTail(),
	}
	for name, in := range cases {
		if stub := BuildOversizedStub([]byte(in)); stub != nil {
			t.Errorf("%s: expected nil stub, got %s", name, stub)
		}
	}
}
