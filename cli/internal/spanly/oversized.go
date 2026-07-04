package spanly

import (
	"bytes"
	"encoding/json"
	"io"
	"sync/atomic"
)

// countingReader tallies bytes read through it. Used to recover the true wire
// size of an oversized frame whose tail is streamed to the peer rather than
// buffered. The count is atomic because http.Transport may write the request
// body from a separate goroutine, so the handler reads the tally concurrently.
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// BuildOversizedStub reconstructs a metadata-only MCP packet from the leading
// bytes of a frame too large to buffer whole. It recovers the fields that
// identify a span — jsonrpc, id, method, and (for tools/call, prompts/get,
// resources/read) the tool name / prompt name / resource uri — from `prefix`,
// which is only the first MaxInspectBytes of the frame and is therefore almost
// always truncated mid-body.
//
// Recovery is best-effort: these fields normally precede the oversized payload
// (arguments/result), so the common case — an oversized tool call or an
// oversized tool result — recovers cleanly. A client that serialises the huge
// field before the identifying ones yields a partial stub (e.g. method without
// tool name). Returns nil when the prefix is not a JSON-RPC 2.0 frame, in which
// case the caller emits no telemetry (matching the non-oversized path).
func BuildOversizedStub(prefix []byte) json.RawMessage {
	f, ok := scanLeadingFields(prefix)
	if !ok || !f.hasJSONRPC {
		return nil
	}
	if !f.hasMethod && !f.hasID {
		return nil
	}

	stub := struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id,omitempty"`
		Method  string          `json:"method,omitempty"`
		Params  json.RawMessage `json:"params,omitempty"`
		Result  json.RawMessage `json:"result,omitempty"`
	}{JSONRPC: "2.0"}

	if f.hasID {
		stub.ID = f.id
	}
	if f.hasMethod {
		stub.Method = f.method
		if p := stubParams(f); p != nil {
			stub.Params = p
		}
	} else if f.hasID {
		// A frame with an id but no method is a response; an empty result keeps
		// the stub a well-formed response so pairing can attach it to the (small)
		// request half, which carries the method and tool name.
		stub.Result = json.RawMessage("{}")
	}

	out, err := json.Marshal(stub)
	if err != nil {
		return nil
	}
	return out
}

func stubParams(f leadingFields) json.RawMessage {
	var key, val string
	switch {
	case f.hasName:
		key, val = "name", f.name
	case f.hasURI:
		key, val = "uri", f.uri
	default:
		return nil
	}
	out, err := json.Marshal(map[string]string{key: val})
	if err != nil {
		return nil
	}
	return out
}

type leadingFields struct {
	hasJSONRPC bool
	id         json.RawMessage
	hasID      bool
	method     string
	hasMethod  bool
	name       string
	hasName    bool
	uri        string
	hasURI     bool
}

// scanLeadingFields streams tokens from the (possibly truncated) prefix,
// reading only the top-level object keys we care about plus name/uri inside
// params. It stops early — on truncation, or once params has been mined — so a
// 16 MiB prefix is never fully walked. ok is false only when the input does not
// begin with a JSON object.
func scanLeadingFields(prefix []byte) (leadingFields, bool) {
	var f leadingFields
	dec := json.NewDecoder(bytes.NewReader(bytes.TrimSpace(prefix)))
	dec.UseNumber()

	tok, err := dec.Token()
	if err != nil {
		return f, false
	}
	if d, isDelim := tok.(json.Delim); !isDelim || d != '{' {
		return f, false
	}

	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return f, true
		}
		key, _ := keyTok.(string)
		switch key {
		case "jsonrpc":
			if v, err := dec.Token(); err == nil {
				if s, ok := v.(string); ok && s == "2.0" {
					f.hasJSONRPC = true
				}
			} else {
				return f, true
			}
		case "id":
			v, err := dec.Token()
			if err != nil {
				return f, true
			}
			switch id := v.(type) {
			case string:
				if raw, err := json.Marshal(id); err == nil {
					f.id, f.hasID = raw, true
				}
			case json.Number:
				f.id, f.hasID = json.RawMessage(id.String()), true
			}
		case "method":
			if v, err := dec.Token(); err == nil {
				if s, ok := v.(string); ok {
					f.method, f.hasMethod = s, true
				}
			} else {
				return f, true
			}
		case "params":
			scanParams(dec, &f)
			// params holds the only nested field we mine; the response body
			// (result/error) is never inspected, so nothing after params is
			// worth the risk of walking a truncated value.
			return f, true
		case "result", "error":
			// A response: id already captured above. The body is oversized and
			// not inspected, so stop before touching it.
			return f, true
		default:
			if err := skipValue(dec); err != nil {
				return f, true
			}
		}
	}
	return f, true
}

func scanParams(dec *json.Decoder, f *leadingFields) {
	tok, err := dec.Token()
	if err != nil {
		return
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return
	}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return
		}
		key, _ := keyTok.(string)
		switch key {
		case "name":
			if v, err := dec.Token(); err == nil {
				if s, ok := v.(string); ok {
					f.name, f.hasName = s, true
				}
			}
			return
		case "uri":
			if v, err := dec.Token(); err == nil {
				if s, ok := v.(string); ok {
					f.uri, f.hasURI = s, true
				}
			}
			return
		default:
			if err := skipValue(dec); err != nil {
				return
			}
		}
	}
}

// skipValue consumes exactly one complete JSON value from dec. It returns an
// error if the value is truncated (the expected outcome when it lands on the
// oversized field), which the callers treat as "stop scanning".
func skipValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	d, ok := tok.(json.Delim)
	if !ok || (d != '{' && d != '[') {
		return nil
	}
	depth := 1
	for depth > 0 {
		t, err := dec.Token()
		if err != nil {
			return err
		}
		if dd, ok := t.(json.Delim); ok {
			if dd == '{' || dd == '[' {
				depth++
			} else {
				depth--
			}
		}
	}
	return nil
}
