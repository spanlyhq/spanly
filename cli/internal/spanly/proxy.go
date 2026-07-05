package spanly

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// MonitorIDHeader is the canonical HTTP header used to override the
	// SpanlyMonitorId on a per-request basis.
	MonitorIDHeader = "X-Spanly-Monitor-Id"

	// MaxInspectBytes bounds how much of a body the proxy buffers for
	// JSON-RPC inspection. Larger payloads are forwarded untouched but
	// produce no telemetry. Also used as the stdio max line size.
	MaxInspectBytes = 16 << 20

	// SessionIDHeader is the MCP session header (RFC-style canonical form).
	SessionIDHeader = "Mcp-Session-Id"

	// SyntheticSessionIDPrefix marks session IDs minted by Spanly rather
	// than the upstream server, so the proxy can recognise (and strip)
	// its own IDs on inbound requests without keeping state.
	SyntheticSessionIDPrefix = "spanly-"

	// SessionTerminatedMethod is the synthetic method recorded when a
	// client ends its session with an HTTP DELETE. Session termination is
	// transport-level in MCP (no JSON-RPC message exists on the wire), so
	// Spanly emits this packet itself; the `spanly/` prefix marks it as
	// synthesized, like the `spanly-` session IDs.
	SessionTerminatedMethod = "spanly/session/terminated"
)

// sessionTerminatedPacket builds the synthetic JSON-RPC packet recorded
// when a DELETE terminates the given session.
func sessionTerminatedPacket(sessionID string) json.RawMessage {
	packet, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  SessionTerminatedMethod,
		"params":  map[string]string{"sessionId": sessionID},
	})
	if err != nil {
		return nil
	}
	return packet
}

// StdioSessionTerminatedPacket builds the synthetic JSON-RPC packet recorded
// when a stdio pipe closes. stdio has no transport-level session ID and no
// close message on the wire, so params stays empty; the packet only marks
// that the wrapper observed a clean end of the pipe (vs. the session simply
// going quiet). Mirrors the HTTP DELETE path above (GAP-2,
// docs/LIVE_SCAN_CAPTURE_GUARANTEES.md).
func StdioSessionTerminatedPacket() json.RawMessage {
	packet, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  SessionTerminatedMethod,
		"params":  map[string]string{},
	})
	if err != nil {
		return nil
	}
	return packet
}

// NewSyntheticSessionID returns a fresh session ID carrying the Spanly
// synthetic prefix.
func NewSyntheticSessionID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Sprintf("spanly: crypto/rand failed: %v", err))
	}
	return SyntheticSessionIDPrefix + hex.EncodeToString(buf)
}

// containsInitializeRequest reports whether a request body holds an MCP
// initialize request (single object or anywhere in a batch).
func containsInitializeRequest(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return false
	}
	type probe struct {
		Method string `json:"method"`
	}
	if trimmed[0] == '[' {
		var probes []probe
		if err := json.Unmarshal(trimmed, &probes); err != nil {
			return false
		}
		for _, p := range probes {
			if p.Method == "initialize" {
				return true
			}
		}
		return false
	}
	var p probe
	if err := json.Unmarshal(trimmed, &p); err != nil {
		return false
	}
	return p.Method == "initialize"
}

// HeaderMapping maps an inbound HTTP header to a PacketContext field.
// Used to attribute traffic per-request (e.g. X-Tenant -> environmentId)
// when the same proxy serves multiple Spanly tenants.
type HeaderMapping struct {
	Header string
	Field  string // one of: environmentId, projectId, organisationId
}

// ValidateContextField returns an error if the given field name is not a
// supported PacketContext target for --context-header mappings.
func ValidateContextField(field string) error {
	switch field {
	case "environmentId", "projectId", "organisationId":
		return nil
	default:
		return fmt.Errorf("unsupported context field %q (allowed: environmentId, projectId, organisationId)", field)
	}
}

// defaultRedactedHeaders are credential-bearing headers whose values are
// replaced with [REDACTED] in captured transport context. Matched
// case-insensitively. The headers themselves are still forwarded verbatim
// to the upstream/client; only the telemetry copy is redacted.
var defaultRedactedHeaders = []string{
	"authorization",
	"cookie",
	"set-cookie",
	"proxy-authorization",
	"x-api-key",
}

// ProxyOptions configures the Proxy.
type ProxyOptions struct {
	Upstream       *url.URL
	Collector      *Collector
	InspectPrefix  []string        // empty/nil = inspect all paths
	ContextHeaders []HeaderMapping // optional per-request overrides
	RedactHeaders  []string        // extra headers to redact beyond the defaults

	// InjectSessionID makes the proxy assign a synthetic Mcp-Session-Id on
	// initialize responses when the upstream server does not set one, so
	// sessionless servers still get per-session grouping in Spanly. The
	// synthetic ID is stripped from upstream-bound requests; the upstream
	// never sees a header it did not create.
	InjectSessionID bool
}

type Proxy struct {
	upstream        *url.URL
	collector       *Collector
	inspectPrefix   []string
	contextHeaders  []HeaderMapping
	redactHeaders   map[string]struct{}
	injectSessionID bool

	client      *http.Client
	passthrough *httputil.ReverseProxy

	inspectReqs     atomic.Int64
	passthroughReqs atomic.Int64
}

func NewProxy(opts ProxyOptions) (*Proxy, error) {
	if opts.Upstream == nil {
		return nil, errors.New("Upstream is required")
	}
	if opts.Collector == nil {
		return nil, errors.New("Collector is required")
	}
	for _, m := range opts.ContextHeaders {
		if err := ValidateContextField(m.Field); err != nil {
			return nil, err
		}
		if m.Header == "" {
			return nil, errors.New("context-header mapping has empty header name")
		}
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		DisableCompression:  true,
	}

	redact := make(map[string]struct{}, len(defaultRedactedHeaders)+len(opts.RedactHeaders))
	for _, h := range defaultRedactedHeaders {
		redact[h] = struct{}{}
	}
	for _, h := range opts.RedactHeaders {
		redact[strings.ToLower(h)] = struct{}{}
	}

	p := &Proxy{
		upstream:        opts.Upstream,
		collector:       opts.Collector,
		inspectPrefix:   opts.InspectPrefix,
		contextHeaders:  opts.ContextHeaders,
		redactHeaders:   redact,
		injectSessionID: opts.InjectSessionID,
		client: &http.Client{
			Timeout:   0,
			Transport: transport,
		},
	}

	upstream := opts.Upstream
	director := func(req *http.Request) {
		req.URL.Scheme = upstream.Scheme
		req.URL.Host = upstream.Host
		req.URL.Path = singleJoiningPath(upstream.Path, req.URL.Path)
		req.URL.RawQuery = mergeQuery(upstream.RawQuery, req.URL.RawQuery)
		req.Host = upstream.Host
	}
	p.passthrough = &httputil.ReverseProxy{
		Director:  director,
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			log.Printf("spanly: passthrough error: %v", err)
			http.Error(w, "bad gateway", http.StatusBadGateway)
		},
	}

	return p, nil
}

// shouldInspect reports whether path matches any configured inspect-prefix.
// Empty prefix list means "inspect all paths".
func (p *Proxy) shouldInspect(path string) bool {
	if len(p.inspectPrefix) == 0 {
		return true
	}
	for _, prefix := range p.inspectPrefix {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// buildOverride extracts per-request PacketContext overrides from request
// headers based on the configured monitor-id header and context-header
// mappings.
func (p *Proxy) buildOverride(r *http.Request) PacketContext {
	override := PacketContext{}
	if v := r.Header.Get(MonitorIDHeader); v != "" {
		override.SpanlyMonitorId = v
	}
	for _, m := range p.contextHeaders {
		v := r.Header.Get(m.Header)
		if v == "" {
			continue
		}
		switch m.Field {
		case "environmentId":
			override.EnvironmentId = v
		case "projectId":
			override.ProjectId = v
		case "organisationId":
			override.OrganisationId = v
		}
	}
	return override
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !p.shouldInspect(r.URL.Path) {
		p.passthroughReqs.Add(1)
		p.passthrough.ServeHTTP(w, r)
		return
	}
	p.inspectReqs.Add(1)

	requestBody, err := io.ReadAll(io.LimitReader(r.Body, MaxInspectBytes+1))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadGateway)
		return
	}
	requestOversized := len(requestBody) > MaxInspectBytes

	override := p.buildOverride(r)

	transport := p.httpTransportContext(r, r.Header)
	if !requestOversized {
		if packet, ok := ParseMCPPacket(requestBody); ok {
			p.collector.Collect("from-client", override, transport, packet)
		}
	}

	// Spanly minted this session ID (the upstream never issued one), so the
	// upstream can't honor a DELETE for a session it never provisioned.
	// Forwarding it would surface a 404/405 to the client and drop the
	// termination event (the collect below is gated on a 2xx response). Own
	// the whole synthetic lifecycle: record the termination and answer 200
	// here instead of forwarding the DELETE upstream.
	if p.injectSessionID && r.Method == http.MethodDelete {
		if sessionID := r.Header.Get(SessionIDHeader); strings.HasPrefix(sessionID, SyntheticSessionIDPrefix) {
			p.collector.Collect("from-client", override, transport, sessionTerminatedPacket(sessionID))
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	outURL := *p.upstream
	outURL.Path = singleJoiningPath(p.upstream.Path, r.URL.Path)
	outURL.RawQuery = mergeQuery(p.upstream.RawQuery, r.URL.RawQuery)

	var outBody io.Reader = bytes.NewReader(requestBody)
	var requestTail *countingReader
	if requestOversized {
		requestTail = &countingReader{r: r.Body}
		outBody = io.MultiReader(bytes.NewReader(requestBody), requestTail)
	}
	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, outURL.String(), outBody)
	if err != nil {
		http.Error(w, "failed to build upstream request", http.StatusBadGateway)
		return
	}
	if requestOversized {
		outReq.ContentLength = r.ContentLength
	}
	copyHeaders(outReq.Header, r.Header)
	outReq.Host = p.upstream.Host

	// Synthetic session IDs exist for telemetry grouping only; the
	// upstream never issued them, so it never gets to see them.
	if p.injectSessionID && strings.HasPrefix(outReq.Header.Get(SessionIDHeader), SyntheticSessionIDPrefix) {
		outReq.Header.Del(SessionIDHeader)
	}

	resp, err := p.client.Do(outReq)
	if err != nil {
		if isCanceled(r.Context(), err) {
			return
		}
		log.Printf("spanly: upstream request failed: %v", err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// The oversized request body has now been streamed upstream, so requestTail
	// carries the true size of the tail we never buffered. Emit a metadata-only
	// event (see GAP-3 in docs/LIVE_SCAN_CAPTURE_GUARANTEES.md): the span stays
	// visible to aggregations and size-based checks instead of vanishing. The
	// timestamp is post-upload, so a huge upload slightly inflates the recorded
	// start; acceptable for a case that would otherwise be invisible.
	if requestOversized {
		if stub := BuildOversizedStub(requestBody); stub != nil {
			p.collector.CollectOversized(
				"from-client", override, transport, stub,
				int64(len(requestBody))+requestTail.n.Load(),
			)
		}
	}

	if p.injectSessionID &&
		r.Method == http.MethodPost &&
		!requestOversized &&
		resp.StatusCode >= 200 && resp.StatusCode < 300 &&
		resp.Header.Get(SessionIDHeader) == "" &&
		containsInitializeRequest(requestBody) {
		// Set on resp.Header (not w.Header()) so the captured response
		// transport context below carries the ID too.
		resp.Header.Set(SessionIDHeader, NewSyntheticSessionID())
	}

	// A DELETE that succeeds ends the session (MCP explicit termination).
	// No JSON-RPC crosses the wire in either direction, so synthesize a
	// packet once the upstream has confirmed. Requires the session header:
	// a DELETE without one terminates nothing.
	if r.Method == http.MethodDelete &&
		resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if sessionID := r.Header.Get(SessionIDHeader); sessionID != "" {
			p.collector.Collect("from-client", override, transport, sessionTerminatedPacket(sessionID))
		}
	}

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	respTransport := p.httpTransportContext(r, resp.Header)
	respTransport.StatusCode = resp.StatusCode

	if isSSE(resp.Header.Get("Content-Type")) {
		p.streamSSE(w, resp.Body, override, respTransport)
		return
	}

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, MaxInspectBytes+1))
	if err != nil {
		log.Printf("spanly: failed to read upstream response: %v", err)
		return
	}
	responseOversized := len(responseBody) > MaxInspectBytes
	if !responseOversized {
		if packet, ok := ParseMCPPacket(responseBody); ok {
			p.collector.Collect("to-client", override, respTransport, packet)
		}
	}
	if _, err := w.Write(responseBody); err != nil {
		log.Printf("spanly: failed to write response to client: %v", err)
		return
	}
	tail, err := io.Copy(w, resp.Body)
	if err != nil {
		log.Printf("spanly: failed to write response to client: %v", err)
	}
	// Metadata-only event for the oversized response (GAP-3): the true size
	// lets SPLY-388 see its own tail — the largest results, which it exists to
	// flag — and stops aggregations under-counting these spans. The stub pairs
	// with the (small) request half, which carries the method and tool name.
	if responseOversized {
		if stub := BuildOversizedStub(responseBody); stub != nil {
			p.collector.CollectOversized(
				"to-client", override, respTransport, stub,
				int64(len(responseBody))+tail,
			)
		}
	}
}

func (p *Proxy) streamSSE(w http.ResponseWriter, body io.Reader, override PacketContext, transport TransportContext) {
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 8*1024)
	var frame bytes.Buffer
	inspect := true
	for {
		n, err := body.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if _, werr := w.Write(chunk); werr != nil {
				log.Printf("spanly: failed to write SSE chunk: %v", werr)
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			if inspect {
				frame.Write(chunk)
				p.drainFrames(&frame, override, transport)
				if frame.Len() > MaxInspectBytes {
					log.Printf("spanly: SSE frame exceeds %d bytes, disabling inspection for this stream", MaxInspectBytes)
					frame.Reset()
					inspect = false
				}
			}
		}
		if err != nil {
			if err != io.EOF && !isClosedErr(err) {
				log.Printf("spanly: SSE read error: %v", err)
			}
			return
		}
	}
}

func (p *Proxy) drainFrames(buf *bytes.Buffer, override PacketContext, transport TransportContext) {
	for {
		data := buf.Bytes()
		idx := bytes.Index(data, []byte("\n\n"))
		crlfIdx := bytes.Index(data, []byte("\r\n\r\n"))
		sepLen := 2
		if crlfIdx >= 0 && (idx < 0 || crlfIdx < idx) {
			idx = crlfIdx
			sepLen = 4
		}
		if idx < 0 {
			return
		}
		frame := make([]byte, idx)
		copy(frame, data[:idx])
		buf.Next(idx + sepLen)
		if packet, ok := ParseMCPPacket(frame); ok {
			p.collector.Collect("to-client", override, transport, packet)
		}
	}
}

// httpTransportContext describes one direction of an exchange; headers
// is r.Header for the request leg, resp.Header for the response leg.
func (p *Proxy) httpTransportContext(r *http.Request, headers http.Header) TransportContext {
	return TransportContext{
		Type:       "http",
		HTTPMethod: normalizeHTTPMethod(r.Method),
		Path:       r.URL.RequestURI(),
		Headers:    flattenHeaders(headers, p.redactHeaders),
	}
}

func normalizeHTTPMethod(m string) string {
	switch strings.ToUpper(m) {
	case http.MethodPost:
		return "post"
	case http.MethodDelete:
		return "delete"
	default:
		return "get"
	}
}

func flattenHeaders(h http.Header, redact map[string]struct{}) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		key := strings.ToLower(k)
		if _, ok := redact[key]; ok {
			out[key] = "[REDACTED]"
			continue
		}
		out[key] = strings.Join(v, ", ")
	}
	return out
}

func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		if isHopByHopHeader(k) {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

var hopByHop = map[string]struct{}{
	"Connection":          {},
	"Proxy-Connection":    {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"Te":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
	"Upgrade":             {},
}

func isHopByHopHeader(name string) bool {
	_, ok := hopByHop[http.CanonicalHeaderKey(name)]
	return ok
}

func isSSE(contentType string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(contentType)), "text/event-stream")
}

func singleJoiningPath(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		if a == "" {
			return b
		}
		return a + "/" + b
	}
	return a + b
}

func mergeQuery(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	default:
		return a + "&" + b
	}
}

func isCanceled(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled)
}

func isClosedErr(err error) bool {
	return errors.Is(err, net.ErrClosed)
}
