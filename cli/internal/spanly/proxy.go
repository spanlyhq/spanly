package spanly

import (
	"bytes"
	"context"
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
)

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
}

type Proxy struct {
	upstream       *url.URL
	collector      *Collector
	inspectPrefix  []string
	contextHeaders []HeaderMapping
	redactHeaders  map[string]struct{}

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
		upstream:       opts.Upstream,
		collector:      opts.Collector,
		inspectPrefix:  opts.InspectPrefix,
		contextHeaders: opts.ContextHeaders,
		redactHeaders:  redact,
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

	requestBody, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadGateway)
		return
	}
	_ = r.Body.Close()

	override := p.buildOverride(r)

	transport := p.requestTransportContext(r)
	if packet, ok := ParseMCPPacket(requestBody); ok {
		p.collector.Collect("from-client", override, transport, packet)
	}

	outURL := *p.upstream
	outURL.Path = singleJoiningPath(p.upstream.Path, r.URL.Path)
	outURL.RawQuery = mergeQuery(p.upstream.RawQuery, r.URL.RawQuery)

	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, outURL.String(), bytes.NewReader(requestBody))
	if err != nil {
		http.Error(w, "failed to build upstream request", http.StatusBadGateway)
		return
	}
	copyHeaders(outReq.Header, r.Header)
	outReq.Host = p.upstream.Host

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

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	respTransport := p.responseTransportContext(r, resp)

	if isSSE(resp.Header.Get("Content-Type")) {
		p.streamSSE(w, resp.Body, override, respTransport)
		return
	}

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("spanly: failed to read upstream response: %v", err)
		return
	}
	if packet, ok := ParseMCPPacket(responseBody); ok {
		p.collector.Collect("to-client", override, respTransport, packet)
	}
	if _, err := w.Write(responseBody); err != nil {
		log.Printf("spanly: failed to write response to client: %v", err)
	}
}

func (p *Proxy) streamSSE(w http.ResponseWriter, body io.Reader, override PacketContext, transport TransportContext) {
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 8*1024)
	var frame bytes.Buffer
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
			frame.Write(chunk)
			p.drainFrames(&frame, override, transport)
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

func (p *Proxy) requestTransportContext(r *http.Request) TransportContext {
	return TransportContext{
		Type:       "http",
		HTTPMethod: normalizeHTTPMethod(r.Method),
		Path:       r.URL.RequestURI(),
		Headers:    flattenHeaders(r.Header, p.redactHeaders),
	}
}

func (p *Proxy) responseTransportContext(r *http.Request, resp *http.Response) TransportContext {
	return TransportContext{
		Type:       "http",
		HTTPMethod: normalizeHTTPMethod(r.Method),
		Path:       r.URL.RequestURI(),
		Headers:    flattenHeaders(resp.Header, p.redactHeaders),
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
	if ctx.Err() != nil {
		return true
	}
	return strings.Contains(err.Error(), "context canceled")
}

func isClosedErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "use of closed network connection")
}
