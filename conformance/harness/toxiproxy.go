package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	"github.com/testcontainers/testcontainers-go/network"
)

// Proxy declares one Toxiproxy route: clients connect to the proxied
// endpoint and traffic is forwarded to Upstream (host:port as reachable from
// inside the toxiproxy container, typically a network alias).
type Proxy struct {
	// Name is this route's identifier, chosen by the caller. It is the
	// name Toxiproxy registers the route under, so it is the `proxy`
	// argument of every [Toxi] call ([Toxi.AddToxic], [Toxi.Latency] and
	// the rest), and it is the key [Toxiproxy] files the route's
	// client-facing endpoint under in Addrs -- which is why two routes of
	// one container sharing a Name would lose one of the two endpoints.
	Name string
	// Upstream is where the route forwards to, as host:port reachable from
	// INSIDE the toxiproxy container -- typically a Docker network alias,
	// never a host-published port, which is why these tests need a shared
	// network.
	Upstream string
}

// Toxiproxy starts a Toxiproxy container attached to nw with the given
// proxies. Addr is the control API host:port; Addrs[proxy.Name] is the
// client-facing endpoint of each proxy. The returned *Toxi drives toxics
// through the control API.
func Toxiproxy(tb testing.TB, nw *testcontainers.DockerNetwork, proxies ...Proxy) (*Broker, *Toxi) {
	tb.Helper()
	requireDocker(tb)
	image := Image("toxiproxy")
	ctx, cancel := startCtx(tb)
	tb.Cleanup(cancel)
	opts := make([]testcontainers.ContainerCustomizer, 0, len(proxies)+1)
	if nw != nil {
		opts = append(opts, network.WithNetwork([]string{"toxiproxy"}, nw))
	}
	for _, p := range proxies {
		opts = append(opts, toxiproxy.WithProxy(p.Name, p.Upstream))
	}
	ctr, err := toxiproxy.Run(ctx, image, opts...)
	testcontainers.CleanupContainer(tb, ctr)
	if err != nil {
		tb.Fatalf("harness: start toxiproxy (%s): %v", image, err)
	}
	b := wrap("toxiproxy", image, ctr)
	uri, err := ctr.URI(ctx)
	if err != nil {
		tb.Fatalf("harness: toxiproxy uri: %v", err)
	}
	b.Addr = trimScheme(uri)
	for i, p := range proxies {
		host, port, err := ctr.ProxiedEndpoint(8666 + i)
		if err != nil {
			tb.Fatalf("harness: toxiproxy proxied endpoint %s: %v", p.Name, err)
		}
		b.Addrs[p.Name] = host + ":" + port
	}
	return b, &Toxi{base: uri, client: http.DefaultClient}
}

// NewNetwork creates a Docker network shared by a broker and Toxiproxy and
// registers its removal with t.Cleanup.
func NewNetwork(tb testing.TB) *testcontainers.DockerNetwork {
	tb.Helper()
	requireDocker(tb)
	ctx, cancel := startCtx(tb)
	defer cancel()
	nw, err := network.New(ctx)
	testcontainers.CleanupNetwork(tb, nw)
	if err != nil {
		tb.Fatalf("harness: network: %v", err)
	}
	return nw
}

// OnNetwork attaches a container to nw under alias; pass it to a broker
// constructor so Toxiproxy can reach the broker as alias:port.
func OnNetwork(alias string, nw *testcontainers.DockerNetwork) testcontainers.CustomizeRequestOption {
	return network.WithNetwork([]string{alias}, nw)
}

// Toxi is a minimal Toxiproxy control-API client (stdlib only).
type Toxi struct {
	base   string
	client *http.Client
}

// Toxic describes a toxic to add to a proxy.
type Toxic struct {
	// Name is optional; Toxiproxy derives "<type>_<stream>" when empty.
	Name string `json:"name,omitempty"`
	// Type is one of latency, bandwidth, slow_close, timeout, reset_peer,
	// slicer, limit_data.
	Type string `json:"type"`
	// Stream is "upstream" or "downstream" (default).
	Stream string `json:"stream,omitempty"`
	// Toxicity is the probability [0,1] the toxic applies to a connection.
	Toxicity float64 `json:"toxicity,omitempty"`
	// Attributes are type-specific, e.g. {"latency": 500, "jitter": 50}.
	Attributes map[string]any `json:"attributes"`
}

// AddToxic adds t to proxy and returns the toxic name.
func (x *Toxi) AddToxic(ctx context.Context, proxy string, t Toxic) (string, error) {
	if t.Attributes == nil {
		t.Attributes = map[string]any{}
	}
	var out struct {
		Name string `json:"name"`
	}
	if err := x.do(ctx, http.MethodPost, "/proxies/"+proxy+"/toxics", t, &out); err != nil {
		return "", err
	}
	return out.Name, nil
}

// RemoveToxic deletes a toxic by name.
func (x *Toxi) RemoveToxic(ctx context.Context, proxy, name string) error {
	return x.do(ctx, http.MethodDelete, "/proxies/"+proxy+"/toxics/"+name, nil, nil)
}

// SetEnabled enables or disables a proxy. A disabled proxy refuses new
// connections and drops existing ones, simulating a broker outage.
func (x *Toxi) SetEnabled(ctx context.Context, proxy string, enabled bool) error {
	return x.do(ctx, http.MethodPost, "/proxies/"+proxy, map[string]any{"enabled": enabled}, nil)
}

// Reset removes all toxics from all proxies and enables every proxy.
func (x *Toxi) Reset(ctx context.Context) error {
	return x.do(ctx, http.MethodPost, "/reset", nil, nil)
}

// Latency adds a downstream latency toxic (milliseconds) and returns a
// function that removes it.
func (x *Toxi) Latency(ctx context.Context, proxy string, ms, jitter int) (func(context.Context) error, error) {
	name, err := x.AddToxic(ctx, proxy, Toxic{
		Type: "latency", Stream: "downstream", Toxicity: 1,
		Attributes: map[string]any{"latency": ms, "jitter": jitter},
	})
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) error { return x.RemoveToxic(ctx, proxy, name) }, nil
}

// Timeout adds a toxic that stops all data and closes the connection after
// ms milliseconds (0 means never close: a black hole).
func (x *Toxi) Timeout(ctx context.Context, proxy string, ms int) (func(context.Context) error, error) {
	name, err := x.AddToxic(ctx, proxy, Toxic{
		Type: "timeout", Stream: "downstream", Toxicity: 1,
		Attributes: map[string]any{"timeout": ms},
	})
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) error { return x.RemoveToxic(ctx, proxy, name) }, nil
}

// ResetPeer adds a toxic that resets connections (TCP RST) after ms
// milliseconds.
func (x *Toxi) ResetPeer(ctx context.Context, proxy string, ms int) (func(context.Context) error, error) {
	name, err := x.AddToxic(ctx, proxy, Toxic{
		Type: "reset_peer", Stream: "downstream", Toxicity: 1,
		Attributes: map[string]any{"timeout": ms},
	})
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) error { return x.RemoveToxic(ctx, proxy, name) }, nil
}

func (x *Toxi) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader = http.NoBody
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, x.base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := x.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("toxiproxy: %s %s: status %s: %s", method, path, strconv.Itoa(resp.StatusCode), trunc(data))
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

func trimScheme(s string) string {
	for _, p := range []string{"http://", "https://"} {
		if len(s) > len(p) && s[:len(p)] == p {
			return s[len(p):]
		}
	}
	return s
}
