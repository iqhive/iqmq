package harness

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestVersion(t *testing.T) {
	cases := map[string]string{
		"redis:7.4-alpine":                           "7.4-alpine",
		"redpandadata/redpanda:v26.2.2":              "26.2.2",
		"ghcr.io/shopify/toxiproxy:2.12.0":           "2.12.0",
		"localhost:5000/foo/bar":                     "latest",
		"localhost:5000/foo/bar:1.2":                 "1.2",
		"nats":                                       "latest",
		"redis:7@sha256:abcdef":                      "7",
		"gcr.io/google.com/cloudsdktool/x:emulators": "emulators",
	}
	for img, want := range cases {
		if got := Version(img); got != want {
			t.Errorf("Version(%q) = %q, want %q", img, got, want)
		}
	}
}

func TestEnvKey(t *testing.T) {
	if got := envKey("activemq-classic", "ADDR"); got != "IQMQ_ACTIVEMQ_CLASSIC_ADDR" {
		t.Fatalf("envKey = %q", got)
	}
}

func TestImageOverride(t *testing.T) {
	t.Setenv("IQMQ_REDIS_IMAGE", "redis:8")
	if got := imageRef(t, "redis"); got != "redis:8" {
		t.Fatalf("Image override = %q", got)
	}
	// Without an override the default is the DIGEST images.lock pins, not the
	// tag Images declares ;
	// TestImageResolvesThroughTheLock holds that for every name.
	if got := imageRef(t, "valkey"); got != lockedImages["valkey"] {
		t.Fatalf("Image default = %q, want the locked %q", got, lockedImages["valkey"])
	}
}

func TestImagesAllPinned(t *testing.T) {
	for name, img := range Images {
		if !strings.Contains(img, ":") {
			t.Errorf("%s: image %q has no tag", name, img)
		}
		if strings.HasSuffix(img, ":latest") {
			t.Errorf("%s: image %q is :latest", name, img)
		}
	}
}

func TestParseAddrs(t *testing.T) {
	got := parseAddrs(" admin=localhost:9644, http = localhost:8082 ,,bad,=x,y=")
	want := map[string]string{"admin": "localhost:9644", "http": "localhost:8082"}
	if len(got) != len(want) {
		t.Fatalf("parseAddrs = %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}

func TestExternal(t *testing.T) {
	if _, ok := external("nats"); ok {
		t.Fatal("external without env should be false")
	}
	t.Setenv("IQMQ_NATS_ADDR", "nats.example:4222")
	t.Setenv("IQMQ_NATS_ADDRS", "monitor=nats.example:8222")
	t.Setenv("IQMQ_NATS_USER", "u")
	t.Setenv("IQMQ_NATS_PASSWORD", "p")
	b, ok := external("nats")
	if !ok || !b.External || b.Addr != "nats.example:4222" || b.Addrs["monitor"] != "nats.example:8222" {
		t.Fatalf("external = %+v, %v", b, ok)
	}
	if b.Auth != (Auth{User: "u", Password: "p"}) {
		t.Fatalf("auth = %+v", b.Auth)
	}
	if b.Endpoint("") != b.Addr || b.Endpoint("monitor") != "nats.example:8222" {
		t.Fatal("Endpoint")
	}
	if err := b.Terminate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b.Logs(context.Background()) != "" || b.Container() != nil {
		t.Fatal("external broker has no container")
	}
	// Constructors return the external broker without touching Docker.
	if got := NATS(t); got.Addr != "nats.example:4222" {
		t.Fatalf("NATS() = %+v", got)
	}
}

func TestParseNATSInfo(t *testing.T) {
	info, err := ParseNATSInfo(`INFO {"server_id":"X","version":"2.14.6","jetstream":true,"port":4222}` + "\r\n")
	if err != nil || info.ServerID != "X" || !info.JetStream || info.Port != 4222 {
		t.Fatalf("ParseNATSInfo = %+v, %v", info, err)
	}
	if _, err := ParseNATSInfo("-ERR nope"); err == nil {
		t.Fatal("expected error for non-INFO line")
	}
	if _, err := ParseNATSInfo("INFO {bad"); err == nil {
		t.Fatal("expected error for bad JSON")
	}
}

func TestCheckAMQPStart(t *testing.T) {
	good := []byte{1, 0, 0, 0, 0, 1, 0, 0, 10, 0, 10}
	if err := CheckAMQPStart(good); err != nil {
		t.Fatal(err)
	}
	bad := [][]byte{
		{1, 0, 0},
		{'A', 'M', 'Q', 'P', 0, 0, 9, 1, 0, 0, 0},
		{2, 0, 0, 0, 0, 1, 0, 0, 10, 0, 10},
		{1, 0, 1, 0, 0, 1, 0, 0, 10, 0, 10},
		{1, 0, 0, 0, 0, 1, 0, 0, 20, 0, 10},
	}
	for i, b := range bad {
		if err := CheckAMQPStart(b); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}

func TestCheckSTOMPConnected(t *testing.T) {
	if err := CheckSTOMPConnected("\nCONNECTED\r\nversion:1.2\n\n"); err != nil {
		t.Fatal(err)
	}
	if err := CheckSTOMPConnected("ERROR\nmessage:denied\n\n"); err == nil {
		t.Fatal("expected error")
	}
	if err := CheckSTOMPConnected("MESSAGE\n\n"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCheckRESPPong(t *testing.T) {
	if err := CheckRESPPong("+PONG\r\n"); err != nil {
		t.Fatal(err)
	}
	if err := CheckRESPPong("-NOAUTH Authentication required.\r\n"); err == nil {
		t.Fatal("expected error")
	}
	if err := CheckRESPPong("$4\r\n"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCheckPostgresSSLReply(t *testing.T) {
	for _, b := range []byte{'S', 'N'} {
		if err := CheckPostgresSSLReply(b); err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range []byte{'E', 'x'} {
		if err := CheckPostgresSSLReply(b); err == nil {
			t.Fatal("expected error")
		}
	}
}

func TestKafkaApiVersionsEncoding(t *testing.T) {
	req := kafkaAPIVersionsRequest(7, "c")
	if size := binary.BigEndian.Uint32(req[:4]); int(size) != len(req)-4 {
		t.Fatalf("size prefix %d, len %d", size, len(req))
	}
	if key := binary.BigEndian.Uint16(req[4:6]); key != 18 {
		t.Fatalf("api key %d", key)
	}
	if corr := binary.BigEndian.Uint32(req[8:12]); corr != 7 {
		t.Fatalf("corr %d", corr)
	}
	resp := binary.BigEndian.AppendUint32(nil, 7)
	resp = binary.BigEndian.AppendUint16(resp, 0)
	if err := checkKafkaAPIVersionsResponse(resp, 7); err != nil {
		t.Fatal(err)
	}
	if err := checkKafkaAPIVersionsResponse(resp, 8); err == nil {
		t.Fatal("expected correlation mismatch")
	}
	resp[5] = 35 // UNSUPPORTED_VERSION
	if err := checkKafkaAPIVersionsResponse(resp, 7); err == nil {
		t.Fatal("expected error code")
	}
	if err := checkKafkaAPIVersionsResponse(resp[:3], 7); err == nil {
		t.Fatal("expected short response error")
	}
}

// fakeServer runs handler for each accepted connection on a loopback port.
func fakeServer(t *testing.T, handler func(net.Conn)) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				handler(c)
			}()
		}
	}()
	return l.Addr().String()
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestPingNATSFake(t *testing.T) {
	addr := fakeServer(t, func(c net.Conn) {
		_, _ = io.WriteString(c, `INFO {"server_id":"f","version":"0","jetstream":true}`+"\r\n")
		r := bufio.NewReader(c)
		line, _ := r.ReadString('\n')
		if strings.TrimSpace(line) == "PING" {
			_, _ = io.WriteString(c, "PONG\r\n")
		}
	})
	if err := PingNATS(testCtx(t), addr); err != nil {
		t.Fatal(err)
	}
	if err := Ping(testCtx(t), &Broker{Name: "nats", Addr: addr}); err != nil {
		t.Fatal(err)
	}
}

func TestPingRESPFake(t *testing.T) {
	addr := fakeServer(t, func(c net.Conn) {
		buf := make([]byte, 64)
		n, _ := c.Read(buf)
		if strings.Contains(string(buf[:n]), "PING") {
			_, _ = io.WriteString(c, "+PONG\r\n")
		}
	})
	if err := Ping(testCtx(t), &Broker{Name: "redis", Addr: addr}); err != nil {
		t.Fatal(err)
	}
	if err := Ping(testCtx(t), &Broker{Name: "valkey", Addr: addr}); err != nil {
		t.Fatal(err)
	}
}

func TestPingSTOMPFake(t *testing.T) {
	var gotLogin string
	addr := fakeServer(t, func(c net.Conn) {
		frame, _ := bufio.NewReader(c).ReadString(0)
		for line := range strings.SplitSeq(frame, "\n") {
			if v, ok := strings.CutPrefix(line, "login:"); ok {
				gotLogin = v
			}
		}
		_, _ = io.WriteString(c, "CONNECTED\nversion:1.2\n\n\x00")
	})
	b := &Broker{Name: "artemis", Addrs: map[string]string{"stomp": addr}, Auth: Auth{User: "artemis", Password: "artemis"}}
	if err := Ping(testCtx(t), b); err != nil {
		t.Fatal(err)
	}
	if gotLogin != "artemis" {
		t.Fatalf("login header = %q", gotLogin)
	}
}

func TestPingAMQPFake(t *testing.T) {
	addr := fakeServer(t, func(c net.Conn) {
		hdr := make([]byte, 8)
		if _, err := io.ReadFull(c, hdr); err != nil || string(hdr[:4]) != "AMQP" {
			return
		}
		_, _ = c.Write([]byte{1, 0, 0, 0, 0, 0, 20, 0, 10, 0, 10})
	})
	if err := Ping(testCtx(t), &Broker{Name: "rabbitmq", Addr: addr}); err != nil {
		t.Fatal(err)
	}
}

func TestPingPostgresFake(t *testing.T) {
	addr := fakeServer(t, func(c net.Conn) {
		msg := make([]byte, 8)
		if _, err := io.ReadFull(c, msg); err == nil && binary.BigEndian.Uint32(msg[4:]) == 80877103 {
			_, _ = c.Write([]byte{'N'})
		}
	})
	if err := Ping(testCtx(t), &Broker{Name: "postgres", Addr: addr}); err != nil {
		t.Fatal(err)
	}
}

func TestPingKafkaFake(t *testing.T) {
	addr := fakeServer(t, func(c net.Conn) {
		var size [4]byte
		if _, err := io.ReadFull(c, size[:]); err != nil {
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(size[:]))
		if _, err := io.ReadFull(c, body); err != nil {
			return
		}
		corr := body[4:8]
		resp := append([]byte{0, 0, 0, 6}, corr...)
		resp = append(resp, 0, 0)
		_, _ = c.Write(resp)
	})
	for _, name := range []string{"kafka", "redpanda"} {
		if err := Ping(testCtx(t), &Broker{Name: name, Addr: addr}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPingHTTPBrokers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ping":
			_, _ = io.WriteString(w, "OK")
		case "/_localstack/health":
			_, _ = io.WriteString(w, `{"services":{"sqs":"running"}}`)
		case "/admin/v2/brokers/health":
			_, _ = io.WriteString(w, "ok")
		case "/version":
			_, _ = io.WriteString(w, "2.12.0")
		case "/v1/projects/iqmq-test/topics":
			_, _ = io.WriteString(w, "{}")
		case "/":
			_, _ = io.WriteString(w, "<ListQueuesResponse/>")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	brokers := []*Broker{
		{Name: "nsq", Addrs: map[string]string{"http": addr, "lookupd-http": addr}},
		{Name: "localstack", Addr: addr},
		{Name: "pulsar", Addrs: map[string]string{"admin": addr}},
		{Name: "toxiproxy", Addr: addr},
		{Name: "pubsub", Addr: addr},
		{Name: "elasticmq", Addr: addr},
		{Name: "unknown", Addr: addr},
	}
	for _, b := range brokers {
		if err := Ping(testCtx(t), b); err != nil {
			t.Errorf("%s: %v", b.Name, err)
		}
	}
	if err := PingHTTP(testCtx(t), srv.URL+"/missing", ""); err == nil {
		t.Fatal("expected 404 error")
	}
	if err := PingHTTP(testCtx(t), srv.URL+"/ping", "NOPE"); err == nil {
		t.Fatal("expected body mismatch error")
	}
	if err := Ping(testCtx(t), nil); err == nil {
		t.Fatal("expected nil broker error")
	}
}

func TestToxiClient(t *testing.T) {
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		calls = append(calls, r.Method+" "+r.URL.Path+" "+string(body))
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/proxies/p/toxics":
			var tx Toxic
			if err := json.Unmarshal(body, &tx); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"name": tx.Type + "_" + tx.Stream})
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Path == "/reset", r.URL.Path == "/proxies/p":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "nope", 500)
		}
	}))
	t.Cleanup(srv.Close)
	x := &Toxi{base: srv.URL, client: srv.Client()}
	ctx := testCtx(t)
	rm, err := x.Latency(ctx, "p", 100, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := rm(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := x.Timeout(ctx, "p", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := x.ResetPeer(ctx, "p", 5); err != nil {
		t.Fatal(err)
	}
	if err := x.SetEnabled(ctx, "p", false); err != nil {
		t.Fatal(err)
	}
	if err := x.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := x.AddToxic(ctx, "other", Toxic{Type: "latency"}); err == nil {
		t.Fatal("expected 500 error")
	}
	want := []string{
		`POST /proxies/p/toxics {"type":"latency","stream":"downstream","toxicity":1,"attributes":{"jitter":10,"latency":100}}`,
		"DELETE /proxies/p/toxics/latency_downstream ",
	}
	for i, w := range want {
		if calls[i] != w {
			t.Errorf("call %d = %q, want %q", i, calls[i], w)
		}
	}
	if trimScheme("http://a:1") != "a:1" || trimScheme("a:1") != "a:1" {
		t.Fatal("trimScheme")
	}
}

func TestFreePortAndFixedPorts(t *testing.T) {
	p, err := freePort()
	if err != nil || p <= 0 {
		t.Fatalf("freePort = %d, %v", p, err)
	}
	if tcpPort(p) != strconv.Itoa(p)+"/tcp" {
		t.Fatal("tcpPort")
	}
	if fixedPorts(p) == nil {
		t.Fatal("fixedPorts")
	}
	// The repository may be checked out under any directory name (increment
	// worktrees are not called iqmq), so identify the root by its go.mod and
	// by the image context repoRoot is used for, which must exist beneath it.
	mod, err := os.ReadFile(filepath.Join(repoRoot(), "go.mod"))
	if err != nil || !strings.HasPrefix(string(mod), "module github.com/iqhive/iqmq\n") {
		t.Fatalf("repoRoot = %q is not the repository root: %v", repoRoot(), err)
	}
	if _, err := os.Stat(filepath.Join(repoRoot(), "hack", "images", "rabbitmq-delayed")); err != nil {
		t.Fatalf("repoRoot = %q: %v", repoRoot(), err)
	}
}
