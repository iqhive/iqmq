package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// PingTimeout bounds a single protocol ping.
const PingTimeout = 15 * time.Second

// Ping performs a protocol-level liveness check appropriate for the broker:
// a Kafka ApiVersions round trip, a NATS PING/PONG, an AMQP or STOMP
// handshake, a RESP PING, a PostgreSQL SSLRequest, or an HTTP health call.
// It returns nil when the broker answered as expected.
func Ping(ctx context.Context, b *Broker) error {
	if b == nil {
		return errors.New("harness: nil broker")
	}
	ctx, cancel := context.WithTimeout(ctx, PingTimeout)
	defer cancel()
	switch b.Name {
	case "redpanda", "kafka":
		return PingKafka(ctx, b.Addr)
	case "nats":
		return PingNATS(ctx, b.Addr)
	case "nsq":
		if err := PingHTTP(ctx, "http://"+b.Addrs["http"]+"/ping", "OK"); err != nil {
			return fmt.Errorf("nsqd: %w", err)
		}
		if lk := b.Addrs["lookupd-http"]; lk != "" {
			if err := PingHTTP(ctx, "http://"+lk+"/ping", "OK"); err != nil {
				return fmt.Errorf("nsqlookupd: %w", err)
			}
		}
		return nil
	case "pubsub":
		project := b.Addrs["project"]
		if project == "" {
			project = PubSubProject
		}
		return PingHTTP(ctx, "http://"+b.Addr+"/v1/projects/"+project+"/topics", "")
	case "elasticmq":
		return PingHTTP(ctx, "http://"+b.Addr+"/?Action=ListQueues&Version=2012-11-05", "ListQueuesResponse")
	case "localstack":
		return PingHTTP(ctx, "http://"+b.Addr+"/_localstack/health", `"sqs"`)
	case "rabbitmq", "rabbitmq-delayed":
		return PingAMQP091(ctx, b.Addr)
	case "artemis":
		return PingSTOMP(ctx, b.Addrs["stomp"], b.Auth)
	case "activemq-classic":
		return PingSTOMP(ctx, b.Addrs["stomp"], b.Auth)
	case "redis", "valkey":
		return PingRESP(ctx, b.Addr)
	case "pulsar":
		return PingHTTP(ctx, "http://"+b.Addrs["admin"]+"/admin/v2/brokers/health", "ok")
	case "postgres":
		return PingPostgres(ctx, b.Addr)
	case "toxiproxy":
		return PingHTTP(ctx, "http://"+b.Addr+"/version", "")
	default:
		return PingTCP(ctx, b.Addr)
	}
}

func dial(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	return conn, nil
}

// PingTCP checks that addr accepts a TCP connection.
func PingTCP(ctx context.Context, addr string) error {
	conn, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	return conn.Close()
}

// PingHTTP performs GET url and requires a 2xx status whose body contains
// want (when want is non-empty).
func PingHTTP(ctx context.Context, url, want string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("GET %s: status %d: %s", url, resp.StatusCode, trunc(body))
	}
	if want != "" && !strings.Contains(string(body), want) {
		return fmt.Errorf("GET %s: body %q does not contain %q", url, trunc(body), want)
	}
	return nil
}

// PingKafka sends an ApiVersions (key 18, version 0) request and checks that
// the broker answers with a matching correlation ID and error code 0.
func PingKafka(ctx context.Context, addr string) error {
	conn, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	const corr = 0x1a2b3c4d
	req := kafkaAPIVersionsRequest(corr, "iqmq-harness")
	if _, err := conn.Write(req); err != nil {
		return err
	}
	var size [4]byte
	if _, err := io.ReadFull(conn, size[:]); err != nil {
		return fmt.Errorf("kafka: read size: %w", err)
	}
	n := binary.BigEndian.Uint32(size[:])
	if n < 6 || n > 1<<20 {
		return fmt.Errorf("kafka: implausible response size %d", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(conn, body); err != nil {
		return fmt.Errorf("kafka: read body: %w", err)
	}
	return checkKafkaAPIVersionsResponse(body, corr)
}

// kafkaAPIVersionsRequest encodes ApiVersionsRequest v0 with a size prefix.
func kafkaAPIVersionsRequest(corr int32, clientID string) []byte {
	var b []byte
	b = binary.BigEndian.AppendUint16(b, 18) // api key ApiVersions
	b = binary.BigEndian.AppendUint16(b, 0)  // api version
	b = binary.BigEndian.AppendUint32(b, uint32(corr))
	b = binary.BigEndian.AppendUint16(b, uint16(len(clientID)))
	b = append(b, clientID...)
	out := binary.BigEndian.AppendUint32(nil, uint32(len(b)))
	return append(out, b...)
}

// checkKafkaAPIVersionsResponse validates the body (without size prefix) of
// an ApiVersionsResponse v0.
func checkKafkaAPIVersionsResponse(body []byte, corr int32) error {
	if len(body) < 6 {
		return errors.New("kafka: short response")
	}
	if got := int32(binary.BigEndian.Uint32(body[:4])); got != corr {
		return fmt.Errorf("kafka: correlation id %d, want %d", got, corr)
	}
	if code := int16(binary.BigEndian.Uint16(body[4:6])); code != 0 {
		return fmt.Errorf("kafka: ApiVersions error code %d", code)
	}
	return nil
}

// PingNATS reads the server INFO line, sends PING and expects PONG.
func PingNATS(ctx context.Context, addr string) error {
	conn, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("nats: read INFO: %w", err)
	}
	if _, err := ParseNATSInfo(line); err != nil {
		return err
	}
	if _, err := io.WriteString(conn, "PING\r\n"); err != nil {
		return err
	}
	for {
		line, err = r.ReadString('\n')
		if err != nil {
			return fmt.Errorf("nats: read PONG: %w", err)
		}
		line = strings.TrimSpace(line)
		switch {
		case line == "PONG":
			return nil
		case line == "PING":
			if _, err := io.WriteString(conn, "PONG\r\n"); err != nil {
				return err
			}
		case strings.HasPrefix(line, "-ERR"):
			return fmt.Errorf("nats: %s", line)
		}
	}
}

// NATSInfo is the subset of the NATS INFO message the harness reads.
//
// Every field is decoded straight from the JSON object of the server's own
// INFO line and is the SERVER's account of itself, never the harness's: a
// field the server omits stays at its zero value, and none of them is checked
// against anything. [PingNATS] itself only needs the line to parse, and
// discards the result; the fields are here for a caller that wants to know
// what it connected to.
type NATSInfo struct {
	// ServerID is the identifier the server generates for itself and puts
	// in its INFO line. It names the PROCESS that answered, so two INFO
	// lines carrying different ServerIDs came from different servers --
	// which is a different question from Version, and the one to reach for
	// when a restarted container is the thing in doubt.
	ServerID string `json:"server_id"`
	// Version is the nats-server release the process reports ("2.14.6"),
	// which is the running build rather than the image tag the harness
	// asked for.
	Version string `json:"version"`
	// JetStream is whether the server advertises JetStream as enabled. A
	// nats-server started without --jetstream answers false; [NATS] always
	// passes it, so this is true against the harness's own container and
	// is only a real question for a server supplied through
	// IQMQ_NATS_ADDR.
	JetStream bool `json:"jetstream"`
	// Port is the client port the SERVER believes it is listening on --
	// 4222 by default. Inside a container that is the container-side port,
	// not the host port the harness dialled, so it is not an address to
	// connect to; [Broker.Addr] is.
	Port int `json:"port"`
}

// ParseNATSInfo parses an "INFO {...}" protocol line.
func ParseNATSInfo(line string) (NATSInfo, error) {
	var info NATSInfo
	rest, ok := strings.CutPrefix(strings.TrimSpace(line), "INFO ")
	if !ok {
		return info, fmt.Errorf("nats: expected INFO, got %q", trunc([]byte(line)))
	}
	if err := json.Unmarshal([]byte(rest), &info); err != nil {
		return NATSInfo{}, fmt.Errorf("nats: INFO payload: %w", err)
	}
	return info, nil
}

// PingAMQP091 sends the AMQP 0-9-1 protocol header and expects a
// Connection.Start method frame (type 1, channel 0, class 10, method 10).
func PingAMQP091(ctx context.Context, addr string) error {
	conn, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}); err != nil {
		return err
	}
	var hdr [11]byte // type(1) channel(2) size(4) class(2) method(2)
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return fmt.Errorf("amqp: read frame: %w", err)
	}
	return CheckAMQPStart(hdr[:])
}

// CheckAMQPStart validates the first 11 bytes of a server greeting frame.
func CheckAMQPStart(hdr []byte) error {
	if len(hdr) < 11 {
		return errors.New("amqp: short frame")
	}
	if bytes.HasPrefix(hdr, []byte("AMQP")) {
		return fmt.Errorf("amqp: server rejected protocol, offered %d.%d.%d", hdr[5], hdr[6], hdr[7])
	}
	if hdr[0] != 1 {
		return fmt.Errorf("amqp: frame type %d, want method (1)", hdr[0])
	}
	if ch := binary.BigEndian.Uint16(hdr[1:3]); ch != 0 {
		return fmt.Errorf("amqp: channel %d, want 0", ch)
	}
	class, method := binary.BigEndian.Uint16(hdr[7:9]), binary.BigEndian.Uint16(hdr[9:11])
	if class != 10 || method != 10 {
		return fmt.Errorf("amqp: class %d method %d, want Connection.Start (10,10)", class, method)
	}
	return nil
}

// PingSTOMP sends a STOMP 1.2 CONNECT frame and expects CONNECTED.
func PingSTOMP(ctx context.Context, addr string, auth Auth) error {
	conn, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	var sb strings.Builder
	sb.WriteString("CONNECT\naccept-version:1.0,1.1,1.2\nhost:/\n")
	if auth.User != "" {
		sb.WriteString("login:" + auth.User + "\npasscode:" + auth.Password.Reveal() + "\n")
	}
	sb.WriteString("\n\x00")
	if _, err := io.WriteString(conn, sb.String()); err != nil {
		return err
	}
	r := bufio.NewReader(conn)
	frame, err := r.ReadString(0)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("stomp: read: %w", err)
	}
	return CheckSTOMPConnected(frame)
}

// CheckSTOMPConnected validates a STOMP server reply frame.
func CheckSTOMPConnected(frame string) error {
	frame = strings.TrimLeft(frame, "\r\n")
	cmd, _, _ := strings.Cut(frame, "\n")
	cmd = strings.TrimRight(cmd, "\r")
	switch cmd {
	case "CONNECTED":
		return nil
	case "ERROR":
		return fmt.Errorf("stomp: ERROR frame: %q", trunc([]byte(frame)))
	default:
		return fmt.Errorf("stomp: unexpected frame %q", trunc([]byte(frame)))
	}
}

// PingRESP sends a RESP PING and expects +PONG.
func PingRESP(ctx context.Context, addr string) error {
	conn, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := io.WriteString(conn, "*1\r\n$4\r\nPING\r\n"); err != nil {
		return err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("resp: read: %w", err)
	}
	return CheckRESPPong(line)
}

// CheckRESPPong validates the reply line to PING.
func CheckRESPPong(line string) error {
	line = strings.TrimSpace(line)
	if line == "+PONG" {
		return nil
	}
	if strings.HasPrefix(line, "-") {
		return fmt.Errorf("resp: error reply %q", line)
	}
	return fmt.Errorf("resp: unexpected reply %q", line)
}

// PingPostgres sends an SSLRequest startup message and expects the server
// to answer 'S' (TLS available) or 'N' (plain), proving a PostgreSQL
// front end is listening.
func PingPostgres(ctx context.Context, addr string) error {
	conn, err := dial(ctx, addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte{0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f}); err != nil {
		return err
	}
	var reply [1]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		return fmt.Errorf("postgres: read: %w", err)
	}
	return CheckPostgresSSLReply(reply[0])
}

// CheckPostgresSSLReply validates the single-byte SSLRequest response.
func CheckPostgresSSLReply(b byte) error {
	switch b {
	case 'S', 'N':
		return nil
	case 'E':
		return errors.New("postgres: server returned ErrorResponse to SSLRequest")
	default:
		return fmt.Errorf("postgres: unexpected reply byte %q", b)
	}
}

func trunc(b []byte) string {
	const max = 200
	if len(b) > max {
		return string(b[:max]) + "..."
	}
	return string(b)
}
