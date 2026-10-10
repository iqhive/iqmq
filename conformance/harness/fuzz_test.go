package harness

import (
	"encoding/binary"
	"strings"
	"testing"
)

// FuzzParseNATSInfo checks the INFO parser never panics and only accepts
// lines that start with "INFO ".
func FuzzParseNATSInfo(f *testing.F) {
	f.Add(`INFO {"server_id":"X","version":"2.14.6","jetstream":true,"port":4222}` + "\r\n")
	f.Add("INFO {}")
	f.Add("-ERR nope")
	f.Add("INFO {bad")
	f.Add("")
	f.Fuzz(func(t *testing.T, line string) {
		info, err := ParseNATSInfo(line)
		if err == nil && !strings.HasPrefix(strings.TrimSpace(line), "INFO ") {
			t.Fatalf("accepted non-INFO line %q", line)
		}
		if err != nil && info != (NATSInfo{}) {
			t.Fatalf("non-zero info with error: %+v", info)
		}
	})
}

// FuzzKafkaAPIVersions checks the response validator against arbitrary bytes
// and that a well-formed response for the encoded request always passes.
func FuzzKafkaAPIVersions(f *testing.F) {
	f.Add(int32(1), "c", []byte{0, 0, 0, 1, 0, 0})
	f.Add(int32(-5), "", []byte{})
	f.Fuzz(func(t *testing.T, corr int32, client string, body []byte) {
		if len(client) > 1<<15 {
			client = client[:1<<15]
		}
		req := kafkaAPIVersionsRequest(corr, client)
		if int(binary.BigEndian.Uint32(req[:4])) != len(req)-4 {
			t.Fatal("size prefix mismatch")
		}
		if int32(binary.BigEndian.Uint32(req[8:12])) != corr {
			t.Fatal("correlation id not encoded")
		}
		_ = checkKafkaAPIVersionsResponse(body, corr)
		good := binary.BigEndian.AppendUint32(nil, uint32(corr))
		good = append(good, 0, 0)
		if err := checkKafkaAPIVersionsResponse(good, corr); err != nil {
			t.Fatal(err)
		}
	})
}

// FuzzProtocolChecks exercises the small frame validators with random input.
func FuzzProtocolChecks(f *testing.F) {
	f.Add([]byte{1, 0, 0, 0, 0, 0, 20, 0, 10, 0, 10}, "CONNECTED\nversion:1.2\n\n", "+PONG\r\n", byte('N'))
	f.Add([]byte("AMQP\x00\x00\x09\x01\x00\x00\x00"), "ERROR\n\n", "-ERR x\r\n", byte('E'))
	f.Fuzz(func(t *testing.T, amqp []byte, stomp, resp string, pg byte) {
		if err := CheckAMQPStart(amqp); err == nil {
			if len(amqp) < 11 || amqp[0] != 1 {
				t.Fatalf("accepted bad AMQP frame %v", amqp)
			}
		}
		if err := CheckSTOMPConnected(stomp); err == nil && !strings.Contains(stomp, "CONNECTED") {
			t.Fatalf("accepted non-CONNECTED frame %q", stomp)
		}
		if err := CheckRESPPong(resp); err == nil && strings.TrimSpace(resp) != "+PONG" {
			t.Fatalf("accepted %q as PONG", resp)
		}
		if err := CheckPostgresSSLReply(pg); (err == nil) != (pg == 'S' || pg == 'N') {
			t.Fatalf("postgres reply %q: %v", pg, err)
		}
		_ = parseAddrs(stomp)
		_ = Version(resp)
		_ = trimScheme(stomp)
	})
}
