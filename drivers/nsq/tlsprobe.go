package nsq

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	gonsq "github.com/nsqio/go-nsq"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// maxProbeFrame bounds the frame the TLS probe reads. nsqd's answer to IDENTIFY
// is a few hundred bytes; a size past this is not nsqd talking, and go-nsq's
// own reader would allocate whatever the first four bytes claim.
const maxProbeFrame = 64 << 10

// checkTLS asks each address in addrs, one after another, whether it agrees to
// TLS, and refuses the first that declines. OpenBackend passes every
// configured address and Ping the one it is about to ping.
//
// go-nsq treats Config.TLS as a request: it asks for TLS in IDENTIFY, upgrades
// only when nsqd's answer agrees, and otherwise sends AUTH and every message in
// the clear, all inside one call with no point at which the driver could stop
// it. This probe makes the same request on a connection of its own and closes
// it without sending AUTH, so an nsqd started without TLS is refused before
// go-nsq talks to it. It is not the guarantee a go-nsq change would give: it
// sees nsqd at the moment it runs, so an nsqd replaced afterwards, one
// discovered through nsqlookupd, go-nsq's own later redials and anything on the
// path that answers the probe with TLS and go-nsq's connection without it are
// all outside it.
//
// An address the probe cannot reach, or whose handshake fails for another
// reason, is not refused here. Nothing in that failure says nsqd declined TLS,
// and the publication or Ping that next connects reports it as it does today.
func checkTLS(ctx context.Context, cfg Config, addrs []string) error {
	for _, addr := range addrs {
		declined, err := tlsDeclined(ctx, cfg, addr)
		if err != nil || !declined {
			continue
		}
		return &mq.OpError{
			Driver: DriverName, Operation: mq.OpOpen, Address: addr, Code: mq.CodeInvalidConfig,
			Err: fmt.Errorf("nsqd %s: %w", addr, errTLSDeclined),
		}
	}
	return nil
}

// tlsDeclined reports whether the nsqd at addr answers an IDENTIFY asking for
// TLS without agreeing to it. When nsqd agrees, the handshake is completed with
// cfg.TLS, verifying nsqd against its own host name as go-nsq does, so that
// nsqd sees a client finish the upgrade rather than abandon it.
func tlsDeclined(ctx context.Context, cfg Config, addr string) (bool, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false, err
	}
	d := net.Dialer{Timeout: cfg.DialTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false, err
	}
	defer func() { _ = conn.Close() }()
	// DialTimeout and not ReadTimeout bounds the exchange: it is one short
	// round trip and a TLS handshake, where ReadTimeout is go-nsq's
	// heartbeat-scale read deadline, a minute by default, which an nsqd that
	// accepts and never answers would hold OpenBackend for.
	if err := conn.SetDeadline(time.Now().Add(cfg.DialTimeout)); err != nil {
		return false, err
	}
	// The caller's context ends the exchange too. The function only moves the
	// deadline, and the wait below is what owns its goroutine.
	cut := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(cut)
		_ = conn.SetDeadline(time.Unix(1, 0))
	})
	defer func() {
		if !stop() {
			<-cut
		}
	}()

	gc := gonsq.NewConfig()
	identity := map[string]any{
		"client_id":           firstSet(cfg.ClientID, gc.ClientID),
		"hostname":            firstSet(cfg.Hostname, gc.Hostname),
		"user_agent":          firstSet(cfg.UserAgent, gc.UserAgent),
		"feature_negotiation": true,
		"tls_v1":              true,
	}
	cmd, err := gonsq.Identify(identity)
	if err != nil {
		return false, err
	}
	if _, err := conn.Write(gonsq.MagicV2); err != nil {
		return false, err
	}
	if _, err := cmd.WriteTo(conn); err != nil {
		return false, err
	}
	frameType, data, err := readProbeFrame(conn)
	if err != nil {
		return false, err
	}
	if frameType == gonsq.FrameTypeError {
		// nsqd refused the IDENTIFY itself, which go-nsq meets the same way
		// and reports as it always has.
		return false, gonsq.ErrIdentify{Reason: string(data)}
	}
	if len(data) == 0 || data[0] != '{' {
		// An nsqd that does not negotiate features answers OK and upgrades
		// nothing, and go-nsq goes on in the clear.
		return true, nil
	}
	var answer struct {
		TLSv1 bool `json:"tls_v1"`
	}
	if err := json.Unmarshal(data, &answer); err != nil {
		return false, err
	}
	if !answer.TLSv1 {
		return true, nil
	}
	conf := cfg.TLS.Clone()
	conf.ServerName = host
	tc := tls.Client(conn, conf)
	if err := tc.HandshakeContext(ctx); err != nil {
		return false, err
	}
	_, _, err = readProbeFrame(tc)
	_ = tc.Close()
	return false, err
}

// readProbeFrame reads one nsqd frame: a 4-byte size, a 4-byte frame type and
// the data.
func readProbeFrame(r io.Reader) (int32, []byte, error) {
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(size[:])
	if n < 4 || n > maxProbeFrame {
		return 0, nil, errProbeFrame
	}
	frame := make([]byte, n)
	if _, err := io.ReadFull(r, frame); err != nil {
		return 0, nil, err
	}
	return int32(binary.BigEndian.Uint32(frame[:4])), frame[4:], nil
}

// firstSet returns the configured value, or go-nsq's default when there is
// none, which is what go-nsq's own IDENTIFY would carry.
func firstSet(configured, fallback string) string {
	if configured != "" {
		return configured
	}
	return fallback
}

// The probe's refusals.
var (
	errTLSDeclined = errcode.New(errcode.InvalidConfig,
		"iqmq/nsq: Config.TLS is set and nsqd declined TLS in the IDENTIFY handshake, so go-nsq would send AUTH and every message to it in the clear; configure TLS on nsqd or clear Config.TLS")
	errProbeFrame = errors.New("iqmq/nsq: the answer to IDENTIFY is not an nsqd frame")
)
