package nsq

import mq "github.com/iqhive/iqmq"

// ResolvedForTest is the defaulting [Config.DSN] renders from, for this
// package's tests only.
//
// A rendering writes every default out, so a configuration parsed back from
// one differs from its source wherever the source left a setting at zero.
// Comparing the two RESOLVED configurations field by field is what says the
// rendering lost nothing, where comparing renderings cannot fail when the
// rendering itself is what dropped the setting.
func ResolvedForTest(c Config) Config { return c.resolve() }

// ClaimForTest is the first half of an Ack: it claims d and returns the rest,
// which writes the FIN, so a test can run Close between the two.
func ClaimForTest(src mq.Source, d *mq.DriverDelivery) (func(), error) {
	s := src.(*source)
	ref, err := s.settleFn.claim(d)
	if err != nil {
		return nil, err
	}
	return func() {
		owed := s.takeOwed(ref)
		ref.msg.Finish()
		s.payOwed(owed)
		s.forget(ref)
	}, nil
}
