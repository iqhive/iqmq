package nack

import mq "bitbucket.org/iqhive/iqmq"

// custom hands the decision to the caller's own function.
//
// It exists because the three built-in answers are policies, and a deployment
// may have a fourth — a side topic per tenant, a bounded in-process retry that
// falls through to a stop, a route that depends on the failure's own error.
// What it does not do is trust the answer: [Plan.Validate] runs on every plan
// the function returns, because the two combinations it refuses are the ones
// that lose a message rather than merely behaving unusually, and a driver that
// followed them would do so silently.
type custom struct{ cfg Config }

// Plan asks the configured function and validates what it returns.
func (s custom) Plan(d Delivery, o mq.NackOptions) (Plan, error) {
	if !o.Action.Valid() {
		return Plan{}, ErrInvalidAction
	}
	p, err := s.cfg.Decide(d, o)
	if err != nil {
		return Plan{}, err
	}
	if err := p.Validate(); err != nil {
		return Plan{}, err
	}
	return p, nil
}

// Kind returns Custom.
func (s custom) Kind() Kind { return Custom }
