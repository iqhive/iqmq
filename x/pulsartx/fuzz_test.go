package pulsartx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/pulsartx"
)

// FuzzTransactionLifecycle holds the one property a transaction handle has, over
// every order of operations: it ends EXACTLY ONCE, and nothing is enrolled in it
// afterwards.
//
// Both halves cost real money when they are wrong. A second ending reaches a
// coordinator that has already forgotten the transaction; work enrolled after
// the ending is work the commit cannot include, so a publication becomes visible
// on its own and an acknowledgement is applied to a message the commit was
// supposed to account for.
//
// The op script is one byte per operation and reads no helper: this is a nested
// module's test and taking the root module's internal fuzz-script helper would
// make a change there able to break it (the reasoning the specification recorded).
func FuzzTransactionLifecycle(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{1})
	f.Add([]byte{2, 3, 0, 2, 3})
	f.Add([]byte{0, 1, 0, 1})
	f.Add([]byte{2, 0, 2, 3, 1})
	f.Add([]byte{4, 0, 1, 2, 3})
	f.Fuzz(func(t *testing.T, script []byte) {
		cl := &fakeClient{}
		tx, err := pulsartx.NewWithClient(cl, time.Minute)
		if err != nil {
			t.Fatalf("NewWithClient: %v", err)
		}
		prod := &fakeProducer{}
		cons := &fakeConsumer{}
		ended := false
		for _, op := range script {
			switch op % 5 {
			case 0:
				check(t, &ended, tx.Commit(context.Background()))
			case 1:
				check(t, &ended, tx.Abort(context.Background()))
			case 2:
				_, perr := tx.Publish(context.Background(), prod, &pulsar.ProducerMessage{Payload: []byte{op}})
				checkWork(t, ended, perr, prod.sent != nil && prod.sent.Transaction == tx.Native())
			case 3:
				aerr := tx.Ack(cons, &fakeMessage{})
				checkWork(t, ended, aerr, cons.txn == tx.Native())
			case 4:
				derr := tx.Do(context.Background(), func(context.Context, pulsar.Transaction) error { return nil })
				if ended {
					// Do begins nothing — a Pulsar
					// transaction is open from the moment it
					// exists — so on a spent handle both its
					// commit and its abort are refused and
					// the error is one of them.
					if derr == nil {
						t.Fatal("Do on a transaction that had already ended reported success")
					}
					continue
				}
				if derr != nil {
					t.Fatalf("Do: %v", derr)
				}
				ended = true
			}
			if tx.Ended() != ended {
				t.Fatalf("the handle reports ended=%v and %v operations have ended it", tx.Ended(), ended)
			}
		}
		// THE INVARIANT: the coordinator saw at most one ending, and
		// exactly one if anything ended it.
		total := cl.txn.commits + cl.txn.aborts
		switch {
		case ended && total != 1:
			t.Fatalf("the coordinator saw %d ending(s) for one transaction that ended", total)
		case !ended && total != 0:
			t.Fatalf("the coordinator saw %d ending(s) for a transaction nothing ended", total)
		}
	})
}

// check holds an ending's outcome: the first one succeeds and every later one is
// refused with ErrAlreadySettled.
func check(t *testing.T, ended *bool, err error) {
	t.Helper()
	if *ended {
		if !errors.Is(err, mq.ErrAlreadySettled) {
			t.Fatalf("a second ending reported %v (code %v), want ErrAlreadySettled", err, mq.CodeOf(err))
		}
		return
	}
	if err != nil {
		t.Fatalf("the first ending reported %v", err)
	}
	*ended = true
}

// checkWork holds work offered to the transaction: it is enrolled while the
// transaction is open and refused once it has ended.
func checkWork(t *testing.T, ended bool, err error, enrolled bool) {
	t.Helper()
	if ended {
		if !errors.Is(err, mq.ErrAlreadySettled) {
			t.Fatalf("work offered to an ended transaction reported %v (code %v), want ErrAlreadySettled",
				err, mq.CodeOf(err))
		}
		return
	}
	if err != nil {
		t.Fatalf("work offered to an open transaction reported %v", err)
	}
	if !enrolled {
		t.Fatal("the work was accepted and not enrolled in the transaction; it would become visible or " +
			"be applied on its own, and the commit that was supposed to account for it would have " +
			"nothing to do")
	}
}
