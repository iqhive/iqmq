package kafka

import "testing"

// TestAFlushWaitsOnlyForWhatWasAdmittedBeforeIt: Flush completes the
// publications already admitted, so a record admitted while a Flush waits is
// not one it waits for. A Flush that waited for the sink to have nothing
// outstanding would never return under the sink's own steady traffic.
func TestAFlushWaitsOnlyForWhatWasAdmittedBeforeIt(t *testing.T) {
	var p pending
	early := p.admit()
	woken := false
	e, waiting := p.begin(func() { woken = true })
	if !waiting {
		t.Fatal("a Flush with a record outstanding is not waiting")
	}
	late := p.admit()
	p.done(early)
	if !woken {
		t.Error("the Flush was not woken when the record admitted before it was answered: it waits for one admitted after it")
	}
	if !p.end(e) {
		t.Error("the Flush reports a record admitted before it still outstanding")
	}
	woken = false
	e, waiting = p.begin(func() { woken = true })
	if !waiting {
		t.Fatal("a second Flush, with a record admitted before it outstanding, is not waiting")
	}
	p.done(late)
	if !woken || !p.end(e) {
		t.Error("the second Flush was not woken when its record was answered")
	}
	if _, waiting := p.begin(func() { t.Error("a Flush with nothing outstanding was woken") }); waiting {
		t.Error("a Flush with nothing outstanding is waiting")
	}
}

// TestAFlushWaitsForEveryEarlierRecord: a record admitted before an earlier
// Flush is admitted before a later one too, so the later Flush is not woken
// while it is outstanding.
func TestAFlushWaitsForEveryEarlierRecord(t *testing.T) {
	var p pending
	first := p.admit()
	var wokeFirst, wokeSecond bool
	e1, _ := p.begin(func() { wokeFirst = true })
	second := p.admit()
	e2, waiting := p.begin(func() { wokeSecond = true })
	if !waiting {
		t.Fatal("the second Flush is not waiting")
	}
	p.done(second)
	if wokeFirst || wokeSecond {
		t.Errorf("answering the record admitted between the Flushes woke the first %v and the second %v, want neither: the record before both is outstanding", wokeFirst, wokeSecond)
	}
	if p.end(e2) {
		t.Error("the second Flush reports nothing outstanding while the first record is")
	}
	e2, _ = p.begin(func() { wokeSecond = true })
	p.done(first)
	if !wokeFirst || !wokeSecond {
		t.Errorf("answering the first record woke the first Flush %v and the second %v, want both", wokeFirst, wokeSecond)
	}
	if !p.end(e1) || !p.end(e2) {
		t.Error("a Flush reports a record outstanding once every one was answered")
	}
}
