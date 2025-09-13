package timer

// bucket is a doubly linked list of entries anchored by head and tail.
type bucket[V any] struct {
	head *Entry[V]
	tail *Entry[V]
	n    int
}

// push prepends e (insertion order within a wheel slot is not significant).
func (b *bucket[V]) push(e *Entry[V]) {
	e.prev = nil
	e.next = b.head
	if b.head != nil {
		b.head.prev = e
	} else {
		b.tail = e
	}
	b.head = e
	b.n++
}

// insertSorted places e so the list stays in non-decreasing due order,
// after existing entries with the same due. Appending at the tail and
// prepending at the head are O(1); otherwise the walk starts from the tail.
func (b *bucket[V]) insertSorted(e *Entry[V]) {
	b.n++
	switch {
	case b.tail == nil || b.tail.due <= e.due:
		e.prev, e.next = b.tail, nil
		if b.tail != nil {
			b.tail.next = e
		} else {
			b.head = e
		}
		b.tail = e
		return
	case e.due < b.head.due:
		e.prev, e.next = nil, b.head
		b.head.prev = e
		b.head = e
		return
	}
	// head.due <= e.due < tail.due: find the last entry not after e.
	p := b.tail.prev
	for p.due > e.due {
		p = p.prev
	}
	e.prev, e.next = p, p.next
	p.next.prev = e
	p.next = e
}

// remove unlinks e, which must be in b.
func (b *bucket[V]) remove(e *Entry[V]) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		b.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		b.tail = e.prev
	}
	e.next, e.prev = nil, nil
	b.n--
}

// takeAll detaches and returns the list head, emptying the bucket.
func (b *bucket[V]) takeAll() *Entry[V] {
	h := b.head
	b.head, b.tail = nil, nil
	b.n = 0
	return h
}
