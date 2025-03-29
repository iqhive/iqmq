package payload

import "unsafe"

// Alias returns a shallow copy of p: a distinct Payload value whose Key,
// Body, header table and header values share p's storage. It is the
// borrowed and zero-copy form of a message (the memory engine): cheap,
// allocation-free apart from the Payload itself, and only valid while p's
// storage is. Use Clone for an independent copy.
func (p *Payload) Alias() *Payload {
	c := *p
	return &c
}

// AliasInto makes dst a shallow copy of p sharing all of p's storage.
// AliasInto(p) is a no-op.
func (p *Payload) AliasInto(dst *Payload) { *dst = *p }

// Overlaps reports whether the storage reachable through a and b intersects:
// whether writing through one (including appends within capacity) can be
// observed through the other. Slices with zero capacity never overlap.
func Overlaps(a, b []byte) bool { return overlaps(a, b) }

// overlaps is Overlaps for any element type. It compares addresses only and
// never dereferences.
func overlaps[T any](a, b []T) bool {
	if cap(a) == 0 || cap(b) == 0 {
		return false
	}
	var zero T
	size := unsafe.Sizeof(zero)
	if size == 0 {
		return false
	}
	pa := uintptr(unsafe.Pointer(unsafe.SliceData(a))) //nolint:gosec // G103: address arithmetic for overlap detection; nothing is dereferenced
	pb := uintptr(unsafe.Pointer(unsafe.SliceData(b))) //nolint:gosec // G103: address arithmetic for overlap detection; nothing is dereferenced
	return pa < pb+uintptr(cap(b))*size && pb < pa+uintptr(cap(a))*size
}

// Aliases reports whether a and b share any mutable storage: the header
// tables, or any pair of byte slices among Key, Body and the header values.
// A payload whose slices all have zero capacity aliases nothing, not even
// itself. Alias always yields an aliasing pair when p has storage; Clone
// and CloneInto never do.
func Aliases(a, b *Payload) bool {
	if a == nil || b == nil {
		return false
	}
	if overlaps(a.Headers, b.Headers) {
		return true
	}
	if overlaps(a.Key, b.Key) || overlaps(a.Key, b.Body) || overlaps(a.Body, b.Key) || overlaps(a.Body, b.Body) {
		return true
	}
	for i := range a.Headers {
		v := a.Headers[i].Value
		if overlaps(v, b.Key) || overlaps(v, b.Body) {
			return true
		}
		for j := range b.Headers {
			if overlaps(v, b.Headers[j].Value) {
				return true
			}
		}
	}
	for j := range b.Headers {
		v := b.Headers[j].Value
		if overlaps(v, a.Key) || overlaps(v, a.Body) {
			return true
		}
	}
	return false
}

// HasStorage reports whether any of p's slices has capacity, that is,
// whether an Alias of p would alias it.
func (p *Payload) HasStorage() bool {
	return cap(p.Key) > 0 || cap(p.Body) > 0 || cap(p.Headers) > 0
}

// Zero overwrites every byte of p's Key, Body and header values, over their
// full capacity, with zeros and then resets p. It backs ZeroOnRelease for
// sensitive payloads. Strings (ID, properties, header keys) are immutable
// and are dropped rather than overwritten.
//
// Zero writes into spare capacity, so it must only be called on storage p
// owns: never on a sub-slice of a caller's buffer or on an Alias.
func (p *Payload) Zero() {
	clear(p.Key[:cap(p.Key)])
	clear(p.Body[:cap(p.Body)])
	hs := p.Headers[:cap(p.Headers)]
	for i := range hs {
		clear(hs[i].Value[:cap(hs[i].Value)])
		hs[i].Key = ""
		hs[i].Value = hs[i].Value[:0]
	}
	p.Key = p.Key[:0]
	p.Body = p.Body[:0]
	p.Headers = p.Headers[:0]
	p.ID = ""
	p.Props = Props{}
}
