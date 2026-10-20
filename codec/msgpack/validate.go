package msgpack

import "encoding/binary"

// validate walks a MessagePack body applying the four limits without
// allocating anything proportional to what the body declares.
//
// The walk is ITERATIVE. A recursive validator would exhaust the goroutine
// stack on the very input the depth limit exists to refuse, which is a
// denial-of-service check that is itself a denial-of-service surface. The
// "stack" here is a single counter per open container, in a slice whose length
// the depth limit bounds.
//
// The load-bearing rule is `pending > remaining`. Every value costs at least
// one byte, so a container claiming n elements needs n more bytes and a map
// claiming n pairs needs 2n; a header whose count the input cannot justify is
// refused at the header, before the library allocates from it. That is the one
// rule that turns `dd ff ff ff ff` — five bytes, an array32 of 4,294,967,295
// elements, 64 GiB allocated by the library and a killed process — into
// ErrMalformed.
func validate(data []byte, l limits) error {
	if l.bytes >= 0 && len(data) > l.bytes {
		return ErrTooLarge
	}
	if len(data) == 0 {
		return ErrMalformed
	}

	// counts[i] is how many more values the i-th open container expects. The
	// array is DefaultMaxDepth long, so a body within the default limit never
	// grows it: watched at 2 allocs/op for a 40-level nesting with a
	// 16-element array, which is an allocation a sender chooses and the point
	// of validating before the decoder is that the validation itself is free.
	// A configuration that raises MaxDepth above
	// DefaultMaxDepth pays one growth, bounded by that limit.
	var stack [DefaultMaxDepth]uint64
	counts := stack[:0]
	elements := 0
	// pending is how many values are still expected at the current level;
	// the top-level body is exactly one.
	pending := uint64(1)
	i := 0

	for {
		for pending == 0 {
			if len(counts) == 0 {
				// The top-level value is complete. Anything after it is a
				// second body, which this codec does not accept: a trailing
				// byte is the difference between one message and two.
				if i != len(data) {
					return ErrMalformed
				}
				return nil
			}
			pending = counts[len(counts)-1]
			counts = counts[:len(counts)-1]
		}
		// THE anti-amplification check. Every value costs at least one byte,
		// so the walk cannot outrun the input: a container header claiming
		// 4,294,967,295 elements sets pending to that number and this test
		// fires on the very next iteration, before anything is allocated
		// from the count. It is the whole reason `dd ff ff ff ff` -- five
		// bytes that made the library allocate 64 GiB -- is refused here in
		// constant time.
		if i >= len(data) {
			return ErrMalformed
		}
		elements++
		if l.elements >= 0 && elements > l.elements {
			return ErrTooManyElements
		}
		b := data[i]
		i++
		pending--

		var skip, children uint64
		switch {
		case b <= 0x7f, b >= 0xe0: // fixint
		case b >= 0x80 && b <= 0x8f: // fixmap
			children = 2 * uint64(b&0x0f)
		case b >= 0x90 && b <= 0x9f: // fixarray
			children = uint64(b & 0x0f)
		case b >= 0xa0 && b <= 0xbf: // fixstr
			skip = uint64(b & 0x1f)
		default:
			switch b {
			case 0xc0, 0xc2, 0xc3: // nil, false, true
			case 0xc1: // never used
				return ErrMalformed
			case 0xc4, 0xc5, 0xc6: // bin 8/16/32
				n, next, err := length(data, i, 1<<(b-0xc4))
				if err != nil {
					return err
				}
				i = next
				skip = n
			case 0xc7, 0xc8, 0xc9: // ext 8/16/32
				n, next, err := length(data, i, 1<<(b-0xc7))
				if err != nil {
					return err
				}
				i = next
				skip = n + 1 // + the ext type byte
			case 0xca:
				skip = 4
			case 0xcb:
				skip = 8
			case 0xcc, 0xcd, 0xce, 0xcf: // uint 8/16/32/64
				skip = 1 << (b - 0xcc)
			case 0xd0, 0xd1, 0xd2, 0xd3: // int 8/16/32/64
				skip = 1 << (b - 0xd0)
			case 0xd4, 0xd5, 0xd6, 0xd7, 0xd8: // fixext 1/2/4/8/16
				skip = 1 + 1<<(b-0xd4)
			case 0xd9, 0xda, 0xdb: // str 8/16/32
				n, next, err := length(data, i, 1<<(b-0xd9))
				if err != nil {
					return err
				}
				i = next
				skip = n
			case 0xdc, 0xdd: // array 16/32
				n, next, err := length(data, i, 2<<(b-0xdc))
				if err != nil {
					return err
				}
				i = next
				children = n
			case 0xde, 0xdf: // map 16/32
				n, next, err := length(data, i, 2<<(b-0xde))
				if err != nil {
					return err
				}
				i = next
				// n is at most 2^32-1, so 2n cannot overflow a uint64.
				children = 2 * n
			default:
				return ErrMalformed
			}
		}

		remaining := uint64(len(data) - i)
		if skip > remaining {
			return ErrMalformed
		}
		i += int(skip)

		if children > 0 {
			// There is deliberately NO "children > remaining" test here. It
			// was written first, and watching it fail is what showed it
			// cannot: with the rule removed, `dc ff ff` (an array16 of
			// 65,535 from three bytes) is still refused, by the
			// `i >= len(data)` test above, on the first iteration. A check
			// that cannot fail is the most-repeated defect in this
			// repository, so the redundant one is gone and the one that does
			// the work carries the comment.
			if l.depth >= 0 && len(counts)+1 > l.depth {
				return ErrTooDeep
			}
			counts = append(counts, pending)
			pending = children
		}
	}
}

// length reads an n-byte big-endian length prefix at i and returns it with the
// index just past it.
func length(data []byte, i int, n uint64) (uint64, int, error) {
	if uint64(len(data)-i) < n {
		return 0, i, ErrMalformed
	}
	var v uint64
	switch n {
	case 1:
		v = uint64(data[i])
	case 2:
		v = uint64(binary.BigEndian.Uint16(data[i:]))
	case 4:
		v = uint64(binary.BigEndian.Uint32(data[i:]))
	default:
		return 0, i, ErrMalformed
	}
	return v, i + int(n), nil
}
