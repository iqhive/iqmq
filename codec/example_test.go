package codec_test

import (
	"errors"
	"fmt"

	"github.com/iqhive/iqmq/codec"
)

type order struct {
	ID  string `json:"id"`
	Qty int    `json:"qty"`
}

// The generic shape binds a codec to one message type, so the compiler
// rejects the wrong one.
func ExampleJSON() {
	c := codec.JSON[order]()
	body, err := c.Encode(order{ID: "o-1", Qty: 3})
	if err != nil {
		panic(err)
	}
	back, err := c.Decode(body)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s %s %+v\n", c.ContentType(), body, back)
	// Output: application/json {"id":"o-1","qty":3} {ID:o-1 Qty:3}
}

// The non-generic shape is what a registry hands out: one codec value that
// serves every message type.
func ExampleRegistry_For() {
	r := codec.NewRegistry()
	json, err := codec.New(codec.Auto)
	if err != nil {
		panic(err)
	}
	if err := r.Register(json); err != nil {
		panic(err)
	}
	if err := r.Register(codec.Raw()); err != nil {
		panic(err)
	}
	r.SetFallback(codec.Raw())

	for _, contentType := range []string{
		"application/json; charset=utf-8", // parameters are ignored
		"application/vnd.acme.order+json", // a suffix falls back to its base
		"",                                // no content type: the fallback
		"text/csv",                        // nothing registered
	} {
		c, err := r.For(contentType)
		if err != nil {
			var cte *codec.ContentTypeError
			fmt.Println(errors.As(err, &cte), err)
			continue
		}
		fmt.Println(c.ContentType())
	}
	// Output:
	// application/json
	// application/json
	// application/octet-stream
	// true codec: content type "text/csv" has no codec
}

// A body that does not carry the content type a codec speaks is a
// *ContentTypeError, which errors.Is matches as an unsupported feature.
func ExampleMatch() {
	fmt.Println(codec.Match("application/json", "application/json; charset=utf-8"))
	fmt.Println(codec.Match("application/json", ""))
	fmt.Println(codec.Match("application/json", "application/octet-stream"))
	// Output:
	// <nil>
	// <nil>
	// codec: content type "application/octet-stream" is not "application/json"
}

// Raw moves bytes without touching them, for callers that already hold an
// encoded body.
func ExampleRaw() {
	c := codec.Raw()
	body, err := c.Marshal([]byte("already encoded"))
	if err != nil {
		panic(err)
	}
	var back []byte
	if err := c.Unmarshal(body, &back); err != nil {
		panic(err)
	}
	fmt.Printf("%s %s\n", c.ContentType(), back)
	// Output: application/octet-stream already encoded
}
