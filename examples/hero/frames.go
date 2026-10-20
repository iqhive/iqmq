package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/iqhive/banner"
)

// All styling, wordmark layout, opening animation, SVG and playback live in banner.
// This file owns project text, deterministic demonstrations and reading holds.
type (
	frame = banner.Frame
)

const (
	cols        = banner.Cols
	rows        = banner.Rows
	plain       = banner.Plain
	dim         = banner.Dim
	muted       = banner.Muted
	bright      = banner.Bright
	lit         = banner.Literal
	hot         = banner.Changed
	on          = banner.On
	off         = banner.Off
	bar         = banner.Bar
	prompt      = banner.Prompt
	caret       = banner.Caret
	full        = banner.Full
	shade       = banner.Shade
	projectName = "iqmq"
	tagline     = "One Go messaging API, with broker capabilities in view."
	subline     = "bytes-first messages · broker-specific capabilities · pre-alpha"
	install     = "go run ./examples/quickstart-memory"
)

var points = []string{"bytes-first messages", "broker-specific capabilities", "pre-alpha"}

type film struct {
	cur, card frame
	frames    []frame
}

func (m *film) cut(ms int) {
	f := m.cur
	f.Hold = time.Duration(ms) * time.Millisecond
	m.frames = append(m.frames, f)
}

// build executes the project fixtures. The seed affects decorative digits;
// iqhash also uses it for the real demonstrated digests.
func build(seed uint64) ([]frame, error) {
	m := &film{}
	opening, card, err := banner.Opening(banner.Card{
		Word: projectName, Tagline: tagline, Points: points, Install: install,
		Theme:    banner.IQCapsTheme(),
		Wordmark: banner.WordmarkOptions{Seed: seed}, ReadHold: 2500 * time.Millisecond,
		TaglineReveal: banner.TaglineWithReveal, PointInterval: 200 * time.Millisecond,
	})
	if err != nil {
		return nil, fmt.Errorf("hero opening: %w", err)
	}
	m.frames, m.card = opening, card
	if err := m.queue(); err != nil {
		return nil, err
	}
	if err := m.redelivery(); err != nil {
		return nil, err
	}
	m.cur = m.card
	m.cut(2500)
	m.cur = frame{HiveLogo: true}
	m.cur.Center(14, "IQ Hive", bright)
	m.cur.Center(16, "iqhive.com", muted)
	m.cut(1000)
	// Keep all scenes and quick motion; distribute extra reading time among
	// settled demonstration results and the final project card.
	extra := 25*time.Second - banner.Duration(m.frames)
	if extra < 0 {
		return nil, errors.New("hero: scene timing exceeds 25 seconds")
	}
	var holds []int
	for i := len(opening); i < len(m.frames)-1; i++ {
		if m.frames[i].Hold >= time.Second {
			holds = append(holds, i)
		}
	}
	for n, i := range holds {
		share := extra / time.Duration(len(holds)-n)
		m.frames[i].Hold += share
		extra -= share
	}
	return m.frames, nil
}

// animation keeps generator failures visible instead of writing guessed output.
func animation(seed uint64) (a banner.Animation, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("hero fixture: %v", p)
		}
	}()
	frames, err := build(seed)
	if err != nil {
		return a, err
	}
	a = banner.Animation{
		Frames: frames, StaticFrame: len(frames) - 2, Theme: banner.IQCapsTheme(),
		Title:       projectName + " — README demonstration",
		Description: "Publish and receive through the real memory driver, then Nack, redelivery and Ack. Pre-alpha.",
		Command:     "make hero",
	}
	return a, a.Validate()
}
