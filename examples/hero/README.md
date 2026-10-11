# iqmq README hero

This example uses [iqhive/banner v1.0.4](https://github.com/iqhive/banner) for
wordmark glyphs and spacing, the entire seeded intro sweep, themes, cell/frame
model, SVG rendering and terminal playback. The root module
and its consumer dependencies are unchanged; this tooling has its own module.

## Run and regenerate

From the repository root:

```sh
make hero-svg
make hero
make hero-check
# Shared CLI flags, including themes and looping:
env GOWORK=off go -C examples/hero run . -loop -theme bluegreen
```

`docs/hero.svg` is generated. Edit the source, then regenerate it. Redirected
playback prints the complete project card without escapes. Reduced motion and
unsupported animation show that same useful card. The window is 836 × 498
with an 80 × 20 terminal grid; captions must fit 80 columns.

## Content and evidence

Header: **One Go messaging API, with broker capabilities in view.**

Points: **bytes-first messages · broker-specific capabilities · pre-alpha**. They reveal individually, 200ms apart.

Publish and receive through the real memory driver, then Nack, redelivery and Ack. Pre-alpha.

The name and header reveal together using `banner.TaglineWithReveal` (default).
`banner.TaglineBefore` shows the header from the first frame;
`banner.TaglineAfter` waits for the wordmark to settle. These are shared options,
not per-project animation implementations.

## Maintenance map

| Change | Edit |
| --- | --- |
| Header, points, installation/build command | Constants and `points` in `frames.go` |
| Demonstrated behavior, output or diagrams | Scene functions in `frames.go` and any local fixture files |
| Reading time | `build`: quick motion retains its holds; extra time is shared across settled results |
| Expected project results and artifact drift | `hero_test.go` |
| Intro, fonts, layout, themes, SVG CSS or playback | The shared banner repository |

The loop is exactly 25 seconds with the existing demonstration scenes,
followed by the project card and a one-second IQ Hive vector logo. Counts and
outputs come from the project code; fixture data is deterministic and never
presented as a live measurement. This project selects `banner.IQCapsTheme()` on both `Card.Theme` and
`Animation.Theme`. The uppercase wordmark has a straight, serif-free `I`:
`IQ` uses the IQ Hive logo's yellow/orange/red gradient; the remaining letters
and every demo screen use the bluegreen palette. All typography, prefix colors
and reveal steps belong to banner. Commands and captions retain their case.
`-theme default`, `-theme bluegreen` and `-theme light` override both SVG and
terminal output; the shared package rebuilds the opening glyphs automatically.
New diagrams use shared semantic styles rather than hard-coded colors.

The generator deduplicates segments and visibility timelines. It emits only
self-contained SVG vectors and discrete CSS keyframes, without JavaScript,
external fonts or raster frames. Rendering and playback tests live in banner;
project fixture assertions stay here. For new typing scenes, the shared
`banner.TypeLine` helper provides input steps and a cursor; keep project-specific
responses (such as the digest of each typed prefix) in the local scene.

## Change the shared package locally

Temporarily add a replacement inside this example module:

```sh
env GOWORK=off go -C examples/hero mod edit -replace github.com/iqhive/banner=../../../banner
make hero-svg
make hero-check
# Verify the shared package before publishing a version.
make -C ../banner verify
# Remove the development override, select the published version, tidy, regenerate.
env GOWORK=off go -C examples/hero mod edit -dropreplace github.com/iqhive/banner
env GOWORK=off go -C examples/hero mod tidy
make hero-svg
make hero-check
```

Do not commit a sibling banner override. The project replacement `../..` is
intentional: demonstrations always run against this checkout's source. After
visual changes, inspect direct SVG and a responsive `<img>` at full and narrow
widths, both demonstrations, the final cards, loop transition and reduced motion.
