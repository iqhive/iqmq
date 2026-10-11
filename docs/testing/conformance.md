# Running the conformance suite

No generated conformance reports or capability matrix are tracked in this
checkout. The suite definitions and test code describe checks that can be run;
they do not establish that any particular broker configuration passed.

Generate reports from the source revision you intend to assess.
A report without an available source revision cannot establish
conformance of the current checkout.

[testkit](testkit.md) is the kit. This page is the machinery around it: how CI
drives it, which reports it writes, what those reports are evidence for, and
how to select one of the seven run modes.

## The manifest

`conformance/suites.json` is the list CI drives from — 22 suites today. Each
entry names the test, the run modes it declares, its broker images and its
reports.

## The seven run modes

`make suites-modes` prints this, and it is the authority rather than this page:

| Mode | State | What it is |
|---|---|---|
| `integration` | implemented | the whole kit against the suite's containerised broker. Writes the reports the capability matrix is generated from. |
| `inprocess` | implemented | the kit against a driver that needs no broker, in the driver's own package. `make conformance-memory` is this, and `make ci` runs it. |
| `critical` | implemented | the driver-specific critical tests, against an in-process fake. Needs no container, so `make ci` runs every driver's set. |
| `matrix` | implemented | the broker-version matrix from the pinned images. Nightly. |
| `chaos` | implemented | fault-injection scenarios against the selected broker |
| `soak` | implemented | long runs with heap, goroutine and settlement observations |
| `differential` | implemented | compare an operation script against memory and a broker |

## Running one

```sh
make conformance-memory                      # the memory driver, in-process, no container
make critical DRIVER=nats                    # one driver's critical tests
make integration DRIVER=nats                 # the full kit against a container
make integration                             # every suite (starts brokers, one at a time)
make matrix DRIVER=kafka                     # the broker-version matrix
make suites-modes                            # what each mode is and what it is missing
```

`make integration` starts containers. On a shared machine start **one broker at
a time**, from the digests `conformance/harness/images.lock` pins, and remove
only the containers you started, by id.

## The reports

Reports record the driver and profile, broker and client versions, Go version,
run date, source revision and scenario outcomes. The testkit writes these fields
when a suite runs. Generate fresh reports with `make conformance-memory-report`
or `make integration DRIVER=<driver>`.

For each scenario, these fields carry most of the meaning:

* **`positive`** — whether this scenario checks that the feature *works* or
  that the unsupported path is *refused*. Both halves of a claim have a row,
  which is what "proved in both directions" means concretely.
* **`outcome`** — `pass`, `fail` or `skip`.
* **`note`** — why it was skipped. A skip with no reason would be
  indistinguishable from a claim nobody checked, so the reason is required and
  is what the matrix renders when it explains a missing verification.
* **`feature`** and **`claimed`** — the capability a scenario is about, and the
  level the driver advertises for it, read from the capability table of the
  client the scenario opened. A scenario that returned before it had a client
  — a documented skip, a topology the driver refused, an `Open` that failed —
  says `"claimed": "unread"`: no level was read, and neither an empty field nor
  a level borrowed from another scenario's client may stand in for one, because
  a driver's level can depend on the topology it was opened for. `confgen`
  refuses a result that names a feature and carries no level at all, and
  renders a claim that no scenario could read as `∅`.
* **`kind`** — absent for a *behaviour*, `guarantee` for a claim one run cannot
  settle. It is what tells a reader whether the evidence is two-sided: a pass
  on a guarantee's positive direction means "this run saw no counter-example",
  and a denied guarantee nothing contradicted renders as `unrefuted` rather
  than `unverified`. A behaviour probe that found the driver *accepted* the
  feature's call and did something else carries it too — for that driver there
  was no call to refuse. [The kit's page](testkit.md) has the whole table.

`evidence` is the one to read first: `inprocess`, `container`, `emulator` or
`managed`. An `emulator` report's cells are marked `◐` in the matrix and are
never presented as the managed service's answer. A report must describe the environment its suite actually used.

## From reports to the matrix

When reports are present, `make generate-check` fails when generated
artifacts are stale and validates the
reports before it renders them: an image `images.lock` does not pin, a commit
that is not a commit, a report no suite publishes, a suite whose report is
missing, a report far behind the newest in the corpus, or one recording a
FAILED scenario is a failure there rather than a capability claim nobody can
support. It also fails on a generated page no report produces — which is the
gap `-check` had, where deleting a driver's report left its capability page
stating capabilities from evidence that no longer existed.

## Using the kit on your own driver

That is the point of it being a published module rather than an internal test
helper. [The driver author guide](../drivers/author-guide.md) has the whole
path; the short version is a `conformance_test.go` in `package <driver>_test`
that calls `testkit.RunConformance` with your `Harness`.

Model it on `drivers/nats/conformance_test.go`. Do **not** model it on
`memory/conformance_test.go`, which reaches unexported engine helpers —
legitimate for that driver's own package and useless as an example, and
recorded as such.
