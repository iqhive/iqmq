SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

GO      ?= go
TOOL    := ./hack/tool.sh
EACH    := ./hack/for-each-module.sh
EACHGO  := ./hack/for-each-module.sh --go

# ---------------------------------------------------------------------------
# The shared CPU policy. On the shared hosts (iqdev, buildbox) every heavy
# entry point below runs under `cpu-policy run`, which waits until the machine
# has room, takes the machine-wide heavy-job lock -- shared with every other
# repository and session of this user, so it serialises against work this tree
# cannot see -- and caps GOMAXPROCS at its budget. On any other host it is a
# pure passthrough, and where it is not installed at all (the GitHub runners)
# these variables expand to nothing and every target runs exactly as before.
#
# The tool owns the GATE, the LOCK and the PINNING. This file owns only the
# WIDTH, because `cpu-policy` bounds one process and cannot know that `go test`
# starts `-p` of them -- see the ceiling arithmetic below, which is why the two
# are not the same job. Do not reimplement the gate, pin CPUs here, or add a
# way to force past a refusal: exit 75 means the machine stayed busy, which is
# an answer. See ~/.local/share/cpu-policy/README.md.
#
# Each wrapped target re-enters make for its `-inner` twin, and the tool
# detects a job already holding the lock by ancestry, so a wrapped target that
# depends on another inherits the lock instead of waiting on it. Wrap at the
# OUTERMOST level only; `-inner` targets must stay unwrapped.
#
# Deliberately NOT named CPU_POLICY: that is the tool's own on/off setting, and
# a make `:=` assignment overrides the environment, so `CPU_POLICY=off make
# test` would hand the tool `CPU_POLICY=cpu-policy run --` and it would gate
# anyway. Measured: that spelling fails with 75 where this one passes through.
CPU_RUN := $(shell command -v cpu-policy >/dev/null 2>&1 && echo cpu-policy run)
# cpu-policy 4 asks a job to SAY WHAT IT IS. The kind decides the class (a unit or
# integration job is light and takes the budget's CPUs; fuzz, bench and `other` are
# heavy and take CPU_PIN_SPREAD times it), and the class decides what else may run
# beside it in the host's capacity. Left unsaid the tool guesses from the command,
# which is a guess this tree does not need to make it. Each of these is empty where
# the tool is absent, so every target still runs unwrapped on a runner.
CPU_UNIT   = $(if $(CPU_RUN),$(CPU_RUN) --kind unit --)
CPU_INTEG  = $(if $(CPU_RUN),$(CPU_RUN) --kind integration --)
CPU_FUZZ   = $(if $(CPU_RUN),$(CPU_RUN) --kind fuzz --)
# `other` for a target that is ALL of them: `make ci` runs unit tests, fuzz seeds,
# TLC and a coverage sweep, so no single kind describes it and the catch-all is the
# honest answer rather than the one that would get it the smallest slot.
CPU_OTHER  = $(if $(CPU_RUN),$(CPU_RUN) --kind other --)
# Benchmarks additionally pin to quiet whole cores and append a record of the
# admission decision beside the results they belong to (hence the directory
# argument, not one shared file).
#
# `--bench` needs sixteen WHOLE cores, both SMT siblings of each, outside the
# reserved cache group. By default it honours the caller's own CPU affinity, so
# from a shell that is already confined -- an agent session here is pinned to
# sixteen loose threads of ninety-six -- there are no whole cores inside the mask
# and it exits 3 saying exactly that. Measure from an unpinned shell, or export
# CPU_POLICY_AFFINITY=ignore deliberately and say so beside the numbers. That is
# NOT a force flag: it widens the pool the tool may choose from and still gates,
# still locks, still pins. There is no force flag, and a 75 is an answer.
CPU_BENCH = $(if $(CPU_RUN),$(CPU_RUN) --kind bench --record $(1)/cpu-policy.jsonl --)
# --no-print-directory because the re-entry must not change what the output LOOKS
# like. A `make ci` log is read by people and grepped for failure lines, and
# because `linearize-selftest` is ci-inner's last prerequisite a complete log ends
# on `linearize-campaign: self-test ok` -- which is how a reader tells a finished
# run from a killed one. Twenty "Entering/Leaving directory" pairs, and a
# "Leaving" as the last line, would take that away for nothing.
#
# To be exact about what is NOT true: no script in this tree greps for those
# directory lines or asserts that final line, so nothing here FAILS if they
# appear. The reason is legibility, not a check. (An earlier version of this
# comment claimed a log verification that does not exist -- that claim came from
# one agent's own habit of reading logs, stated as if it were a mechanism.)
SUBMAKE := $(MAKE) --no-print-directory

# ---------------------------------------------------------------------------
# The host CPU ceiling. No test run and no benchmark run may take more than
# IQMQ_MAX_CPUS of this machine.
#
# `go test` picks GOMAXPROCS from the CPU count and runs up to `-p` package
# binaries at once, and the two MULTIPLY. `-p 4` against 64-thread binaries is
# 256 runnable threads, which is how a `make ci` here drove the load average to
# 127 on a host shared with about fifty other containers.
#
#     AVAIL      = nproc                              # cpuset AND CPU affinity
#     GOMAXPROCS = min(IQMQ_TEST_WIDTH, IQMQ_MAX_CPUS, AVAIL)
#     -p         = max(1, min(IQMQ_MAX_CPUS / GOMAXPROCS, AVAIL))
#
# so  -p x GOMAXPROCS <= (IQMQ_MAX_CPUS / GOMAXPROCS) x GOMAXPROCS <= the
# ceiling, and each factor is separately at most AVAIL -- which is what `go`
# would have chosen unaided, so the cap can only LOWER parallelism, never raise
# it (a 2-core GitHub runner computes the 2 and 2 it already used).
#
# Both are EXPORTED, so every recipe below inherits them and so does every
# `go test`, `go run`, `go build` and `go vet` inside every hack/*.sh that a
# recipe calls -- which is what makes this one place rather than a convention.
# The two module runners and every hack script that invokes `go` itself apply
# the same arithmetic, so they are capped when run directly too -- `make
# cpu-cap-check` fails on a hack script that runs `go` without it. hack/cpucap.sh is the single
# implementation and carries the argument for the split; `make cpu-cap-check`
# is what fails when a path escapes it.
# Under `cpu-policy` the budget is the tool's to set, and it exports it as
# CPU_POLICY_BUDGET into the job -- so an `-inner` target re-parsing this file
# inherits it and there is one ceiling on the host, not a second copy here that
# could drift from it. 16 is the fallback for every other host, unchanged.
IQMQ_MAX_CPUS   ?= $(or $(CPU_POLICY_BUDGET),16)
IQMQ_TEST_WIDTH ?= 4
export IQMQ_MAX_CPUS IQMQ_TEST_WIDTH
IQMQ_CPUCAP := $(shell ./hack/cpucap.sh pair)
GOMAXPROCS  := $(word 1,$(IQMQ_CPUCAP))
GOTEST_P    := $(word 2,$(IQMQ_CPUCAP))
GOFLAGS     := $(strip -p=$(GOTEST_P) $(filter-out -p -p=%,$(GOFLAGS)))
export GOMAXPROCS GOFLAGS
# The fuzz paths have THREE factors, not two, and the first version of this
# ceiling bounded only the middle one.
#
#   jobs     the SHELL fan-out: hack/fuzz.sh `all` runs `xargs -P $jobs`, and
#            `nightly` starts $shards background children
#   workers  `-parallel` on a `-fuzz` run, which starts worker PROCESSES
#   GOMAXPROCS  threads inside each of those processes
#
# so `make fuzz` was 8 jobs x 4 workers x 4 = 128 against a ceiling of 16, and
# `make fuzz-nightly` the same. All three now come from hack/cpucap.sh, which
# returns a (jobs, workers) pair whose product with GOMAXPROCS is within the
# ceiling. SHARDS is the nightly's fan-out as well as its sharding granularity:
# fewer, larger shards cover the same targets in the same total work.
# hack/cpucap.sh fuzz answers "<shards> <workers> <gomaxprocs>" -- a DIFFERENT
# split from `pair`'s "4 4", and deliberately so. A fuzz
# worker runs one input at a time, so width buys it little, while the nightly's
# wall clock is ceil(targets / SHARDS) x FUZZTIME_NIGHTLY -- shards is the only
# factor that shortens the night. 8 x 1 x 2 = 16 is the same ceiling as
# 2 x 2 x 4 and runs in four hours instead of sixteen.
IQMQ_FUZZCAP    := $(shell ./hack/cpucap.sh fuzz)
JOBS            ?= $(word 1,$(IQMQ_FUZZCAP))
SHARDS          ?= $(word 1,$(IQMQ_FUZZCAP))
FUZZ_WORKERS    ?= $(word 2,$(IQMQ_FUZZCAP))
FUZZ_GOMAXPROCS ?= $(word 3,$(IQMQ_FUZZCAP))
export FUZZ_WORKERS
PKG     ?= ./...
PATTERN ?= .
TIME    ?= 20s
FUZZTIME ?= 30s
FUZZTIME_NIGHTLY ?= 10m
SHARD   ?= all
COUNT   ?= 3
BENCHTIME ?= 200ms
# The load ceiling a smoke benchmark is judged against. EMPTY by default, so the
# guard derives one figure for the host (`hack/benchguard.sh headroom`, which
# asks the policy) instead of this file carrying a 32 that was true when 64 CPUs
# were allowed and false once the mask was narrowed to 16.
# Set it to override deliberately.
LOADMAX ?=
BASE    ?= main
GATE    ?= pr
SELFTEST_GATE ?= selftest
# The gate set whose `kind: ratio` budgets `make overhead-gate` judges. It is
# not an interleaved base-against-candidate set: a ratio is two rows of ONE
# run, both arms need a live broker, and `make bench-overhead` is what measures
# them.
OVERHEAD_GATE ?= overhead
SPEC    ?=
# Where the nightly ambiguous-Retryable sweep writes the verdicts it compares
# against hack/pinsweep-baseline.tsv.
PINSWEEP_OUT ?= reports/pinsweep/verdicts.tsv
STRICT  ?= 0
# `./.claude/*` is excluded because agent worktrees are created there and each one
# is a FULL checkout: twelve of them turned this list from 1,721 files into 22,373
# and `make fmt-check` died with "13: unbound variable" from an argument list that
# long. `.gitignore` already hides the directory from git, which is exactly why
# nothing noticed -- `find` does not read .gitignore.
GOFILES  = $(shell find . -name '*.go' -not -path './.git/*' -not -path './.claude/*' -not -path '*/testdata/*' -not -path '*/node_modules/*')

# The `-inner` twins of every target wrapped in $(CPU_RUN): the wrapper re-enters
# make for these, and they must never be wrapped themselves.
.PHONY: vet-inner lint-inner build-inner test-inner test-race-inner test-tags-inner allocs-inner \
        fuzz-smoke-inner fuzz-inner fuzz-long-inner fuzz-nightly-inner cover-inner cover-integration-inner \
        integration-inner integration-all-inner chaos-inner matrix-inner soak-inner linearize-campaign-inner \
        cover-threshold-inner cover-update-inner ci-inner bench-quick-inner bench-native-inner bench-api-inner bench-overhead-inner bench-compare-inner \
        bakeoff-inner bakeoff-campaign-inner gate-inner gate-if-touched-inner

.PHONY: help modules tools fmt fmt-check vet lint stdlib-check build test test-race test-tags allocs \
        fuzz-seeds fuzz-smoke fuzz fuzz-long fuzz-corpus fuzz-manifest fuzz-nightly ratchet ratchet-update adr-check modelcheck tla tla-mutate fuzz-manifest-complete fuzz-targets adr-identifiers \
        workflows-lint ci-parity cover cover-threshold cover-integration cover-update bench-quick bench-native native-baselines bench-overhead overhead-report overhead-gate bench-compare compare-report bench-lint benchguard-test benchgate-test bakeoff-test bakeoff bakeoff-campaign bakeoff-dryrun bench-api bakeoff-adr gate gate-selftest gate-selftest-inner gate-if-touched budgets-lint sysinfo ci work-sync tidy clean cpu-cap cpu-cap-check pinsweep-test pinsweep-nightly pinsweep-baseline \
        scorecard scorecard-check \
        vuln license apidiff apidiff-baseline exported-doc generate generate-check aggregate conformance-memory security-check security-check-selftest conformance-memory-report examples docs-check docs-build docs-serve \
        images-lock image-rabbitmq-delayed harness integration integration-all critical chaos matrix soak differential linearize-campaign linearize-selftest broker-up broker-down

help: ## List targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n",$$1,$$2}'

modules: ## List Go modules
	@./hack/modules.sh

tools: ## Download developer tools declared in hack/tools/go.mod
	@cd hack/tools && GOWORK=off $(GO) mod download

fmt: ## Format all Go files (gofumpt + goimports)
	@$(TOOL) gofumpt -extra -w $(GOFILES)
	@$(TOOL) goimports -local github.com/iqhive/iqmq -w $(GOFILES)

fmt-check: ## Fail if any file is not formatted
	@out="$$($(TOOL) gofumpt -extra -l $(GOFILES))"; if [ -n "$$out" ]; then echo "unformatted:"; echo "$$out"; exit 1; fi
	@out="$$($(TOOL) goimports -local github.com/iqhive/iqmq -l $(GOFILES))"; if [ -n "$$out" ]; then echo "imports:"; echo "$$out"; exit 1; fi

vet: ## go vet every module
	@$(CPU_UNIT) $(SUBMAKE) vet-inner

vet-inner:
	@$(EACHGO) $(GO) vet ./...

lint: ## golangci-lint every module
	@$(CPU_UNIT) $(SUBMAKE) lint-inner

lint-inner:
	@$(EACHGO) $(CURDIR)/hack/lint.sh

stdlib-check: ## The root module must depend on the standard library only
	@./hack/check-stdlib-only.sh

build: ## go build every module (GOWORK=off); output is discarded, never written into the tree
	@$(CPU_UNIT) $(SUBMAKE) build-inner

build-inner:
	@$(EACHGO) $(GO) build -o /dev/null ./...

test: ## go test every module
	@$(CPU_UNIT) $(SUBMAKE) test-inner

test-inner:
	@$(EACHGO) $(GO) test -count=1 -shuffle=on ./...

test-race: ## go test -race every module
	@$(CPU_UNIT) $(SUBMAKE) test-race-inner

test-race-inner:
	@$(EACHGO) $(GO) test -race -count=1 -shuffle=on ./...

test-tags: ## Race-test the unsafe, debug and lane-binding build tags
	@$(CPU_UNIT) $(SUBMAKE) test-tags-inner

test-tags-inner:
	@pkgs="$$(for d in internal x memory; do if [ -d $$d ]; then echo ./$$d/...; fi; done)"; \
	GOWORK=off $(GO) test -race -count=1 -tags iqmq_unsafe,iqmq_debug $$pkgs
	@# the specification binds the engine's lane to a concrete queue type at compile
	@# time; each binding is held to the same model, fuzz and differential
	@# suites as the interface it replaces. Only the two packages the tags
	@# reach are re-run, so the cost is one engine suite per binding.
	@GOWORK=off $(GO) test -race -count=1 -tags iqmq_queue_mpmc ./internal/engine/... ./internal/queue/...
	@GOWORK=off $(GO) test -race -count=1 -tags iqmq_queue_native ./internal/engine/... ./internal/queue/...

allocs: ## Run the allocation-count tests in EVERY module
	@$(CPU_UNIT) $(SUBMAKE) allocs-inner

allocs-inner:
	@# Every module, not just the root: it used to be `go test ./...` from
	@# here, which with GOWORK=off reaches the root module's packages only, so
	@# the eight nested modules that carry a TestAllocs -- five drivers, two
	@# integrations, and codec/jsonv2 -- had theirs run by `make test-race`
	@# and nowhere else. Under -race every one of them SKIPS, because the
	@# detector perturbs allocation counts, so eight modules' allocation
	@# budgets were asserted by a test that never executed (assumption
	@#).
	@$(EACHGO) $(CURDIR)/hack/allocs.sh

fuzz-seeds: ## Run every fuzz target's seed corpus as tests
	@./hack/fuzz.sh seeds

fuzz-smoke: ## Fuzz every target of PKG for TIME each (PKG=./internal/queue TIME=20s)
	@$(CPU_FUZZ) $(SUBMAKE) fuzz-smoke-inner

fuzz-smoke-inner:
	@GOMAXPROCS=$(FUZZ_GOMAXPROCS) ./hack/fuzz.sh smoke $(PKG) $(TIME)

fuzz: ## Fuzz every target of every module for FUZZTIME each, JOBS in parallel
	@$(CPU_FUZZ) $(SUBMAKE) fuzz-inner

fuzz-inner:
	@GOMAXPROCS=$(FUZZ_GOMAXPROCS) ./hack/fuzz.sh all $(FUZZTIME) $(JOBS)

fuzz-long: ## Fuzz one TARGET in PKG for TIME (make fuzz-long PKG=... TARGET=FuzzX TIME=10m)
	@$(CPU_FUZZ) $(SUBMAKE) fuzz-long-inner

fuzz-long-inner:
	@# -parallel is the fuzz WORKER count here, so it multiplies with GOMAXPROCS
	@# the way -p does; it was a hardcoded 8.
	@GOWORK=off GOMAXPROCS=$(FUZZ_GOMAXPROCS) $(GO) test -run='^$$' -fuzz='^$(TARGET)$$' -fuzztime=$(TIME) -parallel $(FUZZ_WORKERS) $(PKG)

fuzz-corpus: ## Export interesting fuzz inputs into testdata/fuzz (max 200 per target, deletes nothing)
	@$(GO) run ./tools/fuzzcorpus export

fuzz-manifest: ## Write reports/fuzz-manifest.json
	@$(GO) run ./tools/fuzzmanifest -results benchmarks/reports/fuzz -o reports/fuzz-manifest.json

# The same manifest with -require-runs, which is what the specification asks the nightly
# for: a shard that died leaves a target with no run result, and that is a
# failure rather than a manifest with a hole in it. Only meaningful over the
# WHOLE target set, so it belongs to the job that has collected every shard's
# results and to nothing else.
fuzz-manifest-complete: ## Write reports/fuzz-manifest.json and fail when a target has no run result
	@$(GO) run ./tools/fuzzmanifest -results benchmarks/reports/fuzz -require-runs -o reports/fuzz-manifest.json

fuzz-targets: ## Print every fuzz target the sharded campaign would run (one "DIR PKG TARGET" per line)
	@./hack/fuzz.sh targets $(SHARDS) $(SHARD)

# the test suite's nightly campaign: FUZZTIME_NIGHTLY per target, sharded,
# then the corpus export and the fuzz manifest. Not installed anywhere -- the
# crontab line a nightly host would use is in the contributor notes's "Nightly and
# pre-landing jobs" section, deliberately as documentation rather than as
# something this repository installs.
fuzz-nightly: ## nightly: FUZZTIME_NIGHTLY per target over SHARDS shards, then corpus + manifest
	@$(CPU_FUZZ) $(SUBMAKE) fuzz-nightly-inner

fuzz-nightly-inner:
	@GOMAXPROCS=$(FUZZ_GOMAXPROCS) ./hack/fuzz.sh nightly $(FUZZTIME_NIGHTLY) $(SHARDS) $(SHARD)

ratchet: ## Fail if a parsing/stateful package lacks a fuzz target
	@$(GO) run ./tools/fuzzratchet

ratchet-update: ## Raise the fuzz-target baseline to the current count
	@$(GO) run ./tools/fuzzratchet -update

adr-check: ## Validate ADRs (STRICT=1 requires every row to be owned)
	@if ls adr/ADR-*.md >/dev/null 2>&1; then if [ "$(STRICT)" = "1" ]; then $(GO) run ./tools/adrcheck -strict -skip-driver; else $(GO) run ./tools/adrcheck; fi; else echo "No ADR records to validate"; fi

# Does the ledger's evidence EXIST? `adr-check` validates assumption
# identifiers and nothing about the Test names and Go identifiers a row cites,
# so a row could describe a defect as current behaviour long after the increment
# that closed it, with the fix's own test as the thing it could not find --
# which is what four rows did.
#
# It is a target of its own rather than part of `adr-check` because it builds
# every module's test binaries to ask `go test -list`: about a minute with a
# warm cache, where `adr-check` answers in a second and is run a dozen times an
# increment. It IS in `make ci`.
adr-identifiers: ## Resolve every Test name and Go identifier the assumption ledger cites
	@if [ -f adr/ADR-017-assumptions.md ]; then GOWORK=off $(GO) run ./tools/adrcheck -identifiers; else echo "No assumption ledger to validate"; fi

modelcheck: ## Cross-check TLA+ invariants against Go models
	@$(GO) run ./tools/modelcheck

# TLC's footprint, not its speed, is what these two bound. The default was
# -workers min(16, NumCPU) and -Xmx8g, which on the 64-core development host
# is sixteen worker threads and an 8 GB heap ceiling per specification, eleven
# times over, beside about fifty other people's containers and the sibling
# increments' own `make ci`. Two workers and a 2 GB heap run all eleven in
# 27.8 s (MemoryLease is 16.2 s of that and every other specification is
# about one second), which is what the configurations are sized for: they
# explore interleavings, not scale. Raise them on a host of your own --
# `make tla TLA_WORKERS=16 TLA_XMX=8g`.
TLA_WORKERS ?= 2
TLA_XMX ?= 2g

tla: ## Model-check every TLA+ spec (or SPEC=Name)
	@if [ -n "$(SPEC)" ]; then $(GO) run ./tools/tlatools run -workers $(TLA_WORKERS) -xmx $(TLA_XMX) models/tla/$(SPEC); else $(GO) run ./tools/tlatools run-all -workers $(TLA_WORKERS) -xmx $(TLA_XMX); fi

tla-mutate: ## Negative control: every checked invariant must fail under its recorded defect
	@$(GO) run ./tools/tlatools mutate -xmx $(TLA_XMX)

ci-parity: ## Fail when a target of `make ci` is not run by the CI workflow
	@./hack/ci-parity.sh

cover: ## the test suite's coverage thresholds over every module the three areas live in
	@$(CPU_UNIT) $(SUBMAKE) cover-inner

cover-inner:
	@./hack/cover.sh $(if $(OUT),$(OUT),)

cover-threshold: ## The coverage gate: its own self-test, then the three areas
	@$(CPU_UNIT) $(SUBMAKE) cover-threshold-inner

cover-threshold-inner:
	@./hack/cover-threshold_test.sh
	@./hack/cover.sh $(if $(OUT),$(OUT),)

cover-integration: ## The coverage figure WITH the container arm, which `make ci` cannot produce (~50 min, needs Docker)
	@$(CPU_UNIT) $(SUBMAKE) cover-integration-inner

cover-integration-inner:
	@./hack/cover-integration.sh $(if $(OUT),$(OUT),)

cover-update: ## Raise a coverage floor to the measured figure (never past its target)
	@$(CPU_UNIT) $(SUBMAKE) cover-update-inner

cover-update-inner:
	@./hack/cover.sh /dev/null >/dev/null 2>&1 || true
	@./hack/cover-threshold.sh -update coverage.out

workflows-lint: ## actionlint + ratchet check on GitHub workflows
	@# actionlint runs shellcheck over every `run:` block only when shellcheck is
	@# on PATH, and says nothing when it is not -- which is how two findings
	@# passed here for weeks and failed on the first GitHub run, where
	@# shellcheck is installed. The warning makes a partial local pass visible.
	@command -v shellcheck >/dev/null 2>&1 || echo "workflows-lint: WARNING: shellcheck is not on PATH, so actionlint did NOT check the run: scripts; CI will" >&2
	@if [ -d .github/workflows ]; then $(TOOL) actionlint; $(TOOL) ratchet lint .github/workflows/*.yml; else echo "no workflows"; fi

bench-quick: ## Quick benchmark smoke for PKG/PATTERN (never used for decisions)
	@mkdir -p benchmarks/reports/quick
	@$(call CPU_BENCH,benchmarks/reports/quick) $(SUBMAKE) bench-quick-inner

bench-quick-inner:
	@mkdir -p benchmarks/reports/quick
	@# -cpu is explicit: this was the one benchmark path with neither a taskset
	@# pin nor a -cpu flag, so it ran at GOMAXPROCS=nproc -- as many threads as
	@# the container is allowed cores, which was 64, several times the ceiling
	@# every other benchmark path is held to
	@#
	@./hack/benchguard.sh run -s $(if $(LOADMAX),-t $(LOADMAX)) -- env GOWORK=off $(GO) test -run='^$$' -bench='$(PATTERN)' -benchtime=$(BENCHTIME) -count=$(COUNT) -cpu=$(IQMQ_MAX_CPUS) -benchmem $(PKG) | tee benchmarks/reports/quick/$$(date -u +%Y%m%dT%H%M%SZ).txt

bench-native: ## Native-client baselines against their containers (CLIENTS=, COUNT=, BENCHTIME=, SMOKE=1)
	@mkdir -p benchmarks/reports/native
	@$(call CPU_BENCH,benchmarks/reports/native) $(SUBMAKE) bench-native-inner

bench-native-inner:
	@./hack/native-baselines.sh $(CLIENTS)

native-baselines: bench-native ## Alias for bench-native

bench-api: ## Measure the frozen public API's own benchmarks and store the run (the root is not a bake-off family)
	@mkdir -p benchmarks/reports/results
	@$(call CPU_BENCH,benchmarks/reports/results) $(SUBMAKE) bench-api-inner

bench-api-inner:
	@./hack/bench-api.sh $(PROFILE) $(if $(BENCHPKG),$(BENCHPKG) $(BENCHID),)

bench-overhead: ## Measure the driver-vs-raw-client overhead locally, one broker at a time (BROKERS=, ROUNDS=)
	@mkdir -p benchmarks/reports/results
	@$(call CPU_BENCH,benchmarks/reports/results) $(SUBMAKE) bench-overhead-inner

bench-overhead-inner:
	@./hack/bench/run-local.sh $(BROKERS)

bench-compare: ## Measure the layer-5 competitor comparison locally, one family at a time (FAMILIES=, ROUNDS=)
	@mkdir -p benchmarks/reports/results
	@$(call CPU_BENCH,benchmarks/reports/results) $(SUBMAKE) bench-compare-inner

bench-compare-inner:
	@./hack/bench/run-compare.sh $(FAMILIES)

compare-report: ## Render the competitor table from a stored run (RESULTS=benchmarks/reports/results/<run>/raw.txt)
	@cd benchmarks && GOWORK=off $(GO) run ./cmd/compare -results $(if $(RESULTS),../$(RESULTS),$(error RESULTS=<run>/raw.txt required))

overhead-report: ## Render the overhead table from a stored run (RESULTS=benchmarks/reports/results/<run>/raw.txt)
	@cd benchmarks && GOWORK=off $(GO) run ./cmd/overhead -results $(if $(RESULTS),../$(RESULTS),$(error RESULTS=<run>/raw.txt required))

overhead-gate: ## Judge the kind: ratio overhead budgets from a stored run (RESULTS=benchmarks/reports/results/<run>/raw.txt.gz)
	@cd benchmarks && GOWORK=off $(GO) run ./cmd/benchgate -ratios -gate $(OVERHEAD_GATE) -budgets budgets.yaml \
	  -run $(if $(RESULTS),../$(RESULTS),$(error RESULTS=<run>/raw.txt.gz required))

bakeoff-dryrun: ## Every family through `decide -dry-run` at 1x: can each one be decided at all? (run before a campaign)
	@./ $(if $(OUT),$(OUT),$(shell mktemp -d))

cpu-cap: ## Print the CPU ceiling this tree computes for itself
	@./hack/cpucap.sh report

cpu-cap-check: ## Fail when a path that runs Go code escapes the IQMQ_MAX_CPUS ceiling (with its own self-test)
	@./hack/cpucap-check.sh --self-test
	@./hack/cpucap-check.sh

benchguard-test: ## Test the benchmark load guard's wait and verdict (every half must be able to fail)
	@./hack/benchguard_test.sh

benchgate-test: ## Test that the gate's exit statuses reach its callers (measures nothing; seconds)
	@./hack/benchgate_test.sh

pinsweep-test: ## Self-test hack/pinsweep.py: the inventory covers the tree, matches the stored baseline, and PINNED/UNPINNED/refusal each reachable
	@./hack/pinsweep_test.sh

# The FULL thirty-two-construction sweep, which is a module test run per
# construction and minutes even on a quiet host -- so it is a nightly job and not
# a gate. It compares against the committed baseline and the
# exit status is the answer: 1 a regression (something stopped pinning an
# ambiguous rule's Retryable flag), 3 drift (the baseline is out of date), 2 make
# flattening either. Call the script directly when you need to tell them apart,
# for the reason this Makefile already gives for hack/benchgate.sh.
pinsweep-nightly: ## Full ambiguous-Retryable sweep against hack/pinsweep-baseline.tsv (minutes; nightly, not a gate)
	@python3 ./hack/pinsweep.py all --baseline hack/pinsweep-baseline.tsv --out $(PINSWEEP_OUT)

pinsweep-baseline: ## Re-run the full sweep and rewrite hack/pinsweep-baseline.tsv (commit the result)
	@python3 ./hack/pinsweep.py all --out hack/pinsweep-baseline.tsv

bakeoff-test: ## Test the bake-off measurement harness (grid selection, filters, lost samples, resume)
	@./hack/bakeoff_test.sh

bench-lint: ## Check every variant kind has model subtests and benchmark names, and every bake-off family a benchmark to measure
	@cd benchmarks && GOWORK=off $(GO) run ./cmd/bakeoff lint -root .. $(PKG)

scorecard: ## Report the committed scorecard (CHECK=1 fails on a missing row or a claimed axis with no metric)
	@cd benchmarks && GOWORK=off $(GO) run ./cmd/bakeoff apiscore -root .. $(if $(CHECK),-check,)

# The scorecard and the tool that wrote it were not brought over from the
# Bitbucket repository, because they held internal, project-specific
# information. The check runs again once a scorecard is committed here.
scorecard-check: ## Fail when a required row has no scorecard result, or a claimed scoring axis has no metric
	@if [ -f benchmarks/reports/apibench-scorecard.json ]; then cd benchmarks && GOWORK=off $(GO) run ./cmd/bakeoff apiscore -root .. -check; else echo "No API scorecard to validate"; fi

bakeoff: ## Full-profile bake-off for FAMILY
	@mkdir -p benchmarks/reports/results
	@$(call CPU_BENCH,benchmarks/reports/results) $(SUBMAKE) bakeoff-inner

bakeoff-inner:
	@$(if $(MATRIX),MATRIX=$(MATRIX) )./hack/bakeoff.sh $(FAMILY) $(PROFILE)

bakeoff-campaign: ## Drive the bake-off over every family (PROFILE=probe|quick|full, FAMILIES=..., RESUME=1)
	@mkdir -p benchmarks/reports/results
	@$(call CPU_BENCH,benchmarks/reports/results) $(SUBMAKE) bakeoff-campaign-inner

bakeoff-campaign-inner:
	@./ $(or $(PROFILE),probe) $(FAMILIES)

bakeoff-adr: ## Render the bake-off ADRs from the stored per-family scorecards (ADR=n for one; DRYRUN=1 to list)
	@cd benchmarks && GOWORK=off $(GO) run ./cmd/bakeoff adr $(if $(ADR),-adr $(ADR),) $(if $(DRYRUN),-dry-run,)

# THE GATE'S EXIT STATUS DOES NOT SURVIVE THIS TARGET, and that is why
# .github/workflows/bench.yml calls hack/benchgate.sh directly instead of
# running `make gate`.
#
# hack/benchgate.sh answers with six statuses -- 0 pass, 1 regression or budget
# breach, 2 setup error, 3 inconclusive, 4 contended, 5 the guard refused or the
# run was swamped -- and `make` replaces all five failures with its own 2. So a
# caller that branches on `make gate` can learn only pass or fail, and cannot
# tell a regression from a host too busy to measure one.
#
# The same applies to `overhead-gate`, `budgets-lint` and any other target here
# that fronts ./cmd/benchgate. Call the script, or the built command, when the
# distinction matters. hack/benchgate_test.sh pins the statuses the script
# itself propagates; it had to be written because the script reached the command
# through `go run`, which collapses every non-zero status to 1 before `make`
# even sees it, so 2 and 3 were unreachable from anywhere.
gate: ## Benchmark regression gate against BASE (git ref; see the note above on exit status)
	@mkdir -p benchmarks/reports/gate
	@$(call CPU_BENCH,benchmarks/reports/gate) $(SUBMAKE) gate-inner

gate-inner:
	@./hack/benchgate.sh $(BASE) $(GATE)

gate-if-touched: ## Run the gate only if this branch changed a package holding a gated benchmark
	@mkdir -p benchmarks/reports/gate
	@$(call CPU_BENCH,benchmarks/reports/gate) $(SUBMAKE) gate-if-touched-inner

gate-if-touched-inner:
	@./hack/gate-if-touched.sh $(BASE) $(GATE)

gate-selftest: ## Prove the gate fails: clean tree passes, injected allocation fails
# It runs the SAME benchmarks `gate` does, twice -- once on a clean tree and
# once on an injected one -- so it belongs in the bench class for the same
# reason `gate` does, and it was the only heavy target here that was not in it.
# Unwrapped on a shared host the clean half competes with whatever else is
# running and comes back inconclusive (exit 3) or contended (exit 4), which
# proves nothing about the gate either way; `hack/benchgate.sh` says so itself
# with "NOT PINNED: cpu-policy is not driving this run". The reservation is
# taken once, around the whole script, because both halves and the worktree
# build have to be inside it.
	@mkdir -p benchmarks/reports/gate
	@$(call CPU_BENCH,benchmarks/reports/gate) $(SUBMAKE) gate-selftest-inner

gate-selftest-inner:
	@./hack/gate-selftest.sh $(SELFTEST_GATE)

budgets-lint: ## Check benchmarks/budgets.yaml against the stored campaign (no measurement)
	@cd benchmarks && GOWORK=off $(GO) run ./cmd/benchgate -self-test
	@cd benchmarks && GOWORK=off $(GO) run ./cmd/benchgate -lint -budgets budgets.yaml

security-check: ## the security checks's source-readable rules: Config renderings, credential types, Insecure naming, supply-chain workflow coverage
	@GOWORK=off $(GO) run ./tools/securitycheck -root .

security-check-selftest: ## Prove every check can fail: each one against a fixture that breaks it and one that does not
	@GOWORK=off $(GO) run ./tools/securitycheck -self-test

vuln: ## govulncheck every module, judged against securitycheck's advisory waivers (needs the network; not in ci)
	@./hack/vuln.sh

license: ## License allow-list check for modules with dependencies
	@./hack/license.sh

apidiff: ## Compare exported APIs against hack/api baselines (names every module it cannot check)
	@./hack/apidiff.sh

exported-doc: ## the documentation checks's doc-comment coverage, restatement ceiling and example list (also run by make apidiff)
	@GOWORK=off $(GO) run ./tools/exportdoc $(if $(filter 1,$(STRICT)),-list-restatements -list-fields,)

apidiff-baseline: ## Rewrite the hack/api baselines
	@./hack/apidiff.sh -w

conformance-memory: ## Run the memory driver's conformance suite and refresh its report and the generated artefacts
	@$(MAKE) --no-print-directory conformance-memory-report
	@$(MAKE) --no-print-directory generate

conformance-memory-report: ## Refresh conformance/reports/latest/memory.json and nothing else
	@GOWORK=off IQMQ_CONFORMANCE_REPORTS=$(CURDIR)/conformance/reports/latest \
	  IQMQ_COMMIT="$$(git rev-parse HEAD 2>/dev/null || echo unknown)" \
	  $(GO) test -count=1 -run TestConformance ./memory

generate: ## Regenerate capability artefacts from conformance reports
	@cd conformance && GOWORK=off $(GO) run ./cmd/confgen -reports reports/latest -root ..

# INCOMING is where the nightly's downloaded reports land. The aggregate step
# is NOT `cp -f $(INCOMING)/*.json ... || true; make generate`, which is what
# the integration workflow used to run and what could not fail: a download
# that produced nothing was swallowed by the `||`, generation regenerated the
# COMMITTED reports, the diff was empty and the job reported success having
# refreshed nothing.
INCOMING ?= conformance/reports/incoming

aggregate: ## Attest a nightly conformance delivery (INCOMING=dir), merge it and regenerate
	@cd conformance && GOWORK=off $(GO) run ./cmd/confgen -reports reports/latest -incoming $(if $(filter /%,$(INCOMING)),$(INCOMING),../$(INCOMING)) -root ..

generate-check: ## Fail if generated capability artefacts are stale
	@if [ -d conformance/reports/latest ]; then cd conformance && GOWORK=off $(GO) run ./cmd/confgen -reports reports/latest -root .. -check; else test ! -f capabilities.json || { echo "capabilities.json has no source reports" >&2; exit 1; }; echo "No generated capability claims to check"; fi

examples: ## Build, vet and RUN every example module, and name the broker arms that ran
	@# hack/examples.sh carries the rationale: the test run is the half that
	@# matters, and the broker-backed examples of the specification report which of their
	@# arms reached a broker so that an all-skip run cannot read as green.
	@GO=$(GO) ./hack/examples.sh

images-lock: ## Pull every broker image and record digests in conformance/harness/images.lock
	@./hack/images-lock.sh

image-rabbitmq-delayed: ## Build the RabbitMQ image with the delayed-message plugin
	@docker build -t iqmq/rabbitmq-delayed:4.2.9-plugin4.2.0 hack/images/rabbitmq-delayed

harness: ## Start and ping every broker image (Docker)
	@./hack/conformance.sh harness

suites: ## List the conformance suites (MODE=integration|critical|matrix|inprocess selects one run)
	@./hack/conformance.sh list $(MODE)

suites-modes: ## Print every run mode and say what each one is missing
	@./hack/conformance.sh modes

suites-check: ## Hold every entry of conformance/suites.json to this repository
	@GOWORK=off $(GO) run ./tools/suites -check

integration: ## Conformance suite for DRIVER against its broker (empty = all; DRIVER is a suites.json name)
	@$(CPU_INTEG) $(SUBMAKE) integration-inner

integration-inner:
	@./hack/conformance.sh integration $(DRIVER)

integration-all: ## Conformance suites for every driver
	@$(CPU_INTEG) $(SUBMAKE) integration-all-inner

integration-all-inner:
	@./hack/conformance.sh integration

critical: ## Driver-specific critical tests for DRIVER (in-process; make ci runs them too)
	@./hack/conformance.sh critical $(DRIVER)

chaos: ## layer-3 toxiproxy fault scenarios for DRIVER (EXTENDED=1 adds the weekly set)
	@$(CPU_INTEG) $(SUBMAKE) chaos-inner

chaos-inner:
	@EXTENDED=$(EXTENDED) ./hack/conformance.sh chaos $(DRIVER)

matrix: ## Broker-version matrix for DRIVER
	@$(CPU_INTEG) $(SUBMAKE) matrix-inner

matrix-inner:
	@./hack/conformance.sh matrix $(DRIVER)

soak: ## layer-4 soak for DRIVER for TIME with profile diffs (under 10m grades INCONCLUSIVE, not clean)
	@$(CPU_INTEG) $(SUBMAKE) soak-inner

soak-inner:
	@./hack/conformance.sh soak $(DRIVER) $(TIME)

# the test suite's memory linearizability campaign, which `soak.yml` has
# called since the job was written and which did not exist: the weekly job
# failed on "No rule to make target 'linearize-campaign'" every week, so the
# campaign has never run.
linearize-campaign: ## linearizability campaign: the checker, the engine's histories, then the fuzzer for TIME
	@$(CPU_FUZZ) $(SUBMAKE) linearize-campaign-inner

linearize-campaign-inner:
	@./hack/linearize-campaign.sh $(TIME)

linearize-selftest: ## Prove the campaign can fail: every stage over a history that is not linearizable
	@./hack/linearize-campaign.sh --selftest

differential: ## one op script against memory and DRIVER's broker (settled multisets, per-key order)
	@./hack/conformance.sh differential $(DRIVER)

broker-up: ## Start a broker with docker compose (NAME=redpanda|nats|nsq|pubsub|sqs|rabbitmq|artemis|activemq-classic|redis|pulsar|toxiproxy)
	@docker compose -f hack/compose/$(NAME).yml up -d

broker-down: ## Stop a broker started with broker-up
	@docker compose -f hack/compose/$(NAME).yml down -v

docs-check: ## driver sections, internal links and anchors, nav reachability, README (no Python)
	@GOWORK=off $(GO) run ./tools/docscheck -root .

docs-build: ## Build the docs site strictly
	@./hack/docs.sh build

docs-serve: ## Serve the docs site locally
	@./hack/docs.sh serve

sysinfo: ## Print host description JSON
	@cd benchmarks && GOWORK=off $(GO) run ./cmd/sysinfo

# `ci` holds the heavy-job lock ONCE for the whole run. Its prerequisites are
# themselves wrapped targets, and they pass straight through rather than
# queueing behind it, because the tool recognises this job as their ancestor.
#
# The prerequisites live on `ci-inner` and the help text stays here, so `make
# help` still names `ci`. hack/ci-parity.sh reads the list from `ci-inner:` --
# if you move it again, move that sed with it, or parity silently checks
# nothing.
ci: ## Everything the CI workflow runs
	@$(CPU_OTHER) $(SUBMAKE) ci-inner

ci-inner: fmt-check vet lint stdlib-check build test-race test-tags allocs fuzz-seeds ratchet adr-check adr-identifiers modelcheck tla tla-mutate generate-check suites-check work-sync examples apidiff license workflows-lint ci-parity cover-threshold scorecard-check bench-lint budgets-lint benchguard-test benchgate-test bakeoff-test docs-check security-check security-check-selftest cpu-cap-check pinsweep-test linearize-selftest

work-sync: ## Regenerate go.work from the module list and fail on drift
	@# GOWORK is cleared for this one command because CI exports GOWORK=off for
	@# the whole job, and `go work use` then refuses with "no go.work file
	@# found" -- which failed the first GitHub run while every local run passed.
	@tmp="$$(mktemp)"; cp go.work "$$tmp"; GOWORK= $(GO) work use -r .; if ! diff -u "$$tmp" go.work; then echo "go.work was out of date (now regenerated); commit it" >&2; rm -f "$$tmp"; exit 1; fi; rm -f "$$tmp"

tidy: ## go mod tidy every module
	@$(EACH) $(GO) mod tidy

clean: ## Remove build artefacts
	@rm -rf benchmarks/reports/quick models/tla/states

.PHONY: hero hero-svg
hero: ## Play the README hero in a terminal
	env GOWORK=off $(GO) -C examples/hero run .

hero-svg: ## Regenerate docs/hero.svg
	env GOWORK=off $(GO) -C examples/hero generate .

.PHONY: hero-check
hero-check: ## Check real fixtures and generated hero freshness
	env GOWORK=off $(GO) -C examples/hero test ./...
