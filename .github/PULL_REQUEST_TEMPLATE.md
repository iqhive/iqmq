## Summary

<!-- What changes and why. Reference the specification section or ADR. -->

## Checklist

- [ ] `make ci` is green locally
- [ ] Benchmarks attached (required for `perf-critical` paths); `make gate` verdict included
- [ ] Fuzz seeds added for new parsers or state machines; `make ratchet` passes
- [ ] Docs updated (every exported identifier documented; driver page sections complete)
- [ ] `apidiff` reviewed — breaking change? label `breaking` and link the ADR
- [ ] Dependency review — new direct dependency? justify below
- [ ] Capability claims verified by conformance (drivers)

## Dependency review

<!-- New direct dependencies, their licenses and why a stdlib solution was not viable. "none" otherwise. -->
