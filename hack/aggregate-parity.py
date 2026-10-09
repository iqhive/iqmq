"""Hold the nightly capability-refresh job to a shape that can fail.

`.github/workflows/integration.yml`'s aggregate job used to copy the
downloaded reports with `cp -f … || true` and then run `make generate`, which
cannot fail on stale input: an empty download was swallowed by the `||`,
generation regenerated the COMMITTED reports, the diff was empty and the job
reported success having refreshed nothing. Read by hack/ci-parity.sh, which
`make ci` runs (assumption B17.1).
"""

import sys

SWALLOW = ("|| true", "|| :", "continue-on-error: true")


def job(text, name):
    """Return the lines of one top-level job of a workflow, comments dropped."""
    out, inside = [], False
    for line in text.splitlines():
        if line.startswith("  " + name + ":"):
            inside = True
            continue
        if inside:
            stripped = line.strip()
            if line[:3] not in ("   ", "") and not line.startswith("    "):
                # A line at job indentation: the next job has started.
                if stripped and not line.startswith("   "):
                    break
            if stripped.startswith("#"):
                continue
            out.append(line)
    return out, inside


def main():
    text = open(sys.argv[1]).read()
    lines, found = job(text, "aggregate")
    if not found:
        print("integration.yml has no aggregate job, so nothing refreshes the capability artefacts")
        return
    steps = "\n".join(lines)
    if "make aggregate" not in steps:
        print("the aggregate job does not run `make aggregate`, which is the step that attests the delivery")
    for swallow in SWALLOW:
        if swallow in steps:
            print(
                "the aggregate job contains %r: a nightly that cannot fail on an empty "
                "download republishes the committed reports as if they were fresh" % swallow
            )
    if "cp -f conformance/reports" in steps:
        print("the aggregate job copies reports by hand; `make aggregate` is what refuses a delivery that refreshes nothing")


main()
