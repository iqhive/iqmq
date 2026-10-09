# Merge Go coverage profiles by UNION, taking the highest count per statement
# block: `awk -f hack/cover-merge.awk p1.out p2.out ... > merged.out`.
#
# It is a file of its own so that hack/cover-threshold_test.sh can hold it to a
# table. The first version lived inline in hack/cover.sh and reported the
# repository's core coverage as 100.00 %, because an awk array element that was
# never assigned prints as the EMPTY STRING: a block no module covered came out
# as a two-field row, which both `go tool cover` and the threshold script skip,
# so the covered statements became the whole denominator. A merge nobody can
# test is a check that cannot fail (assumption C02b.2).
#
# Why a union and not a concatenation: every module is measured with
# `-coverpkg=github.com/iqhive/iqmq/...`, so a package appears in the profile
# of every module that compiled it, and concatenating would count its
# statements once per module -- moving numerator and denominator both. The
# header is written by the caller, since the mode is the caller's choice.

/^mode:/ { next }

# A profile row is `path:startLine.col,endLine.col numStmt count`.
NF == 3 {
	k = $1
	if (!(k in stmt)) {
		stmt[k] = $2
		cnt[k] = 0
		order[++n] = k
	}
	if ($3 + 0 > cnt[k] + 0) {
		cnt[k] = $3 + 0
	}
	next
}

# Anything else is not a profile row. Refusing is the point: a silently
# ignored row is a statement that leaves the denominator.
{
	printf "cover-merge: %s:%d: not a coverage row: %s\n", FILENAME, FNR, $0 > "/dev/stderr"
	bad = 1
}

END {
	if (bad) {
		exit 1
	}
	# cnt[k]+0 on the way out as well as on the way in, so a block nothing
	# covered is a real `0` and stays in the denominator.
	for (i = 1; i <= n; i++) {
		k = order[i]
		print k, stmt[k], cnt[k] + 0
	}
}
