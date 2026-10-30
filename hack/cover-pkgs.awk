# The `-coverpkg` list for one module's run of hack/cover.sh: the iqmq packages
# the run builds from THIS tree, comma-separated, on one line.
#
#   go list -deps -test -f '{{if .Module}}{{.ImportPath}}{{"\t"}}{{.Dir}}{{end}}' ./... \
#     | awk -v root=ROOT -f hack/cover-pkgs.awk
#
# Under GOWORK=off a nested module builds the iqmq modules it requires from the
# module cache, at the release its go.mod names. `-coverpkg=github.com/iqhive/iqmq/...`
# instrumented those copies as well, so a file changed since the release came
# back in two decompositions, the tree's and the release's, and hack/cover-threshold.sh
# refused the merged profile. A package is named here only when its directory
# is ROOT or under it.
#
# A test variant ("p [p.test]") and a test main ("p.test") are not packages
# `-coverpkg` can name, and a standard-library package prints an empty line.
BEGIN { FS = "\t" }

NF == 2 && index($2 "/", root "/") == 1 && $1 !~ / / && $1 !~ /\.test$/ \
	&& ($1 == "github.com/iqhive/iqmq" || index($1, "github.com/iqhive/iqmq/") == 1) {
	if (!($1 in seen)) {
		seen[$1] = 1
		list = list (list == "" ? "" : ",") $1
	}
}

END { print list }
