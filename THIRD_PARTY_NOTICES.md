# Third-party notices

The following references identify the upstream implementations used by the hash ports and known-answer generators. They identify external source revisions, not commits in this repository.

| Component | Reference | Local files | Licence |
|---|---|---|---|
| xxHash 0.8.3 | [xxhash.h at `e626a72bc232`](https://github.com/Cyan4973/xxHash/blob/e626a72bc2321cd320e953a0ccf1584cad60f363/xxhash.h) | `internal/hash/xxh3.go`, `internal/hash/testdata/kat/gen/keys.h` | [BSD two-clause licence](licenses/xxhash.LICENSE), [source notice](licenses/xxhash.NOTICE) |
| wyhash final version 4.3 | [wyhash.h at `e4764a0b637d`](https://github.com/wangyi-fudan/wyhash/blob/e4764a0b637d34d3421a7760affada9288b625a8/wyhash.h) | `internal/hash/wyhash.go` | [Unlicense](licenses/wyhash.UNLICENSE) |
| Apache Kafka 4.1.0 | [Utils.java at `13f70256db3c`](https://github.com/apache/kafka/blob/13f70256db3c994c590e5d262a7cc50b9e973204/clients/src/main/java/org/apache/kafka/common/utils/Utils.java) | `internal/hash/testdata/kat/gen/Murmur2Kat.java` | [Apache licence 2.0](licenses/kafka.LICENSE), [Kafka notice](licenses/kafka.NOTICE) |

The Go hash ports adapt the upstream scalar operations to Go and the package's byte/string input helpers. The Kafka generator extracts `murmur2` and `toPositive` into a standalone harness and adds key generation and partition cases. Its utility methods are taken from the pinned source above.
