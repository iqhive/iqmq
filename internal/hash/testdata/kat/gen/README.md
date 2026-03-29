# Known-answer generators

These generators create functional test vectors for the hash implementations. They do not measure performance.

Use the pinned references in [third-party notices](../../../../../THIRD_PARTY_NOTICES.md):

- xxHash 0.8.3: `https://raw.githubusercontent.com/Cyan4973/xxHash/e626a72bc2321cd320e953a0ccf1584cad60f363/xxhash.h`
- wyhash final version 4.3: `https://raw.githubusercontent.com/wangyi-fudan/wyhash/e4764a0b637d34d3421a7760affada9288b625a8/wyhash.h`
- Kafka 4.1.0: `murmur2` and `toPositive` from `Utils.java` at `13f70256db3c994c590e5d262a7cc50b9e973204`; the standalone Java harness includes the upstream licence notice.

Download the headers beside `gen_xxh3.c` and `gen_wy.c`, then compile with `gcc -O2`. Compile `Murmur2Kat.java` with `javac` and run `java Murmur2Kat`. `keys.h` supplies deterministic inputs shared with the Go tests.
