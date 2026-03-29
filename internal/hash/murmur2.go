package hash

// KafkaSeed is the seed Kafka's default partitioner hashes with. The Murmur2
// kind seeded with it reproduces org.apache.kafka.common.utils.Utils.murmur2
// bit for bit (as an unsigned 32-bit value).
const KafkaSeed uint64 = 0x9747b28c

// Murmur2 mixing constants.
const (
	murmur2M = 0x5bd1e995
	murmur2R = 24
)

// murmur2 is 32-bit MurmurHash2 in the form Kafka uses: little-endian
// 4-byte blocks, the length xored into the seed, the standard tail and
// finaliser. The result is below 1<<32.
func murmur2[T bytesOrString](seed uint64, b T) uint64 {
	n := len(b)
	h := murmur2Seed(seed) ^ uint32(n)
	i := 0
	for ; n-i >= 4; i += 4 {
		k := uint32(load32(b, i))
		k *= murmur2M
		k ^= k >> murmur2R
		k *= murmur2M
		h *= murmur2M
		h ^= k
	}
	switch n - i {
	case 3:
		h ^= uint32(b[i+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(b[i+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(b[i])
		h *= murmur2M
	}
	h ^= h >> 13
	h *= murmur2M
	h ^= h >> 15
	return uint64(h)
}

// murmur2Seed folds a 64-bit seed into MurmurHash2's 32-bit seed. Seeds
// below 1<<32 are used verbatim (Kafka compatibility, standard MurmurHash2
// seeds); the high word is multiplied by an odd constant before it is xored
// in, so seeds that differ only in their high word still differ.
func murmur2Seed(seed uint64) uint32 {
	return uint32(seed) ^ uint32(seed>>32)*0x9E3779B1
}

// Murmur2Kafka returns Kafka's Utils.murmur2(key) as an unsigned 32-bit
// value (the same bits as Java's int).
func Murmur2Kafka(key []byte) uint32 { return uint32(murmur2(KafkaSeed, key)) }

// KafkaPartition returns the partition Kafka's default partitioner assigns
// to a non-null key: toPositive(murmur2(key)) % numPartitions. It returns 0
// when numPartitions <= 1.
func KafkaPartition(key []byte, numPartitions int) int {
	if numPartitions <= 1 {
		return 0
	}
	return int(uint64(Murmur2Kafka(key)&0x7fffffff) % uint64(numPartitions))
}
