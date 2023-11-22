package partitioners

import (
	"hash/fnv"
	"testing"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fnv32a returns the FNV-1a hash of a byte slice.
// The same hash sarama and segmentio use:
// https://github.com/IBM/sarama/blob/d09a287f30bdf73b1c9f53c3f7a4b2ecc2bd41d3/partitioner.go#L178
// https://github.com/segmentio/kafka-go/blob/c6378c391a970fc19c1dbd664016f09c6a73ea8d/balancer.go#L114
func fnv32a(b []byte) uint32 {
	h := fnv.New32a()
	h.Reset()
	h.Write(b)
	return h.Sum32()
}

func SaramaCompatHasher(hashFn func([]byte) uint32) kgo.PartitionerHasher {
	return func(key []byte, n int) int {
		p := int32(hashFn(key)) % int32(n)
		if p < 0 {
			p = -p
		}
		return int(p)
	}
}

func TestPartitioner(t *testing.T) {
	// this is the default in Sarama:
	// https://github.com/IBM/sarama/blob/d09a287f30bdf73b1c9f53c3f7a4b2ecc2bd41d3/config.go#L529C27-L529C45
	saramaP := sarama.NewHashPartitioner("anytopic")
	// This is "sarama-compatible" from segmentio:
	// https://github.com/segmentio/kafka-go/blob/c6378c391a970fc19c1dbd664016f09c6a73ea8d/balancer.go#L127
	segmentioP := &kafka.Hash{Hasher: nil}

	// Franz declares this "sarama-compatible" but is not.
	// https://github.com/twmb/franz-go/blob/a6d10d4ad93528f7c2e8ed735a40bb1bd8a03c8a/pkg/kgo/partitioner.go#L509
	franzP := kgo.StickyKeyPartitioner(kgo.SaramaHasher(fnv32a)).ForTopic("anytopic")

	// With this modified hasher it works as expected.
	franzPmod := kgo.StickyKeyPartitioner(SaramaCompatHasher(fnv32a)).ForTopic("anytopic")

	for n := 2; n < 25; n++ {
		for i := 0; i < 100; i++ {
			key := uuid.NewString()
			partitions := make([]int, n)
			for j := 0; j < n; j++ {
				partitions[j] = j
			}
			pseg := segmentioP.Balance(kafka.Message{Key: []byte(key)}, partitions...)
			pfrz := franzP.Partition(
				&kgo.Record{
					Key: []byte(key),
				},
				n,
			)
			pfrzmod := franzPmod.Partition(
				&kgo.Record{
					Key: []byte(key),
				},
				n,
			)
			psar, _ := saramaP.Partition(&sarama.ProducerMessage{
				Key: sarama.StringEncoder(key),
			}, int32(n))

			// Sarama and segmentio are always equal.
			if pseg != int(psar) {
				t.Errorf("key %q, partitions %d, segmentio %d, sarama %d", key, n, pseg, psar)
			}

			// But franz gives different partition number.
			if int(psar) != pfrz {
				t.Errorf("key %q, partitions %d, sarama %d, franz %d", key, n, psar, pfrz)
			}

			// With the modified hasher franz gives the same partition number.
			if int(psar) != pfrzmod {
				t.Errorf("key %q, partitions %d, sarama %d, franz (modified) %d", key, n, psar, pfrzmod)
			}
		}
	}
}
