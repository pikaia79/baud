package uuid

import (
	"testing"
)

func BenchmarkFlakeUUID(b *testing.B) {
	b.Run("UUID.FlakeUUID", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				FlakeUUID()
			}
		})
	})
}
