package cmap

import "testing"

func BenchmarkItems(b *testing.B) {
	m := New()

	// Insert 100 elements.
	for i := 0; i < 10000; i++ {
		id := Int64Key(i)
		m.Set(id, Animal{id})
	}
	for i := 0; i < b.N; i++ {
		m.Items()
	}
}

func BenchmarkMarshalJson(b *testing.B) {
	m := New()

	// Insert 100 elements.
	for i := 0; i < 10000; i++ {
		id := Int64Key(i)
		m.Set(id, Animal{id})
	}
	for i := 0; i < b.N; i++ {
		m.MarshalJSON()
	}
}

func BenchmarkSingleInsertAbsent(b *testing.B) {
	m := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := Int64Key(i)
		m.Set(id, "value")
	}
}

func BenchmarkSingleInsertPresent(b *testing.B) {
	m := New()
	m.Set(Int64Key(111), "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(Int64Key(111), "value")
	}
}

func benchmarkMultiInsertDifferent(b *testing.B, shardsCount int) {
	m := New()
	finished := make(chan struct{}, b.N)
	_, set := GetSet(m, finished)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := Int64Key(i)
		set(id, "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

func BenchmarkMultiInsertDifferent_1_Shard(b *testing.B) {
	benchmarkMultiInsertDifferent(b, 1)
}
func BenchmarkMultiInsertDifferent_16_Shard(b *testing.B) {
	benchmarkMultiInsertDifferent(b, 16)
}
func BenchmarkMultiInsertDifferent_32_Shard(b *testing.B) {
	benchmarkMultiInsertDifferent(b, 32)
}
func BenchmarkMultiInsertDifferent_256_Shard(b *testing.B) {
	benchmarkMultiGetSetDifferent(b, 256)
}

func BenchmarkMultiInsertSame(b *testing.B) {
	m := New()
	finished := make(chan struct{}, b.N)
	_, set := GetSet(m, finished)
	m.Set(Int64Key(111), "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set(Int64Key(111), "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

func BenchmarkMultiGetSame(b *testing.B) {
	m := New()
	finished := make(chan struct{}, b.N)
	get, _ := GetSet(m, finished)
	m.Set(Int64Key(111), "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		get(Int64Key(111), "value")
	}
	for i := 0; i < b.N; i++ {
		<-finished
	}
}

func benchmarkMultiGetSetDifferent(b *testing.B, shardsCount int) {
	m := NewN(shardsCount)
	finished := make(chan struct{}, 2*b.N)
	get, set := GetSet(m, finished)
	m.Set(Int64Key(0), "value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := Int64Key(i)
		set(id, "value")
		get(id+1, "value")
	}
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func BenchmarkMultiGetSetDifferent_1_Shard(b *testing.B) {
	benchmarkMultiGetSetDifferent(b, 1)
}
func BenchmarkMultiGetSetDifferent_16_Shard(b *testing.B) {
	benchmarkMultiGetSetDifferent(b, 16)
}
func BenchmarkMultiGetSetDifferent_32_Shard(b *testing.B) {
	benchmarkMultiGetSetDifferent(b, 32)
}
func BenchmarkMultiGetSetDifferent_256_Shard(b *testing.B) {
	benchmarkMultiGetSetDifferent(b, 256)
}

func benchmarkMultiGetSetBlock(b *testing.B, shardsCount int) {
	m := NewN(shardsCount)
	finished := make(chan struct{}, 2*b.N)
	get, set := GetSet(m, finished)
	for i := 0; i < b.N; i++ {
		id := Int64Key(i)
		m.Set(id%100, "value")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := Int64Key(i)
		set(id%100, "value")
		get(id%100, "value")
	}
	for i := 0; i < 2*b.N; i++ {
		<-finished
	}
}

func BenchmarkMultiGetSetBlock_1_Shard(b *testing.B) {
	benchmarkMultiGetSetBlock(b, 1)
}
func BenchmarkMultiGetSetBlock_16_Shard(b *testing.B) {
	benchmarkMultiGetSetBlock(b, 16)
}
func BenchmarkMultiGetSetBlock_32_Shard(b *testing.B) {
	benchmarkMultiGetSetBlock(b, 32)
}
func BenchmarkMultiGetSetBlock_256_Shard(b *testing.B) {
	benchmarkMultiGetSetBlock(b, 256)
}

func GetSet(m *ConcurrentMap, finished chan struct{}) (set func(key KeyType, value string), get func(key KeyType, value string)) {
	return func(key KeyType, value string) {
			for i := 0; i < 10; i++ {
				m.Get(key)
			}
			finished <- struct{}{}
		}, func(key KeyType, value string) {
			for i := 0; i < 10; i++ {
				m.Set(key, value)
			}
			finished <- struct{}{}
		}
}

func BenchmarkKeys(b *testing.B) {
	m := New()

	// Insert 100 elements.
	for i := 0; i < 10000; i++ {
		id := Int64Key(i)
		m.Set(id, Animal{id})
	}
	for i := 0; i < b.N; i++ {
		m.Keys()
	}
}
