package cmap

import (
	"hash/fnv"
	"testing"
)

func TestFnv32(t *testing.T) {
	key := "ABC"

	hasher := fnv.New32()
	hasher.Write([]byte(key))
	got := uint32(StringKey(key).ShardKey())
	want := hasher.Sum32()
	if got != want {
		t.Errorf("Bundled StringKey.ShardKey produced %d, expected result from hash/fnv32 is %d", got, want)
	}
}
