package cmap

import (
	"encoding/json"
	"strconv"
)

type KeyType interface {
	ShardKey() uint
	MarshalText() (text []byte, err error)
}

// Int64Key
type Int64Key int64

func (key Int64Key) ShardKey() uint {
	return uint(key)
}

// MarshalText Int64Key
func (key Int64Key) MarshalText() ([]byte, error) {
	return []byte(strconv.FormatInt(int64(key), 10)), nil
}

// StringKey
type StringKey string

const (
	offset32 = 2166136261
	prime32  = 16777619
)

func (key StringKey) ShardKey() uint {
	// FNV-1 https://golang.org/src/hash/fnv/fnv.go#L94
	hash := uint32(offset32)
	for _, c := range key {
		hash *= prime32
		hash ^= uint32(c)
	}
	return uint(hash)
}

// MarshalText Int64Key
func (key StringKey) MarshalText() ([]byte, error) {
	return json.Marshal(string(key))
}
