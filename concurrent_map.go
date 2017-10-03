package cmap

import (
	"encoding/json"
	"sync"
)

// ConcurrentMap A "thread" safe map of type KeyType:Anything.
// To avoid lock bottlenecks this map is dived to several (SHARD_COUNT) map shards.
type ConcurrentMap struct {
	shards []*mapShard
}

// mapShard A "thread" safe KeyType to anything map.
type mapShard struct {
	items        map[KeyType]interface{}
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

func (s *mapShard) Set(key KeyType, value interface{}) {
	s.Lock()
	s.items[key] = value
	s.Unlock()
}

// New creates a new concurrent map with 32 shards
func New() *ConcurrentMap {
	return NewN(32)
}

// NewN creates a new concurrent map with N shards
func NewN(shardCount int) *ConcurrentMap {
	shards := make([]*mapShard, shardCount)
	for i := 0; i < len(shards); i++ {
		shards[i] = &mapShard{items: make(map[KeyType]interface{})}
	}
	return &ConcurrentMap{shards}
}

// getShard returns shard under given key
func (m *ConcurrentMap) getShard(key KeyType) *mapShard {
	return m.shards[key.ShardKey()%uint(len(m.shards))]
}

// MSet func
func (m *ConcurrentMap) MSet(data map[KeyType]interface{}) {
	for key, value := range data {
		m.getShard(key).Set(key, value)
	}
}

// Set sets the given value under the specified key.
func (m *ConcurrentMap) Set(key KeyType, value interface{}) {
	m.getShard(key).Set(key, value)
}

// UpsertCb Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
type UpsertCb func(exist bool, valueInMap interface{}, newValue interface{}) interface{}

// Upsert - Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m *ConcurrentMap) Upsert(key KeyType, value interface{}, cb UpsertCb) (res interface{}) {
	shard := m.getShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// SetIfAbsent Sets the given value under the specified key if no value was associated with it.
func (m *ConcurrentMap) SetIfAbsent(key KeyType, value interface{}) bool {
	// Get map shard.
	shard := m.getShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

// Get retrieves an element from map under given key.
func (m *ConcurrentMap) Get(key KeyType) (interface{}, bool) {
	// Get shard
	shard := m.getShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Miss func
type Miss func(key KeyType) (interface{}, error)

// MustGet retrieves an element from map under given key. If the key doesn't exist, it's value is loaded
func (m *ConcurrentMap) MustGet(key KeyType, load Miss) (val interface{}, err error) {
	// Get shard
	shard := m.getShard(key)
	shard.Lock()
	// Get item from shard.
	val, ok := shard.items[key]
	if ok {
		shard.Unlock()
	} else {
		defer shard.Unlock() // use defer in case `load` panics
		if val, err = load(key); err != nil {
			val = nil
		} else {
			shard.items[key] = val
		}
	}
	return
}

// Count returns the number of elements within the map.
func (m *ConcurrentMap) Count() int {
	count := 0
	for i := 0; i < len(m.shards); i++ {
		shard := m.shards[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Has looks up an item under specified key
func (m *ConcurrentMap) Has(key KeyType) bool {
	// Get shard
	shard := m.getShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Remove removes an element from the map.
func (m *ConcurrentMap) Remove(key KeyType) {
	// Try to get shard.
	shard := m.getShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// Pop removes an element from the map and returns it
func (m *ConcurrentMap) Pop(key KeyType) (v interface{}, exists bool) {
	// Try to get shard.
	shard := m.getShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// IsEmpty return true if the map is empty.
func (m *ConcurrentMap) IsEmpty() bool {
	return m.Count() == 0
}

// Tuple is used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type Tuple struct {
	Key KeyType
	Val interface{}
}

// Iter returns an iterator which could be used in a for range loop.
//
// Deprecated: using IterBuffered() will get a better performence
func (m ConcurrentMap) Iter() <-chan Tuple {
	chans := m.snapshot()
	ch := make(chan Tuple)
	go fanIn(chans, ch)
	return ch
}

// IterBuffered returns a buffered iterator which could be used in a for range loop.
func (m *ConcurrentMap) IterBuffered() <-chan Tuple {
	chans := m.snapshot()
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan Tuple, total)
	go fanIn(chans, ch)
	return ch
}

// snapshot returns a array of channels that contains elements in each shard.
// It returns once the size of each buffered channel is determined,
// before all the channels are populated using goroutines.
func (m *ConcurrentMap) snapshot() (chans []chan Tuple) {
	shardCount := len(m.shards)
	chans = make([]chan Tuple, shardCount)
	wg := sync.WaitGroup{}
	wg.Add(shardCount)
	// Foreach shard.
	for index, shard := range m.shards {
		go func(index int, shard *mapShard) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan Tuple, len(shard.items))
			wg.Done()
			for key, val := range shard.items {
				chans[index] <- Tuple{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

// fanIn reads elements from channels `chans` into channel `out`
func fanIn(chans []chan Tuple, out chan Tuple) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch chan Tuple) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

// Items returns all items as map[KeyType]interface{}
func (m *ConcurrentMap) Items() map[KeyType]interface{} {
	tmp := make(map[KeyType]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

// IterCb is called for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the shards
type IterCb func(key KeyType, v interface{})

// IterCb callback based iterator, cheapest way to read
// all elements in a map.
func (m *ConcurrentMap) IterCb(fn IterCb) {
	for idx := range m.shards {
		shard := m.shards[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

// Keys returns all keys as []KeyType
func (m *ConcurrentMap) Keys() []KeyType {
	count := m.Count()
	ch := make(chan KeyType, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(len(m.shards))
		for _, shard := range m.shards {
			go func(shard *mapShard) {
				// Foreach key, value pair.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys
	keys := make([]KeyType, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

// MarshalJSON ConcurrentMap "private" variables to json marshal.
func (m *ConcurrentMap) MarshalJSON() ([]byte, error) {
	// Create a temporary map, which will hold all items spread across shards.
	tmp := make(map[KeyType]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

//
//
//

// LockKey creates a lock for the given key and allows actions to be performed with that key.
func (m *ConcurrentMap) LockKey(key KeyType, fn func(*KeyMap)) {
	// Get shard
	shard := m.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	fn(&KeyMap{key, shard.items})
}

// KeyMap struct
type KeyMap struct {
	key   KeyType
	items map[KeyType]interface{}
}

// Set sets the given value under the specified key.
func (m *KeyMap) Set(value interface{}) {
	m.items[m.key] = value
}

// Get retrieves an element from map under given key.
func (m *KeyMap) Get() (interface{}, bool) {
	val, ok := m.items[m.key]
	return val, ok
}

// Has looks up an item under specified key
func (m *KeyMap) Has() bool {
	_, ok := m.items[m.key]
	return ok
}

// Remove removes an element from the map.
func (m *KeyMap) Remove() {
	delete(m.items, m.key)
}

// Pop removes an element from the map and returns it
func (m *KeyMap) Pop() (v interface{}, exists bool) {
	v, exists = m.items[m.key]
	delete(m.items, m.key)
	return v, exists
}
