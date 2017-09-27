package cmap

import (
	"encoding/json"
	"errors"
	"sort"
	"testing"
)

type Animal struct {
	id KeyType
}

func TestMapCreation(t *testing.T) {
	m := New()
	if m == nil {
		t.Error("map is null.")
	}

	if m.Count() != 0 {
		t.Error("new map should be empty.")
	}
}

func TestInsert(t *testing.T) {
	m := New()
	elephant := Animal{Int64Key(111)}
	monkey := Animal{Int64Key(222)}

	m.Set(Int64Key(111), elephant)
	m.Set(Int64Key(222), monkey)

	if m.Count() != 2 {
		t.Error("map should contain exactly two elements.")
	}
}

func TestInsertAbsent(t *testing.T) {
	m := New()
	elephant := Animal{Int64Key(111)}
	monkey := Animal{Int64Key(222)}

	m.SetIfAbsent(Int64Key(111), elephant)
	if ok := m.SetIfAbsent(Int64Key(111), monkey); ok {
		t.Error("map set a new value even the entry is already present")
	}
}

func TestGet(t *testing.T) {
	m := New()

	// Get a missing element.
	val, ok := m.Get(Int64Key(333))

	if ok == true {
		t.Error("ok should be false when item is missing from map.")
	}

	if val != nil {
		t.Error("Missing values should return as null.")
	}

	elephant := Animal{Int64Key(111)}
	m.Set(Int64Key(Int64Key(111)), elephant)

	// Retrieve inserted element.

	tmp, ok := m.Get(Int64Key(111))
	elephant = tmp.(Animal) // Type assertion.

	if ok == false {
		t.Error("ok should be true for item stored within the map.")
	}

	if &elephant == nil {
		t.Error("expecting an element, not null.")
	}

	if elephant.id != Int64Key(111) {
		t.Error("item was modified.")
	}
}

func TestMustGet(t *testing.T) {
	m := New()

	// Get a missing element, but return an error.
	val, err := m.MustGet(Int64Key(111), func(id KeyType) (interface{}, error) {
		return Animal{id}, errors.New("error")
	})
	if err == nil {
		t.Error("err should not be nil when func returns an error.")
	}
	if val != nil {
		t.Error("Missing values should return as null.")
	}

	// Get a missing element.
	val, err = m.MustGet(Int64Key(111), func(id KeyType) (interface{}, error) {
		return Animal{id}, nil
	})
	elephant := val.(Animal) // Type assertion.
	if err != nil {
		t.Error("err should be nil when func succeeds.")
	}
	if &elephant == nil {
		t.Error("expecting an element, not null.")
	}
	if elephant.id != Int64Key(111) {
		t.Error("item was modified.")
	}
}

func TestHas(t *testing.T) {
	m := New()

	// Get a missing element.
	if m.Has(Int64Key(333)) == true {
		t.Error("element shouldn't exists")
	}

	elephant := Animal{Int64Key(111)}
	m.Set(Int64Key(111), elephant)

	if m.Has(Int64Key(111)) == false {
		t.Error("element exists, expecting Has to return True.")
	}
}

func TestRemove(t *testing.T) {
	m := New()

	monkey := Animal{Int64Key(222)}
	m.Set(Int64Key(222), monkey)

	m.Remove(Int64Key(222))

	if m.Count() != 0 {
		t.Error("Expecting count to be zero once item was removed.")
	}

	temp, ok := m.Get(Int64Key(222))

	if ok != false {
		t.Error("Expecting ok to be false for missing items.")
	}

	if temp != nil {
		t.Error("Expecting item to be nil after its removal.")
	}

	// Remove a none existing element.
	m.Remove(Int64Key(10001))
}

func TestPop(t *testing.T) {
	m := New()

	monkey := Animal{Int64Key(222)}
	m.Set(Int64Key(222), monkey)

	v, exists := m.Pop(Int64Key(222))

	if !exists {
		t.Error("Pop didn't find a monkey.")
	}

	m1, ok := v.(Animal)

	if !ok || m1 != monkey {
		t.Error("Pop found something else, but monkey.")
	}

	v2, exists2 := m.Pop(Int64Key(222))
	m1, ok = v2.(Animal)

	if exists2 || ok || m1 == monkey {
		t.Error("Pop keeps finding monkey")
	}

	if m.Count() != 0 {
		t.Error("Expecting count to be zero once item was Pop'ed.")
	}

	temp, ok := m.Get(Int64Key(222))

	if ok != false {
		t.Error("Expecting ok to be false for missing items.")
	}

	if temp != nil {
		t.Error("Expecting item to be nil after its removal.")
	}
}

func TestCount(t *testing.T) {
	m := New()
	for i := 0; i < 100; i++ {
		id := Int64Key(i)
		m.Set(id, Animal{id})
	}

	if m.Count() != 100 {
		t.Error("Expecting 100 element within map.")
	}
}

func TestIsEmpty(t *testing.T) {
	m := New()

	if m.IsEmpty() == false {
		t.Error("new map should be empty")
	}

	m.Set(Int64Key(111), Animal{Int64Key(111)})

	if m.IsEmpty() != false {
		t.Error("map shouldn't be empty.")
	}
}

func TestIterator(t *testing.T) {
	m := New()

	// Insert 100 elements.
	for i := 0; i < 100; i++ {
		id := Int64Key(i)
		m.Set(id, Animal{id})
	}

	counter := 0
	// Iterate over elements.
	for item := range m.Iter() {
		val := item.Val

		if val == nil {
			t.Error("Expecting an object.")
		}
		counter++
	}

	if counter != 100 {
		t.Error("We should have counted 100 elements.")
	}
}

func TestBufferedIterator(t *testing.T) {
	m := New()

	// Insert 100 elements.
	for i := 0; i < 100; i++ {
		id := Int64Key(i)
		m.Set(id, Animal{id})
	}

	counter := 0
	// Iterate over elements.
	for item := range m.IterBuffered() {
		val := item.Val

		if val == nil {
			t.Error("Expecting an object.")
		}
		counter++
	}

	if counter != 100 {
		t.Error("We should have counted 100 elements.")
	}
}

func TestIterCb(t *testing.T) {
	m := New()

	// Insert 100 elements.
	for i := 0; i < 100; i++ {
		id := Int64Key(i)
		m.Set(id, Animal{id})
	}

	counter := 0
	// Iterate over elements.
	m.IterCb(func(key KeyType, v interface{}) {
		_, ok := v.(Animal)
		if !ok {
			t.Error("Expecting an animal object")
		}

		counter++
	})
	if counter != 100 {
		t.Error("We should have counted 100 elements.")
	}
}

func TestItems(t *testing.T) {
	m := New()

	// Insert 100 elements.
	for i := 0; i < 100; i++ {
		id := Int64Key(i)
		m.Set(id, Animal{id})
	}

	items := m.Items()

	if len(items) != 100 {
		t.Error("We should have counted 100 elements.")
	}
}

func TestConcurrent(t *testing.T) {
	m := New()
	ch := make(chan int)
	const iterations = 1000
	var a [iterations]int

	// Using go routines insert 1000 ints into our map.
	go func() {
		for i := 0; i < iterations/2; i++ {
			id := Int64Key(i)
			// Add item to map.
			m.Set(id, i)

			// Retrieve item from map.
			val, _ := m.Get(id)

			// Write to channel inserted value.
			ch <- val.(int)
		} // Call go routine with current index.
	}()

	go func() {
		for i := iterations / 2; i < iterations; i++ {
			id := Int64Key(i)
			// Add item to map.
			m.Set(id, i)

			// Retrieve item from map.
			val, _ := m.Get(id)

			// Write to channel inserted value.
			ch <- val.(int)
		} // Call go routine with current index.
	}()

	// Wait for all go routines to finish.
	counter := 0
	for elem := range ch {
		a[counter] = elem
		counter++
		if counter == iterations {
			break
		}
	}

	// Sorts array, will make is simpler to verify all inserted values we're returned.
	sort.Ints(a[0:iterations])

	// Make sure map contains 1000 elements.
	if m.Count() != iterations {
		t.Error("Expecting 1000 elements.")
	}

	// Make sure all inserted values we're fetched from map.
	for i := 0; i < iterations; i++ {
		if i != a[i] {
			t.Error("missing value", i)
		}
	}
}

func TestJsonMarshal(t *testing.T) {
	expected := "{\"1\":1,\"2\":2}"
	m := NewN(2)
	m.Set(Int64Key(1), 1)
	m.Set(Int64Key(2), 2)
	j, err := json.Marshal(m)
	if err != nil {
		t.Error(err)
	}

	if string(j) != expected {
		t.Error("json", string(j), "differ from expected", expected)
		return
	}
}

func TestKeys(t *testing.T) {
	m := New()

	// Insert 100 elements.
	for i := 0; i < 100; i++ {
		id := Int64Key(i)
		m.Set(id, Animal{id})
	}

	keys := m.Keys()
	if len(keys) != 100 {
		t.Error("We should have counted 100 elements.")
	}
}

func TestMInsert(t *testing.T) {
	animals := map[KeyType]interface{}{
		Int64Key(111): Animal{Int64Key(111)},
		Int64Key(222): Animal{Int64Key(222)},
	}
	m := New()
	m.MSet(animals)

	if m.Count() != 2 {
		t.Error("map should contain exactly two elements.")
	}
}

func TestUpsert(t *testing.T) {
	dolphin := Animal{Int64Key(444)}
	whale := Animal{Int64Key(555)}
	tiger := Animal{Int64Key(666)}
	lion := Animal{Int64Key(777)}

	cb := func(exists bool, valueInMap interface{}, newValue interface{}) interface{} {
		nv := newValue.(Animal)
		if !exists {
			return []Animal{nv}
		}
		res := valueInMap.([]Animal)
		return append(res, nv)
	}

	m := New()
	m.Set(Int64Key(888), []Animal{dolphin})
	m.Upsert(Int64Key(888), whale, cb)
	m.Upsert(Int64Key(999), tiger, cb)
	m.Upsert(Int64Key(999), lion, cb)

	if m.Count() != 2 {
		t.Error("map should contain exactly two elements.")
	}

	compare := func(a, b []Animal) bool {
		if a == nil || b == nil {
			return false
		}

		if len(a) != len(b) {
			return false
		}

		for i, v := range a {
			if v != b[i] {
				return false
			}
		}
		return true
	}

	marineAnimals, ok := m.Get(Int64Key(888))
	if !ok || !compare(marineAnimals.([]Animal), []Animal{dolphin, whale}) {
		t.Error("Set, then Upsert failed")
	}

	predators, ok := m.Get(Int64Key(999))
	if !ok || !compare(predators.([]Animal), []Animal{tiger, lion}) {
		t.Error("Upsert, then Upsert failed")
	}
}

func TestUpsertStringKey(t *testing.T) {
	dolphin := Animal{StringKey("dolphin")}
	whale := Animal{StringKey("whale")}
	tiger := Animal{StringKey("tiger")}
	lion := Animal{StringKey("lion")}

	cb := func(exists bool, valueInMap interface{}, newValue interface{}) interface{} {
		nv := newValue.(Animal)
		if !exists {
			return []Animal{nv}
		}
		res := valueInMap.([]Animal)
		return append(res, nv)
	}

	m := New()
	m.Set(StringKey("marine"), []Animal{dolphin})
	m.Upsert(StringKey("marine"), whale, cb)
	m.Upsert(StringKey("predator"), tiger, cb)
	m.Upsert(StringKey("predator"), lion, cb)

	if m.Count() != 2 {
		t.Error("map should contain exactly two elements.")
	}

	compare := func(a, b []Animal) bool {
		if a == nil || b == nil {
			return false
		}

		if len(a) != len(b) {
			return false
		}

		for i, v := range a {
			if v != b[i] {
				return false
			}
		}
		return true
	}

	marineAnimals, ok := m.Get(StringKey("marine"))
	if !ok || !compare(marineAnimals.([]Animal), []Animal{dolphin, whale}) {
		t.Error("Set, then Upsert failed")
	}

	predators, ok := m.Get(StringKey("predator"))
	if !ok || !compare(predators.([]Animal), []Animal{tiger, lion}) {
		t.Error("Upsert, then Upsert failed")
	}
}

func TestKeysWhenRemoving(t *testing.T) {
	m := New()

	// Insert 100 elements.
	Total := 100
	for i := 0; i < Total; i++ {
		key := Int64Key(i + 1)
		m.Set(key, Animal{key})
	}

	// Remove 10 elements concurrently.
	Num := 10
	for i := 0; i < Num; i++ {
		go func(c *ConcurrentMap, n int) {
			c.Remove(Int64Key(n + 1))
		}(m, i)
	}
	keys := m.Keys()
	for _, k := range keys {
		n, ok := k.(Int64Key)
		if !ok {
			t.Error("Wrong key type returned")
		}
		if n == 0 {
			t.Error("Empty key returned")
		}
	}
}

func TestUndrainedIter(t *testing.T) {
	testUndrainedIterator(t, false)
}
func TestUndrainedIterBuffered(t *testing.T) {
	testUndrainedIterator(t, true)
}
func testUndrainedIterator(t *testing.T, buffered bool) {
	m := New()

	// Insert 100 elements
	Total := 100
	for i := 0; i < Total; i++ {
		key := Int64Key(i + 1)
		m.Set(key, Animal{key})
	}

	// Iterate over some elements
	counter := 0
	var ch <-chan Tuple
	if buffered {
		ch = m.IterBuffered()
	} else {
		ch = m.Iter()
	}
	for item := range ch {
		if item.Val == nil {
			t.Error("Expected an object.")
		}
		counter++
		if counter == 42 {
			break
		}
	}

	// Insert 100 more elements
	for i := Total; i < 2*Total; i++ {
		key := Int64Key(i + 1)
		m.Set(key, Animal{key})
	}

	// Finish previous iteration over 100 elements
	for item := range ch {
		if item.Val == nil {
			t.Error("Expected an object.")
		}
		counter++
	}
	if counter != Total {
		t.Error("We should have been right where we stopped")
	}

	// Iterate over all elements
	counter = 0
	if buffered {
		ch = m.IterBuffered()
	} else {
		ch = m.Iter()
	}
	for item := range ch {
		if item.Val == nil {
			t.Error("Expected an object.")
		}
		counter++
	}
	if counter != 2*Total {
		t.Error("We should have counted 200 elements.")
	}
}
