package cmap

import (
	"encoding/json"
	"errors"
	"sort"
	"testing"
)

type Animal struct {
	id int64
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
	elephant := Animal{111}
	monkey := Animal{222}

	m.Set(111, elephant)
	m.Set(222, monkey)

	if m.Count() != 2 {
		t.Error("map should contain exactly two elements.")
	}
}

func TestInsertAbsent(t *testing.T) {
	m := New()
	elephant := Animal{111}
	monkey := Animal{222}

	m.SetIfAbsent(111, elephant)
	if ok := m.SetIfAbsent(111, monkey); ok {
		t.Error("map set a new value even the entry is already present")
	}
}

func TestGet(t *testing.T) {
	m := New()

	// Get a missing element.
	val, ok := m.Get(333)

	if ok == true {
		t.Error("ok should be false when item is missing from map.")
	}

	if val != nil {
		t.Error("Missing values should return as null.")
	}

	elephant := Animal{111}
	m.Set(111, elephant)

	// Retrieve inserted element.

	tmp, ok := m.Get(111)
	elephant = tmp.(Animal) // Type assertion.

	if ok == false {
		t.Error("ok should be true for item stored within the map.")
	}

	if &elephant == nil {
		t.Error("expecting an element, not null.")
	}

	if elephant.id != 111 {
		t.Error("item was modified.")
	}
}

func TestMustGet(t *testing.T) {
	m := New()

	// Get a missing element, but return an error.
	val, err := m.MustGet(111, func(id int64) (interface{}, error) {
		return Animal{id}, errors.New("error")
	})
	if err == nil {
		t.Error("err should not be nil when func returns an error.")
	}
	if val != nil {
		t.Error("Missing values should return as null.")
	}

	// Get a missing element.
	val, err = m.MustGet(111, func(id int64) (interface{}, error) {
		return Animal{id}, nil
	})
	elephant := val.(Animal) // Type assertion.
	if err != nil {
		t.Error("err should be nil when func succeeds.")
	}
	if &elephant == nil {
		t.Error("expecting an element, not null.")
	}
	if elephant.id != 111 {
		t.Error("item was modified.")
	}
}

func TestHas(t *testing.T) {
	m := New()

	// Get a missing element.
	if m.Has(333) == true {
		t.Error("element shouldn't exists")
	}

	elephant := Animal{111}
	m.Set(111, elephant)

	if m.Has(111) == false {
		t.Error("element exists, expecting Has to return True.")
	}
}

func TestRemove(t *testing.T) {
	m := New()

	monkey := Animal{222}
	m.Set(222, monkey)

	m.Remove(222)

	if m.Count() != 0 {
		t.Error("Expecting count to be zero once item was removed.")
	}

	temp, ok := m.Get(222)

	if ok != false {
		t.Error("Expecting ok to be false for missing items.")
	}

	if temp != nil {
		t.Error("Expecting item to be nil after its removal.")
	}

	// Remove a none existing element.
	m.Remove(10001)
}

func TestPop(t *testing.T) {
	m := New()

	monkey := Animal{222}
	m.Set(222, monkey)

	v, exists := m.Pop(222)

	if !exists {
		t.Error("Pop didn't find a monkey.")
	}

	m1, ok := v.(Animal)

	if !ok || m1 != monkey {
		t.Error("Pop found something else, but monkey.")
	}

	v2, exists2 := m.Pop(222)
	m1, ok = v2.(Animal)

	if exists2 || ok || m1 == monkey {
		t.Error("Pop keeps finding monkey")
	}

	if m.Count() != 0 {
		t.Error("Expecting count to be zero once item was Pop'ed.")
	}

	temp, ok := m.Get(222)

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
		id := int64(i)
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

	m.Set(111, Animal{111})

	if m.IsEmpty() != false {
		t.Error("map shouldn't be empty.")
	}
}

func TestIterator(t *testing.T) {
	m := New()

	// Insert 100 elements.
	for i := 0; i < 100; i++ {
		id := int64(i)
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
		id := int64(i)
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
		id := int64(i)
		m.Set(id, Animal{id})
	}

	counter := 0
	// Iterate over elements.
	m.IterCb(func(key int64, v interface{}) {
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
		id := int64(i)
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
			id := int64(i)
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
			id := int64(i)
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
	m.Set(1, 1)
	m.Set(2, 2)
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
		id := int64(i)
		m.Set(id, Animal{id})
	}

	keys := m.Keys()
	if len(keys) != 100 {
		t.Error("We should have counted 100 elements.")
	}
}

func TestMInsert(t *testing.T) {
	animals := map[int64]interface{}{
		111: Animal{111},
		222: Animal{222},
	}
	m := New()
	m.MSet(animals)

	if m.Count() != 2 {
		t.Error("map should contain exactly two elements.")
	}
}

func TestUpsert(t *testing.T) {
	dolphin := Animal{444}
	whale := Animal{555}
	tiger := Animal{666}
	lion := Animal{777}

	cb := func(exists bool, valueInMap interface{}, newValue interface{}) interface{} {
		nv := newValue.(Animal)
		if !exists {
			return []Animal{nv}
		}
		res := valueInMap.([]Animal)
		return append(res, nv)
	}

	m := New()
	m.Set(888, []Animal{dolphin})
	m.Upsert(888, whale, cb)
	m.Upsert(999, tiger, cb)
	m.Upsert(999, lion, cb)

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

	marineAnimals, ok := m.Get(888)
	if !ok || !compare(marineAnimals.([]Animal), []Animal{dolphin, whale}) {
		t.Error("Set, then Upsert failed")
	}

	predators, ok := m.Get(999)
	if !ok || !compare(predators.([]Animal), []Animal{tiger, lion}) {
		t.Error("Upsert, then Upsert failed")
	}
}
