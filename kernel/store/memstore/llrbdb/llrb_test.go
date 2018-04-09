package llrbdb

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestSetGetDelete(t *testing.T) {
	db, err := NewDB()
	if err != nil {
		t.Fatal(err)
	}
	var expectedValues [][]byte
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("val_%d", i))
		err = db.Update(func(tx *Tx) error {
			_, _, err = tx.Set(key, value)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		expectedValues = append(expectedValues, value)
	}
	var realValues [][]byte
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		err = db.View(func(tx *Tx) error {
			v, err := tx.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			realValues = append(realValues, v.([]byte))
			return nil
		})
	}
	if !reflect.DeepEqual(realValues, expectedValues) {
		t.Fatalf("expected values %v, got %v", expectedValues, realValues)
	}

	// delete key_1, key_2
	for i := 1; i <= 2; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		err = db.Update(func(tx *Tx) error {
			_, err = tx.Delete(key)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= 2; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		err = db.View(func(tx *Tx) error {
			_, err = tx.Get(key)
			return err
		})
		if err == nil {
			t.Fatalf("get delete key %s", key)
		}
	}
	expectedValues = expectedValues[:0]
	for i := 0; i < 10; i++ {
		if i == 1 || i == 2 {
			continue
		}
		value := []byte(fmt.Sprintf("val_%d", i))
		expectedValues = append(expectedValues, value)
	}
	realValues = realValues[:0]
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		err = db.View(func(tx *Tx) error {
			v, err := tx.Get(key)
			if err != nil {
				return err
			}
			realValues = append(realValues, v.([]byte))
			return nil
		})
	}
	if !reflect.DeepEqual(realValues, expectedValues) {
		t.Fatalf("expected values %v, got %v", expectedValues, realValues)
	}

	expectedValues = expectedValues[:0]
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("val_#%d", i))
		err = db.Update(func(tx *Tx) error {
			_, _, err = tx.Set(key, value)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		expectedValues = append(expectedValues, value)
	}
	realValues = realValues[:0]
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		err = db.View(func(tx *Tx) error {
			v, err := tx.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			realValues = append(realValues, v.([]byte))
			return nil
		})
	}
	if !reflect.DeepEqual(realValues, expectedValues) {
		t.Fatalf("expected values %v, got %v", expectedValues, realValues)
	}
}

func TestWriteTransaction(t *testing.T) {
	db, err := NewDB()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	var expectedValues [][]byte
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("val_%d", i))
		_, _, err = tx.Set(key, value)
		if err != nil {
			t.Fatal(err)
		}
		expectedValues = append(expectedValues, value)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	var realValues [][]byte
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		err = db.View(func(tx *Tx) error {
			v, err := tx.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			realValues = append(realValues, v.([]byte))
			return nil
		})
	}
	if !reflect.DeepEqual(realValues, expectedValues) {
		t.Fatalf("expected values %v, got %v", expectedValues, realValues)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("val_#%d", i))
		_, _, err = tx.Set(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}
	// rollback
	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	var realValues1 [][]byte
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		err = db.View(func(tx *Tx) error {
			v, err := tx.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			realValues1 = append(realValues1, v.([]byte))
			return nil
		})
	}
	if !reflect.DeepEqual(realValues1, expectedValues) {
		t.Fatalf("expected values %v, got %v", expectedValues, realValues1)
	}
}

func TestReadTransaction(t *testing.T) {
	db, err := NewDB()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	var expectedValues [][]byte
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("val_%d", i))
		_, _, err = tx.Set(key, value)
		if err != nil {
			t.Fatal(err)
		}
		expectedValues = append(expectedValues, value)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for j := 0; j < 2; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_tx, err := db.Begin(false)
			if err != nil {
				t.Fatal(err)
			}
			var realValues [][]byte
			for i := 0; i < 10; i++ {
				key := []byte(fmt.Sprintf("key_%d", i))
				v, err := _tx.Get(key)
				if err != nil {
					t.Fatal(err)
				}
				realValues = append(realValues, v.([]byte))
			}
			if !reflect.DeepEqual(realValues, expectedValues) {
				t.Fatalf("expected values %v, got %v", expectedValues, realValues)
			}
		}()
	}
	wg.Wait()
}

type testRow struct {
	key []byte
	val interface{}
}

func batchWriteRows(db *DB, rows []testRow) (err error) {
	var tx *Tx
	tx, err = db.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	for _, row := range rows {
		_, _, err = tx.Set(row.key, row.val)
		if err != nil {
			return err
		}
	}

	return nil
}

func TestAscend(t *testing.T) {
	data := []testRow{
		{[]byte("apple"), []byte("val")},
		{[]byte("cat1"), []byte("val")},
		{[]byte("cat2"), []byte("val")},
		{[]byte("cat3"), []byte("val")},
		{[]byte("dog1"), []byte("val")},
		{[]byte("dog2"), []byte("val")},
		{[]byte("dog4"), []byte("val")},
		{[]byte("elephant"), []byte("val")},
	}
	expectedAll := [][]byte{
		[]byte("apple"),
		[]byte("cat1"),
		[]byte("cat2"),
		[]byte("cat3"),
		[]byte("dog1"),
		[]byte("dog2"),
		[]byte("dog4"),
		[]byte("elephant"),
	}

	expectedCats := [][]byte{
		[]byte("cat1"),
		[]byte("cat2"),
		[]byte("cat3"),
	}

	expectedBeforeDog := [][]byte{
		[]byte("apple"),
		[]byte("cat1"),
		[]byte("cat2"),
		[]byte("cat3"),
	}

	expectedAfterCat := [][]byte{
		[]byte("dog1"),
		[]byte("dog2"),
		[]byte("dog4"),
		[]byte("elephant"),
	}

	db, err := NewDB()
	if err != nil {
		t.Fatal(err)
	}
	err = batchWriteRows(db, data)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	var realKeys [][]byte
	tx.Ascend(func(key []byte, val interface{}) bool {
		realKeys = append(realKeys, key)
		return true
	})
	if !reflect.DeepEqual(realKeys, expectedAll) {
		t.Fatalf("expected keys %v, got %v", expectedAll, realKeys)
	}
	realKeys = realKeys[:0]
	tx.AscendKeys([]byte("cat*"), func(key []byte, val interface{}) bool {
		realKeys = append(realKeys, key)
		return true
	})
	if !reflect.DeepEqual(realKeys, expectedCats) {
		t.Fatalf("expected cats %v, got %v", expectedCats, realKeys)
	}

	realKeys = realKeys[:0]
	tx.AscendRange([]byte("cat1"), []byte("dog1"), func(key []byte, val interface{}) bool {
		realKeys = append(realKeys, key)
		return true
	})
	if !reflect.DeepEqual(realKeys, expectedCats) {
		t.Fatalf("expected cats %v, got %v", expectedCats, realKeys)
	}

	realKeys = realKeys[:0]
	tx.AscendLessThan([]byte("dog1"), func(key []byte, val interface{}) bool {
		realKeys = append(realKeys, key)
		return true
	})
	if !reflect.DeepEqual(realKeys, expectedBeforeDog) {
		t.Fatalf("expected before dog %v, got %v", expectedBeforeDog, realKeys)
	}

	realKeys = realKeys[:0]
	tx.AscendGreaterOrEqual([]byte("dog1"), func(key []byte, val interface{}) bool {
		realKeys = append(realKeys, key)
		return true
	})
	if !reflect.DeepEqual(realKeys, expectedAfterCat) {
		t.Fatalf("expected after cat %v, got %v", expectedAfterCat, realKeys)
	}

	realKeys = realKeys[:0]
	tx.AscendEqual([]byte("dog1"), func(key []byte, val interface{}) bool {
		realKeys = append(realKeys, key)
		return true
	})
	if !reflect.DeepEqual(realKeys, [][]byte{[]byte("dog1")}) {
		t.Fatalf("expected dog1, got %v", realKeys)
	}
}

func TestDescend(t *testing.T) {
	data := []testRow{
		{[]byte("apple"), []byte("val")},
		{[]byte("cat1"), []byte("val")},
		{[]byte("cat2"), []byte("val")},
		{[]byte("cat3"), []byte("val")},
		{[]byte("dog1"), []byte("val")},
		{[]byte("dog2"), []byte("val")},
		{[]byte("dog4"), []byte("val")},
		{[]byte("elephant"), []byte("val")},
	}
	expectedAll := [][]byte{
		[]byte("elephant"),
		[]byte("dog4"),
		[]byte("dog2"),
		[]byte("dog1"),
		[]byte("cat3"),
		[]byte("cat2"),
		[]byte("cat1"),
		[]byte("apple"),
	}

	expectedCats := [][]byte{
		[]byte("cat3"),
		[]byte("cat2"),
		[]byte("cat1"),
	}

	expectedBeforeDog := [][]byte{
		[]byte("cat3"),
		[]byte("cat2"),
		[]byte("cat1"),
		[]byte("apple"),
	}

	expectedAfterCat := [][]byte{
		[]byte("elephant"),
		[]byte("dog4"),
		[]byte("dog2"),
		[]byte("dog1"),
	}

	db, err := NewDB()
	if err != nil {
		t.Fatal(err)
	}
	err = batchWriteRows(db, data)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	var realKeys [][]byte
	tx.Descend(func(key []byte, val interface{}) bool {
		realKeys = append(realKeys, key)
		return true
	})
	if !reflect.DeepEqual(realKeys, expectedAll) {
		t.Fatalf("expected keys %v, got %v", expectedAll, realKeys)
	}
	realKeys = realKeys[:0]
	tx.DescendKeys([]byte("cat*"), func(key []byte, val interface{}) bool {
		realKeys = append(realKeys, key)
		return true
	})
	if !reflect.DeepEqual(realKeys, expectedCats) {
		t.Fatalf("expected cats %v, got %v", expectedCats, realKeys)
	}

	realKeys = realKeys[:0]
	fmt.Println("DescendRange ......")
	tx.DescendRange([]byte("cat3"), []byte("apple"), func(key []byte, val interface{}) bool {
		realKeys = append(realKeys, key)
		return true
	})
	if !reflect.DeepEqual(realKeys, expectedCats) {
		t.Fatalf("expected cats %v, got %v", expectedCats, realKeys)
	}

	realKeys = realKeys[:0]
	tx.DescendGreaterThan([]byte("cat3"), func(key []byte, val interface{}) bool {
		realKeys = append(realKeys, key)
		return true
	})
	if !reflect.DeepEqual(realKeys, expectedAfterCat) {
		t.Fatalf("expected after cat %v, got %v", expectedAfterCat, realKeys)
	}

	realKeys = realKeys[:0]
	tx.DescendLessOrEqual([]byte("dog"), func(key []byte, val interface{}) bool {
		realKeys = append(realKeys, key)
		return true
	})
	if !reflect.DeepEqual(realKeys, expectedBeforeDog) {
		t.Fatalf("expected defore dog %v, got %v", expectedBeforeDog, realKeys)
	}

	realKeys = realKeys[:0]
	tx.DescendEqual([]byte("dog1"), func(key []byte, val interface{}) bool {
		realKeys = append(realKeys, key)
		return true
	})
	if !reflect.DeepEqual(realKeys, [][]byte{[]byte("dog1")}) {
		t.Fatalf("expected dog1, got %v", realKeys)
	}
}
