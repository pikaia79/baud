package index

import (
	"testing"
	"github.com/tiglabs/baudengine/kernel/document"
)

func TestDecodeEncodeFileName(t *testing.T) {
	key := encodeStoreFieldKey([]byte("1"), "name")
	name, err := decodeStoreFieldKey(key)
	if err != nil {
		t.Fatal("encode field failed")
	}
	if name != "name" {
		t.Fatal("encode field failed")
	}
}

func TestDecodeEncodeFileValue(t *testing.T) {
	field := document.NewTextField("text", []byte("hello word"), document.StoreField)
	key, row, err := encodeStoreField([]byte("1"), []document.Field{field})
	if err != nil {
		t.Fatal("encode field failed")
	}
	name, err := decodeStoreFieldKey(key)
	if err != nil {
		t.Fatal("decode field failed")
	}
	if name != "text" {
		t.Fatal("decode field failed")
	}
	fs, err := decodeStoreField("text", row)
	if err != nil {
		t.Fatal("decode field failed")
	}
	if len(fs) != 1 {
		t.Fatal("decode field failed")
	}
	f := fs[0]
	if tf, ok := f.(*document.TextField); !ok {
		t.Fatal("invalid field")
	} else {
		if tf.Name() != "text" {
			t.Fatal("invalid field")
		}
		if string(tf.Value()) != "hello word" {
			t.Fatal("invalid field")
		}
	}
}

func TestDecodeEncodeFilesValue(t *testing.T) {
	field1 := document.NewTextField("text", []byte("hello word"), document.StoreField)
	field2 := document.NewTextField("text", []byte("good, baud"), document.StoreField)
	key, row, err := encodeStoreField([]byte("1"), []document.Field{field1, field2})
	if err != nil {
		t.Fatal("encode field failed")
	}
	name, err := decodeStoreFieldKey(key)
	if err != nil {
		t.Fatal("decode field failed")
	}
	if name != "text" {
		t.Fatal("decode field failed")
	}
	fs, err := decodeStoreField("text", row)
	if err != nil {
		t.Fatal("decode field failed")
	}
	if len(fs) != 2 {
		t.Fatal("decode field failed")
	}
	for i, f := range fs {
		if tf, ok := f.(*document.TextField); !ok {
			t.Fatal("invalid field")
		} else {
			if tf.Name() != "text" {
				t.Fatal("invalid field")
			}
			if i == 0 {
				if string(tf.Value()) != "hello word" {
					t.Fatal("invalid field")
				}
			} else {
				if string(tf.Value()) != "good, baud" {
					t.Fatal("invalid field")
				}
			}

		}
	}
}
