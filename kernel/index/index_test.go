package index

import (
	"testing"
	"os"

	"github.com/tiglabs/baudengine/kernel/store/kvstore"
	"github.com/tiglabs/baudengine/kernel/store/kvstore/boltdb"
	"github.com/tiglabs/baudengine/kernel/document"
	"golang.org/x/net/context"
)


func open(t *testing.T) kvstore.KVStore {
	rv, err := boltdb.New(&boltdb.StoreConfig{Path: "test"})
	if err != nil {
		t.Fatal(err)
	}
	return rv
}

func cleanup(t *testing.T, s kvstore.KVStore) {
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = os.RemoveAll("test")
	if err != nil {
		t.Fatal(err)
	}
}

func TestAddDocument(t *testing.T) {
	store := open(t)
	defer cleanup(t, store)
	driver := NewIndexDriver(store)
	doc := document.NewDocument([]byte("1"))
	tf := document.NewTextField("text", []byte("hello, baud"), document.StoreField)
	doc.AddField(tf)

	err := driver.AddDocument(context.Background(), doc, 1)
	if err != nil {
		t.Fatalf("add document failed, err %v", err)
	}
	fvs, find := driver.GetDocument(context.Background(), []byte("1"), []string{"text"})
	if !find {
		t.Fatal("get docment failed")
	}
	if fvs["text"].(string) != "hello, baud" {
		t.Fatal("get document failed")
	}
}

func TestGetDocument(t *testing.T) {
	store := open(t)
	defer cleanup(t, store)
	driver := NewIndexDriver(store)
	doc := document.NewDocument([]byte("1"))
	tf := document.NewTextField("text", []byte("hello, baud"), document.StoreField)
	bf := document.NewBooleanField("bool", true, document.StoreField)
	doc.AddField(tf)
	doc.AddField(bf)

	err := driver.AddDocument(context.Background(), doc, 1)
	if err != nil {
		t.Fatalf("add document failed, err %v", err)
	}
	fvs, find := driver.GetDocument(context.Background(), []byte("1"), []string{"text"})
	if !find {
		t.Fatal("get docment failed")
	}
	if fvs["text"].(string) != "hello, baud" {
		t.Fatal("get document failed")
	}
	fvs, find = driver.GetDocument(context.Background(), []byte("1"), []string{"bool"})
	if !find {
		t.Fatal("get docment failed")
	}
	if fvs["bool"].(bool) != true {
		t.Fatal("get document failed")
	}
}

func TestDelDocument(t *testing.T) {
	store := open(t)
	defer cleanup(t, store)
	driver := NewIndexDriver(store)
	doc := document.NewDocument([]byte("1"))
	tf := document.NewTextField("text", []byte("hello, baud"), document.StoreField)
	doc.AddField(tf)

	err := driver.AddDocument(context.Background(), doc, 1)
	if err != nil {
		t.Fatalf("add document failed, err %v", err)
	}
	fvs, find := driver.GetDocument(context.Background(), []byte("1"), []string{"text"})
	if !find {
		t.Fatal("get docment failed")
	}
	if fvs["text"].(string) != "hello, baud" {
		t.Fatal("get document failed")
	}
	n, err := driver.DeleteDocument(context.Background(), []byte("1"), 1)
	if err != nil {
		t.Fatalf("del document failed, err %v", err)
	}
	if n != 1 {
		t.Fatal("del document failed")
	}
	_, find = driver.GetDocument(context.Background(), []byte("1"), []string{"text"})
	if find {
		t.Fatal("get docment failed")
	}
}

func TestUpdateDocument(t *testing.T) {
	store := open(t)
	defer cleanup(t, store)
	driver := NewIndexDriver(store)
	doc := document.NewDocument([]byte("1"))
	tf := document.NewTextField("text", []byte("hello, baud"), document.StoreField)
	vf := document.NewTextField("_version", []byte("1"), document.StoreField)
	doc.AddField(tf)
	doc.AddField(vf)

	err := driver.AddDocument(context.Background(), doc, 1)
	if err != nil {
		t.Fatalf("add document failed, err %v", err)
	}
	fvs, find := driver.GetDocument(context.Background(), []byte("1"), []string{"text"})
	if !find {
		t.Fatal("get docment failed")
	}
	if fvs["text"].(string) != "hello, baud" {
		t.Fatal("get document failed")
	}
	tf = document.NewTextField("text", []byte("hello, now"), document.StoreField)
	doc.DeleteField("text")
	doc.DeleteField("_version")
	vf = document.NewTextField("_version", []byte("1"), document.StoreField)
	doc.AddField(tf)
	doc.AddField(vf)
	found, err := driver.UpdateDocument(context.Background(), doc, false, 1)
	if err != nil {
		t.Fatalf("update document failed, err %v", err)
	}
	if !found {
		t.Fatal("update document failed")
	}

	fvs, find = driver.GetDocument(context.Background(), []byte("1"), []string{"text"})
	if !find {
		t.Fatal("get docment failed")
	}
	if fvs["text"].(string) != "hello, now" {
		t.Fatal("get document failed")
	}
}
