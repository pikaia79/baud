package store

import (
	"fmt"
	"encoding/binary"

	"github.com/tiglabs/baud/util"
)

type KEY_TYPE byte

const (
	TYPE_S KEY_TYPE = 'S'
	TYPE_F KEY_TYPE = 'F'
)

var KeyType_name = map[byte]string{
	'S': "Source",
	'F': "Field",
}

var KeyType_value = map[string]byte{
	"Source":   'S',
	"Field":    'F',
}

func (x KEY_TYPE) String() string {
	s, ok := KeyType_name[byte(x)]
	if ok {
		return s
	}
	return "unknown"
}

// document 存储格式: <key>                       <value>
//               [type]+[slot ID]+[doc ID]      [document]
// slot ID 编码到key中可以方便整体迁移指定slot上的文档

func decodeStoreKey(key []byte) (docID string, slotID uint32, err error) {
	if len(key) <= 5 {
		err = fmt.Errorf("key[%+v] can't be identified", key)
		return
	}
	if key[0] != byte(TYPE_S) {
		err = fmt.Errorf("invalid key type %s", KEY_TYPE(key[0]).String())
		return
	}
	slotID = binary.BigEndian.Uint32(key[1:])
	docID = util.SliceToString(key[5:])
	return
}

func encodeStoreKey(slotID uint32, docID string) []byte {
	key := make([]byte, 5 + len(docID))
	key[0] = byte(TYPE_S)
	binary.BigEndian.PutUint32(key[1:], slotID)
	copy(key[5:], util.StringToSlice(docID))
	return key
}

func encodeStoreValue(value []byte) []byte {
	return value
}

func decodeStoreValue(value []byte) []byte {
	return value
}
