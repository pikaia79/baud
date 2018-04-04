package store

import (
	"fmt"
	"errors"
	"encoding/binary"

	"baud/kernel/document"
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

// document 存储格式: <key>                                                                          <value>
//               [type(1 byte)]+[space ID(4 bytes)]+[slot ID(4 bytes)]+[Custom ID(8 bytes)] ...     [document]
// slot ID 编码到key中可以方便整体迁移指定slot上的文档
// 对于 edge的UID pair的编码,暂时按照<UID,UID>编码,后面再考虑优化,比如spaceID或者slotID相同
func decodeUID(key []byte) (doc document.UID, err error) {
	if len(key) < 16 {
		err = fmt.Errorf("key[%+v] can't be identified", key)
		return
	}
	spaceID := binary.BigEndian.Uint32(key[1:])
	slotID := binary.BigEndian.Uint32(key[5:])
	cusID := binary.BigEndian.Uint64(key[9:])
	return document.UID{SpaceID: spaceID, SlotID: slotID, AutoIncrID: cusID}, nil
}

func decodeStoreKey(key []byte) (uids []document.UID, err error) {
	if len(key) != 17 && len(key) != 33 {
		err = fmt.Errorf("key[%+v] can't be identified", key)
		return
	}
	if key[0] != byte(TYPE_S) {
		err = fmt.Errorf("invalid key type %s", KEY_TYPE(key[0]).String())
		return
	}
	if len(key) == 17 {
		uids = make([]document.UID, 1)
		var doc document.UID
		doc, err = decodeUID(key[1:])
		if err != nil {
			return
		}
		uids[0] = doc
	} else {
		uids = make([]document.UID, 2)
		var doc document.UID
		doc, err = decodeUID(key[1:])
		if err != nil {
			return
		}
		uids[0] = doc
		doc, err = decodeUID(key[17:])
		if err != nil {
			return
		}
		uids[1] = doc
	}

	return
}

func encodeUID(uid document.UID) ([]byte, error) {
	if !uid.Valid() {
		return nil, fmt.Errorf("invalid uid %v", uid)
	}
	key := make([]byte, 16)
	binary.BigEndian.PutUint32(key[:], uid.SpaceID)
	binary.BigEndian.PutUint32(key[4:], uid.SlotID)
	binary.BigEndian.PutUint64(key[8:], uid.AutoIncrID)
	return key, nil
}

func encodeStoreKey(uids ...document.UID) ([]byte, error) {
	var key []byte
	if len(uids) == 1 {
		key = make([]byte, 17)
		key[0] = byte(TYPE_S)
		u, err := encodeUID(uids[0])
		if err != nil {
			return nil, err
		}
		copy(key[1:], u)
	} else if len(uids) == 2 {
		key = make([]byte, 1, 33)
		key[0] = byte(TYPE_S)
		u, err := encodeUID(uids[0])
		if err != nil {
			return nil, err
		}
		copy(key[1:], u)
		u, err = encodeUID(uids[1])
		if err != nil {
			return nil, err
		}
		copy(key[17:], u)
	} else {
		return nil, errors.New("")
	}
	return key, nil
}

func encodeStoreValue(value []byte) ([]byte, error) {
	return value
}

func decodeStoreValue(value []byte) ([]byte, error) {
	return value
}
