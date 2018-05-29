package index

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/proto/metapb"
)

func (r *IndexDriver) GetApplyID() (uint64, error) {
	if r == nil {
		return 0, nil
	}
	v, err := r.store.Get(RAFT_APPLY_ID)
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, errors.New("invalid applyID value in store")
	}
	return binary.BigEndian.Uint64(v), nil
}

func (r *IndexDriver) GetDocument(ctx context.Context, docID metapb.Key, fields []uint32) (map[uint32]pspb.FieldValue, bool) {
	tx, err := r.store.NewTransaction(false)
	if err != nil {
		// todo panic ???
		return nil, false
	}
	defer tx.Rollback()
	fieldValues := make(map[uint32]pspb.FieldValue)
	for _, fieldId := range fields {
		value, err := tx.Get(encodeStoreFieldKey(docID, fieldId))
		if err != nil {
			return nil, false
		}
		field, err := decodeStoreField(fieldId, value)
		if err != nil {
			return nil, false
		}
		fieldValues[fieldId] = field.FieldValue
	}
	if len(fieldValues) > 0 {
		return fieldValues, true
	}
	return nil, false
}

func (r *IndexDriver) Close() error {
	if r.store != nil {
		return r.store.Close()
	}
	return nil
}