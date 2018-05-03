package keys

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util/encoding"
)

const (
	docIDSeparator = "/"
)

// EncodeDocID encode DocID to bytes
func EncodeDocID(id *metapb.DocID) metapb.Key {
	key := encoding.EncodeUvarintAscending(nil, uint64(id.SlotID))
	key = encoding.EncodeUvarintAscending(key, id.SeqNo)
	return key
}

// EncodeDocIDToString encode DocID to string
func EncodeDocIDToString(id *metapb.DocID) string {
	return strings.Join([]string{strconv.FormatUint(uint64(id.SlotID), 10), strconv.FormatUint(id.SeqNo, 10)}, docIDSeparator)
}

// DecodeDocID decode bytes to DocID
func DecodeDocID(id metapb.Key) (*metapb.DocID, error) {
	key, slotID, err := encoding.DecodeUvarintAscending(id)
	if err != nil {
		return nil, err
	}
	_, seqNo, err := encoding.DecodeUvarintAscending(key)
	if err != nil {
		return nil, err
	}
	return &metapb.DocID{SlotID: metapb.SlotID(slotID), SeqNo: seqNo}, nil
}

// DecodeDocIDToString decode bytes to string
func DecodeDocIDToString(id metapb.Key) (string, error) {
	key, slotID, err := encoding.DecodeUvarintAscending(id)
	if err != nil {
		return "", err
	}
	_, seqNo, err := encoding.DecodeUvarintAscending(key)
	if err != nil {
		return "", err
	}
	return strings.Join([]string{strconv.FormatUint(slotID, 10), strconv.FormatUint(seqNo, 10)}, docIDSeparator), nil
}

// DecodeDocIDFromString decode string to DocID
func DecodeDocIDFromString(id string) (*metapb.DocID, error) {
	ids := strings.Split(id, docIDSeparator)
	if len(ids) != 2 {
		return nil, fmt.Errorf("The given id(%s) is not a valid DocID", id)
	}
	slotID, err := strconv.ParseUint(ids[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("The given id(%s) parse slotID error: %s", id, err.Error())
	}
	seqNo, err := strconv.ParseUint(ids[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("The given id(%s) parse seqNo error: %s", id, err.Error())
	}

	return &metapb.DocID{SlotID: metapb.SlotID(slotID), SeqNo: seqNo}, nil
}
