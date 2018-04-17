package keys

import (
	"strings"

	"strconv"

	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/util/encoding"
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
	return &metapb.DocID{SlotID: uint32(slotID), SeqNo: seqNo}, nil
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
