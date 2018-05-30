package metapb

type (
	// DBID is a custom type for database ID
	DBID = uint32
	// SpaceID is a custom type for space ID
	SpaceID = uint32
	// PartitionID is a custom type for partition ID
	PartitionID = uint64
	// SlotID is a custom type for slot ID
	SlotID = uint32
	// ReplicaID is a custom type for repl ID
	ReplicaID = uint64
	// NodeID is a custom type for node ID
	NodeID = uint32
	// Key is a custom type for key
	Key = []byte
	// Value is a custom type for key
	Value = []byte
	// RespCode Response code
	RespCode = uint16
)

const (
	/** Common Response Code **/
	RESP_CODE_OK RespCode = 0
	//RESP_CODE_OK           RespCode = 200
	RESP_CODE_MSG_TOOLARGE RespCode = 414
	RESP_CODE_SERVER_ERROR RespCode = 500
	RESP_CODE_TIMEOUT      RespCode = 504
	RESP_CODE_SERVER_BUSY  RespCode = 509
	RESP_CODE_SERVER_STOP  RespCode = 511

	/** Master Service Response Code **/
	MASTER_RESP_CODE_HEARTBEAT_REGISTRY   RespCode = 600
	MASTER_RESP_CODE_EMPTY_FOLLOWERS      RespCode = 601
	MASTER_RESP_CODE_NOT_LEADER           RespCode = 602
	MASTER_RESP_CODE_DB_NOTEXISTS         RespCode = 603
	MASTER_RESP_CODE_SPACE_NOTEXISTS      RespCode = 604
	MASTER_RESP_CODE_ROUTE_NOTEXISTS      RespCode = 605
	MASTER_RESP_CODE_PS_NOTEXISTS         RespCode = 606
	MASTER_RESP_CODE_NO_FOLLOWER_LEADER   RespCode = 607
	MASTER_RESP_CODE_NO_LEADER            RespCode = 608
	MASTER_RESP_CODE_METHOD_NOT_IMPLEMENT RespCode = 609

	/** PS Service Response Code **/
	PS_RESP_CODE_NOT_LEADER     RespCode = 400
	PS_RESP_CODE_NO_PARTITION   RespCode = 404
	PS_RESP_CODE_NO_LEADER      RespCode = 503
	PS_RESP_CODE_KEY_EXISTS     RespCode = 409
	PS_RESP_CODE_KEY_NOT_EXISTS RespCode = 410
)
