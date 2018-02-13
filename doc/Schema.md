#Baud Schema Descriptions

enum Type {
	STRING = 0;
	INT = 1;
	FLOAT = 2;
	BOOL = 3;
	DATATIME = 4;
}

enum Index {
	NOINDEX = 0;
	UNIQUE = 1;
	NONUNIQUE = 2;
	FULLTEXT = 3;
	ENGLISH = 4;
	CHINESE = 5;
}

message Field {
	uint16 id = 2;
	int value_type = 3;
	int index_type = 4;
}

message Class {
	uint16 class_id = 1;
	map[string, Field] fields = 2;
	string partition_key = 3;
}

message Space {
	string name = 1;
	uint32 space_id = 2;	//note space_id is global
	map[string, Class] classes = 3;
}

message OID {
	uint32 space_id;
	uint16 partition_hash;
	uint16 class_id;
	uint64 auto_incr_id;
}

message FID {
	uint32 space_id;
	uint16 class_id;
	uint16 field_id;
}

message Object {
	OID oid = 1;
	map[uint16, bytes] fields =2;
}

message Mutation {
	repeated Object inserts;
	repeated Object updates;
	repeated Object deletes;
}


