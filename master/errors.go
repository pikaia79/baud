package master

import (
	"github.com/pkg/errors"
)

//master global error definitions
var (
	ErrSuc			 			= errors.New("success")
	ErrIntarnalError			= errors.New("internal error")
	ErrParamError				= errors.New("param error")

	ErrDupDb					= errors.New("duplicated database")
	ErrDbNotExists				= errors.New("db not exists")
	ErrDupSpace					= errors.New("duplicated space")
	ErrSpaceNotExists			= errors.New("space not exists")

	ErrGenIdFailed 				= errors.New("generate id is failed")
	ErrBoltDbOpsFailed			= errors.New("boltdb operation error")
	ErrUnknownRaftCmdType 		= errors.New("unknown raft command type")
	//ErrEntryNotFound		    = errors.New("storage entry not found")
)

// http response error code and error message definitions
const (
	ERRCODE_SUCCESS 				= iota
	ERRCODE_INTERNAL_ERROR
	ERRCODE_PARAM_ERROR
	ERRCODE_DUP_DB
)
var httpErrMap = map[string]int32 {
	ErrSuc:					ERRCODE_SUCCESS,
	ErrIntarnalError:		ERRCODE_INTERNAL_ERROR,
	ErrParamError:			ERRCODE_PARAM_ERROR,
	ErrDupDb:				ERRCODE_DUP_DB,
}
