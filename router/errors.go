package router

import "errors"

var (
	ErrSuccess			 		= errors.New("success")
	ErrInternalError 			= errors.New("internal error")
	ErrSysBusy          		= errors.New("system busy")
	ErrParamError				= errors.New("param error")
)

const (
	ERRCODE_SUCCESS = iota
	ERRCODE_INTERNAL_ERROR
	ERRCODE_SYSBUSY
	ERRCODE_PARAM_ERROR
)

var Err2CodeMap = map[error]int32 {
	ErrSuc:           ERRCODE_SUCCESS,
	ErrInternalError: ERRCODE_INTERNAL_ERROR,
	ErrSysBusy:       ERRCODE_SYSBUSY,
	ErrParamError:    ERRCODE_PARAM_ERROR,
}
