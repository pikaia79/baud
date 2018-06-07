package query

import "errors"

var (
	ErrInvalidMinMatch                = errors.New("invalid minimum_should_match")
    ErrInvalidTermQuery               = errors.New("invalid term query")
    ErrNotSupportPhrasePrefix         = errors.New("not support phrase_prefix match")
)
