package master

import (
	"context"
	"time"
)

type Processor struct {
	ctx 	context.Context
	ctxCancel context.CancelFunc
	timeout 	time.Duration
	pipeline   chan
}
