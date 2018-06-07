package selector

import (
	"math/rand"
	"time"
)

type SimpleSelector struct {
}

func NewSimpleSelector() Selector {
	rand.Seed(time.Now().UnixNano())
	return &SimpleSelector{}
}

func (s *SimpleSelector) SelectTarget(iterator Iterator, count int) []Item {
	candidateItems := make([]Item, 0)
	for {
		item := iterator.Next()
		if item == nil {
			break
		}
		candidateItems = append(candidateItems, item)
	}
	if len(candidateItems) == 0 {
		return nil
	}

	resultItems := make([]Item, 0)
	for i := 0; i < count; i++ {
		index := rand.Intn(len(candidateItems))
		item := candidateItems[index]
		resultItems = append(resultItems, item)
		candidateItems = append(candidateItems[:index], candidateItems[index + 1:]...)
		if len(candidateItems) == 0 {
			break
		}
	}
	return resultItems
}
