package index

import (
	"github.com/tiglabs/baud/schema"
)

type Mutation struct {
	entitiesToDel []schema.UID
	factsToAdd    []schema.Fact
	factsToDel    []schema.Fact
}

//note: o.UID is given by the PS
func (m *Mutation) AddFacts(facts []chema.Fact) error {
	return nil
}

func (m *Mutation) DelFacts(facts []schema.Fact) error {
	return nil
}

func (m *Mutation) DelEntity(uid schema.UID) error {
	return nil
}
