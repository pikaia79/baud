package gm

import (
	"github.com/tiglabs/baudengine/topo"
	"math/rand"
	"time"
)

type Selector interface {
	SelectTarget(zonesName []string) string
}

type ZoneSelector struct {
}

func NewZoneSelector() Selector {
	rand.Seed(time.Now().UnixNano())
	return &ZoneSelector{}
}

func (s *ZoneSelector) SelectTarget(zonesName []string) string {
	if zonesName == nil || len(zonesName) == 0 {
		return ""
	}

	candidateZoneNames := make([]string, 0)
	for _, zoneName := range zonesName {
		if zoneName == topo.GlobalZone {
			continue
		}
		candidateZoneNames = append(candidateZoneNames, zoneName)
	}
	if len(candidateZoneNames) == 0 {
		return ""
	}
	return candidateZoneNames[rand.Intn(len(candidateZoneNames))]
}
