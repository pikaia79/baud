package boltdb

import "encoding/json"

type stats struct {
	s *Store
}

func (s *stats) MarshalJSON() ([]byte, error) {
	bs := s.s.db.Stats()
	return json.Marshal(bs)
}
