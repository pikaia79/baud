package engine

type Shards struct {
	Total     int     `json:"total"`
	Successful int    `json:"successful"`
	Failed     int    `json:"failed"`
}

type HitDoc struct {
	Index      string       `json:"_index"`
	Type       string       `json:"_type"`
	Id         string       `json:"_id"`
	Score      float64      `json:"_score"`
	Source     interface{}       `json:"_source"`
}

type Hits struct {
	Total      uint64      `json:"total"`
	MaxScore   float64    `json:"max_score"`
    Hits       []HitDoc   `json:"hits"`
}

type SearchResult struct {
	//TODO
	Took     int64       `json:"took"`
	TimeOut  bool        `json:"time_out"`
	Shards   Shards      `json:"_shards,omitempty"`
	Hits     Hits        `json:"hits"`
}
