package engine

import "encoding/json"

type SearchRequest struct {
	//TODO
	Index string   `json:"_index""`
	Type  string   `json:"_type"`
	// default json
	ProtocolType string  `json:"-"`
	// default 10
	Size      int    `json:"size,omitempty"`
	// default 0
	From      int    `json:"from,omitempty"`
	Query     string `json:"query"`
}

func NewSearchQuery(_index, _type string) *SearchRequest {
	return &SearchRequest{Index:_index, Type:_type, ProtocolType: "json", Size: 10, From: 0}
}

func (r *SearchRequest) SetSize(size int) {
	r.Size = size
}

func (r *SearchRequest) SetFrom(from int) {
	r.From = from
}

func (r *SearchRequest) SetQuery(query string) {
	r.Query = query
}

func (r *SearchRequest)UnmarshalJSON(data []byte) error{
	tmp := struct {
		Size  int `json:"size,omitempty"`
		From  int `json:"from,omitempty"`
		Query json.RawMessage `json:"query,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	r.SetFrom(tmp.From)
	r.SetSize(tmp.Size)
	r.SetQuery(string(tmp.Query))
	return nil
}

func (r *SearchRequest)Parse(data []byte) (error) {
	err := json.Unmarshal(data, r)
	if err != nil {
		return err
	}
	return nil
}