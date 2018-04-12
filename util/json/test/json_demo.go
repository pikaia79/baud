package main

import (
	"fmt"
	"math"
	"time"

	"csm/util"
	"csm/util/json"
)

// C for test
type C struct {
	Name     string `json:"user_name"`
	BirthDay time.Time
	Phone    string
	Siblings int64
	Spouse   bool `json:",omitempty"`
	Money    float64
}

// B for test
type B struct {
	Name   string
	Money  float64
	Create time.Time
}

//MarshalJSON for test
func (b *B) MarshalJSON() ([]byte, error) {
	str := fmt.Sprintf("{\"b_name\":\"%s\",\"b_mone\":%f,\"b_create\":%v}", b.Name, b.Money, b.Create)
	return util.StringToBytes(str), nil
}

func main() {
	a := &C{Name: "test", BirthDay: time.Now(), Phone: "12138888", Siblings: time.Now().UnixNano(), Money: 1.8888888888888}
	b, _ := json.Marshal(a)
	fmt.Println(util.BytesToString(b))

	a.Siblings = math.MaxInt64
	a.Spouse = true
	a.Money = math.MaxFloat64
	b, _ = json.Marshal(a)
	fmt.Println(util.BytesToString(b))

	b1 := &B{Name: "test2", Money: math.MaxFloat64, Create: time.Now()}
	b, _ = json.Marshal(b1)
	fmt.Println(util.BytesToString(b))
}
