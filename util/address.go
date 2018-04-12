package util

import (
	"fmt"
	"util/log"
	"strings"
)

func BuildAddr(ip, port string) string {
	return fmt.Sprintf("%s:%s", ip, port)
}

func ParseAddr(addr string) ([]string) {
	pair := strings.Split(addr, ":")
	if len(pair) != 2 {
		log.Error("try to parse invalid address:[%v]", addr)
		return nil
	}
	return pair
}
