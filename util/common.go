package util

import (
    "fmt"
    "strings"
    "util/log"
)

func BuildAddr(ip string, port int) string {
    return fmt.Sprintf("%s:%d", ip, port)
}

func ParseAddr(addr string) ([]string) {
    pair := strings.Split(addr, ":")
    if len(pair) != 2 {
        log.Error("try to parse invalid address:[%v]", addr)
        return nil
    }
    return pair
}

func BytesPrefix(prefix []byte) ([]byte, []byte) {
    var limit []byte
    for i := len(prefix) - 1; i >= 0; i-- {
        c := prefix[i]
        if c < 0xff {
            limit = make([]byte, i+1)
            copy(limit, prefix)
            limit[i] = c + 1
            break
        }
    }
    return prefix, limit
}
