package util

import (
    "fmt"
    "strings"
    "github.com/tiglabs/baud/util/log"
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

func SlotSplit(start, end uint32, n uint64) []uint32 {
    if n <= 0 {
        return nil
    }
    if uint64(end - start) + 1 < (n) {
        return nil
    }

    var min, max uint32
    if start <= end {
        min = start
        max = end
    } else {
        min = end
        max = start
    }

    ret := make([]uint32, 0)
    switch n {
    case 1:
        ret = append(ret, min)
    case 2:
        ret = append(ret, min)
        ret = append(ret, max)
    default:
        step := (max - min) / uint32(n - 1)
        ret = append(ret, min)
        for i := uint64(1) ; i < n - 1; i++ {
            ret = append(ret, min + uint32(i) * step)
        }
        ret = append(ret, max)
    }

    return ret
}

