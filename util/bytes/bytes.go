package bytes

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// IEC Sizes.
const (
	Byte = 1 << (iota * 10)
	KB
	MB
	GB
	TB
	PB
	EB
)

// SI Sizes.
const (
	IByte = 1
	IKB   = IByte * 1000
	IMB   = IKB * 1000
	IGB   = IMB * 1000
	ITB   = IGB * 1000
	IPB   = ITB * 1000
	IEB   = IPB * 1000
)

var byteSizes = map[string]uint64{
	"b":   Byte,
	"kib": KB,
	"kb":  IKB,
	"mib": MB,
	"mb":  IMB,
	"gib": GB,
	"gb":  IGB,
	"tib": TB,
	"tb":  ITB,
	"pib": PB,
	"pb":  IPB,
	"eib": EB,
	"eb":  IEB,

	"":   Byte,
	"ki": KB,
	"k":  IKB,
	"mi": MB,
	"m":  IMB,
	"gi": GB,
	"g":  IGB,
	"ti": TB,
	"t":  ITB,
	"pi": PB,
	"p":  IPB,
	"ei": EB,
	"e":  IEB,
}

var (
	siSizes  = []string{"B", "kB", "MB", "GB", "TB", "PB", "EB"}
	iecSizes = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

	jsonPrefix = []byte("{")
)

func logn(n, b float64) float64 {
	return math.Log(n) / math.Log(b)
}

func humanFormat(s uint64, base float64, sizes []string) string {
	if s < 10 {
		return fmt.Sprintf("%d B", s)
	}
	e := math.Floor(logn(float64(s), base))
	suffix := sizes[int(e)]
	val := math.Floor(float64(s)/math.Pow(base, e)*10+0.5) / 10
	f := "%.0f %s"
	if val < 10 {
		f = "%.1f %s"
	}
	return fmt.Sprintf(f, val, suffix)
}

// FormatByte convert uint64 to human-readable byte strings
func FormatByte(s uint64) string {
	return humanFormat(s, 1000, siSizes)
}

// FormatIByte convert uint64 to human-readable byte strings
func FormatIByte(s uint64) string {
	return humanFormat(s, 1024, iecSizes)
}

// ParseByte convert human-readable byte strings to uint64
func ParseByte(s string) (uint64, error) {
	lastDigit := 0
	hasComma := false
	for _, r := range s {
		if !(unicode.IsDigit(r) || r == '.' || r == ',') {
			break
		}
		if r == ',' {
			hasComma = true
		}
		lastDigit++
	}

	num := s[:lastDigit]
	if hasComma {
		num = strings.Replace(num, ",", "", -1)
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, err
	}

	extra := strings.ToLower(strings.TrimSpace(s[lastDigit:]))
	if m, ok := byteSizes[extra]; ok {
		f *= float64(m)
		if f >= math.MaxUint64 {
			return 0, fmt.Errorf("too large: %v", s)
		}
		return uint64(f), nil
	}
	return 0, fmt.Errorf("unhandled size name: %v", extra)
}

// BitLen calculated bit length
func BitLen(x int64) (n int64) {
	for ; x >= 0x8000; x >>= 16 {
		n += 16
	}
	if x >= 0x80 {
		x >>= 8
		n += 8
	}
	if x >= 0x8 {
		x >>= 4
		n += 4
	}
	if x >= 0x2 {
		x >>= 2
		n += 2
	}
	if x >= 0x1 {
		n++
	}
	return
}

func CloneBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}
