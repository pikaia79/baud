package util

func CloneBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}
