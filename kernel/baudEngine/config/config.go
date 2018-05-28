package config

type Config struct {
	dictPath         map[string]string
}

var config *Config

func New() *Config {
	return &Config{dictPath: make(map[string]string)}
}

func SetWordDictPath(name, path string) {
	config.dictPath[name] = path
}

func GetWordDictPath(name string) string {
	if path, ok := config.dictPath[name]; ok {
		return path
	}
	panic("invalid word dict name")
}


func init() {
	config = New()
}
