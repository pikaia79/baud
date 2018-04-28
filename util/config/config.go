package config

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/tiglabs/baudengine/util/bytes"
	"github.com/tiglabs/baudengine/util/json"
)

// Config configuration information reading tool class
type Config struct {
	data map[string]interface{}
	Raw  []byte
}

func newConfig() *Config {
	result := new(Config)
	result.data = make(map[string]interface{})
	return result
}

// LoadConfigFile loads config information from a JSON file
func LoadConfigFile(filename string) *Config {
	result := newConfig()
	err := result.parse(filename)
	if err != nil {
		log.Fatalf("error loading config file %s: %s", filename, err)
	}
	return result
}

// LoadConfigString loads config information from a JSON string
func LoadConfigString(s string) *Config {
	result := newConfig()
	err := json.Unmarshal([]byte(s), &result.data)
	if err != nil {
		log.Fatalf("error parsing config string %s: %s", s, err)
	}
	return result
}

func (c *Config) parse(fileName string) error {
	jsonFileBytes, err := ioutil.ReadFile(fileName)
	c.Raw = jsonFileBytes
	if err == nil {
		err = json.Unmarshal(jsonFileBytes, &c.data)
	}
	return err
}

// GetString Returns a string for the config variable key
func (c *Config) GetString(key string) string {
	if env := os.Getenv(key); env != "" {
		return env
	}

	result, present := c.data[key]
	if !present {
		return ""
	}
	return result.(string)
}

// GetFloat Returns a float for the config variable key
func (c *Config) GetFloat(key string) float64 {
	if env := os.Getenv(key); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			return val
		}
	}

	x, ok := c.data[key]
	if !ok {
		return -1
	}
	return x.(float64)
}

// GetBool Returns a bool for the config variable key
func (c *Config) GetBool(key string) bool {
	if env := os.Getenv(key); env != "" {
		if strings.EqualFold(env, "true") {
			return true
		}
		return false
	}

	x, ok := c.data[key]
	if !ok {
		return false
	}
	return x.(bool)
}

// GetArray Returns an array for the config variable key
func (c *Config) GetArray(key string) []interface{} {
	if env := os.Getenv(key); env != "" {
		var data interface{}
		if err := json.Unmarshal(bytes.StringToByte(env), &data); err == nil {
			return data.([]interface{})
		}
	}

	result, present := c.data[key]
	if !present {
		return []interface{}(nil)
	}
	return result.([]interface{})
}
