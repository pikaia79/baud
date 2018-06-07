package engine

import (
	"errors"
	"fmt"
)

var (
	engines map[string]EngineBuilder

	ErrorNameInvalid = errors.New("Registration name is invalid")
)

func init() {
	engines = make(map[string]EngineBuilder, 8)
}

// EngineBuilder is used to build the engine.
type EngineBuilder func(cfg EngineConfig) (Engine, error)

// EngineConfig holds all configuration parameters used in setting up a new Engine instance.
type EngineConfig struct {
	// Path is the data directory.
	Path string
	// ReadOnly will open the engine in read only mode if set to true.
	ReadOnly bool
	// ExtraOptions contains extension options using a json format ("{key1:value1,key2:value2}").
	ExtraOptions string

	// Schema
	Schema  string
}

// Register is used to register the engine implementers in the initialization phase.
func Register(name string, builder EngineBuilder) {
	if name == "" || builder == nil {
		panic("Registration name and builder cannot be empty")
	}
	if _, ok := engines[name]; ok {
		panic(fmt.Sprintf("Duplicate registration engine name for %s", name))
	}

	engines[name] = builder
}

// Build create an engine based on the specified name.
func Build(name string, cfg EngineConfig) (e Engine, err error) {
	if name == "" {
		return nil, ErrorNameInvalid
	}

	if builder := engines[name]; builder != nil {
		e, err = builder(cfg)
	} else {
		err = fmt.Errorf("Registered engine[%s] does not exist", name)
	}
	return
}

// Exist return whether the engine exists.
func Exist(name string) bool {
	if builder := engines[name]; builder != nil {
		return true
	}

	return false
}
