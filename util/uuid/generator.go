package uuid

// Generator is used to generate opaque unique strings.
type Generator interface {
	GetUUID() string
}

var (
	flakeUUIDGenerator = NewFlakeGenerator()
)

// FlakeUUID Generates a time-based UUID (similar to Flake IDs)
func FlakeUUID() string {
	return flakeUUIDGenerator.GetUUID()
}
