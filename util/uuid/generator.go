package uuid

// Generator is used to generate opaque unique strings.
type Generator interface {
	GetUUID() string
}

var (
	flakeUUIDGenerator = NewFlakeGenerator()
	timeUUIDGenerator  = NewTimeGenerator()
)

// FlakeUUID Generates an UUID (similar to Flake IDs)
func FlakeUUID() string {
	return flakeUUIDGenerator.GetUUID()
}

// TimeUUID Generates a time-based UUID for tests.
func TimeUUID() string {
	return timeUUIDGenerator.GetUUID()
}
