package document

type FieldType int

const (
	String FieldType = iota
	Number
	Boolean
	Date
)

type Field interface {
	// Name returns the path of the field from the root DocumentMapping.
	// A root field path is "field", a sub document field is "parent.field".
	// for example,
	// struct {
	//     name   string
	//     parent  struct {
	//         field1   string
	//         field2   string
	//     }
	// }
	// the name of field1 is parent.field1
	Name() string

	Value() []byte

	// NumPlainTextBytes should return the number of plain text bytes
	// that this field represents - this is a common metric for tracking
	// the rate of indexing
	NumPlainTextBytes() uint64
}
