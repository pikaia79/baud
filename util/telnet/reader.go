package telnet


type Reader interface {
	ReadLine() (string, error)
}
