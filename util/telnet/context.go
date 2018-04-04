package telnet


type Context interface {
	Write(string)
}


type internalContext struct {
	logger Logger
}


func NewContext() Context {
	ctx := internalContext{}

	return &ctx
}

