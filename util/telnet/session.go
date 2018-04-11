package telnet

import (
	"net"
	"bufio"
	"bytes"
	"strings"
	"errors"
)

type Session interface {
	Logger() Logger
	SetLogger(logger Logger) Session
	ReaderWriter() *bufio.ReadWriter
}

const CMD_LINUX_CTRL_C string = "\xFF\xF4\xFF\xFD\x06"
const CMD_LINUX_PAUSE string = "\xFF\xED\xFF\xFD\x06"
const CMD_CTRL_ENTER string = "\r\n"
const CMD_UP string = "\x1B\x5B\x41"
const CMD_DOWN string = "\x1B\x5B\x42"

type internalSession struct {
	logger Logger
	conn net.Conn
	prompt string
	lineBuf bytes.Buffer
	startCmdPos int
	maxLineBuf int
	telnetCmdCB TelnetCmdCB
	history []string
	readerWriter *bufio.ReadWriter
	internalCmds map[string]func(string) error
	running bool
}

func NewSession(prompt string, conn net.Conn, telnetCmdCB TelnetCmdCB) Session {
	session := internalSession { prompt: prompt, maxLineBuf: 10240, conn: conn, telnetCmdCB: telnetCmdCB }
	session.internalCmds = make(map[string]func(string) error, 10)
	session.registerInternalCommand()

	return &session
}

func (session *internalSession) registerInternalCommand() {
	session.internalCmds[CMD_LINUX_CTRL_C] = func(cmdName string) error {
		return session.inputCtrlC()
	}
	session.internalCmds[CMD_LINUX_PAUSE] = func(cmdName string) error {
		return nil
	}
	session.internalCmds[CMD_CTRL_ENTER] = func(cmdName string) error {
		return nil
	}
	session.internalCmds[CMD_UP] = func(cmdName string) error {
		return nil
	}
	session.internalCmds[CMD_DOWN] = func(cmdName string) error {
		return nil
	}
}

func (session *internalSession) Start() {
	defer session.Stop()

	logger := session.logger
	defer func(){
		if r := recover(); nil != r {
			if nil != logger {
				logger.Errorf("Recovered from: (%T) %v", r, r)
			}
		}
	}()

	writer := bufio.NewWriter(session.conn)
	reader := bufio.NewReader(session.conn)
	session.readerWriter = bufio.NewReadWriter(reader, writer)

	session.running = true
	for session.running {
		writer.WriteString(session.prompt)
		inChar, err := reader.ReadByte()
		if err != nil {
			return
		}
		switch {
		case inChar == '\n': // ENTER
			err = session.processCmd()
		case inChar == '\x03': // WIN_CTRL_C
			err = session.inputCtrlC()
		case inChar == '\b': // BACKSPACE
			err = session.inputBackSpace()
		default:
			err = session.inputChar(inChar)
		}
		if err != nil {
			logger.Error(err.Error())
			break
		}
	}
}

func (session *internalSession) ReaderWriter() *bufio.ReadWriter {
	return session.readerWriter
}

func (session *internalSession) Logger() Logger {
	return session.logger
}

func (session *internalSession) SetLogger(logger Logger) Session {
	session.logger = logger

	return session
}

func (session *internalSession) Stop() {
	session.conn.Close()
}

func (session *internalSession) parseCommand(line string) (cmdName string, params []string, err error) {
	subStrs := strings.Split(line, " ")
	if len(subStrs) == 0 || len(subStrs[0]) == 0 {
		err = errors.New("bad line: " + line)
		return
	}
	return subStrs[0], subStrs[1:], nil
}

func (session *internalSession) processCmd() error {
	session.startCmdPos = 0
	cmdName, params, err := session.parseCommand(session.lineBuf.String())
	session.lineBuf.Reset()
	if err != nil {
		return err
	}
	if cmdName == "quit" {
		session.running = false
		return nil
	}
	return session.telnetCmdCB(session, cmdName, params)
}

func (session *internalSession) inputChar(b byte) error {
	session.lineBuf.WriteByte(b)
	if session.lineBuf.Len() > session.maxLineBuf {
		return errors.New("telnet input chars exceeded buf size")
	}
	cmdName := string(session.lineBuf.Bytes()[session.startCmdPos:])
	for internalCmdName, internalCmd := range session.internalCmds {
		if strings.HasPrefix(internalCmdName, cmdName) {
			if internalCmdName == cmdName {
				return internalCmd(cmdName)
			}
			return nil
		}
	}
	session.startCmdPos = session.lineBuf.Len()
	return nil
}

func (session *internalSession) inputBackSpace() error {
	if session.lineBuf.Len() > 0 {
		session.lineBuf.Truncate(session.lineBuf.Len() - 1)
	}
	if session.lineBuf.Len() < session.startCmdPos {
		session.startCmdPos = session.lineBuf.Len()
	}
	return nil
}

func (session *internalSession) inputCtrlC() error {
	return nil
}
