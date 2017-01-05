package rsqlib

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"rsql/msgp"
)

const BATCH_TEXT_SIZE_MAX = 100000 // batch of 100 KB. Same value as in rsql_server/aaa_const_specification_serv.go, else, message for batch too large may not appear to client.

//**** ATTENTION: Response_t and Request_t constants are duplicated in the server package "rsql" ****

// message types sent from server to client
//
type Response_t uint8

const (
	RESTYP_LOGIN_FAILED  Response_t = 0
	RESTYP_LOGIN_SUCCESS Response_t = 1

	RESTYP_RECORD_LAYOUT   Response_t = 3
	RESTYP_RECORD          Response_t = 4
	RESTYP_RECORD_FINISHED Response_t = 5

	RESTYP_EXECUTION_FINISHED Response_t = 7

	RESTYP_PRINT   Response_t = 10
	RESTYP_MESSAGE Response_t = 11
	RESTYP_ERROR   Response_t = 12

	RESTYP_BATCH_END Response_t = 14
)

// Request_t is the message types sent from client to server
//
type Request_t uint8

const (
	REQTYP_AUTH      Request_t = 20
	REQTYP_BATCH     Request_t = 21
	REQTYP_KEEPALIVE Request_t = 30
)

// A new Session is created by the Connect function.
//
// Once created, the fields of a Session object are NEVER changed.
//
type Session struct {
	login_name    string
	remote_server string

	conn net.Conn // golang doc: Multiple goroutines may invoke methods on a Conn simultaneously.

	mw_lock sync.Mutex // all writes must be serialized, because keepalive messages are sent from another goroutine
	mw      *msgp.Writer
	mr      *msgp.Reader

	ticker      *time.Ticker
	ticker_done chan struct{}
}

type Error_info struct {
	src_file      string
	src_line_no   int64
	src_funcname  string
	src_backtrace string

	category string
	message  string
	severity string
	state    int64
	text     string
	line_no  int64
	line_pos int64
}

func (e Error_info) String() string {

	s := fmt.Sprintf("%d:%d [%s/%s/%s/%d] %s", e.line_no, e.line_pos, e.category, e.message, e.severity, e.state, e.text)

	return s
}

func (e Error_info) String_format(level int, wrap bool) string {

	ss := " "

	if wrap {
		ss = "\n"
	}

	switch level {
	case 0:
		return fmt.Sprintf("%d:%d [%s/%s/%s/%d]%s<%s>", e.line_no, e.line_pos, e.category, e.message, e.severity, e.state, ss, e.text)
	case 1:
		return fmt.Sprintf("%s.%s:%d %d:%d [%s/%s/%s/%d]%s<%s>", e.src_file, e.src_funcname, e.src_line_no, e.line_no, e.line_pos, e.category, e.message, e.severity, e.state, ss, e.text)
	default:
		return fmt.Sprintf("%s.%s:%d %d:%d [%s/%s/%s/%d]%s<%s>\n%s", e.src_file, e.src_funcname, e.src_line_no, e.line_no, e.line_pos, e.category, e.message, e.severity, e.state, ss, e.text, e.src_backtrace)
	}
}

func (e Error_info) Src_file() string {
	return e.src_file
}

func (e Error_info) Src_line_no() int64 {
	return e.src_line_no
}

func (e Error_info) Src_funcname() string {
	return e.src_funcname
}

func (e Error_info) Src_backtrace() string {
	return e.src_backtrace
}

func (e Error_info) Category() string {
	return e.category
}

func (e Error_info) Message() string {
	return e.message
}

func (e Error_info) Severity() string {
	return e.severity
}

func (e Error_info) State() int64 {
	return e.state
}

func (e Error_info) Text() string {
	return e.text
}

func (e Error_info) Line_no() int64 {
	return e.line_no
}

func (e Error_info) Line_pos() int64 {
	return e.line_pos
}

type Options struct {
	Showtree bool // show AST tree
	No_cf    bool // no constant folding, for debugging
	No_exec  bool // don't run the batches
}

// Connect returns a Session if login has been successful.
// This Session object contains an open net.Conn connection.
//
// If login or connection failed, it just returns an error.
//
// If no error occurred, a valid Session object is returned. You must call Session.Close() when you are finished with it or if an error occurs during its use.
//
func Connect(remote_server string, login_name string, password string, database string, opt *Options, keepalive_interval int) (*Session, error) {
	var (
		err       error
		conn      net.Conn
		mw        *msgp.Writer
		mr        *msgp.Reader
		u         uint8
		resp_type Response_t
	)

	if conn, err = net.Dial("tcp", remote_server); err != nil {
		return nil, err
	}

	mw = msgp.NewWriter(conn)
	mr = msgp.NewReader(conn)

	//--- send authentication info ---

	auth_message := map[string]interface{}{
		"login_name": login_name,
		"password":   password,
		"database":   database,
	}

	if opt.Showtree { // send options only if needed
		auth_message["opt_showtree"] = opt.Showtree
	}

	if opt.No_cf {
		auth_message["opt_no_cf"] = opt.No_cf
	}

	if opt.No_exec {
		auth_message["opt_no_exec"] = opt.No_exec
	}

	mw.WriteUint8(uint8(REQTYP_AUTH))
	mw.WriteMapStrSimpleType(auth_message)

	if err = mw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}

	//--- read authentication response ---

	if u, err = mr.ReadUint8(); err != nil {
		conn.Close()
		return nil, err
	}

	resp_type = Response_t(u)

	if resp_type != RESTYP_LOGIN_SUCCESS {
		conn.Close()
		return nil, errors.New("Login failed")
	}

	//--- create session object ---

	session := &Session{
		login_name:    login_name,
		remote_server: remote_server,

		conn: conn,
		mw:   mw,
		mr:   mr,

		ticker:      time.NewTicker(time.Duration(keepalive_interval) * time.Second),
		ticker_done: make(chan struct{}), // no need to have buffered channel for "done" channels, as close(done) doesn't block
	}

	//--- spawn goroutine to send keepalive message ---

	go func(done chan struct{}) { // keep sending keepalive message as long as possible, until session is closed or a connection problem occurs
		for {
			select {
			case <-session.ticker.C: // note: ticker method Stop() doesn't close the channel

			case <-session.ticker_done: // that's why session.Close() uses this other channel to notify the goroutine that it can terminate
				return
			}

			//println("tick")

			if err := session.Send_special_request(REQTYP_KEEPALIVE); err != nil { // until connection is closed by client or server, or any connection problem occurs
				session.ticker.Stop() // release Ticker resources. Stop() can be called by multiple goroutines.
				return
			}
		}
	}(session.ticker_done)

	return session, nil
}

func (session *Session) Mr() *msgp.Reader {
	return session.mr
}

// Close closes the session and underlying connection socket.
//
// Returns an error if the internal call session.conn.Close() has failed, but it can be ignored, as there is nothing much to do in this case.
//
// To cancel a running query, just call session.Close(). The server will notice that the connection has been closed and will free the resources.
//
// This function can be called asynchronously from another goroutine, as it is thread safe and can be called multiple times.
//
func (session *Session) Close() error {

	session.ticker.Stop() // release Ticker resources. Stop() can be called by multiple goroutines. NOTE: Stop() doesn't close the channel.
	close(session.ticker_done) // signal to the goroutine that sends keepalive messages that it can terminate

	err := session.conn.Close() // Close() is thread safe. Golang doc: Multiple goroutines may invoke methods on a Conn simultaneously.

	return err
}

// Send_batch sends a batch SQL text to the server.
// If it fails, because connection is broken, or data doesn't comply with the communication protocol, an error is returned.
//
// Send_batch and Send_special_request can be called from multiple goroutines, as the calls are serialized by session.mw_lock.
//
func (session *Session) Send_batch(batch_text []byte) error {

	session.mw_lock.Lock()
	defer session.mw_lock.Unlock()

	session.mw.WriteUint8(uint8(REQTYP_BATCH))
	session.mw.WriteStringFromBytes(batch_text)

	if err := session.mw.Flush(); err != nil {
		if len(batch_text) > BATCH_TEXT_SIZE_MAX { // server has sent ERROR_BATCH_TOO_LARGE message and closed the connection, but the client won't read it, as batch has not been sent.
			err = fmt.Errorf("Connection closed by server. Batch size too large, must be < %d bytes.", BATCH_TEXT_SIZE_MAX)
		}
		return err
	}

	return nil
}

// Send_special_request sends a keepalive message to the server.
//
// Request must be REQTYP_KEEPALIVE.
//
func (session *Session) Send_special_request(reqtyp Request_t) error {

	if reqtyp != REQTYP_KEEPALIVE {
		panic("bad request type")
	}

	session.mw_lock.Lock()
	defer session.mw_lock.Unlock()

	session.mw.WriteUint8(uint8(reqtyp))

	if err := session.mw.Flush(); err != nil {
		return err
	}

	return nil
}

// Read_response_type reads just one byte from the connection, to identify the type of the response received from the server.
//
func (session *Session) Read_response_type() (Response_t, error) {
	var (
		err error
		u   uint8
	)

	// read type of the server response

	if u, err = session.mr.ReadUint8(); err != nil {
		return 0, err
	}

	return Response_t(u), nil
}

// Read_Error_info reads error information returned by server.
//
// Used to read content of message RESTYP_BATCH_ERROR.
//
func (session *Session) Read_Error_info() (*Error_info, error) {
	var (
		err         error
		errobj_size uint32
		key         string
		error_info  Error_info
	)

	// read fields of error message

	if errobj_size, err = session.mr.ReadMapHeader(); err != nil {
		return nil, err
	}

	for i := 0; i < int(errobj_size); i++ {
		if key, err = session.mr.ReadString(); err != nil {
			return nil, err
		}

		switch key {
		case "src_file":
			error_info.src_file, err = session.mr.ReadString()
		case "src_line_no":
			error_info.src_line_no, err = session.mr.ReadInt64()
		case "src_funcname":
			error_info.src_funcname, err = session.mr.ReadString()
		case "src_backtrace":
			error_info.src_backtrace, err = session.mr.ReadString()

		case "category":
			error_info.category, err = session.mr.ReadString()
		case "message":
			error_info.message, err = session.mr.ReadString()
		case "severity":
			error_info.severity, err = session.mr.ReadString()
		case "state":
			error_info.state, err = session.mr.ReadInt64()
		case "text":
			error_info.text, err = session.mr.ReadString()
		case "line_no":
			error_info.line_no, err = session.mr.ReadInt64()
		case "line_pos":
			error_info.line_pos, err = session.mr.ReadInt64()
		}

		if err != nil {
			return nil, err
		}
	}

	return &error_info, nil
}

// Read_batch_end_RC reads a return code value when batch ends.
//
// Used to read content of message RESTYP_BATCH_END.
//
func (session *Session) Read_batch_end_RC() (rc int64, err error) {

	// read return code

	if rc, err = session.mr.ReadInt64(); err != nil {
		return 0, err
	}

	return rc, nil
}
