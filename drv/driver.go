// Copyright 2017 Nicolas RIESCH
// Use of this source code is governed by the license found in the LICENCE file.


/*package drv implements a driver to connect to a RSQL database server.

The main types are Connection and Batch.

The SQLtext and SQLpart types are provided for convenience, to easily create SQL script with parameters.
But you can also create a complete SQL script yourself and pass it directly to the Query or Execute methods.

    IMPORTANT: RSQL server aggressively close connections that are idle after 30 seconds.
               So, if you don't use a connection for a while, you must close it. You will create a new connection when needed.

RSQL implements T-SQL, the SQL dialect of MS SQL Server. It is a powerful extension to the plain SQL language and you can declare variables, use IF and WHILE statements, etc.

When you send a SQL batch to RSQL, it should contain all the statements needed to accomplish the intended task. If these statements modify the data, they will often be enclosed by BEGIN TRAN and COMMIT statements to ensure that if an error occurs, nothing will be updated in the database. For example:

	SET NOCOUNT ON
	BEGIN TRAN

	DECLARE @orderid INT;
	INSERT INTO mydb..orders (customerid, orderdate, total) VALUES (123, '20161204', 127.50);
	SET @orderid = SCOPE_IDENTITY();

	INSERT INTO mydb..items (orderid, itemno, product, price) VALUES (@orderid, 0, 'chocolate', 100)
	INSERT INTO mydb..items (orderid, itemno, product, price) VALUES (@orderid, 1, 'book', 20)
	INSERT INTO mydb..items (orderid, itemno, product, price) VALUES (@orderid, 2, 'apples', 7.50)

	COMMIT

note: the table orders in the example above has been created by the statement below. The column 'orderid' is auto-incrementing, and the last inserted value is retrieved by the function SCOPE_IDENTITY():

	CREATE TABLE mydb..orders (
		orderid    INT   NOT NULL IDENTITY(1000,1) PRIMARY KEY,
		customerid INT   NOT NULL,
		orderdate  DATE  NULL,
		total      MONEY NULL
	);


It is also convenient and more efficient to send many SELECT statements in a batch, to retrieve multiple recordsets. For example:

	SELECT parentId, firstName, lastName FROM mydb..parents WHERE parentId = 1234;
	SELECT childId, parentId, fname, lname, birthdate FROM mydb..children WHERE parentId = 1234;

For multiple recorsets, see the method ExistsNextRecordset.

Most database servers and drivers encourage developers to send one statement after the another to the server, making a lot of roundtrips.
As MS SQL Server and RSQL implement T-SQL, there is no need to make all these roundtrips as all statements necessary for a given task should be sent in one batch.



The sample code below shows how to use the driver.


	func main() {
		var (
			err  error
			conn *drv.Connection
			b    *drv.Batch

			orderPart   *drv.SQLpart
			itemPart    *drv.SQLpart
			totalPrice  float64 = 0
			batchString string
		)

		if conn, err = drv.NewConnection("server=localhost;login=sa;password=changeme;database=mydb"); err != nil {
			log.Fatalf("%s", err)
		}
		defer conn.Close()

		//=== create orders and items tables ===

		if b, err = conn.Execute(`
			IF OBJECT_ID('mydb..orders') IS NULL
				CREATE TABLE mydb..orders (
					orderid    INT   NOT NULL IDENTITY(1000,1) PRIMARY KEY,
					customerid INT   NOT NULL,
					orderdate  DATE  NULL,
					total      MONEY NULL
				);

			IF OBJECT_ID('mydb..items') IS NULL
				CREATE TABLE mydb..items (
					orderid INT          NOT NULL,
					itemno  INT          NOT NULL,
					product VARCHAR(100) NULL,
					price   MONEY        NULL,
					CONSTRAINT pk_items PRIMARY KEY (orderid, itemno)
				);
		`); err != nil {
			log.Fatalf("%s", err)
		}

		fmt.Printf("tables orders and items exist or created. Rc=%d\n", b.Rc())

		//=== fill one order and items ==

		const CUSTOMERID = 123 // a customer id

		type Item struct {
			product string
			price   float64
		}

		items := []Item{{"chocolate", 100}, {"book", 20}, {"apples", 7.50}}

		t1 := `
			SET NOCOUNT ON
			BEGIN TRAN

			DECLARE @orderid INT;
			INSERT INTO mydb..orders (customerid, orderdate, total) VALUES ({{custid}}, {{odate}}, {{total}});
			SET @orderid = SCOPE_IDENTITY();
		`
		t2 := "INSERT INTO mydb..items (orderid, itemno, product, price) VALUES (@orderid, {{itemno}}, {{product}}, {{price}})"

		sqltext := drv.NewSQLtext()

		// order

		orderPart = drv.NewSQLpart(t1)

		orderPart.BindInt("custid", CUSTOMERID).BindDate("odate", time.Now())
		sqltext.Addln(orderPart)

		// order items

		for i, item := range items {
			itemPart = drv.NewSQLpart(t2)

			itemPart.BindInt("itemno", i).BindStr("product", item.product).BindFloat64("price", item.price)
			totalPrice += item.price
			sqltext.Addln(itemPart)
		}

		orderPart.BindFloat64("total", totalPrice)

		sqltext.Addln(drv.NewSQLpart("\nCOMMIT"))

		if batchString, err = sqltext.Text(); err != nil {
			log.Fatalf("%s", err)
		}

		fmt.Println(batchString)

		// insert order and items

		if b, err = conn.Execute(batchString); err != nil {
			log.Fatalf("%s", err)
		}

		fmt.Printf("order and items inserted. Rc=%d\n", b.Rc())

		//=== read table orders and items ==

		fmt.Println("SELECT from order table")

		batchString, err = drv.NewSQLpart(`
			SELECT orders.orderid, customerid, orderdate, total, itemno, product, price
			FROM mydb..orders, mydb..items
			WHERE customerid = {{custid}} and items.orderid = orders.orderid;
		`).BindInt("custid", CUSTOMERID).Text()
		if err != nil {
			log.Fatalf("%s", err)
		}

		if b, err = conn.Query(batchString); err != nil {
			log.Fatalf("%s", err)
		}

		for b.Next() { // for each record
			for i := 0; i < b.ColCount(); i++ { // print columns
				s, isnull := b.ColString(i)
				if isnull {
					s = "NULL"
				}
				fmt.Print("\"" + s + "\"\t")
			}
			fmt.Printf("\n")
		}

		if b.Err() != nil {
			log.Fatalf("%s", b.Err())
		}

		fmt.Printf("\n(%d row(s) affected)\n\n", b.RecordCount())

		if err := b.Finalize(); err != nil { // in fact, calling Finalize is not necessary if you have read all records
			log.Fatalf("%s", err)
		}
	}

*/
package drv

import (
	"fmt"
	"strings"

	"rsql/rsqlib"
)

var KEEPALIVE_INTERVAL = 20 // in seconds, 20 is default value. This value can be changed before Connections are created.

// Connection contains the attributes needed to establish a connection with the database server.
//
//    The connection string format is: "Server=myServerAddress:port;Database=myDataBase;Login=myUsername;Password=myPassword"
//    Port and Database attributes can be omitted.
//
type Connection struct {
	connString string

	serverAddr string
	login      string // in lower case
	password   string
	database   string // in lower case

	keepalive_interval int             // in seconds. By default, 20 seconds.
	session            *rsqlib.Session // it is the real connection to the server
	isDirty            bool            // last batch is still running or has not cleanly terminated. Connection cannot be used for another batch.
}

// connStringAttributes is the connection string, split up into attribute and value pairs.
// It is returned by splitConnString() function.
//
type connStringAttributes struct {
	serverAddr string
	login      string
	password   string
	database   string
}

// status is the internal state of execution of the batch.
type status uint8

const (
	sTATUS_BATCH_SENT              status = iota + 1 // SQL text has been sent to the server
	sTATUS_RECORD_LAYOUT_AVAILABLE                   // set when recordset is detected, returning control to the caller
	sTATUS_RECORD_AVAILABLE                          // a record is available for read
	sTATUS_RECORD_END                                // no more record in recordset
	sTATUS_BATCH_END                                 // batch has terminated (successfully or because of an error)
)

// Batch contains the running or terminated batch information.
// Records are read from the batch object, as well as record count, error or return code.
//
// A Batch object cannot be reused. To send another batch to the server, you must create another Batch object with the connection methods Query or Execute.
//
type Batch struct {
	conn *Connection

	text string // original SQL text

	status          status
	recordsetCount  int
	colnameList     []string
	colnameMap      map[string]int // column name to field position in record
	record          []rsqlib.IField
	recordCount     int64 // record count for SELECT statement
	execRecordCount int64 // record count for statements like INSERT, UDDATE, DELETE, etc
	err             error // if an error occurs, the client should close the connection which is useless as it still contains pending information. err can be a *BatchError, which is an error that occurred during batch execution (syntax error, division by 0, duplicate in unique index, etc).
	rc              int64 // return code of batch
}

// NewConnection returns a new Connection object.
// A connection is established with the server.
//
//    RSQL server closes connections that are idle for more than 30 seconds.
//    So, there should be no pause between consecutive batches on the same connection.
//    Else, close the connection and open a new one later when needed.
//
func NewConnection(connectionString string) (*Connection, error) {
	var (
		err        error
		conn       *Connection
		attributes *connStringAttributes

		session *rsqlib.Session
		opt     rsqlib.Options
	)

	// connection string must contain at least one attr=val pair

	if strings.Contains(connectionString, "=") == false {
		return nil, fmt.Errorf("Connection string must contain attr=val pairs separated by semicolon.")
	}

	// create Connection object

	conn = &Connection{}
	conn.connString = connectionString

	if attributes, err = splitConnString(connectionString); err != nil {
		return nil, err
	}

	conn.serverAddr = attributes.serverAddr
	conn.login = attributes.login
	conn.password = attributes.password
	conn.database = attributes.database

	conn.keepalive_interval = KEEPALIVE_INTERVAL // in seconds, default value

	// open the connection

	opt = rsqlib.Options{} // empty option object

	// send login info to server

	if session, err = rsqlib.Connect(conn.serverAddr, conn.login, conn.password, conn.database, &opt, conn.keepalive_interval); err != nil { // expects RESTYP_LOGIN_SUCCESS
		return nil, fmt.Errorf("Connection: login failed.") // because err is just "EOF", as server dropped the connection when login failed
	}

	conn.session = session // it is the real connection to the server
	conn.isDirty = false

	return conn, nil
}

// ConnectionString returns the original connection string.
//
func (conn *Connection) ConnectionString() string {

	return conn.connString
}

// KeepaliveInterval returns the keepalive interval, in seconds.
// The driver sends periodically a message to the server to signal that it is alive.
//
func (conn *Connection) KeepaliveInterval() int {

	return conn.keepalive_interval
}

// Close closes the connection.
//
// To cancel a running query, you can call conn.Close() from another goroutine. The server will notice that the connection has been closed and will free the resources.
//
// This function can be called asynchronously from another goroutine, as it is thread safe and can be called multiple times.
//
func (conn *Connection) Close() {

	conn.session.Close()
}

// splitConnString splits up the connection string into pairs of attribute and value pairs.
//
func splitConnString(s string) (*connStringAttributes, error) {
	var (
		attributes *connStringAttributes
		items      []string
	)

	attributes = &connStringAttributes{}

	items = strings.Split(s, ";")

	for _, item := range items {
		var parts []string

		if strings.TrimSpace(item) == "" { // consecutive or terminating semicolons, e.g.   "server = 127.0.0.1; ; login=john;"
			continue
		}

		parts = strings.Split(item, "=")

		if len(parts) != 2 {
			return nil, fmt.Errorf("Connection string must contain attr=val pairs separated by semicolon.")
		}

		attr := strings.ToLower(strings.TrimSpace(parts[0]))
		if attr == "" {
			return nil, fmt.Errorf("Connection string: attributes cannot be empty string.")
		}
		val := strings.TrimSpace(parts[1])
		if val == "" {
			return nil, fmt.Errorf("Connection string: value for attribute \"%s\" cannot be empty string.", attr)
		}

		switch attr {
		case "server":
			attributes.serverAddr = val
			if strings.Contains(val, ":") == false {
				attributes.serverAddr = val + ":7777"
			}
		case "login":
			attributes.login = strings.ToLower(val)
		case "password":
			attributes.password = val // original case
		case "database":
			attributes.database = strings.ToLower(val)
		default:
			return nil, fmt.Errorf("Connection string attribute \"%s\" is not supported.", attr)
		}
	}

	return attributes, nil
}

// Query creates a Batch object with the specified SQL text, and sends the SQL text on connection conn to the server.
//
// The SQL text of the batch can contain one or many SELECT statements. In fact, it can also contain statements of any kind (INSERT, UPDATE, etc).
//
// The SQL text should contain at least one SELECT statement. Else, it will simply execute the whole batch, like the Execute method.
//
// If the batch contains PRINT statements or sends informative messages (e.g. BULK INSERT periodically sends the number of records inserted so far), they are just ignored.
//
// The Query method returns as soon as the first recordset is available.
//
// If an error is returned, you should close the connection.
//
// You can use SQLtext and SQLpart types to easily create SQL text by using placeholders and BindStr, BindInt, etc methods.
//
func (conn *Connection) Query(text string) (*Batch, error) {
	var (
		b       *Batch
		session *rsqlib.Session
	)

	// connection

	b = &Batch{}

	if conn == nil {
		b.err = fmt.Errorf("Batch: connection argument cannot be nil.")
		return nil, b.err
	}
	b.conn = conn

	if b.conn.isDirty {
		b.err = fmt.Errorf("Batch: connection still contains data from previous batch.")
		return nil, b.err
	}
	b.conn.isDirty = true

	b.text = text

	// send batch

	session = b.conn.session

	if err := session.Send_batch([]byte(b.text)); err != nil {
		b.err = err
		return nil, b.err
	}

	b.status = sTATUS_BATCH_SENT

	// receive messages from server and stop at first recordset

	_ = b.step(sTEP_NEXT_RECORD)

	return b, nil
}

// Execute creates a Batch object with the specified SQL text, and sends the SQL text on connection conn to the server.
//
// The SQL text of the batch can contain many SQL statements of any kind (INSERT, UPDATE, etc), but there should be no SELECT statement.
// If SELECT statements are encountered, they are executed but the records returned by the server are just discarded.
//
// If the batch contains PRINT statements or sends informative messages (e.g. BULK INSERT periodically sends the number of records inserted so far), they are just ignored.
//
// The Execute method returns only when the batch is finished.
//
// The returned error can be *BatchError. If an error is returned, you should close the connection.
//
// You can use SQLtext and SQLpart types to easily create SQL text by using placeholders and BindStr, BindInt, etc methods.
//
func (conn *Connection) Execute(text string) (*Batch, error) {
	var (
		b       *Batch
		session *rsqlib.Session
	)

	// connection

	b = &Batch{}

	if conn == nil {
		b.err = fmt.Errorf("Batch: connection argument cannot be nil.")
		return nil, b.err
	}
	b.conn = conn

	if b.conn.isDirty {
		b.err = fmt.Errorf("Batch: connection still contains data from previous batch.")
		return nil, b.err
	}
	b.conn.isDirty = true

	b.text = text

	// send batch

	session = b.conn.session

	if err := session.Send_batch([]byte(b.text)); err != nil {
		b.err = err
		return nil, b.err
	}

	b.status = sTATUS_BATCH_SENT

	// receive and discard all messages from server

	_ = b.Finalize() // Finalize puts error in b.err if any

	return b, b.err
}

// String returns the SQL text sent to the server.
//
func (b *Batch) String() string {

	return b.text
}

// Columns return the column name list of current recordset.
//
func (b *Batch) Columns() ([]string, error) {

	if !(b.status == sTATUS_RECORD_LAYOUT_AVAILABLE || b.status == sTATUS_RECORD_AVAILABLE) {
		return nil, fmt.Errorf("Column list not available, no recordset found.") // no need to put error in b.err
	}

	return b.colnameList, nil
}

// RecordCount returns the record count of the last SELECT statement that has terminated.
//
func (b *Batch) RecordCount() int64 {

	return b.recordCount
}

// ExecRecordCount returns the record count of the last INSERT, UPDATE, DELETE, etc statement that has terminated.
//
// If SET NOCOUNT is ON, this information is not available.
//
func (b *Batch) ExecRecordCount() int64 {

	return b.execRecordCount
}

// Err returns an error that occurred during batch execution.
// The returned error can be caused by a network problem.
// But usually, the error is a *BatchError, which is generated during batch execution (syntax error, division by 0, duplicate in unique index, etc).
//
// If an error occurs, the client should close the connection which is useless as it still contains pending information.
//
// NOTE: in fact, if error is of type *BatchError and State() is not 127, you can safely continue to use the connection (for *BatchError with state 127, the server has closed the connection and you should do the same). However, the easiest way to cope with errors is to always close the connection, and open a new one if needed.
//
func (b *Batch) Err() error {

	return b.err
}

// Rc returns the return code of the batch, after it has terminated.
//
func (b *Batch) Rc() int64 {

	return b.rc
}

// stepOption specifies if the message loop in step function returns on each record, of if it continues until end of batch.
type stepOption uint8

const (
	sTEP_NEXT_RECORD stepOption = iota // return at next record
	sTEP_FINALIZE                      // process the batch until it terminates
)

// Next reads all messages sent from the server, until a record is reached.
// If the batch contains PRINT statements or sends informative messages (e.g. BULK INSERT periodically sends the number of records inserted so far), they are just ignored.
//
// If no more record is available, or if an error occurred, Next returns false.
//
// After Next returned false, you must call the batch Err() method to check if an error occurred.
//
func (b *Batch) Next() bool {

	return b.step(sTEP_NEXT_RECORD)
}

// ExistsNextRecordset checks if a recordset is available.
// A batch can fetch multiple recordsets.
// You usually KNOW how many recordsets you will receive. So, you will usually write:
//
//    if b.ExistsNextRecordset() == false {
//        ... process this unexpected event
//    }
//
// For example:
//
//	if conn, err = drv.NewConnection("server=localhost;login=sa;password=changeme;database=mydb"); err != nil {
//		log.Fatalf("%s", err)
//	}
//	defer conn.Close()
//
//	text := `
//		SELECT parentId, firstName, lastName FROM mydb..parents WHERE parentId = 1234;
//		SELECT childId, parentId, fname, lname, birthdate FROM mydb..children WHERE parentId = 1234;
//	`
//
//	if b, err = conn.Query(text); err != nil { // send batch
//		log.Fatalf("%s", err)
//	}
//
//	//========= get first recordset
//
//	for b.Next() {
//		... process record
//	}
//
//	if b.Err() != nil {
//		log.Fatalf("%s", b.Err())
//	}
//
//	fmt.Printf("(%d row(s) affected)\n", b.RecordCount())
//
//	//========= get second recordset
//
//	if b.ExistsNextRecordset() == false {
//		log.Fatal("no second recorset")
//	}
//
//	for b.Next() {
//		... process record
//	}
//
//	if b.Err() != nil {
//		log.Fatalf("%s", b.Err())
//	}
//
//	fmt.Printf("(%d row(s) affected)\n", b.RecordCount())
//
//	//========= gracefully terminate the batch
//
//	if err := b.Finalize(); err != nil { // calling Finalize is not necessary if you have read all records
//		log.Fatalf("%s", err)
//	}
//
func (b *Batch) ExistsNextRecordset() bool {

	return b.status == sTATUS_RECORD_LAYOUT_AVAILABLE
}

// step reads all the response message sent by the server.
//
// It returns when a recordset is reached (for batch sent by conn.Query), or executes all or remaining statements until the batch terminates (for batch sent by conn.Execute).
//
// This function returns true if a record is available and its column values can be read.
//
// If an error is encountered, it is put in b.err and the method returns false.
//
func (b *Batch) step(option stepOption) bool {
	var (
		err     error
		session *rsqlib.Session

		resp rsqlib.Response_t

		record []rsqlib.IField
	)

	if b.err != nil {
		return false
	}

	session = b.conn.session

	//=== read response ===

	for {
		if resp, err = session.Read_response_type(); err != nil {
			b.err = err
			return false
		}

		switch resp {
		case rsqlib.RESTYP_RECORD_LAYOUT:
			var colnameList []string

			// create colname list and map

			if colnameList, err = session.Create_colname_list(); err != nil { // create list
				b.err = err
				return false
			}

			b.colnameList = colnameList

			colnameMap := make(map[string]int, len(colnameList)) // create map
			for i, name := range colnameList {
				if name == "" {
					continue
				}

				if _, ok := colnameMap[name]; ok == true {
					colnameMap[name] = i
				} else {
					delete(colnameMap, name) // ambiguous column name
				}
			}

			b.colnameMap = colnameMap

			// create record
			if record, err = session.Create_row(); err != nil {
				b.err = err
				return false
			}

			b.record = record

			b.recordCount = 0
			b.recordsetCount++
			b.status = sTATUS_RECORD_LAYOUT_AVAILABLE

			// return if sTEP_NEXT_RECORD

			if option == sTEP_NEXT_RECORD {
				return false
			}

		case rsqlib.RESTYP_RECORD: // a record is available
			// fill record

			if err = session.Fill_row_with_values(b.record); err != nil {
				b.err = err
				return false
			}

			b.recordCount++
			b.status = sTATUS_RECORD_AVAILABLE

			if option == sTEP_NEXT_RECORD {
				return true
			}

		case rsqlib.RESTYP_RECORD_FINISHED: // record count is available
			var recordCount int64

			if recordCount, err = session.Read_int64(); err != nil {
				b.err = err
				return false
			}

			if recordCount != b.recordCount {
				b.err = fmt.Errorf("Batch: recordcount mismatch (RSQL bug).", recordCount, b.recordCount)
				return false
			}

			// discard record

			b.colnameList = nil
			b.colnameMap = nil
			b.record = nil
			b.recordCount = recordCount

			b.status = sTATUS_RECORD_END

		case rsqlib.RESTYP_EXECUTION_FINISHED: // if SET NOCOUNT ON, INSERT etc statements don't send this information
			var execRecordCount int64

			if execRecordCount, err = session.Read_int64(); err != nil {
				b.err = err
				return false
			}

			b.execRecordCount = execRecordCount

		case rsqlib.RESTYP_PRINT:
			var row []rsqlib.IField

			// create row

			if row, err = session.Create_row(); err != nil {
				b.err = err
				return false
			}

			if err = session.Fill_row_with_values(row); err != nil {
				b.err = err
				return false
			}

			//fmt.Printf("PRINT detected\n") // ignore row

		case rsqlib.RESTYP_MESSAGE:
			var msg_string string

			if msg_string, err = session.Read_string(); err != nil {
				b.err = err
				return false
			}

			_ = msg_string

			//fmt.Println(msg_string) // ignore message

		case rsqlib.RESTYP_ERROR:
			var error_info *rsqlib.Error_info

			if error_info, err = session.Read_Error_info(); err != nil {
				b.err = err
				return false
			}

			be := newBatchError(error_info)

			b.err = be

			// the server will send RESTYP_BATCH_END after it has sent this error.
			// if state == 127 (only THROW or ERROR_SERVER_ABORT can generate it), server also closed the connection.

		case rsqlib.RESTYP_BATCH_END: // batch is finished, no more messages are expected from server for this batch
			var rc int64

			if rc, err = session.Read_batch_end_RC(); err != nil {
				b.err = err
				return false
			}

			b.rc = rc

			b.status = sTATUS_BATCH_END

			b.conn.isDirty = false // connection can be used for another batch

			return false

		default:
			panic("impossible")
		}
	} // end of response loop

}

// Finalize executes all remaining statements until end of a Query batch.
//
// It is only useful to gracefully terminate a batch created by the Query method. But if you have read all records from a batch, this method is useless and does nothing.
//
// If you are reading a record, decide that you don't need to read the remaining records and just want to silently execute the remaining statements, you must call Finalize().
//
// Note that if you want to discard the remaining of the batch, you can also just close the connection (but the remaining statements will not be executed, though).
//
// Finalize does nothing on a batch created by the Execute method.
//
func (b *Batch) Finalize() error {

	if b.err != nil {
		return b.err
	}

	if b.status != sTATUS_BATCH_END {
		_ = b.step(sTEP_FINALIZE)
	}

	return b.err
}

// BatchError contains an error that occurred during execution of the batch, such as syntax error, division by 0, overflow, constraint violation, etc.
//
// If the error is a *BatchError, the connection can be used to send other batches. But if State is 127, it won't be possible because the server has closed the connection.
//
type BatchError struct {
	SrcFile      string // for debugging only
	SrcLineNo    int64  // for debugging only
	SrcFuncname  string // for debugging only
	SrcBacktrace string // for debugging only

	Category string // for debugging only
	Message  string // for debugging only
	Severity string // for debugging only
	State    int64  // usually 1. If 127, the server has closed the connection.
	Text     string // message of the error
	LineNo   int64  // line in the batch causing the error
	LinePos  int64  // position in the line causing the error
}

// Error implements the error interface.
// It returns the line and position in the batch where the error occurred, and the state between brackets.
// If state is 127, the server has closed the connection.
//
func (be *BatchError) Error() string {

	return fmt.Sprintf("%d:%d[%d] %s", be.LineNo, be.LinePos, be.State, be.Text)
}

// newBatchError creates a new BatchError by copying information from a rsqlib.Error_info.
//
func newBatchError(e *rsqlib.Error_info) *BatchError {

	be := &BatchError{}

	be.SrcFile = e.Src_file()
	be.SrcLineNo = e.Src_line_no()
	be.SrcFuncname = e.Src_funcname()
	be.SrcBacktrace = e.Src_backtrace()

	be.Category = e.Category()
	be.Message = e.Message()
	be.Severity = e.Severity()
	be.State = e.State()
	be.Text = e.Text()
	be.LineNo = e.Line_no()
	be.LinePos = e.Line_pos()

	return be
}
