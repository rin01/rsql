package rsqlib

import (
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"rsql/msgp"
)

type Dtype_t uint8

const (
	DTYPE_VOID      Dtype_t = 1
	DTYPE_BOOLEAN   Dtype_t = 2
	DTYPE_VARBINARY Dtype_t = 4
	DTYPE_VARCHAR   Dtype_t = 6

	DTYPE_BIT      Dtype_t = 9
	DTYPE_TINYINT  Dtype_t = 10
	DTYPE_SMALLINT Dtype_t = 11
	DTYPE_INT      Dtype_t = 12
	DTYPE_BIGINT   Dtype_t = 13

	DTYPE_MONEY   Dtype_t = 15
	DTYPE_NUMERIC Dtype_t = 16
	DTYPE_FLOAT   Dtype_t = 17

	DTYPE_DATE     Dtype_t = 19
	DTYPE_TIME     Dtype_t = 20
	DTYPE_DATETIME Dtype_t = 21
)

func assert(val bool) {
	if val == false {
		panic("assertion failed")
	}
}

const (
	SECONDS_PER_DAY     = 24 * 3600    // 86400
	UNIX_SEC_LOWEST     = -62135596800 // -719162 days * 24 * 3600
	UNIX_SEC_1900_01_01 = -2208988800  // -25567 days * 24 * 3600
	UNIX_SEC_1970_01_01 = 0
)

//===============================================================
//                      row and fields
//===============================================================

// Int, Float, etc implement IField interface.
// A series of IFields makes up a row, which will receive the deserialized values sent by the server.
//
// The user can then access to the value stored in each field. He can then copy these values to a suitable variable type, e.g. copy Numeric values into math.big.Rat or decnum.Quad type.
//
type IField interface {
	Datatype() Dtype_t
	IsNull() bool
	String() string

	read_value(mr *msgp.Reader) error
}

type Void struct {
	Is_Null bool // always true
}

type Boolean struct {
	Is_Null bool
	Val     bool
}

type Varbinary struct {
	Precision uint16
	Is_Null   bool
	Val       []byte
}

type Varchar struct {
	Precision uint16
	Fixlen    bool
	Is_Null   bool
	Val       []byte
}

type Bit struct {
	Is_Null bool
	Val     uint8 // 0 or 1
}

type Tinyint struct {
	Is_Null bool
	Val     uint8
}

type Smallint struct {
	Is_Null bool
	Val     int16
}

type Int struct {
	Is_Null bool
	Val     int32
}

type Bigint struct {
	Is_Null bool
	Val     int64
}

type Money struct {
	Precision uint16
	Scale     uint16
	Is_Null   bool
	Val       []byte // the value is received as string, and we only want to display it. No need to convert it to e.g. big.Rat.
}

type Numeric struct {
	Precision uint16
	Scale     uint16
	Is_Null   bool
	Val       []byte // the value is received as string, and we only want to display it. No need to convert it to e.g. big.Rat.
}

type Float struct {
	Is_Null bool
	Val     float64
}

type Date struct {
	Is_Null bool
	Val     time.Time
}

type Time struct {
	Is_Null bool
	Val     time.Time
}

type Datetime struct {
	Is_Null bool
	Val     time.Time
}

//--- Datatype() methods ---

func (field *Void) Datatype() Dtype_t {
	return DTYPE_VOID
}

func (field *Boolean) Datatype() Dtype_t {
	return DTYPE_BOOLEAN
}

func (field *Varbinary) Datatype() Dtype_t {
	return DTYPE_VARBINARY
}

func (field *Varchar) Datatype() Dtype_t {
	return DTYPE_VARCHAR
}

func (field *Bit) Datatype() Dtype_t {
	return DTYPE_BIT
}

func (field *Tinyint) Datatype() Dtype_t {
	return DTYPE_TINYINT
}

func (field *Smallint) Datatype() Dtype_t {
	return DTYPE_SMALLINT
}

func (field *Int) Datatype() Dtype_t {
	return DTYPE_INT
}

func (field *Bigint) Datatype() Dtype_t {
	return DTYPE_BIGINT
}

func (field *Money) Datatype() Dtype_t {
	return DTYPE_MONEY
}

func (field *Numeric) Datatype() Dtype_t {
	return DTYPE_NUMERIC
}

func (field *Float) Datatype() Dtype_t {
	return DTYPE_FLOAT
}

func (field *Date) Datatype() Dtype_t {
	return DTYPE_DATE
}

func (field *Time) Datatype() Dtype_t {
	return DTYPE_TIME
}

func (field *Datetime) Datatype() Dtype_t {
	return DTYPE_DATETIME
}

//--- IsNull() methods ---

func (field *Void) IsNull() bool {
	return field.Is_Null
}

func (field *Boolean) IsNull() bool {
	return field.Is_Null
}

func (field *Varbinary) IsNull() bool {
	return field.Is_Null
}

func (field *Varchar) IsNull() bool {
	return field.Is_Null
}

func (field *Bit) IsNull() bool {
	return field.Is_Null
}

func (field *Tinyint) IsNull() bool {
	return field.Is_Null
}

func (field *Smallint) IsNull() bool {
	return field.Is_Null
}

func (field *Int) IsNull() bool {
	return field.Is_Null
}

func (field *Bigint) IsNull() bool {
	return field.Is_Null
}

func (field *Money) IsNull() bool {
	return field.Is_Null
}

func (field *Numeric) IsNull() bool {
	return field.Is_Null
}

func (field *Float) IsNull() bool {
	return field.Is_Null
}

func (field *Date) IsNull() bool {
	return field.Is_Null
}

func (field *Time) IsNull() bool {
	return field.Is_Null
}

func (field *Datetime) IsNull() bool {
	return field.Is_Null
}

//--- String() methods ---

const NULL_STRING = "<NULL>"

func (field *Void) String() string {
	return NULL_STRING
}

func (field *Boolean) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	if field.Val == false {
		return "false"
	}

	return "true"
}

func (field *Varbinary) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	return fmt.Sprintf("0x%x", field.Val)
}

func (field *Varchar) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	return string(field.Val)
}

func (field *Bit) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	if field.Val == 0 {
		return "0"
	}

	return "1"
}

func (field *Tinyint) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	return strconv.FormatInt(int64(field.Val), 10)
}

func (field *Smallint) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	return strconv.FormatInt(int64(field.Val), 10)
}

func (field *Int) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	return strconv.FormatInt(int64(field.Val), 10)
}

func (field *Bigint) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	return strconv.FormatInt(field.Val, 10)
}

func (field *Money) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	return string(field.Val)
}

func (field *Numeric) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	return string(field.Val)
}

func (field *Float) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	return strconv.FormatFloat(field.Val, 'g', -1, 64)
}

func (field *Date) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	return field.Val.Format("2006-01-02")
}

func (field *Time) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	if field.Val.Nanosecond() == 0 {
		return field.Val.Format("15:04:05")
	}

	return field.Val.Format("15:04:05.000000000")
}

func (field *Datetime) String() string {
	if field.Is_Null {
		return NULL_STRING
	}

	if field.Val.Nanosecond() == 0 {
		return field.Val.Format("2006-01-02 15:04:05")
	}

	return field.Val.Format("2006-01-02 15:04:05.000000000")
}

//======================= create list of column names, as described by the server response  ================================

// Create_colname_list returns a list of column names from a messagepack Reader.
//
func (session *Session) Create_colname_list() ([]string, error) {
	var (
		err      error
		row_size uint32
		colname  string
	)

	// read column names

	if row_size, err = session.mr.ReadArrayHeader(); err != nil {
		return nil, err
	}

	colname_list := make([]string, 0, row_size)

	for i := 0; i < int(row_size); i++ {
		if colname, err = session.mr.ReadString(); err != nil {
			return nil, err
		}

		colname_list = append(colname_list, colname)
	}

	return colname_list, nil
}

//======================= create row with proper fields, as described by the server response  ================================

// new_fields returns a IField object, created by reading from messagepack Reader. It returns e.g. *Int, *Numeric, *Date, etc.
//
func new_field(mr *msgp.Reader) (IField, error) {
	var (
		err       error
		sz        uint32
		u         uint8
		precision uint16
		scale     uint16
		fixlen    bool
	)

	if sz, err = mr.ReadArrayHeader(); err != nil { // each datatype information is contained in an array
		return nil, err
	}

	if u, err = mr.ReadUint8(); err != nil { // read datatype
		return nil, err
	}

	switch Dtype_t(u) {
	case DTYPE_VOID:
		assert(sz == 1)
		return &Void{Is_Null: true}, nil

	case DTYPE_BOOLEAN:
		assert(sz == 1)
		return &Boolean{Is_Null: true}, nil

	case DTYPE_VARBINARY:
		assert(sz == 2)
		if precision, err = mr.ReadUint16(); err != nil {
			return nil, err
		}

		return &Varbinary{
			Precision: precision,
			Is_Null:   true,
		}, nil

	case DTYPE_VARCHAR:
		assert(sz == 3)
		if precision, err = mr.ReadUint16(); err != nil {
			return nil, err
		}

		if fixlen, err = mr.ReadBool(); err != nil {
			return nil, err
		}

		return &Varchar{
			Precision: precision,
			Fixlen:    fixlen,
			Is_Null:   true,
		}, nil

	case DTYPE_BIT:
		assert(sz == 1)
		return &Bit{Is_Null: true}, nil

	case DTYPE_TINYINT:
		assert(sz == 1)
		return &Tinyint{Is_Null: true}, nil

	case DTYPE_SMALLINT:
		assert(sz == 1)
		return &Smallint{Is_Null: true}, nil

	case DTYPE_INT:
		assert(sz == 1)
		return &Int{Is_Null: true}, nil

	case DTYPE_BIGINT:
		assert(sz == 1)
		return &Bigint{Is_Null: true}, nil

	case DTYPE_MONEY:
		assert(sz == 3)
		if precision, err = mr.ReadUint16(); err != nil {
			return nil, err
		}

		if scale, err = mr.ReadUint16(); err != nil {
			return nil, err
		}

		return &Money{
			Precision: precision,
			Scale:     scale,
			Is_Null:   true,
		}, nil

	case DTYPE_NUMERIC:
		assert(sz == 3)
		if precision, err = mr.ReadUint16(); err != nil {
			return nil, err
		}

		if scale, err = mr.ReadUint16(); err != nil {
			return nil, err
		}

		return &Numeric{
			Precision: precision,
			Scale:     scale,
			Is_Null:   true,
		}, nil

	case DTYPE_FLOAT:
		assert(sz == 1)
		return &Float{Is_Null: true}, nil

	case DTYPE_DATE:
		assert(sz == 1)
		return &Date{Is_Null: true}, nil

	case DTYPE_TIME:
		assert(sz == 1)
		return &Time{Is_Null: true}, nil

	case DTYPE_DATETIME:
		assert(sz == 1)
		return &Datetime{Is_Null: true}, nil

	default:
		return nil, errors.New("Unknown datatype received")
	}
}

// Create_row creates a row from a messagepack Reader.
//
func (session *Session) Create_row() ([]IField, error) {
	var (
		err      error
		field    IField
		row      []IField
		row_size uint32
	)

	// read field datatypes and create row

	if row_size, err = session.mr.ReadArrayHeader(); err != nil {
		return nil, err
	}

	row = make([]IField, row_size)

	for i := 0; i < int(row_size); i++ {
		if field, err = new_field(session.mr); err != nil {
			return nil, err
		}

		row[i] = field
	}

	return row, nil
}

//===============================================================
//                fill-in values into row fields
//===============================================================

func (field *Void) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// always NULL

	assert(objtype == msgp.NilType)

	if mr.ReadNil(); err != nil {
		return err
	}

	field.Is_Null = true

	return nil
}

func (field *Boolean) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
		val     bool
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = false
		return nil
	}

	// value

	if val, err = mr.ReadBool(); err != nil {
		return err
	}

	field.Is_Null = false
	field.Val = val

	return nil
}

func (field *Varbinary) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
		val     []byte
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = field.Val[:0]
		return nil
	}

	// value

	if val, err = mr.ReadBytes(field.Val[:0]); err != nil {
		return err
	}

	field.Is_Null = false
	field.Val = val

	return nil
}

func (field *Varchar) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
		val     []byte
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = field.Val[:0]
		return nil
	}

	// value

	if val, err = mr.ReadStringAsBytes(field.Val[:0]); err != nil {
		return err
	}

	field.Is_Null = false
	field.Val = val

	// pad for fixed length ("CHAR" SQL datatype)

	if field.Fixlen == true {
		rune_count := utf8.RuneCount(field.Val)

		if rune_count < int(field.Precision) {
			current_length := len(field.Val)
			padding_length := int(field.Precision) - rune_count

			field.Val = append(field.Val, make([]byte, padding_length)...) // append padding 0s
			for i := current_length; i < len(field.Val); i++ {             // replace padding 0s with blanks
				field.Val[i] = ' '
			}
		}
	}

	return nil
}

func (field *Bit) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
		val     uint8
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = 0
		return nil
	}

	// value

	if val, err = mr.ReadUint8(); err != nil {
		return err
	}

	assert(val <= 1)

	field.Is_Null = false
	field.Val = val

	return nil
}

func (field *Tinyint) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
		val     uint8
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = 0
		return nil
	}

	// value

	if val, err = mr.ReadUint8(); err != nil {
		return err
	}

	field.Is_Null = false
	field.Val = val

	return nil
}

func (field *Smallint) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
		val     int16
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = 0
		return nil
	}

	// value

	if val, err = mr.ReadInt16(); err != nil {
		return err
	}

	field.Is_Null = false
	field.Val = val

	return nil
}

func (field *Int) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
		val     int32
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = 0
		return nil
	}

	// value

	if val, err = mr.ReadInt32(); err != nil {
		return err
	}

	field.Is_Null = false
	field.Val = val

	return nil
}

func (field *Bigint) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
		val     int64
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = 0
		return nil
	}

	// value

	if val, err = mr.ReadInt64(); err != nil {
		return err
	}

	field.Is_Null = false
	field.Val = val

	return nil
}

func (field *Money) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
		val     []byte
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = field.Val[:0]
		return nil
	}

	// value

	if val, err = mr.ReadStringAsBytes(field.Val[:0]); err != nil {
		return err
	}

	field.Is_Null = false
	field.Val = val

	return nil
}

func (field *Numeric) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
		val     []byte
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = field.Val[:0]
		return nil
	}

	// value

	if val, err = mr.ReadStringAsBytes(field.Val[:0]); err != nil {
		return err
	}

	field.Is_Null = false
	field.Val = val

	return nil
}

func (field *Float) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
		val     float64
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = 0
		return nil
	}

	// value

	if val, err = mr.ReadFloat64(); err != nil {
		return err
	}

	field.Is_Null = false
	field.Val = val

	return nil
}

func (field *Date) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type

		delta_days uint32

		unix_sec int64
		val      time.Time
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = time.Time{}
		return nil
	}

	// value

	if delta_days, err = mr.ReadUint32(); err != nil {
		return err
	}

	unix_sec = UNIX_SEC_LOWEST + int64(delta_days)*SECONDS_PER_DAY

	val = time.Unix(unix_sec, 0).UTC()

	field.Is_Null = false
	field.Val = val

	return nil
}

func (field *Time) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
		sz      uint32

		delta_seconds uint32
		delta_ns      uint32

		unix_sec int64
		val      time.Time
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = time.Time{}
		return nil
	}

	// value

	if sz, err = mr.ReadArrayHeader(); err != nil {
		return err
	}

	assert(sz == 2)

	if delta_seconds, err = mr.ReadUint32(); err != nil {
		return err
	}

	if delta_ns, err = mr.ReadUint32(); err != nil {
		return err
	}

	unix_sec = UNIX_SEC_1900_01_01 + int64(delta_seconds)

	val = time.Unix(unix_sec, int64(delta_ns)).UTC()

	field.Is_Null = false
	field.Val = val

	return nil
}

func (field *Datetime) read_value(mr *msgp.Reader) error {
	var (
		err     error
		objtype msgp.Type
		sz      uint32

		delta_days    uint32
		delta_seconds uint32
		delta_ns      uint32

		unix_sec int64
		val      time.Time
	)

	if objtype, err = mr.NextType(); err != nil {
		return err
	}

	// NULL

	if objtype == msgp.NilType {
		if mr.ReadNil(); err != nil {
			return err
		}

		field.Is_Null = true
		field.Val = time.Time{}
		return nil
	}

	// value

	if sz, err = mr.ReadArrayHeader(); err != nil {
		return err
	}

	assert(sz == 3)

	if delta_days, err = mr.ReadUint32(); err != nil {
		return err
	}

	if delta_seconds, err = mr.ReadUint32(); err != nil {
		return err
	}

	if delta_ns, err = mr.ReadUint32(); err != nil {
		return err
	}

	unix_sec = (UNIX_SEC_LOWEST + int64(delta_days)*SECONDS_PER_DAY) + int64(delta_seconds)

	val = time.Unix(unix_sec, int64(delta_ns)).UTC()

	field.Is_Null = false
	field.Val = val

	return nil
}

// Fill_row_with_values fills in values into row fields, from a messagepack Reader.
//
func (session *Session) Fill_row_with_values(row []IField) error {
	var (
		err      error
		row_size uint32
	)

	// read field values and fill-in row

	if row_size, err = session.mr.ReadArrayHeader(); err != nil {
		return err
	}

	assert(len(row) == int(row_size))

	for _, field := range row {
		if err := field.read_value(session.mr); err != nil {
			return err
		}
	}

	return nil
}

// Read_string reads a string, from a messagepack Reader.
//
func (session *Session) Read_string() (string, error) {
	var (
		err error
		s   string
	)

	// read string

	if s, err = session.mr.ReadString(); err != nil {
		return "", err
	}

	return s, nil
}

// Read_int64 reads int64, from a messagepack Reader.
//
func (session *Session) Read_int64() (int64, error) {
	var (
		err error
		val int64
	)

	// read string

	if val, err = session.mr.ReadInt64(); err != nil {
		return 0, err
	}

	return val, nil
}
