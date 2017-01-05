package drv

import (
	"fmt"
	"time"
	"math"
	"strconv"

	"rsql/rsqlib"
)

// Datatype is the datatype of a record column.
//
type Datatype uint32

const (
	VOID Datatype = 1 << iota // NULL literal
	VARBINARY
	VARCHAR // also if original datatype was CHAR

	BIT
	TINYINT
	SMALLINT
	INT
	BIGINT

	MONEY
	NUMERIC
	FLOAT

	DATE
	TIME
	DATETIME
)

// String returns the datatype as string.
//
func (dt Datatype) String() string {

	switch dt {
	case VOID:
		return "VOID"
	case VARBINARY:
		return "VARBINARY"
	case VARCHAR:
		return "VARCHAR"
	case BIT:
		return "BIT"
	case TINYINT:
		return "TINYINT"
	case SMALLINT:
		return "SMALLINT"
	case INT:
		return "INT"
	case BIGINT:
		return "BIGINT"
	case MONEY:
		return "MONEY"
	case NUMERIC:
		return "NUMERIC"
	case FLOAT:
		return "FLOAT"
	case DATE:
		return "DATE"
	case TIME:
		return "TIME"
	case DATETIME:
		return "DATETIME"
	default:
		panic(fmt.Sprintf("unknown datatype %d", dt))
	}
}

// ColCount returns the number of columns in the current recordset.
//
func (b *Batch) ColCount() int {

	return len(b.record)
}

// ColDatatype returns the datatype of the column i of the record.
//
func (b *Batch) ColDatatype(i int) Datatype {
	var (
		field rsqlib.IField
	)

	field = b.record[i]

	switch field.Datatype() {
	case rsqlib.DTYPE_VOID:
		return VOID
	case rsqlib.DTYPE_VARBINARY:
		return VARBINARY
	case rsqlib.DTYPE_VARCHAR:
		return VARCHAR
	case rsqlib.DTYPE_BIT:
		return BIT
	case rsqlib.DTYPE_TINYINT:
		return TINYINT
	case rsqlib.DTYPE_SMALLINT:
		return SMALLINT
	case rsqlib.DTYPE_INT:
		return INT
	case rsqlib.DTYPE_BIGINT:
		return BIGINT
	case rsqlib.DTYPE_MONEY:
		return MONEY
	case rsqlib.DTYPE_NUMERIC:
		return NUMERIC
	case rsqlib.DTYPE_FLOAT:
		return FLOAT
	case rsqlib.DTYPE_DATE:
		return DATE
	case rsqlib.DTYPE_TIME:
		return TIME
	case rsqlib.DTYPE_DATETIME:
		return DATETIME
	default:
		panic(fmt.Sprintf("unknown datatype in field %d.", i))
	}
}

// ColIsNull returns true if column i contains the NULL value.
//
func (b *Batch) ColIsNull(i int) bool {

	return b.record[i].IsNull()
}

// ColBool returns a bool containing the value of column i.
// If the column is NULL, false is returned and isnull is true.
//
// This method can only be called on columns of type VARCHAR, BIT, TINYINT, SMALLINT, INT, BIGINT, FLOAT.
//
// If column is VARCHAR, true is returned for the values '1', 't', 'T', 'TRUE', 'true', 'True'.
// If column is a numeric type, true is returned if value is not 0. Else, false is returned.
//
func (b *Batch) ColBool(i int) (val bool, isnull bool) {
	var (
		field rsqlib.IField
	)

	field = b.record[i]

	if field.IsNull() {
		return false, true
	}

	switch field.Datatype() {
	case rsqlib.DTYPE_VARCHAR:
		var res bool
		var err error
		if res, err = strconv.ParseBool(string(field.(*rsqlib.Varchar).Val)); err != nil {
			return false, false
		}

		return res, false

	case rsqlib.DTYPE_BIT:
		return field.(*rsqlib.Bit).Val != 0, false

	case rsqlib.DTYPE_TINYINT:
		return field.(*rsqlib.Tinyint).Val != 0, false

	case rsqlib.DTYPE_SMALLINT:
		return field.(*rsqlib.Smallint).Val != 0, false

	case rsqlib.DTYPE_INT:
		return field.(*rsqlib.Int).Val != 0, false

	case rsqlib.DTYPE_BIGINT:
		return field.(*rsqlib.Bigint).Val != 0, false

	case rsqlib.DTYPE_FLOAT:
		return field.(*rsqlib.Float).Val != 0, false

	default:
		panic(fmt.Sprintf("record field %d of type VARBINARY, MONEY, NUMERIC, DATE, TIME or DATETIME cannot be converted to bool.", i))
	}
}

// ColBinary returns a byte slice containing the value of column i.
// If the column is NULL, nil is returned and isnull is true.
//
//       NOTE: the returned byte slice is owned by the driver and will be modified when the next record is read.
//       You should not modify this byte slice, but only read it. If you want to keep it or modify it, you must make a copy.
//
// This method can only be called on columns of type VARBINARY.
//
func (b *Batch) ColBinary(i int) (val []byte, isnull bool) {
	var (
		field rsqlib.IField
	)

	field = b.record[i]

	if field.IsNull() {
		return nil, true
	}

	switch field.Datatype() {
	case rsqlib.DTYPE_VARBINARY:
		return field.(*rsqlib.Varbinary).Val, false

	default:
		panic(fmt.Sprintf("record field %d is not a binary datatype.", i))
	}
}

// ColString returns a string containing the value of column i.
// If the column is NULL, an empty string is returned and isnull is true.
//
// This method can be called on columns of any datatype.
//
func (b *Batch) ColString(i int) (val string, isnull bool) {
	var (
		field rsqlib.IField
	)

	field = b.record[i]

	if field.IsNull() {
		return "", true
	}

	switch field.Datatype() {
	case rsqlib.DTYPE_VARCHAR:
		return string(field.(*rsqlib.Varchar).Val), false

	case rsqlib.DTYPE_MONEY:
		return string(field.(*rsqlib.Money).Val), false

	case rsqlib.DTYPE_NUMERIC:
		return string(field.(*rsqlib.Numeric).Val), false

	default:
		return field.String(), false
	}
}

// ColInt64 returns an int64 containing the value of column i.
// If the column is NULL, 0 is returned and isnull is true.
//
// This method can only be called on columns of type BIT, TINYINT, SMALLINT, INT, BIGINT.
//
func (b *Batch) ColInt64(i int) (val int64, isnull bool) {
	var (
		field rsqlib.IField
	)

	field = b.record[i]

	if field.IsNull() {
		return 0, true
	}

	switch field.Datatype() {
	case rsqlib.DTYPE_BIT:
		return int64(field.(*rsqlib.Bit).Val), false

	case rsqlib.DTYPE_TINYINT:
		return int64(field.(*rsqlib.Tinyint).Val), false

	case rsqlib.DTYPE_SMALLINT:
		return int64(field.(*rsqlib.Smallint).Val), false

	case rsqlib.DTYPE_INT:
		return int64(field.(*rsqlib.Int).Val), false

	case rsqlib.DTYPE_BIGINT:
		return int64(field.(*rsqlib.Bigint).Val), false

	default:
		panic(fmt.Sprintf("record field %d is not an integer datatype.", i))
	}
}

// ColInt is the same as ColInt64, but returns int.
// It is just provided for convenience.
//
func (b *Batch) ColInt(i int) (val int, isnull bool) {

	val64, isnull := b.ColInt64(i)

	return int(val64), isnull
}

// ColNumeric returns a string containing the value of column i.
// If the column is NULL, an empty string is returned and isnull is true.
//
// The result is the same as ColString, but the function name just emphasizes that the result is a numeric value.
//
// This method can only be called on columns of type BIT, TINYINT, SMALLINT, INT, BIGINT, MONEY, NUMERIC.
//
func (b *Batch) ColNumeric(i int) (val string, isnull bool) {
	var (
		field rsqlib.IField
	)

	field = b.record[i]

	if field.IsNull() {
		return "", true
	}

	switch field.Datatype() {
	case rsqlib.DTYPE_BIT, rsqlib.DTYPE_TINYINT, rsqlib.DTYPE_SMALLINT, rsqlib.DTYPE_INT, rsqlib.DTYPE_BIGINT:
		return field.String(), false

	case rsqlib.DTYPE_MONEY:
		return string(field.(*rsqlib.Money).Val), false

	case rsqlib.DTYPE_NUMERIC:
		return string(field.(*rsqlib.Numeric).Val), false

	default:
		panic(fmt.Sprintf("record field %d is not an integer, money or numeric datatype.", i))
	}
}

// ColFloat64 returns a float64 containing the value of column i.
// If the column is NULL, 0 is returned and isnull is true.
//
// This method can only be called on columns of type FLOAT.
//
func (b *Batch) ColFloat64(i int) (val float64, isnull bool) {
	var (
		field rsqlib.IField
	)

	field = b.record[i]

	if field.IsNull() {
		return 0, true
	}

	switch field.Datatype() {
	case rsqlib.DTYPE_FLOAT:
		return field.(*rsqlib.Float).Val, false

	default:
		panic(fmt.Sprintf("record field %d is not a float datatype.", i))
	}
}

// ColDatetime returns a time.Time containing the value of column i, with location UTC.
// If the column is NULL, the zero time.Time value (0001-01-01) is returned and isnull is true.
//
// For columns of datatype TIME, the returned value is the time on 1900.01.01 UTC, which is the zero date on SQL Server.
//
// This method can only be called on columns of type DATE, TIME, DATETIME.
//
func (b *Batch) ColDatetimeUTC(i int) (val time.Time, isnull bool) {
	var (
		field rsqlib.IField
	)

	field = b.record[i]

	if field.IsNull() {
		return time.Time{}, true
	}

	switch field.Datatype() {
	case rsqlib.DTYPE_DATE:
		return field.(*rsqlib.Date).Val, false

	case rsqlib.DTYPE_TIME:
		return field.(*rsqlib.Time).Val, false // year is 1900.01.01

	case rsqlib.DTYPE_DATETIME:
		return field.(*rsqlib.Datetime).Val, false

	default:
		panic(fmt.Sprintf("record field %d is not a date, time or datetime datatype.", i))
	}
}

// ColDatetime returns the same value as ColDatetimeUTC, but for columns of datatype DATE and DATETIME, the Time location is set to local time.
//
// For columns of datatype TIME, the returned value has location in UTC.
//
func (b *Batch) ColDatetime(i int) (val time.Time, isnull bool) {
	var (
		field rsqlib.IField
	)

	field = b.record[i]

	if field.IsNull() {
		return time.Time{}, true
	}

	if field.Datatype() == rsqlib.DTYPE_TIME { // if TIME, the result is in UTC, because computation on time should be independent of summer time
		return field.(*rsqlib.Time).Val, false // year is 1900.01.01, UTC
	}

	valUTC, isnull := b.ColDatetimeUTC(i)

	if isnull { // never happens
		panic("impossible: DATE or DATETIME is NULL.")
	}

	return LocalizeTime(valUTC), isnull
}

// LocalizeTime is a utility function that returns a time.Time with same year, month, day, hour, minute, second, ns as t, but as seen in local time.
// Most often, the absolute time of the result will be shifted so that the presentation time in local time is the same.
//
//    t := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
//    t2 := drv.LocalizeTime(t)
//
//    fmt.Println(t)             // 2009-11-10 23:00:00 +0000 UTC
//    fmt.Println(t2)            // 2009-11-10 23:00:00 +0100 CET
//    fmt.Println(t.Equal(t2))   // false, because absolute times are different
//
func LocalizeTime(t time.Time) time.Time {
	var res time.Time

	year, month, day := t.Date()
	hour, minute, second := t.Clock()
	nanosecond := t.Nanosecond()
	res = time.Date(year, month, day, hour, minute, second, nanosecond, time.Local)

	return res
}

// Scan copies the columns in the current record into dest.
//
// The dest arguments count must be the same as the record columns count.
//
// dest is a list of pointers of type:
//
//     &bool, &[]byte, &string, &int8, &int16, &int32, &int64, &int, &uint8, &uint16, &uint32, &uint64, &uint, &float64, &time.Time
//
// Example:
//
//	func main() {
//		var (
//			err  error
//			conn *drv.Connection
//			b    *drv.Batch
//
//			aa int
//			bb string
//		)
//
//		if conn, err = drv.NewConnection("server=localhost;login=sa;password=changeme;database=mytest"); err != nil {
//			log.Fatalf("%s", err)
//		}
//		defer conn.Close()
//
//		// create table t1
//
//		if b, err = conn.Execute(`drop table mytest..t1; create table mytest..t1 (a int null, b varchar(20) null)`); err != nil {
//			log.Fatalf("%s", err)
//		}
//
//		// insert records into table t1
//
//		if b, err = conn.Execute(`insert into mytest..t1 values (10, 'Hello'), (null, null), (20, 'World');`); err != nil {
//			log.Fatalf("%s", err)
//		}
//
//		// select a, b from t1
//
//		if b, err = conn.Query(`select a, b from mytest..t1 order by a;`); err != nil {
//			log.Fatalf("%s", err)
//		}
//
//		for b.Next() { // for each record
//			if err := b.Scan(&aa, &bb); err != nil {
//				log.Fatalf("%s", err)
//			}
//
//			nullaa := ""
//			if b.ColIsNull(0) {
//				nullaa = "(null)"
//			}
//
//			nullbb := ""
//			if b.ColIsNull(1) {
//				nullbb = "(null)"
//			}
//
//			fmt.Printf("%10d %10s  %10s %10s\n", aa, nullaa, "\""+bb+"\"", nullbb)
//		}
//
//		if b.Err() != nil {
//			log.Fatalf("%s", b.Err())
//		}
//
//		fmt.Printf("\n(%d row(s) affected)\n", b.RecordCount())
//	}
//
// The result is:
//
//	         0     (null)          ""     (null)
//	        10                "Hello"           
//	        20                "World"           
//
//	(3 row(s) affected)
//
func (b *Batch) Scan(dest ...interface{}) error {

	if b.err != nil {
		return b.err
	}

	if b.status != sTATUS_RECORD_AVAILABLE {
		return fmt.Errorf("scan: record not available.")
	}

	if len(dest) != b.ColCount() {
		return fmt.Errorf("scan: dest arguments count must be the same as record columns count (%d).", b.ColCount())
	}

	for i, dt := range dest {
		switch dt := dt.(type) {

		// bool

		case *bool:
			val, _ := b.ColBool(i)
			*dt = val

		// byte string

		case *[]byte:
			val, _ := b.ColBinary(i)
			*dt = append((*dt)[:0], val...) // copy bytes to dest

		// string

		case *string:
			val, _ := b.ColString(i)
			*dt = val

		// signed int

		case *int8:
			val, _ := b.ColInt64(i)
			if val < math.MinInt8 || val > math.MaxInt8 {
				return fmt.Errorf("scan: column %d to int8: overflow.", i)
			}
			*dt = int8(val)

		case *int16:
			val, _ := b.ColInt64(i)
			if val < math.MinInt16 || val > math.MaxInt16 {
				return fmt.Errorf("scan: column %d to int16: overflow.", i)
			}
			*dt = int16(val)

		case *int32:
			val, _ := b.ColInt64(i)
			if val < math.MinInt32 || val > math.MaxInt32 {
				return fmt.Errorf("scan: column %d to int32: overflow.", i)
			}
			*dt = int32(val)

		case *int64:
			val, _ := b.ColInt64(i)
			*dt = val

		case *int:
			val, _ := b.ColInt(i)
			*dt = val

		// unsigned int

		case *uint8:
			val, _ := b.ColInt64(i)
			if val < 0 || val > math.MaxUint8 {
				return fmt.Errorf("scan: column %d to uint8: overflow.", i)
			}
			*dt = uint8(val)

		case *uint16:
			val, _ := b.ColInt64(i)
			if val <0 || val > math.MaxUint16 {
				return fmt.Errorf("scan: column %d to uint16: overflow.", i)
			}
			*dt = uint16(val)

		case *uint32:
			val, _ := b.ColInt64(i)
			if val < 0 || val > math.MaxUint32 {
				return fmt.Errorf("scan: column %d to uint32: overflow.", i)
			}
			*dt = uint32(val)

		case *uint64:
			val, _ := b.ColInt64(i)
			if val < 0 {
				return fmt.Errorf("scan: column %d to uint64: overflow.", i)
			}
			*dt = uint64(val)

		case *uint:
			val, _ := b.ColInt64(i)
			if val < 0 {
				return fmt.Errorf("scan: column %d to uint64: overflow.", i)
			}
			*dt = uint(val)

		// float64

		case *float64:
			val, _ := b.ColFloat64(i)
			*dt = val

		// time.Time

		case *time.Time:
			val, _ := b.ColDatetime(i)
			*dt = val

		// default

		default:
			return fmt.Errorf("scan: destination type not supported.")
		}
	}

	return nil
}



