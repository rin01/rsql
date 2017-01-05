package drv

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// BindNULL replaces all occurrences of the specified placeholder by the literal NULL.
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindNULL(param string) *SQLpart {

	if part.err != nil {
		return part
	}

	part.setParam(param, "NULL") // put error in part.err if any

	return part
}

// BindBytes replaces all occurrences of the specified placeholder by a literal binary string.
// E.g. 0x1234
//
// If b is nil or empty, the replacing value is 0x. To put the NULL constant instead, use BindNULL.
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindBytes(param string, b []byte) *SQLpart {
	var val string

	if part.err != nil {
		return part
	}

	val = "0x"
	if len(b) > 0 {
		val = fmt.Sprintf("%#x", b) // print leading 0x
	}

	part.setParam(param, val) // put error in part.err if any

	return part
}

// BindStr replaces all occurrences of the specified placeholder by a literal string.
// E.g.   'Hello O''Hara'
//
// All single quotes in s are replaced by two single quotes.
// This method encloses the string by single quotes.
//
// If s is empty string, the replacing value is also the empty string ''. To put the NULL constant instead, use BindNULL.
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindStr(param string, s string) *SQLpart {
	var val string

	if part.err != nil {
		return part
	}

	val = "'" + strings.Replace(s, "'", "''", -1) + "'" // replace all single quote by two single quotes, and quote the string

	part.setParam(param, val) // put error in part.err if any

	return part
}

// BindInt replaces all occurrences of the specified placeholder by a literal integer.
// E.g. 1234
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindInt(param string, i int) *SQLpart {

	return part.BindInt64(param, int64(i))
}

// BindInt8 replaces all occurrences of the specified placeholder by a literal integer.
// E.g. 123
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindInt8(param string, i int8) *SQLpart {

	return part.BindInt64(param, int64(i))
}

// BindInt16 replaces all occurrences of the specified placeholder by a literal integer.
// E.g. 1234
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindInt16(param string, i int16) *SQLpart {

	return part.BindInt64(param, int64(i))
}

// BindInt32 replaces all occurrences of the specified placeholder by a literal integer.
// E.g. 1234
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindInt32(param string, i int32) *SQLpart {

	return part.BindInt64(param, int64(i))
}

// BindInt64 replaces all occurrences of the specified placeholder by a literal integer.
// E.g. 1234
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindInt64(param string, i int64) *SQLpart {
	var val string

	if part.err != nil {
		return part
	}

	val = strconv.FormatInt(i, 10)

	part.setParam(param, val) // put error in part.err if any

	return part
}

// BindUint replaces all occurrences of the specified placeholder by a literal integer.
// E.g. 1234
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindUint(param string, i uint) *SQLpart {

	return part.BindUint64(param, uint64(i))
}

// BindUint8 replaces all occurrences of the specified placeholder by a literal integer.
// E.g. 123
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindUint8(param string, i uint8) *SQLpart {

	return part.BindUint64(param, uint64(i))
}

// BindUint16 replaces all occurrences of the specified placeholder by a literal integer.
// E.g. 1234
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindUint16(param string, i uint16) *SQLpart {

	return part.BindUint64(param, uint64(i))
}

// BindUint32 replaces all occurrences of the specified placeholder by a literal integer.
// E.g. 1234
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindUint32(param string, i uint32) *SQLpart {

	return part.BindUint64(param, uint64(i))
}

// BindUint64 replaces all occurrences of the specified placeholder by a literal integer.
// E.g. 1234
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindUint64(param string, i uint64) *SQLpart {
	var val string

	if part.err != nil {
		return part
	}

	val = strconv.FormatUint(i, 10)

	part.setParam(param, val) // put error in part.err if any

	return part
}

// BindNumstr replaces all occurrences of the specified placeholder by the string numstr.
// E.g. -1.234e-3
//
// numstr must be a valid number, containing only digits, sign and 'e' or 'E' symbols. Else, an error is put in the SQLpart object.
//
// You should use this function for numbers that are not Go primitive types, such as decimal, numeric, big.Rat etc.
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindNumstr(param string, numstr string) *SQLpart {
	var (
		decimalDotPos     int
		exponentSymbolPos int
	)

	if part.err != nil {
		return part
	}

	decimalDotPos = -1
	exponentSymbolPos = -1

	numstr = strings.TrimSpace(numstr)

	for i, r := range numstr { // check that numstr contains valid characters
		if r >= '0' && r <= '9' {
			continue
		}

		switch r {
		case 'e', 'E':
			if exponentSymbolPos != -1 { // only one E can exist
				part.err = fmt.Errorf("param \"%s\": number %s is invalid.", param, numstr)
				return part
			}
			exponentSymbolPos = i

		case '+', '-':
			if !(i == 0 || i == exponentSymbolPos+1) { // sign must be first character, or just after E
				part.err = fmt.Errorf("param \"%s\": number %s is invalid.", param, numstr)
				return part
			}

		case '.':
			if decimalDotPos != -1 { // only one decimal dot can exist
				part.err = fmt.Errorf("param \"%s\": number %s is invalid.", param, numstr)
				return part
			}
			decimalDotPos = i

		default:
			part.err = fmt.Errorf("param \"%s\": number %s is invalid.", param, numstr)
			return part
		}
	}

	part.setParam(param, numstr) // put error in part.err if any

	return part
}

// BindFloat64 replaces all occurrences of the specified placeholder by a literal float.
// E.g. 1234.5e6
//
// NaN and Infinite cannot be stored in SQL Server, so you should check that f doesn't contain these special values.
//
// If an error occurs (e.g. NaN of Infinite), it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindFloat64(param string, f float64) *SQLpart {
	var val string

	if part.err != nil {
		return part
	}

	if math.IsInf(f, 0) {
		part.err = fmt.Errorf("param \"%s\": invalid float64, is Infinite.", param)
		return part
	}

	if math.IsNaN(f) {
		part.err = fmt.Errorf("param \"%s\": invalid float64, is NaN.", param)
		return part
	}

	val = strconv.FormatFloat(f, 'E', -1, 64)

	part.setParam(param, val) // put error in part.err if any

	return part
}

// BindDate replaces all occurrences of the specified placeholder by a literal date as string, enclosed by single quotes.
// E.g. '20060102'
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindDate(param string, d time.Time) *SQLpart {

	if part.err != nil {
		return part
	}

	s := "'" + d.Format("20060102") + "'"

	part.setParam(param, s) // put error in part.err if any

	return part
}

// BindTime replaces all occurrences of the specified placeholder by a literal time as string, enclosed by single quotes.
// E.g. '15:04:05', or '15:04:05.999999999' if miliiseconds are not 0.
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindTime(param string, t time.Time) *SQLpart {

	if part.err != nil {
		return part
	}

	formatstring := "15:04:05"
	if t.Nanosecond() != 0 {
		formatstring = "15:04:05.999999999"
	}

	s := "'" + t.Format(formatstring) + "'"

	part.setParam(param, s) // put error in part.err if any

	return part
}

// BindDatetime replaces all occurrences of the specified placeholder by a literal datetime as string, enclosed by single quotes.
// E.g. '20060102', or '2006-01-02T15:04:05' or '2006-01-02T15:04:05.999999999' if time part is not 0.
//
// If an error occurs, it is put in the SQLpart object, and can be checked by calling part.Err() method.
//
func (part *SQLpart) BindDatetime(param string, dt time.Time) *SQLpart {
	var formatstring string

	if part.err != nil {
		return part
	}

	switch {
	case dt.Nanosecond() != 0:
		formatstring = "2006-01-02T15:04:05.999999999"
	case !(dt.Hour() == 0 && dt.Minute() == 0 && dt.Second() == 0):
		formatstring = "2006-01-02T15:04:05"
	default:
		formatstring = "20060102"
	}

	s := "'" + dt.Format(formatstring) + "'"

	part.setParam(param, s) // put error in part.err if any

	return part
}

// setParam replaces all occurrences of the specified placeholder by val.
//
// If an error occurs, it is put in part.err.
//
func (part *SQLpart) setParam(param string, val string) {
	var (
		targets []int
		ok      bool
	)

	if part.err != nil {
		return
	}

	param = strings.ToLower(param)

	if targets, ok = part.placeholderMap[param]; ok == false {
		err := fmt.Errorf("param \"%s\": not known.", param)
		part.err = err
		return
	}

	for _, pos := range targets { // replace all placeholders of the same name
		part.textFragments[pos] = val
	}
}
