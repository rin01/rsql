// Copyright 2017 Nicolas RIESCH
// Use of this source code is governed by the license found in the LICENCE file.

package msgp

import (
	"bufio"
	"fmt"
	"io"
	"math"
)

//*******************************************
//           NextType() method
//*******************************************

type Type byte

const (
	InvalidType Type = iota

	BinType
	StrType

	NilType
	BoolType
	UintType
	IntType
	Float32Type
	Float64Type

	ArrayType
	MapType
)

func (m *Reader) NextType() (Type, error) {
	var (
		err    error
		prefix uint8
	)

	// peek prefix

	if prefix, err = m.peek_byte(); err != nil {
		return InvalidType, err
	}

	// analyze prefix

	if prefix <= 127 { // fixint
		return IntType, nil
	}

	if prefix >= M_NEGATIVE_FIXINT_BASE { // negative fixint
		return IntType, nil
	}

	if prefix&PREFIX_FIXSTR_MASK == M_FIXSTR_BASE { // fixstr
		return StrType, nil
	}

	if prefix&PREFIX_FIXARRAY_MASK == M_FIXARRAY_BASE { // fixarray
		return ArrayType, nil
	}

	if prefix&PREFIX_FIXMAP_MASK == M_FIXMAP_BASE { // fixmap
		return MapType, nil
	}

	switch prefix {
	case M_NIL:
		return NilType, nil
	case M_FALSE,
		M_TRUE:
		return BoolType, nil
	case M_BIN8,
		M_BIN16,
		M_BIN32:
		return BinType, nil
	case M_FLOAT32:
		return Float32Type, nil
	case M_FLOAT64:
		return Float64Type, nil
	case M_UINT8,
		M_UINT16,
		M_UINT32,
		M_UINT64:
		return UintType, nil
	case M_INT8,
		M_INT16,
		M_INT32,
		M_INT64:
		return IntType, nil
	case M_STR8,
		M_STR16,
		M_STR32:
		return StrType, nil
	case M_ARRAY16,
		M_ARRAY32:
		return ArrayType, nil
	case M_MAP16,
		M_MAP32:
		return MapType, nil
	default:
		return InvalidType, nil
	}
}

//*******************************************
//         messagepack Reader
//*******************************************

const (
	READER_SCRATCH_BUFFER_DEFAULT_CAPACITY = 1024 // ReadString() may need a large buffer, if string being read is large
)

// Reader reads msgpack data from a buffered reader.
//
// All Read... methods returns an error.
//       errors can be:
//            - connection is broken
//            - a unexpected datatype is being read. This means the communication protocol is messed up
//            - integer overflow. This also means the communication protocol is messed up
//
//       In case of error, the cause is really serious and cannot be recovered.
//       The best thing to do is to terminate the connection and the session, and making all necessary cleaning up of resources.
//
type Reader struct {
	br      *bufio.Reader // messagepack stream is read from this bufio.Reader
	scratch []byte        // messagepack subparts (e.g. prefix byte, uint8, uint16 etc raw integers) are read from bufio.Reader into this little buffer to be decoded. ReadString() also reads the entire string into this buffer, before converting it to string.
}

// NewReader returns a messagepack Reader.
// A bufio.Reader will be created internally if argument is not a *bufio.Reader.
//
func NewReader(rd io.Reader) *Reader {
	var (
		br *bufio.Reader
		ok bool
	)

	if br, ok = rd.(*bufio.Reader); ok == false {
		br = bufio.NewReader(rd)
	}

	m := &Reader{}

	m.br = br
	m.scratch = make([]byte, 0, READER_SCRATCH_BUFFER_DEFAULT_CAPACITY)

	return m
}

func error_bad_prefix(funcname string, prefix uint8) error {

	return fmt.Errorf("msgp %s: bad prefix byte %08b", funcname, prefix)
}

//*******************************************
//                  read_N
//*******************************************

// read_N reads exactly n bytes from internal reader.
// The internal m.scratch buffer is overwritten, and is returned to the caller, having length n.
//
//     THE CALLER SHOULD NOT KEEP IT, as this is the internal buffer of the Reader.
//
func (m *Reader) read_N(n int) ([]byte, error) {
	var (
		err  error
		buff []byte
	)

	if buff, err = m.ReadNBytes(m.scratch, n); err != nil {
		return nil, err
	}

	m.scratch = buff

	return buff, nil
}

// ReadNBytes reads exactly n bytes from internal reader.
// dest buffer is overwritten, and is returned to the caller.
// If dest capacity < n, a new larger buffer is returned.
//
// If success, the returned buffer is always of length n.
//
func (m *Reader) ReadNBytes(dest []byte, n int) (res []byte, err error) {

	buff := dest
	capacity := cap(buff)

	if n > capacity {
		extra := n - capacity
		buff = append(buff[:capacity], make([]byte, extra)...)
	}

	buff = buff[:n]

	if _, err := io.ReadFull(m.br, buff); err != nil {
		return dest, err
	}

	return buff, nil
}

//*******************************************
//           utility functions
//*******************************************

func (m *Reader) peek_byte() (bb uint8, err error) {
	var (
		p []byte
	)

	if p, err = m.br.Peek(1); err != nil {
		return 0, err
	}

	return p[0], nil
}

func (m *Reader) read_prefix() (prefix uint8, err error) {
	var (
		buff []byte
	)

	if buff, err = m.read_N(1); err != nil {
		return 0, err
	}

	return buff[0], nil
}

func (m *Reader) read_raw_uint8() (val uint8, err error) {
	var (
		buff []byte
	)

	if buff, err = m.read_N(1); err != nil {
		return 0, err
	}

	return buff[0], nil
}

func (m *Reader) read_raw_uint16() (val uint16, err error) {
	var (
		buff []byte
	)

	if buff, err = m.read_N(2); err != nil {
		return 0, err
	}

	val = uint16(buff[0])<<8 | uint16(buff[1])

	return val, nil
}

func (m *Reader) read_raw_uint32() (val uint32, err error) {
	var (
		buff []byte
	)

	if buff, err = m.read_N(4); err != nil {
		return 0, err
	}

	val = uint32(buff[0])<<24 | uint32(buff[1])<<16 | uint32(buff[2])<<8 | uint32(buff[3])

	return val, nil
}

func (m *Reader) read_raw_uint64() (val uint64, err error) {
	var (
		buff []byte
	)

	if buff, err = m.read_N(8); err != nil {
		return 0, err
	}

	val = uint64(buff[0])<<56 | uint64(buff[1])<<48 | uint64(buff[2])<<40 | uint64(buff[3])<<32 | uint64(buff[4])<<24 | uint64(buff[5])<<16 | uint64(buff[6])<<8 | uint64(buff[7])

	return val, nil
}

func (m *Reader) read_raw_int8() (val int8, err error) {
	var val_8 uint8

	val_8, err = m.read_raw_uint8() // value of err, nil or not nil, is returned next line

	return int8(val_8), err
}

func (m *Reader) read_raw_int16() (val int16, err error) {
	var val_16 uint16

	val_16, err = m.read_raw_uint16() // value of err, nil or not nil, is returned next line

	return int16(val_16), err
}

func (m *Reader) read_raw_int32() (val int32, err error) {
	var val_32 uint32

	val_32, err = m.read_raw_uint32() // value of err, nil or not nil, is returned next line

	return int32(val_32), err
}

func (m *Reader) read_raw_int64() (val int64, err error) {
	var val_64 uint64

	val_64, err = m.read_raw_uint64() // value of err, nil or not nil, is returned next line

	return int64(val_64), err
}

func first_bits_4(b uint8) uint8 {
	return b & 0x0f
}

func first_bits_5(b uint8) uint8 {
	return b & 0x1f
}

//*******************************************
//         public API functions
//*******************************************

func (m *Reader) ReadNil() (err error) {
	var (
		prefix uint8
	)

	if prefix, err = m.read_prefix(); err != nil {
		return err
	}

	if prefix == M_NIL {
		return nil
	}

	return error_bad_prefix("read nil", prefix)
}

func (m *Reader) ReadBool() (val bool, err error) {
	var (
		prefix uint8
	)

	if prefix, err = m.read_prefix(); err != nil {
		return false, err
	}

	switch prefix {
	case M_FALSE:
		return false, nil

	case M_TRUE:
		return true, nil

	default:
		return false, error_bad_prefix("read bool", prefix)
	}
}

func (m *Reader) ReadUint8() (val uint8, err error) {
	var in uint64

	if in, err = m.ReadUint64(); err != nil {
		return 0, err
	}

	if in > math.MaxUint8 {
		return 0, fmt.Errorf("msgp: ReadUint8 overflow, read %d", in)
	}

	val = uint8(in)

	return val, nil
}

func (m *Reader) ReadUint16() (val uint16, err error) {
	var in uint64

	if in, err = m.ReadUint64(); err != nil {
		return 0, err
	}

	if in > math.MaxUint16 {
		return 0, fmt.Errorf("msgp: ReadUint16 overflow, read %d", in)
	}

	val = uint16(in)

	return val, nil
}

func (m *Reader) ReadUint32() (val uint32, err error) {
	var in uint64

	if in, err = m.ReadUint64(); err != nil {
		return 0, err
	}

	if in > math.MaxUint32 {
		return 0, fmt.Errorf("msgp: ReadUint32 overflow, read %d", in)
	}

	val = uint32(in)

	return val, nil
}

func (m *Reader) ReadUint64() (val uint64, err error) {
	var (
		prefix uint8
		val_8  uint8
		val_16 uint16
		val_32 uint32
	)

	if prefix, err = m.read_prefix(); err != nil {
		return 0, err
	}

	if prefix <= 127 { // positive fixint
		return uint64(prefix), nil
	}

	switch prefix {
	case M_UINT8:
		if val_8, err = m.read_raw_uint8(); err != nil {
			return 0, err
		}

		val = uint64(val_8)

	case M_UINT16:
		if val_16, err = m.read_raw_uint16(); err != nil {
			return 0, err
		}

		val = uint64(val_16)

	case M_UINT32:
		if val_32, err = m.read_raw_uint32(); err != nil {
			return 0, err
		}

		val = uint64(val_32)

	case M_UINT64:
		if val, err = m.read_raw_uint64(); err != nil {
			return 0, err
		}

	default:
		return 0, error_bad_prefix("read uint", prefix)
	}

	return val, nil
}

func (m *Reader) ReadInt8() (val int8, err error) {
	var in int64

	if in, err = m.ReadInt64(); err != nil {
		return 0, err
	}

	if in < math.MinInt8 || in > math.MaxInt8 {
		return 0, fmt.Errorf("msgp: ReadInt8 overflow, read %d", in)
	}

	val = int8(in)

	return val, nil
}

func (m *Reader) ReadInt16() (val int16, err error) {
	var in int64

	if in, err = m.ReadInt64(); err != nil {
		return 0, err
	}

	if in < math.MinInt16 || in > math.MaxInt16 {
		return 0, fmt.Errorf("msgp: ReadInt16 overflow, read %d", in)
	}

	val = int16(in)

	return val, nil
}

func (m *Reader) ReadInt32() (val int32, err error) {
	var in int64

	if in, err = m.ReadInt64(); err != nil {
		return 0, err
	}

	if in < math.MinInt32 || in > math.MaxInt32 {
		return 0, fmt.Errorf("msgp: ReadInt32 overflow, read %d", in)
	}

	val = int32(in)

	return val, nil
}

func (m *Reader) ReadInt64() (val int64, err error) {
	var (
		prefix uint8
		val_8  int8
		val_16 int16
		val_32 int32
	)

	if prefix, err = m.read_prefix(); err != nil {
		return 0, err
	}

	if prefix <= 127 { // positive fixint
		return int64(prefix), nil
	}

	if prefix >= M_NEGATIVE_FIXINT_BASE { // negative fixint
		return int64(int8(prefix)), nil
	}

	switch prefix {
	case M_INT8:
		if val_8, err = m.read_raw_int8(); err != nil {
			return 0, err
		}

		val = int64(val_8)

	case M_INT16:
		if val_16, err = m.read_raw_int16(); err != nil {
			return 0, err
		}

		val = int64(val_16)

	case M_INT32:
		if val_32, err = m.read_raw_int32(); err != nil {
			return 0, err
		}

		val = int64(val_32)

	case M_INT64:
		if val, err = m.read_raw_int64(); err != nil {
			return 0, err
		}

	default:
		return 0, error_bad_prefix("read int", prefix)
	}

	return val, nil
}

func (m *Reader) ReadFloat32() (val float32, err error) {
	var (
		prefix     uint8
		float_bits uint32
	)

	if prefix, err = m.read_prefix(); err != nil {
		return 0, err
	}

	if prefix != M_FLOAT32 {
		return 0, error_bad_prefix("read float32", prefix)
	}

	if float_bits, err = m.read_raw_uint32(); err != nil {
		return 0, err
	}

	val = math.Float32frombits(float_bits)

	return val, nil
}

func (m *Reader) ReadFloat64() (val float64, err error) {
	var (
		prefix     uint8
		float_bits uint64
	)

	if prefix, err = m.read_prefix(); err != nil {
		return 0, err
	}

	if prefix != M_FLOAT64 {
		return 0, error_bad_prefix("read float64", prefix)
	}

	if float_bits, err = m.read_raw_uint64(); err != nil {
		return 0, err
	}

	val = math.Float64frombits(float_bits)

	return val, nil
}

func (m *Reader) ReadString() (val string, err error) {
	var buff []byte

	if buff, err = m.ReadStringAsBytes(m.scratch[:0]); err != nil {
		return "", err
	}

	m.scratch = buff

	return string(buff), nil
}

func (m *Reader) ReadStringAsBytes(dest []byte) (res []byte, err error) {
	var (
		buff []byte
		sz   uint32
	)

	if sz, err = m.ReadStringHeader(); err != nil {
		return nil, err
	}

	if buff, err = m.ReadNBytes(dest, int(sz)); err != nil {
		return dest, err
	}

	return buff, nil
}

func (m *Reader) ReadBytes(dest []byte) (res []byte, err error) {
	var (
		buff []byte
		sz   uint32
	)

	if sz, err = m.ReadBytesHeader(); err != nil {
		return nil, err
	}

	if buff, err = m.ReadNBytes(dest, int(sz)); err != nil {
		return dest, err
	}

	return buff, nil
}

func (m *Reader) ReadStringHeader() (sz uint32, err error) {
	var (
		prefix uint8
		sz_8   uint8
		sz_16  uint16
	)

	if prefix, err = m.read_prefix(); err != nil {
		return 0, err
	}

	if prefix&PREFIX_FIXSTR_MASK == M_FIXSTR_BASE { // fixstr
		sz = uint32(first_bits_5(prefix))

		return sz, nil
	}

	switch prefix {
	case M_STR8:
		if sz_8, err = m.read_raw_uint8(); err != nil {
			return 0, err
		}

		return uint32(sz_8), nil

	case M_STR16:
		if sz_16, err = m.read_raw_uint16(); err != nil {
			return 0, err
		}

		return uint32(sz_16), nil

	case M_STR32:
		if sz, err = m.read_raw_uint32(); err != nil {
			return 0, err
		}

		return sz, nil

	default:
		return 0, error_bad_prefix("read string", prefix)
	}
}

func (m *Reader) ReadBytesHeader() (sz uint32, err error) {
	var (
		prefix uint8
		sz_8   uint8
		sz_16  uint16
	)

	if prefix, err = m.read_prefix(); err != nil {
		return 0, err
	}

	switch prefix {
	case M_BIN8:
		if sz_8, err = m.read_raw_uint8(); err != nil {
			return 0, err
		}

		return uint32(sz_8), nil

	case M_BIN16:
		if sz_16, err = m.read_raw_uint16(); err != nil {
			return 0, err
		}

		return uint32(sz_16), nil

	case M_BIN32:
		if sz, err = m.read_raw_uint32(); err != nil {
			return 0, err
		}

		return sz, nil

	default:
		return 0, error_bad_prefix("read bin", prefix)
	}
}

func (m *Reader) ReadArrayHeader() (sz uint32, err error) {
	var (
		prefix uint8
		sz_16  uint16
	)

	if prefix, err = m.read_prefix(); err != nil {
		return 0, err
	}

	if prefix&PREFIX_FIXARRAY_MASK == M_FIXARRAY_BASE { // fixarray
		sz = uint32(first_bits_4(prefix))

		return sz, nil
	}

	switch prefix {
	case M_ARRAY16:
		if sz_16, err = m.read_raw_uint16(); err != nil {
			return 0, err
		}

		return uint32(sz_16), nil

	case M_ARRAY32:
		if sz, err = m.read_raw_uint32(); err != nil {
			return 0, err
		}

		return sz, nil

	default:
		return 0, error_bad_prefix("read array", prefix)
	}
}

func (m *Reader) ReadMapHeader() (sz uint32, err error) {
	var (
		prefix uint8
		sz_16  uint16
	)

	if prefix, err = m.read_prefix(); err != nil {
		return 0, err
	}

	if prefix&PREFIX_FIXMAP_MASK == M_FIXMAP_BASE { // fixmap
		sz = uint32(first_bits_4(prefix))

		return sz, nil
	}

	switch prefix {
	case M_MAP16:
		if sz_16, err = m.read_raw_uint16(); err != nil {
			return 0, err
		}

		return uint32(sz_16), nil

	case M_MAP32:
		if sz, err = m.read_raw_uint32(); err != nil {
			return 0, err
		}

		return sz, nil

	default:
		return 0, error_bad_prefix("read map", prefix)
	}
}

// ReadFull is a method that just calls io.ReadFull.
//
func (m *Reader) ReadFull(dest []byte) (n int, err error) {

	return io.ReadFull(m.br, dest)
}

func (m *Reader) ReadSimpleType() (interface{}, error) {
	var (
		err     error
		objtype Type
	)

	if objtype, err = m.NextType(); err != nil {
		return nil, err
	}

	switch objtype {
	case NilType:
		return nil, m.ReadNil()

	case BoolType:
		return m.ReadBool()

	case UintType:
		return m.ReadUint64()

	case IntType:
		return m.ReadInt64()

	case Float32Type:
		return m.ReadFloat32()

	case Float64Type:
		return m.ReadFloat64()

	case BinType:
		return m.ReadBytes(nil)

	case StrType:
		return m.ReadString()

	default:
		return nil, fmt.Errorf("msgp: ReadSimpleType: type not supported")
	}
}
