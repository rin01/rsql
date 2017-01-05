// Copyright 2017 Nicolas RIESCH
// Use of this source code is governed by the license found in the LICENCE file.

package msgp

import (
	"math"
)

const (
	M_NIL                  byte = 0xc0
	M_FALSE                byte = 0xc2
	M_TRUE                 byte = 0xc3
	M_UINT8                byte = 0xcc
	M_UINT16               byte = 0xcd
	M_UINT32               byte = 0xce
	M_UINT64               byte = 0xcf
	M_INT8                 byte = 0xd0
	M_INT16                byte = 0xd1
	M_INT32                byte = 0xd2
	M_INT64                byte = 0xd3
	M_FLOAT32              byte = 0xca
	M_FLOAT64              byte = 0xcb
	M_FIXSTR_BASE          byte = 0xa0 // 3 MSB bits are significant
	M_STR8                 byte = 0xd9
	M_STR16                byte = 0xda
	M_STR32                byte = 0xdb
	M_BIN8                 byte = 0xc4
	M_BIN16                byte = 0xc5
	M_BIN32                byte = 0xc6
	M_FIXARRAY_BASE        byte = 0x90 // 4 MSB bits are significant
	M_ARRAY16              byte = 0xdc
	M_ARRAY32              byte = 0xdd
	M_FIXMAP_BASE          byte = 0x80 // 4 MSB bits are significant
	M_MAP16                byte = 0xde
	M_MAP32                byte = 0xdf
	M_NEGATIVE_FIXINT_BASE byte = 0xe0 // 11100000 to 11111111 are negative fixint numbers

	PREFIX_FIXSTR_MASK   byte = 0xe0 // 11100000
	PREFIX_FIXARRAY_MASK byte = 0xf0 // 11110000
	PREFIX_FIXMAP_MASK   byte = 0xf0 // 11110000
)

func AppendNil(dest []byte) []byte {

	dest = append(dest, M_NIL)

	return dest
}

func AppendBool(dest []byte, val bool) []byte {
	var b byte

	b = M_FALSE // false
	if val {
		b = M_TRUE // true
	}

	dest = append(dest, b)

	return dest
}

func AppendUint8(dest []byte, val uint8) []byte {

	return AppendUint64(dest, uint64(val))
}

func AppendUint16(dest []byte, val uint16) []byte {

	return AppendUint64(dest, uint64(val))
}

func AppendUint32(dest []byte, val uint32) []byte {

	return AppendUint64(dest, uint64(val))
}

func AppendUint64(dest []byte, val uint64) []byte {

	switch {
	case val <= 127:
		dest = append(dest, uint8(val)) // positive fixint

	case val <= math.MaxUint8:
		dest = append(dest, M_UINT8, uint8(val))

	case val <= math.MaxUint16:
		dest = append(dest, M_UINT16, uint8(val>>8), uint8(val))

	case val <= math.MaxUint32:
		dest = append(dest, M_UINT32, uint8(val>>24), uint8(val>>16), uint8(val>>8), uint8(val))

	default:
		dest = append(dest, M_UINT64, uint8(val>>56), uint8(val>>48), uint8(val>>40), uint8(val>>32), uint8(val>>24), uint8(val>>16), uint8(val>>8), uint8(val))
	}

	return dest
}

func AppendInt8(dest []byte, val int8) []byte {

	return AppendInt64(dest, int64(val))
}

func AppendInt16(dest []byte, val int16) []byte {

	return AppendInt64(dest, int64(val))
}

func AppendInt32(dest []byte, val int32) []byte {

	return AppendInt64(dest, int64(val))
}

func AppendInt64(dest []byte, val int64) []byte {

	if val >= 0 {
		switch {
		case val <= 127:
			dest = append(dest, uint8(val)) // positive fixint

		//case val <= math.MaxInt8:  // not used, as it matches    case val <= 127
		//      dest = append(dest, M_INT8, uint8(val))

		case val <= math.MaxInt16:
			dest = append(dest, M_INT16, uint8(val>>8), uint8(val))

		case val <= math.MaxInt32:
			dest = append(dest, M_INT32, uint8(val>>24), uint8(val>>16), uint8(val>>8), uint8(val))

		default:
			dest = append(dest, M_INT64, uint8(val>>56), uint8(val>>48), uint8(val>>40), uint8(val>>32), uint8(val>>24), uint8(val>>16), uint8(val>>8), uint8(val))
		}

		return dest
	}

	// negative number

	switch {
	case val >= -32: // 0xe0  11100000
		dest = append(dest, uint8(val)) // negative fixint

	case val >= math.MinInt8:
		dest = append(dest, M_INT8, uint8(val))

	case val >= math.MinInt16:
		dest = append(dest, M_INT16, uint8(val>>8), uint8(val))

	case val >= math.MinInt32:
		dest = append(dest, M_INT32, uint8(val>>24), uint8(val>>16), uint8(val>>8), uint8(val))

	default:
		dest = append(dest, M_INT64, uint8(val>>56), uint8(val>>48), uint8(val>>40), uint8(val>>32), uint8(val>>24), uint8(val>>16), uint8(val>>8), uint8(val))
	}

	return dest
}

func AppendFloat32(dest []byte, f float32) []byte {
	var fbits uint32

	fbits = math.Float32bits(f)

	dest = append(dest, M_FLOAT32, uint8(fbits>>24), uint8(fbits>>16), uint8(fbits>>8), uint8(fbits))

	return dest
}

func AppendFloat64(dest []byte, f float64) []byte {
	var fbits uint64

	fbits = math.Float64bits(f)

	dest = append(dest, M_FLOAT64, uint8(fbits>>56), uint8(fbits>>48), uint8(fbits>>40), uint8(fbits>>32), uint8(fbits>>24), uint8(fbits>>16), uint8(fbits>>8), uint8(fbits))

	return dest
}

func AppendString(dest []byte, s string) []byte {
	var sz int

	sz = len(s)

	if sz > math.MaxUint32 {
		panic("msgp: string too long")
	}

	dest = AppendStringHeader(dest, uint32(sz))

	dest = append(dest, s...)

	return dest
}

func AppendStringFromBytes(dest []byte, s []byte) []byte {
	var sz int

	sz = len(s)

	if sz > math.MaxUint32 {
		panic("msgp: string too long")
	}

	dest = AppendStringHeader(dest, uint32(sz))

	dest = append(dest, s...)

	return dest
}

func AppendBytes(dest []byte, bts []byte) []byte {
	var sz int

	sz = len(bts)

	if sz > math.MaxUint32 {
		panic("msgp: byte slice too long")
	}

	dest = AppendBytesHeader(dest, uint32(sz))

	dest = append(dest, bts...)

	return dest
}

func AppendStringHeader(dest []byte, sz uint32) []byte {

	switch {
	case sz <= 31: // 0x1f  00011111
		dest = append(dest, M_FIXSTR_BASE|uint8(sz)) // fixstr

	case sz <= math.MaxUint8:
		dest = append(dest, M_STR8)
		dest = append(dest, uint8(sz))

	case sz <= math.MaxUint16:
		dest = append(dest, M_STR16)
		dest = append(dest, uint8(sz>>8), uint8(sz))

	default:
		dest = append(dest, M_STR32)
		dest = append(dest, uint8(sz>>24), uint8(sz>>16), uint8(sz>>8), uint8(sz))
	}

	return dest
}

func AppendBytesHeader(dest []byte, sz uint32) []byte {

	switch {
	case sz <= math.MaxUint8:
		dest = append(dest, M_BIN8)
		dest = append(dest, uint8(sz))

	case sz <= math.MaxUint16:
		dest = append(dest, M_BIN16)
		dest = append(dest, uint8(sz>>8), uint8(sz))

	default:
		dest = append(dest, M_BIN32)
		dest = append(dest, uint8(sz>>24), uint8(sz>>16), uint8(sz>>8), uint8(sz))
	}

	return dest
}

func AppendArrayHeader(dest []byte, sz uint32) []byte {

	switch {
	case sz <= 15: // 0x0f    00001111
		dest = append(dest, M_FIXARRAY_BASE|uint8(sz))

	case sz <= math.MaxUint16:
		dest = append(dest, M_ARRAY16)
		dest = append(dest, uint8(sz>>8), uint8(sz))

	default:
		dest = append(dest, M_ARRAY32)
		dest = append(dest, uint8(sz>>24), uint8(sz>>16), uint8(sz>>8), uint8(sz))
	}

	return dest
}

func AppendMapHeader(dest []byte, sz uint32) []byte {

	switch {
	case sz <= 15: // 0x0f    00001111
		dest = append(dest, M_FIXMAP_BASE|uint8(sz))

	case sz <= math.MaxUint16:
		dest = append(dest, M_MAP16)
		dest = append(dest, uint8(sz>>8), uint8(sz))

	default:
		dest = append(dest, M_MAP32)
		dest = append(dest, uint8(sz>>24), uint8(sz>>16), uint8(sz>>8), uint8(sz))
	}

	return dest
}

//========= more complex types =========

func AppendSimpleType(dest []byte, i interface{}) []byte {

	if i == nil {
		return AppendNil(dest)
	}

	switch i := i.(type) {
	case string:
		return AppendString(dest, i)
	case []byte:
		return AppendBytes(dest, i)

	case bool:
		return AppendBool(dest, i)
	case uint:
		return AppendUint64(dest, uint64(i))
	case uint8:
		return AppendUint8(dest, i)
	case uint16:
		return AppendUint16(dest, i)
	case uint32:
		return AppendUint32(dest, i)
	case uint64:
		return AppendUint64(dest, i)
	case int8:
		return AppendInt8(dest, i)
	case int16:
		return AppendInt16(dest, i)
	case int32:
		return AppendInt32(dest, i)
	case int64:
		return AppendInt64(dest, i)
	case int:
		return AppendInt64(dest, int64(i))
	case float32:
		return AppendFloat32(dest, i)
	case float64:
		return AppendFloat64(dest, i)

	default:
		panic("msgp: AppendIntf: type not supported")
	}
}

func AppendMapStrStr(dest []byte, m map[string]string) []byte {
	var sz int

	sz = len(m)
	if sz > math.MaxUint32 {
		panic("msgp: map has too many elements")
	}

	dest = AppendMapHeader(dest, uint32(sz))

	for key, val := range m {
		dest = AppendString(dest, key)
		dest = AppendString(dest, val)
	}

	return dest
}

func AppendMapStrSimpleType(dest []byte, m map[string]interface{}) []byte {
	var sz int

	sz = len(m)
	if sz > math.MaxUint32 {
		panic("msgp: map has too many elements")
	}

	dest = AppendMapHeader(dest, uint32(sz))

	for key, val := range m {
		dest = AppendString(dest, key)
		dest = AppendSimpleType(dest, val)
	}

	return dest
}

func AppendMapStrStrFromList(dest []byte, args ...string) []byte {
	var sz int

	sz = len(args)
	if uint64(sz)&0x01 != 0 {
		panic("msgp: args count must be even")
	}

	sz = sz / 2

	if sz > math.MaxUint32 {
		panic("msgp: map has too many elements")
	}

	dest = AppendMapHeader(dest, uint32(sz))

	for i := 0; i < len(args); i += 2 {
		dest = AppendString(dest, args[i])
		dest = AppendString(dest, args[i+1])
	}

	return dest
}
