// Copyright 2017 Nicolas RIESCH
// Use of this source code is governed by the license found in the LICENCE file.

package msgp

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func Test_nil(t *testing.T) {
	var (
		err error
		bbb []byte
	)

	// append

	bbb = AppendNil(bbb[:0])
	length := len(bbb)

	if length != 1 {
		t.Fatalf("length %d != %d", length, 1)
	}

	// read

	buff := bytes.NewBuffer(bbb)
	m := NewReader(buff)

	if err = m.ReadNil(); err != nil {
		t.Fatalf("%s", err)
	}
}

func Test_nil_error(t *testing.T) {
	var (
		err error
		bbb []byte
	)

	bbb = append(bbb[:0], 34) // garbage

	buff := bytes.NewBuffer(bbb)
	m := NewReader(buff)

	if err = m.ReadNil(); err != nil {
		return
	}

	t.Fatalf("%s", "error was expected")
}

func Test_bool(t *testing.T) {
	var (
		err error
		bbb []byte
		res bool
	)

	var samples = []struct {
		u      bool
		length int
	}{
		{false, 1},
		{true, 1},
	}

	for _, sample := range samples {
		// append

		bbb = AppendBool(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("length %d != %d", length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadBool(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %t != %t", res, sample.u)
		}
	}
}

func Test_bool_error(t *testing.T) {
	var (
		err error
		bbb []byte
	)

	bbb = append(bbb[:0], 34) // garbage

	buff := bytes.NewBuffer(bbb)
	m := NewReader(buff)

	if _, err = m.ReadBool(); err != nil {
		return
	}

	t.Fatalf("%s", "error was expected")
}

func Test_uint8(t *testing.T) {
	var (
		err error
		bbb []byte
		res uint8
	)

	var samples = []struct {
		u      uint8
		length int
	}{
		{0, 1},
		{1, 1},
		{126, 1},
		{127, 1},
		{128, 2},
		{200, 2},
		{255, 2},
	}

	for _, sample := range samples {
		// append

		bbb = AppendUint8(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadUint8(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %d != %d", res, sample.u)
		}
	}
}

func Test_uint8_overflow(t *testing.T) {
	var (
		err error
		bbb []byte
		res uint8
	)

	var samples = []struct {
		u      uint64
		length int
	}{
		{256, 3},
		{math.MaxUint64, 9},
	}

	for _, sample := range samples {
		// append

		bbb = AppendUint64(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadUint8(); err != nil {
			continue
		}

		t.Fatalf("%s for %d", "error was expected", res)
	}
}

func Test_uint16(t *testing.T) {
	var (
		err error
		bbb []byte
		res uint16
	)

	var samples = []struct {
		u      uint16
		length int
	}{
		{0, 1},
		{1, 1},
		{126, 1},
		{127, 1},
		{128, 2},
		{200, 2},
		{255, 2},
		{256, 3},
		{math.MaxUint16 / 3, 3},
		{math.MaxUint16 - 1, 3},
		{math.MaxUint16, 3},
	}

	for _, sample := range samples {
		// append

		bbb = AppendUint16(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadUint16(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %d != %d", res, sample.u)
		}
	}
}

func Test_uint16_overflow(t *testing.T) {
	var (
		err error
		bbb []byte
		res uint16
	)

	var samples = []struct {
		u      uint64
		length int
	}{
		{math.MaxUint16 + 1, 5},
		{math.MaxUint64, 9},
	}

	for _, sample := range samples {
		// append

		bbb = AppendUint64(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadUint16(); err != nil {
			continue
		}

		t.Fatalf("%s for %d", "error was expected", res)
	}
}

func Test_uint32(t *testing.T) {
	var (
		err error
		bbb []byte
		res uint32
	)

	var samples = []struct {
		u      uint32
		length int
	}{
		{0, 1},
		{1, 1},
		{126, 1},
		{127, 1},
		{128, 2},
		{200, 2},
		{255, 2},
		{256, 3},
		{math.MaxUint16 / 3, 3},
		{math.MaxUint16 - 1, 3},
		{math.MaxUint16, 3},
		{math.MaxUint16 + 1, 5},
		{math.MaxUint32 / 3, 5},
		{math.MaxUint32 - 1, 5},
		{math.MaxUint32, 5},
	}

	for _, sample := range samples {
		// append

		bbb = AppendUint32(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadUint32(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %d != %d", res, sample.u)
		}
	}
}

func Test_uint32_overflow(t *testing.T) {
	var (
		err error
		bbb []byte
		res uint32
	)

	var samples = []struct {
		u      uint64
		length int
	}{
		{math.MaxUint32 + 1, 9},
		{math.MaxUint64, 9},
	}

	for _, sample := range samples {
		// append

		bbb = AppendUint64(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadUint32(); err != nil {
			continue
		}

		t.Fatalf("%s for %d", "error was expected", res)
	}
}

func Test_uint64(t *testing.T) {
	var (
		err error
		bbb []byte
		res uint64
	)

	var samples = []struct {
		u      uint64
		length int
	}{
		{0, 1}, // fixint
		{1, 1},
		{2, 1},
		{126, 1},
		{127, 1},
		{128, 2}, // uint8
		{129, 2},
		{255, 2},
		{256, 3}, // uint16
		{1000, 3},
		{10000, 3},
		{math.MaxUint16 / 3, 3},
		{math.MaxUint16 - 1, 3},
		{math.MaxUint16, 3},
		{math.MaxUint16 + 1, 5}, // uint32
		{math.MaxUint16 + 13425, 5},
		{math.MaxUint32 / 3, 5},
		{math.MaxUint32 - 536478, 5},
		{math.MaxUint32 - 1, 5},
		{math.MaxUint32, 5},
		{math.MaxUint32 + 1, 9}, // uint64
		{math.MaxUint32 + 1546378, 9},
		{math.MaxUint64 / 3, 9},
		{math.MaxUint64 - 1, 9},
		{math.MaxUint64, 9},
	}

	for _, sample := range samples {
		// append

		bbb = AppendUint64(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadUint64(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %d != %d", res, sample.u)
		}
	}

}

func Test_int8(t *testing.T) {
	var (
		err error
		bbb []byte
		res int8
	)

	var samples = []struct {
		u      int8
		length int
	}{
		{0, 1},
		{1, 1},
		{127, 1},

		{-1, 1},
		{-5, 1},
		{-32, 1},
		{-33, 2},
		{-128, 2},
	}

	for _, sample := range samples {
		// append

		bbb = AppendInt8(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadInt8(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %d != %d", res, sample.u)
		}
	}
}

func Test_int8_overflow(t *testing.T) {
	var (
		err error
		bbb []byte
		res int8
	)

	var samples = []struct {
		u      int64
		length int
	}{
		{128, 3},
		{math.MaxInt64, 9},

		{-129, 3},
		{math.MinInt64, 9},
	}

	for _, sample := range samples {
		// append

		bbb = AppendInt64(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadInt8(); err != nil {
			continue
		}

		t.Fatalf("%s for %d", "error was expected", res)
	}
}

func Test_int16(t *testing.T) {
	var (
		err error
		bbb []byte
		res int16
	)

	var samples = []struct {
		u      int16
		length int
	}{
		{0, 1},
		{1, 1},
		{127, 1},
		{128, 3},
		{math.MaxInt16 / 3, 3},
		{math.MaxInt16 - 1, 3},
		{math.MaxInt16, 3},

		{-1, 1},
		{-5, 1},
		{-32, 1},
		{-33, 2},
		{-128, 2},
		{math.MinInt16 / 3, 3},
		{math.MinInt16 + 1, 3},
		{math.MinInt16, 3},
	}

	for _, sample := range samples {
		// append

		bbb = AppendInt16(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadInt16(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %d != %d", res, sample.u)
		}
	}
}

func Test_int16_overflow(t *testing.T) {
	var (
		err error
		bbb []byte
		res int16
	)

	var samples = []struct {
		u      int64
		length int
	}{
		{math.MaxInt16 + 1, 5},
		{math.MaxInt64, 9},

		{math.MinInt16 - 1, 5},
		{math.MinInt64, 9},
	}

	for _, sample := range samples {
		// append

		bbb = AppendInt64(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadInt16(); err != nil {
			continue
		}

		t.Fatalf("%s for %d", "error was expected", res)
	}
}

func Test_int32(t *testing.T) {
	var (
		err error
		bbb []byte
		res int32
	)

	var samples = []struct {
		u      int32
		length int
	}{
		{0, 1},
		{1, 1},
		{127, 1},
		{128, 3},
		{math.MaxInt16 / 3, 3},
		{math.MaxInt16 - 1, 3},
		{math.MaxInt16, 3},
		{math.MaxInt16 + 1, 5},
		{math.MaxInt32 / 3, 5},
		{math.MaxInt32 - 1, 5},
		{math.MaxInt32, 5},

		{-1, 1},
		{-5, 1},
		{-32, 1},
		{-33, 2},
		{-128, 2},
		{math.MinInt16 / 3, 3},
		{math.MinInt16 + 1, 3},
		{math.MinInt16, 3},
		{math.MinInt16 - 1, 5},
		{math.MinInt32 / 3, 5},
		{math.MinInt32 + 1, 5},
		{math.MinInt32, 5},
	}

	for _, sample := range samples {
		// append

		bbb = AppendInt32(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadInt32(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %d != %d", res, sample.u)
		}
	}
}

func Test_int32_overflow(t *testing.T) {
	var (
		err error
		bbb []byte
		res int32
	)

	var samples = []struct {
		u      int64
		length int
	}{
		{math.MaxInt32 + 1, 9},
		{math.MaxInt64, 9},

		{math.MinInt32 - 1, 9},
		{math.MinInt64, 9},
	}

	for _, sample := range samples {
		// append

		bbb = AppendInt64(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadInt32(); err != nil {
			continue
		}

		t.Fatalf("%s for %d", "error was expected", res)
	}
}

func Test_int64(t *testing.T) {
	var (
		err error
		bbb []byte
		res int64
	)

	var samples = []struct {
		u      int64
		length int
	}{
		{0, 1}, // fixint
		{1, 1},
		{2, 1},
		{126, 1},
		{127, 1},
		{128, 3}, // int16
		{129, 3},
		{255, 3},
		{256, 3},
		{1000, 3},
		{10000, 3},
		{math.MaxInt16 / 3, 3},
		{math.MaxInt16 - 1, 3},
		{math.MaxInt16, 3},
		{math.MaxInt16 + 1, 5}, // int32
		{math.MaxInt16 + 13425, 5},
		{math.MaxInt32 / 3, 5},
		{math.MaxInt32 - 536478, 5},
		{math.MaxInt32 - 1, 5},
		{math.MaxInt32, 5},
		{math.MaxInt32 + 1, 9}, // int64
		{math.MaxInt32 + 1546378, 9},
		{math.MaxInt64 / 3, 9},
		{math.MaxInt64 - 1, 9},
		{math.MaxInt64, 9},

		{-1, 1}, // negative fixint
		{-20, 1},
		{-31, 1},
		{-32, 1},
		{-33, 2}, // int8
		{-126, 2},
		{-127, 2},
		{-128, 2},
		{-129, 3}, // int16
		{-255, 3},
		{-256, 3},
		{-1000, 3},
		{-10000, 3},
		{math.MinInt16 / 3, 3},
		{math.MinInt16 + 1, 3},
		{math.MinInt16, 3},
		{math.MinInt16 - 1, 5}, // int32
		{math.MinInt16 - 13425, 5},
		{math.MinInt32 / 3, 5},
		{math.MinInt32 + 536478, 5},
		{math.MinInt32 + 1, 5},
		{math.MinInt32, 5},
		{math.MinInt32 - 1, 9}, // int64
		{math.MinInt32 - 1546378, 9},
		{math.MinInt64 / 3, 9},
		{math.MinInt64 + 1, 9},
		{math.MinInt64, 9},
	}

	for _, sample := range samples {
		// append

		bbb = AppendInt64(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadInt64(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %d != %d", res, sample.u)
		}
	}

}

func Test_float32(t *testing.T) {
	var (
		err error
		bbb []byte
		res float32
	)

	var samples = []struct {
		u      float32
		length int
	}{
		{0.0, 5},
		{123456.473652, 5},
		{123456.4736523 - 44, 5},
		{-123456.4736523e12, 5},
	}

	for _, sample := range samples {
		// append

		bbb = AppendFloat32(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%f: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadFloat32(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %f != %f", res, sample.u)
		}
	}
}

func Test_float64(t *testing.T) {
	var (
		err error
		bbb []byte
		res float64
	)

	var samples = []struct {
		u      float64
		length int
	}{
		{0.0, 9},
		{123456.473652, 9},
		{123456.4736523 - 44, 9},
		{123456.4736523e132, 9},
	}

	for _, sample := range samples {
		// append

		bbb = AppendFloat64(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%f: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadFloat64(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %f != %f", res, sample.u)
		}
	}
}

func Test_string(t *testing.T) {
	var (
		err error
		bbb []byte
		res string
	)

	var samples = []struct {
		u      string
		length int
	}{
		{"", 1},
		{"a", 2},
		{"nicolas", 8},
		{"1234567890123456789012345678901", 32},
		{"12345678901234567890123456789012", 34},
		{strings.Repeat("a", 255), 257},
		{strings.Repeat("a", 256), 259},
		{strings.Repeat("a", math.MaxUint16), math.MaxUint16 + 3},
		{strings.Repeat("a", math.MaxUint16+1), math.MaxUint16 + 1 + 5},
		{strings.Repeat("a", math.MaxUint16*3), (math.MaxUint16 * 3) + 5},
	}

	for _, sample := range samples {
		// append

		bbb = AppendString(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%.100s: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadString(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result \"%s\" != \"%s\"", res, sample.u)
		}
	}
}

func Test_bytes(t *testing.T) {
	var (
		err error
		bbb []byte
		res []byte
	)

	var samples = []struct {
		u      string
		length int
	}{
		{"", 2},
		{"a", 3},
		{"nicolas", 9},
		{"nicoläs", 10},
		{"1234567890123456789012345678901", 33},
		{"12345678901234567890123456789012", 34},
		{strings.Repeat("a", 255), 257},
		{strings.Repeat("a", 256), 259},
		{strings.Repeat("a", math.MaxUint16), math.MaxUint16 + 3},
		{strings.Repeat("a", math.MaxUint16+1), math.MaxUint16 + 1 + 5},
		{strings.Repeat("a", math.MaxUint16*3), (math.MaxUint16 * 3) + 5},
	}

	for _, sample := range samples {
		// append

		bbb = AppendBytes(bbb[:0], []byte(sample.u))
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%.100s: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadBytes(nil); err != nil {
			t.Fatalf("%s", err)
		}

		if string(res) != sample.u {
			t.Fatalf("result \"%s\" != \"%s\"", res, sample.u)
		}
	}
}

func Test_string_header(t *testing.T) {
	var (
		err error
		bbb []byte
		res uint32
	)

	var samples = []struct {
		u      uint32
		length int
	}{
		{0, 1}, // fixstr
		{10, 1},
		{15, 1},
		{16, 1},
		{31, 1},
		{32, 2}, // str8
		{100, 2},
		{255, 2},
		{256, 3}, // str16
		{math.MaxUint16 / 2, 3},
		{math.MaxUint16, 3},
		{math.MaxUint16 + 1, 5}, // str32
		{math.MaxUint32 / 3, 5},
		{math.MaxUint32, 5},
	}

	for _, sample := range samples {
		// append

		bbb = AppendStringHeader(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadStringHeader(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %d != %d", res, sample.u)
		}
	}
}

func Test_bytes_header(t *testing.T) {
	var (
		err error
		bbb []byte
		res uint32
	)

	var samples = []struct {
		u      uint32
		length int
	}{
		{0, 2}, // bin8
		{10, 2},
		{15, 2},
		{16, 2},
		{31, 2},
		{32, 2},
		{100, 2},
		{255, 2},
		{256, 3}, // bin16
		{math.MaxUint16 / 2, 3},
		{math.MaxUint16, 3},
		{math.MaxUint16 + 1, 5}, // bin32
		{math.MaxUint32 / 3, 5},
		{math.MaxUint32, 5},
	}

	for _, sample := range samples {
		// append

		bbb = AppendBytesHeader(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadBytesHeader(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %d != %d", res, sample.u)
		}
	}
}

func Test_array_header(t *testing.T) {
	var (
		err error
		bbb []byte
		res uint32
	)

	var samples = []struct {
		u      uint32
		length int
	}{
		{0, 1}, // fixarray
		{10, 1},
		{15, 1},
		{16, 3}, // array16
		{math.MaxUint16 / 3, 3},
		{math.MaxUint16, 3},
		{math.MaxUint16 + 1, 5}, // array32
		{math.MaxUint32 / 3, 5},
		{math.MaxUint32, 5},
	}

	for _, sample := range samples {
		// append

		bbb = AppendArrayHeader(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadArrayHeader(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %d != %d", res, sample.u)
		}
	}
}

func Test_map_header(t *testing.T) {
	var (
		err error
		bbb []byte
		res uint32
	)

	var samples = []struct {
		u      uint32
		length int
	}{
		{0, 1}, // fixmap
		{10, 1},
		{15, 1},
		{16, 3}, // map16
		{math.MaxUint16 / 3, 3},
		{math.MaxUint16, 3},
		{math.MaxUint16 + 1, 5}, // map32
		{math.MaxUint32 / 3, 5},
		{math.MaxUint32, 5},
	}

	for _, sample := range samples {
		// append

		bbb = AppendMapHeader(bbb[:0], sample.u)
		length := len(bbb)

		if length != sample.length {
			t.Fatalf("%d: length %d != %d", sample.u, length, sample.length)
		}

		// read

		buff := bytes.NewBuffer(bbb)
		m := NewReader(buff)

		if res, err = m.ReadMapHeader(); err != nil {
			t.Fatalf("%s", err)
		}

		if res != sample.u {
			t.Fatalf("result %d != %d", res, sample.u)
		}
	}
}
