// Copyright 2017 Nicolas RIESCH
// Use of this source code is governed by the license found in the LICENCE file.

package msgp

import (
	"bufio"
	"io"
)

//*******************************************
//         messagepack Writer
//*******************************************

const (
	WRITER_STAGING_BUFFER_DEFAULT_CAPACITY = 1024 // quite large because large string can be written
)

// Writer writes msgpack data to a buffered writer.
//
// Use the WriteNil(), WriteBool(), etc functions to write the data into the underlying bufio.Writer.
// These functions don't write anything if the Writer is in error state (mw.doomed != nil).
//
// Then, when all you have written all your data, you MUST call Flush() to flush the underlying bufio.Writer, and check its return value for error.
//
//      Note: the doomed field is an error, that occurs because Write has failed. Most probably because connection is broken.
//            When such failure occurs, it is unrecoverable and the connection should be just closed. The Writer cannot be used any more.
//
type Writer struct {
	bw      *bufio.Writer
	staging []byte // data are encoded as messagepack in this staging buffer before being sent to the bufio.Writer.
	doomed  error  // if not nil, a Write() has failed. It is a unrecoverable error, the connection is certainly broken.
}

// NewWriter returns a messagepack Writer.
// A bufio.Writer will be created internally if argument is not a *bufio.Writer.
//
func NewWriter(wt io.Writer) *Writer {
	var (
		bw *bufio.Writer
		ok bool
	)

	if bw, ok = wt.(*bufio.Writer); ok == false {
		bw = bufio.NewWriter(wt)
	}

	mw := &Writer{}

	mw.bw = bw
	mw.staging = make([]byte, 0, WRITER_STAGING_BUFFER_DEFAULT_CAPACITY)

	return mw
}

func (mw *Writer) TruncatedStaging() []byte {

	return mw.staging[:0]
}

func (mw *Writer) SetStaging(staging_buff []byte) {

	mw.staging = staging_buff
}

func (mw *Writer) WriteStaging() {

	if mw.doomed != nil {
		return
	}

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

//******************************************************************************
//         Write methods
//         they append msgpack encoded value to the internal mw.staging buffer
//         and write the buffer to the underlying bufio.Writer
//******************************************************************************

func (mw *Writer) WriteNil() {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendNil(mw.staging[:0])

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteBool(val bool) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendBool(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteUint8(val uint8) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendUint8(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteUint16(val uint16) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendUint16(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteUint32(val uint32) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendUint32(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteUint64(val uint64) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendUint64(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteInt8(val int8) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendInt8(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteInt16(val int16) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendInt16(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteInt32(val int32) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendInt32(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteInt64(val int64) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendInt64(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteFloat32(val float32) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendFloat32(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteFloat64(val float64) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendFloat64(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteString(val string) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendString(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteStringFromBytes(val []byte) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendStringFromBytes(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteBytes(val []byte) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendBytes(mw.staging[:0], val)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteStringHeader(sz uint32) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendStringHeader(mw.staging[:0], sz)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteBytesHeader(sz uint32) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendBytesHeader(mw.staging[:0], sz)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteArrayHeader(sz uint32) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendArrayHeader(mw.staging[:0], sz)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteMapHeader(sz uint32) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendMapHeader(mw.staging[:0], sz)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteSimpleType(i interface{}) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendSimpleType(mw.staging[:0], i)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteMapStrStr(arg map[string]string) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendMapStrStr(mw.staging[:0], arg)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteMapStrSimpleType(arg map[string]interface{}) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendMapStrSimpleType(mw.staging[:0], arg)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

func (mw *Writer) WriteMapStrStrFromList(args ...string) {

	if mw.doomed != nil {
		return
	}

	mw.staging = AppendMapStrStrFromList(mw.staging[:0], args...)

	if _, err := mw.bw.Write(mw.staging); err != nil { // in Go, no short write occurs
		mw.doomed = err
		return
	}
}

//******************************************************************************
//                            Flush and Error method
//******************************************************************************

// Flush flushes the underlying bufio.Buffer.
//
//    IF AN ERROR IS RETURNED, IT MEANS THE WRITE HAS FAILED BECAUSE CONNECTION HAS FAILED.
//    This error could have occurred in any previous operation.
//
//
func (mw *Writer) Flush() (doomed error) {

	if mw.doomed != nil {
		return mw.doomed
	}

	if err := mw.bw.Flush(); err != nil {
		mw.doomed = err
		return err
	}

	return nil
}

// Error returns the error state of the Writer.
//
func (mw *Writer) Error() (doomed error) {

	return mw.doomed
}
