package util

import (
	"errors"
	"io"
	"os"
)

type IOSeeker struct {
	io.ReadWriteSeeker
	io.WriterAt
	io.ReaderAt
}

var _ io.ReadWriteSeeker = (*FileWrapper)(nil)

// FileWrapper is a utility class that can be used to read a file starting at an offset.
// More importantly, compared to a regular os.File, it uses `ReadAt` consistently, which
// does not mutate the underlying file descriptor state, allowing multiple readers to be
// safely created over a single file.
type FileWrapper struct {
	file   *os.File
	offset uint64
}

func NewFileWrapper(file *os.File) FileWrapper {
	return FileWrapper{
		file:   file,
		offset: 0,
	}
}

func NewFileWrapperAt(file *os.File, offset uint64) FileWrapper {
	return FileWrapper{
		file:   file,
		offset: offset,
	}
}

func (me *FileWrapper) Read(b []byte) (n int, err error) {
	n, err = me.file.ReadAt(b, int64(me.offset))
	me.offset += uint64(n)
	return n, err
}

func (me *FileWrapper) Write(b []byte) (n int, err error) {
	n, err = me.file.WriteAt(b, int64(me.offset))
	me.offset += uint64(n)
	return n, err
}

func (me *FileWrapper) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case io.SeekCurrent:
		me.offset += uint64(offset)
	case io.SeekStart:
		me.offset = uint64(offset)
	default:
		return -1, errors.New("unsupported operation")
	}
	return int64(me.offset), nil
}

func (me *FileWrapper) Sync() error {
	return me.file.Sync()
}

func (me *FileWrapper) Copy() FileWrapper {
	return FileWrapper{
		file:   me.file,
		offset: me.offset,
	}
}