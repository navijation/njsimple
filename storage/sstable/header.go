package sstable

import (
	"io"

	"github.com/navijation/njsimple/util"
)

type Header struct {
	ID         [16]byte
	FileSize   uint64
	NumEntries uint64
	Version    uint64
}

func (me Header) WithNewSize(fileSize, numEntries uint64) Header {
	me.FileSize = fileSize
	me.NumEntries = numEntries
	return me
}

func (me *Header) WriteTo(writer io.Writer) (n int64, _ error) {
	dn, err := writer.Write(me.ID[:])
	n += int64(dn)
	if err != nil {
		return n, err
	}

	dn, err = util.WriteUint64s(writer, me.Version, me.FileSize, me.NumEntries)
	return n + int64(dn), err
}

func (me *Header) ReadFrom(reader io.Reader) (n int64, _ error) {
	dn, err := reader.Read(me.ID[:])
	n += int64(dn)
	if err != nil {
		return n, err
	}

	dn, err = util.ReadUint64s(reader, &me.Version, &me.FileSize, &me.NumEntries)
	return n + int64(dn), err
}

func (me *Header) SizeOf() uint64 {
	return 16 + 3*8
}
