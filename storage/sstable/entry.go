package sstable

import (
	"io"

	"github.com/navijation/njsimple/util"
)

const (
	tombstoneMask = (uint64)(1) << 63
	keySizeMask   = ^tombstoneMask
)

type SSTableEntry struct {
	Location  EntryLocation
	KeySize   uint64
	ValueSize uint64
	Key       []byte
	Value     []byte
	IsDeleted bool
}

type KeyValuePair struct {
	Key       []byte
	Value     []byte
	IsDeleted bool
}

type EntryLocation struct {
	EntryNumber uint64
	Offset      uint64
}

// ______________________________________________________________________________
// | 1 bit     |   63 bits  | (key size) bytes | 8 bytes    | (value size) bytes |
// |-----------------------------------------------------------------------------|
// | tombstone |   key size |     key          | value size |      value         |
// |-----------------------------------------------------------------------------|
type internalSSTableEntry struct {
	keySizeAndTombstone uint64
	ValueSize           uint64
	Key                 []byte
	Value               []byte
}

func (me *KeyValuePair) ToInternalSSTableEntry() internalSSTableEntry {
	out := internalSSTableEntry{
		keySizeAndTombstone: uint64(len(me.Key)),
		ValueSize:           uint64(len(me.Value)),
		Key:                 me.Key,
		Value:               me.Value,
	}
	out.SetIsDeleted(me.IsDeleted)

	return out
}

func (me *internalSSTableEntry) ToSSTableEntry(location EntryLocation) SSTableEntry {
	return SSTableEntry{
		Location:  location,
		KeySize:   me.KeySize(),
		ValueSize: me.ValueSize,
		Key:       me.Key,
		Value:     me.Value,
		IsDeleted: me.IsDeleted(),
	}
}

func (me *internalSSTableEntry) KeySize() uint64 {
	return keySizeMask & me.keySizeAndTombstone
}

func (me *internalSSTableEntry) IsDeleted() bool {
	return tombstoneMask&me.keySizeAndTombstone != 0
}

func (me *internalSSTableEntry) SetIsDeleted(isDeleted bool) {
	if isDeleted {
		me.keySizeAndTombstone |= tombstoneMask
	} else {
		me.keySizeAndTombstone &= ^tombstoneMask
	}
}

func (me *internalSSTableEntry) WriteTo(writer io.Writer) (n int64, _ error) {
	if dn, err := util.WriteUint64(writer, me.keySizeAndTombstone); err != nil {
		return n + int64(dn), err
	} else {
		n += int64(dn)
	}

	if dn, err := writer.Write(me.Key); err != nil {
		return n + int64(dn), err
	} else {
		n += int64(dn)
	}

	if dn, err := util.WriteUint64(writer, me.ValueSize); err != nil {
		return n + int64(dn), err
	} else {
		n += int64(dn)
	}

	if dn, err := writer.Write(me.Value); err != nil {
		return n + int64(dn), err
	} else {
		n += int64(dn)
	}

	return n, nil
}

func (me *internalSSTableEntry) ReadFrom(reader io.Reader) (n int64, err error) {
	keySizeAndTombstone, dn, err := util.ReadUint64(reader)
	n += int64(dn)
	if err != nil {
		return n, err
	}

	me.keySizeAndTombstone = keySizeAndTombstone

	me.Key = make([]byte, me.KeySize())
	dn, err = io.ReadAtLeast(reader, me.Key, int(me.KeySize()))
	n += int64(dn)
	if err != nil {
		return n, err
	}

	valueSize, dn, err := util.ReadUint64(reader)
	n += int64(dn)
	if err != nil {
		return n, err
	}
	me.ValueSize = valueSize

	me.Value = make([]byte, valueSize)
	dn, err = io.ReadAtLeast(reader, me.Value, int(valueSize))
	n += int64(dn)
	if err != nil {
		return n, err
	}

	return n, nil
}

func (me *internalSSTableEntry) SizeOf() uint64 {
	return 8 + me.KeySize() + 8 + me.ValueSize
}

func (me *SSTableEntry) ToKeyValuePair() KeyValuePair {
	return KeyValuePair{
		Key:       me.Key,
		Value:     me.Value,
		IsDeleted: me.IsDeleted,
	}
}
