package keyvaluepair

import (
	"io"

	"github.com/navijation/njsimple/util"
)

func (me *KeyValuePair) ToStoredKeyValuePair() StoredKeyValuePair {
	out := StoredKeyValuePair{
		keySizeAndTombstone: uint64(len(me.Key)),
		ValueSize:           uint64(len(me.Value)),
		Key:                 me.Key,
		Value:               me.Value,
	}
	out.SetIsDeleted(me.IsDeleted)

	return out
}

func (me *StoredKeyValuePair) KeySize() uint64 {
	return keySizeMask & me.keySizeAndTombstone
}

func (me *StoredKeyValuePair) IsDeleted() bool {
	return tombstoneMask&me.keySizeAndTombstone != 0
}

func (me *StoredKeyValuePair) SetIsDeleted(isDeleted bool) {
	if isDeleted {
		me.keySizeAndTombstone |= tombstoneMask
	} else {
		me.keySizeAndTombstone &= ^tombstoneMask
	}
}

func (me *StoredKeyValuePair) WriteTo(writer io.Writer) (n int64, _ error) {
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

	var (
		valueSize uint64
		value     []byte
	)
	if me.IsDeleted() {
		valueSize = 0
		value = nil
	} else {
		valueSize = me.ValueSize
		value = me.Value
	}

	if dn, err := util.WriteUint64(writer, valueSize); err != nil {
		return n + int64(dn), err
	} else {
		n += int64(dn)
	}

	if dn, err := writer.Write(value); err != nil {
		return n + int64(dn), err
	} else {
		n += int64(dn)
	}

	return n, nil
}

func (me *StoredKeyValuePair) ReadFrom(reader io.Reader) (n int64, err error) {
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

	if me.IsDeleted() {
		me.Value = nil
	}

	return n, nil
}

func (me *StoredKeyValuePair) ToKeyValuePair() KeyValuePair {
	return KeyValuePair{
		Key:       me.Key,
		Value:     me.Value,
		IsDeleted: me.IsDeleted(),
	}
}

func (me *StoredKeyValuePair) SizeOf() uint64 {
	out := 8 + me.KeySize() + 8
	if !me.IsDeleted() {
		out += me.ValueSize
	}
	return out
}
