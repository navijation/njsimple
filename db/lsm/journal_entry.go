package lsm

import (
	"fmt"
	"io"

	"github.com/navijation/njsimple/storage/journal"
	"github.com/navijation/njsimple/storage/keyvaluepair"
	"github.com/navijation/njsimple/util"
)

type journalEntryType byte

const (
	journalEntryTypeCUD journalEntryType = iota
	journalEntryTypeCreateTable
	journalEntryTypeMergeTables
)

func parseJournalEntry(entry *journal.JournalEntry) (any, error) {
	if len(entry.Content) == 0 {
		return nil, fmt.Errorf("journal entry is empty")
	}
	entryTypeByte := journalEntryType(entry.Content[0])
	switch entryTypeByte {
	case journalEntryTypeCUD:
		return util.ValueFromBytes[CUDKeyValueEntry](entry.Content)
	case journalEntryTypeCreateTable:
		return util.ValueFromBytes[CreateSSTableEntry](entry.Content)
	}
	return nil, fmt.Errorf("unsupported entry type: %d", entryTypeByte)
}

// Create, update, or delete a key-value pair
type CUDKeyValueEntry struct {
	StoredKeyValuePair keyvaluepair.StoredKeyValuePair
}

type CreateSSTableEntry struct {
	SSTableNumber       uint64
	WriteAheadLogNumber uint64

	// in-memory only
	index *InMemoryIndex
}

func (me *CUDKeyValueEntry) SizeOf() uint64 {
	return me.StoredKeyValuePair.SizeOf() + 1
}

func (me *CUDKeyValueEntry) WriteTo(writer io.Writer) (n int64, _ error) {
	dn, err := writer.Write([]byte{byte(journalEntryTypeCUD)})
	n += int64(dn)
	if err != nil {
		return n, err
	}

	dn2, err := me.StoredKeyValuePair.WriteTo(writer)
	n += int64(dn2)

	return n, err
}

func (me *CUDKeyValueEntry) ReadFrom(reader io.Reader) (n int64, _ error) {
	var byteBuf [1]byte
	dn, err := reader.Read(byteBuf[:])
	n += int64(dn)
	if err != nil {
		return n, err
	}

	dn2, err := me.StoredKeyValuePair.ReadFrom(reader)
	n += int64(dn2)

	return n, err
}

func (me *CreateSSTableEntry) SizeOf() uint64 {
	return 1 + 8
}

func (me *CreateSSTableEntry) ReadFrom(reader io.Reader) (n int64, _ error) {
	var byteBuf [1]byte
	dn, err := reader.Read(byteBuf[:])
	n += int64(dn)
	if err != nil {
		return n, err
	}

	me.SSTableNumber, dn, err = util.ReadUint64(reader)
	n += int64(dn)
	if err != nil {
		return n, err
	}

	me.WriteAheadLogNumber, dn, err = util.ReadUint64(reader)
	n += int64(dn)

	return n, err
}

func (me *CreateSSTableEntry) WriteTo(writer io.Writer) (n int64, _ error) {
	dn, err := writer.Write([]byte{byte(journalEntryTypeCreateTable)})
	n += int64(dn)
	if err != nil {
		return n, err
	}

	dn, err = util.WriteUint64(writer, me.SSTableNumber)
	n += int64(dn)
	if err != nil {
		return n, err
	}

	dn, err = util.WriteUint64(writer, me.WriteAheadLogNumber)
	n += int64(dn)

	return n, err
}
