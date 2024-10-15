package journal

import (
	"hash"
	"io"

	"github.com/navijation/njsimple/util"
)

type JournalEntry struct {
	EntryNumber uint64
	Offset      uint64
	ContentSize uint64
	Content     []byte
	Signature   []byte
}

type internalJournalEntry struct {
	contentSize uint64
	content     []byte
	signature   [32]byte
}

func (me *JournalEntry) SizeOf() uint64 {
	return 8 + me.ContentSize + 32
}

func (me *JournalEntry) EndOffset() uint64 {
	return me.Offset + me.SizeOf()
}

func (me *internalJournalEntry) SizeOf() uint64 {
	return 8 + me.contentSize + 32
}

func (me *internalJournalEntry) WriteTo(writer io.Writer) (n int64, err error) {
	if dn, err := util.WriteUint64(writer, me.contentSize); err != nil {
		return n + int64(dn), err
	} else {
		n += int64(dn)
	}

	if dn, err := writer.Write(me.content[:]); err != nil {
		return n + int64(dn), err
	} else {
		n += int64(dn)
	}

	if dn, err := writer.Write(me.signature[:]); err != nil {
		return n + int64(dn), err
	} else {
		n += int64(dn)
	}

	return n, nil
}

func (me *internalJournalEntry) WriteHash(hash hash.Hash) {
	_, err := util.WriteUint64(hash, me.contentSize)
	util.AssertNoError(err)

	_, err = hash.Write(me.content)
	util.AssertNoError(err)
}

func (me *internalJournalEntry) ReadSignature(hash hash.Hash) {
	hashBytes := hash.Sum(nil)

	copy(me.signature[:], hashBytes[:])

}
