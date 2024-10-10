package journal

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"hash"
	"io"

	"github.com/navijation/njsimple/util"
)

// Journal cursor allows forward iteration over entries of a journal file.
type JournalCursor struct {
	parent         *JournalFile
	entryNumber    uint64
	offset         uint64
	buffer         *bufio.Reader
	shouldCheckSum bool
	hash           hash.Hash

	hasCurrentEntry bool
	currentEntry    JournalEntry
}

func (me *JournalFile) NewCursor(checkSum bool) (out JournalCursor, _ error) {
	// skip header
	offset := me.header.Size()

	out = JournalCursor{
		parent:         me,
		entryNumber:    me.header.start,
		offset:         offset,
		buffer:         me.fileBufferAt(offset),
		shouldCheckSum: checkSum,
		hash:           nil,

		hasCurrentEntry: false,
		currentEntry:    JournalEntry{},
	}

	if out.shouldCheckSum {
		out.hash = sha256.New()
		me.header.WriteHash(out.hash)
	}
	return out, nil
}

// Next entry gets the next entry
func (me *JournalCursor) NextEntry() (out JournalEntry, exists bool, _ error) {
	if me.offset == me.parent.size {
		return out, false, nil
	}

	contentSize, _, err := util.ReadUint64(me.buffer)
	if err != nil {
		return out, false, err
	}

	if me.offset+contentSize > me.parent.size {
		return out, false, ErrInvalidContentSize
	}

	content := make([]byte, contentSize)
	if _, err := io.ReadAtLeast(me.buffer, content, int(contentSize)); err != nil {
		return out, false, err
	}

	if me.shouldCheckSum {
		contentSizeWord := util.Uint64ToWord64(contentSize)
		_, noErr := me.hash.Write(contentSizeWord[:])
		util.AssertNoError(noErr)

		_, noErr = me.hash.Write(content)
		util.AssertNoError(noErr)
	}

	var signature [32]byte
	if _, err := io.ReadAtLeast(me.buffer, signature[:], len(signature)); err != nil {
		return out, false, err
	}

	internalEntry := internalJournalEntry{
		contentSize: contentSize,
		content:     content,
		signature:   signature,
	}

	if me.shouldCheckSum && !bytes.Equal(me.hash.Sum(nil), signature[:]) {
		return out, true, ErrSignatureMismatch
	}

	me.currentEntry = JournalEntry{
		EntryNumber: me.entryNumber,
		Offset:      me.offset,
		ContentSize: contentSize,
		Content:     content,
		Signature:   signature[:],
	}

	me.hasCurrentEntry = true
	me.entryNumber++
	me.offset += internalEntry.Size()

	return me.currentEntry, true, nil
}

func (me *JournalCursor) Entry() (out JournalEntry, _ error) {
	if !me.hasCurrentEntry {
		return out, errors.New("no entry")
	}
	return me.currentEntry, nil
}

func (me *JournalCursor) HashState() hash.Hash {
	return me.hash
}
