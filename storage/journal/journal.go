package journal

import (
	"bufio"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/google/uuid"

	"github.com/navijation/njsimple/util"
)

var (
	ErrSignatureMismatch  = errors.New("signature does not match")
	ErrInvalidContentSize = errors.New("content size is invalid")
)

// Journal file implements an append-only journal file that uses cryptographic signatures to
// ensure data integrity of writes. If power failures occur after a partial write, the
// cryptographic signature will not be written to disk, so the entry will be discarded
// upon a subsequent load.
//
// Based partially on https://www.sqlite.org/atomiccommit.html, but the use of cryptographic
// signatures makes this more of a true append-only data structure. The journal used in sqlite
// updates the file header after each write, which also requires an extra sync operation; this
// journal never modifies the header after creation.
type JournalFile struct {
	// constant metadata
	path string

	// header
	header journalFileHeader

	// file descriptor
	file *os.File

	// userspace tracking of (expected) file size
	size uint64

	// running calculation of checksum
	hash hash.Hash

	// current number of entries in journal
	numberOfEntries uint64

	// indicates that there was a failed append that wasn't fully rolled back
	isBad bool
}

type OpenArgs struct {
	Path    string
	Create  bool
	StartAt uint64
}

func Open(args OpenArgs) (out JournalFile, err error) {
	flags := os.O_RDWR
	if args.Create {
		flags |= (os.O_CREATE | os.O_EXCL)
	}
	file, err := os.OpenFile(args.Path, flags, 0o644)
	if err != nil {
		return out, err
	}

	defer func() {
		if args.Create && err != nil {
			_ = file.Close()
			_ = os.Remove(args.Path)
		}
	}()

	fileInfo, err := file.Stat()
	if err != nil {
		return out, err
	}

	out = JournalFile{
		header: journalFileHeader{},
		path:   args.Path,
		file:   file,
		size:   uint64(fileInfo.Size()),
		hash:   sha256.New(),
	}

	fileW := out.fileWrapperAt(0)

	if args.Create {
		out.header.id = util.NewRandomUUIDBytes()
		out.header.start = args.StartAt
		if _, err := out.header.WriteTo(&fileW); err != nil {
			return out, err
		}
	}

	if _, err := out.checkSum(); err != nil {
		return out, err
	}

	return out, err
}

func (me *JournalFile) Close() error {
	return me.file.Close()
}

func (me *JournalFile) AppendEntry(content []byte) (out JournalEntry, err error) {
	internalEntry := internalJournalEntry{
		contentSize: uint64(len(content)),
		content:     content,
		signature:   [32]byte{},
	}

	defer func() {
		if err != nil {
			// TODO: the hash won't be valid if appending an entry fails; need to somehow preserve old
			// hash value; Go's library doesn't make it easy, so just going to recompute entire journal
			// checksum for now whenever something fails
			_, _ = me.checkSum()
		}
	}()

	if me.isBad {
		return out, errors.New("journal is in invalid state")
	}

	internalEntry.WriteHash(me.hash)
	internalEntry.ReadSignature(me.hash)

	endOfFile := me.fileWrapperAt(me.size)
	if _, err := internalEntry.WriteTo(&endOfFile); err != nil {
		return out, err
	}

	if err := me.file.Sync(); err != nil {
		return out, err
	}

	out = JournalEntry{
		EntryNumber: me.header.start + me.numberOfEntries,
		Offset:      me.size,
		ContentSize: internalEntry.contentSize,
		Content:     content,
		Signature:   internalEntry.signature[:],
	}

	me.numberOfEntries++
	me.size += internalEntry.Size()

	return
}

func (me *JournalFile) fileWrapperAt(offset uint64) util.FileWrapper {
	return util.NewFileWrapperAt(me.file, offset)
}

func (me *JournalFile) fileBufferAt(offset uint64) *bufio.Reader {
	return bufio.NewReader(util.Ptr(me.fileWrapperAt(offset)))
}

func (me *JournalFile) checkSum() (sumInitiallyMatches bool, err error) {
	defer func() {
		me.isBad = (err != nil)
	}()

	if isValid, err := me.checkSumOnce(); err != nil {
		// unexpected error
		return false, err
	} else if !isValid {
		// If the checksum was invalid, the invalid entries should have been discarded, so the
		// subsequent checksum should be valid
		if secondIsValid, secondErr := me.checkSumOnce(); secondErr != nil {
			return false, secondErr
		} else if !secondIsValid {
			return false, fmt.Errorf("invalid checksum after correction")
		} else {
			return false, nil
		}
	}
	return true, nil
}

func (me *JournalFile) checkSumOnce() (sumMatches bool, _ error) {
	if err := me.header.Read(util.Ptr(me.fileWrapperAt(0))); err != nil {
		return false, err
	}

	cursor, err := me.NewCursor(true)
	if err != nil {
		return false, err
	}

	me.hash = cursor.hash
	offset := me.header.Size()
	me.numberOfEntries = 0
	for {
		entry, exists, err := cursor.NextEntry()
		if err != nil {
			if errors.Is(err, io.EOF) ||
				errors.Is(err, io.ErrUnexpectedEOF) ||
				errors.Is(err, ErrSignatureMismatch) ||
				errors.Is(err, ErrInvalidContentSize) {
				// errors due to corruption; break early and truncate file up to last valid offset
				break
			} else {
				// unexpected error; do NOT truncate file prematurely
				return false, err
			}
		}
		if !exists {
			// got to end means checksum integrity is satisfied
			sumMatches = true
			break
		}
		offset = entry.EndOffset()
		me.hash = cursor.HashState()
		me.numberOfEntries++
	}

	if err := me.file.Truncate(int64(offset)); err != nil {
		return sumMatches, err
	}
	me.size = offset

	if err := me.file.Sync(); err != nil {
		return sumMatches, err
	}

	return sumMatches, nil
}

type internalJournalEntry struct {
	contentSize uint64
	content     []byte
	signature   [32]byte
}

func (me *internalJournalEntry) Size() uint64 {
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

type journalFileHeader struct {
	id    [16]byte
	start uint64
}

func (me *journalFileHeader) Read(reader io.Reader) error {
	var idWord [16]byte
	_, err := io.ReadAtLeast(reader, idWord[:], len(idWord))
	if err != nil {
		return err
	}

	me.id = uuid.Must(uuid.FromBytes(idWord[:]))

	startWord, err := util.Word64{}.Read(reader)
	if err != nil {
		return err
	}
	me.start = startWord.Uint64()

	return nil
}

func (me *journalFileHeader) WriteTo(writer io.Writer) (n int64, err error) {
	if dn, err := writer.Write(me.id[:]); err != nil {
		return n + int64(dn), err
	} else {
		n += int64(dn)
	}

	// try writing everything
	if dn, err := util.WriteUint64(writer, me.start); err != nil {
		return n + int64(dn), err
	} else {
		n += int64(dn)
	}

	return n, nil
}

func (me *journalFileHeader) WriteHash(h hash.Hash) {
	_, err := me.WriteTo(h)
	util.AssertNoError(err)
}

func (me *journalFileHeader) Size() uint64 {
	return 24
}

type JournalEntry struct {
	EntryNumber uint64
	Offset      uint64
	ContentSize uint64
	Content     []byte
	Signature   []byte
}

func (me *JournalEntry) Size() uint64 {
	return 8 + me.ContentSize + 32
}

func (me *JournalEntry) EndOffset() uint64 {
	return me.Offset + me.Size()
}
