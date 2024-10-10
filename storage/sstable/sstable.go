package sstable

import (
	"bufio"
	"bytes"
	"fmt"
	"iter"
	_ "iter"
	"log"
	"os"
	"slices"

	"github.com/navijation/njsimple/util"
)

const (
	defaultChunkSize uint64 = 10
)

type SSTable struct {
	path string

	header Header

	file    *os.File
	index   SparseMemIndex
	lastKey []byte
}

type OpenArgs struct {
	Path           string
	Create         bool
	Version        uint64
	IndexChunkSize util.Optional[uint64]
}

func Open(args OpenArgs) (out SSTable, _ error) {
	flags := os.O_RDWR
	if args.Create {
		flags |= (os.O_CREATE | os.O_EXCL)
	}
	file, err := os.OpenFile(args.Path, flags, 0o644)
	if err != nil {
		return out, err
	}

	out = SSTable{
		path: args.Path,

		file: file,
		index: SparseMemIndex{
			ChunkSize: args.IndexChunkSize.Or(defaultChunkSize),
		},
	}

	defer func() {
		if args.Create && err != nil {
			_ = file.Close()
			_ = os.Remove(args.Path)
		}
	}()

	if args.Create {
		out.header.ID = util.NewRandomUUIDBytes()
		out.header.Version = args.Version
		out.header.FileSize = out.header.SizeOf()
		if _, err := out.header.WriteTo(util.Ptr(out.fileWrapperAt(0))); err != nil {
			return out, err
		}
	} else {
		if _, err := out.header.ReadFrom(out.readBufferAt(0)); err != nil {
			return out, err
		}
	}

	// not a big issue if this fails; structure will pretend as if file size is smaller even if
	// file is larger
	_ = out.truncateToHeader()

	if err := out.Reindex(); err != nil {
		return out, err
	}

	return out, nil
}

func (me *SSTable) Close() error {
	return me.file.Close()
}

func (me *SSTable) Header() Header {
	return me.header
}

func (me *SSTable) Index() SparseMemIndex {
	return SparseMemIndex{
		ChunkSize: me.index.ChunkSize,
		IndexedEntries: util.CloneSliceFunc(
			me.index.IndexedEntries,
			func(entry SparseMemIndexEntry) SparseMemIndexEntry {
				return SparseMemIndexEntry{
					Key:      slices.Clone(entry.Key),
					Location: entry.Location,
				}
			},
		),
	}
}

func (me *SSTable) LookupEntry(key []byte) (out SSTableEntry, exists bool, _ error) {
	location := me.index.LookupSearchLocation(key)

	for entry, err := range me.EntriesAt(location) {
		if err != nil {
			return out, false, err
		}
		switch bytes.Compare(key, entry.Key) {
		case -1:
			// key < entry.Key => no match found
			return out, false, nil
		case 0:
			// key == entry.Key => match found
			return entry, true, nil
		}
	}

	return out, false, nil
}

func (me *SSTable) Entries() iter.Seq2[SSTableEntry, error] {
	return me.EntriesAt(EntryLocation{
		EntryNumber: 0,
	})
}

func (me *SSTable) EntriesAt(location EntryLocation) iter.Seq2[SSTableEntry, error] {
	if location.EntryNumber == 0 {
		location.Offset = me.header.SizeOf()
	}

	return func(yield func(SSTableEntry, error) bool) {
		buffer := me.readBufferAt(location.Offset)

		for location := location; location.Offset < me.header.FileSize; {
			var entry internalSSTableEntry
			n, err := entry.ReadFrom(buffer)

			if !yield(entry.ToSSTableEntry(location), err) {
				return
			}

			if err != nil {
				return
			}

			location.Offset += uint64(n)
			location.EntryNumber++
		}
	}
}

func (me *SSTable) AppendEntries(keyValuePairs iter.Seq[KeyValuePair]) (err error) {
	fileWrapper := util.NewFileWrapperAt(me.file, me.header.FileSize)

	defer func() {
		if err != nil {
			_ = me.truncateToHeader()
		}
	}()

	var (
		offset       int64
		entriesAdded uint64
		lastKey      = me.lastKey
	)
	for keyValuePair := range keyValuePairs {
		if bytes.Compare(keyValuePair.Key, lastKey) != 1 {
			log.Printf("tried to append %v after last key %v", keyValuePair.Key, lastKey)
			return fmt.Errorf("out of order entry append attempt")
		}
		entry := keyValuePair.ToInternalSSTableEntry()

		n, err := entry.WriteTo(&fileWrapper)
		if err != nil {
			return err
		}
		offset += n
		entriesAdded++
		lastKey = entry.Key
	}

	// do a double sync on file contents and then header, to ensure disk doesn't write header first
	// and then crash before updating entries
	if err := me.file.Sync(); err != nil {
		return err
	}

	newSize := me.header.FileSize + uint64(offset)
	newEntries := me.header.NumEntries + entriesAdded

	if err := me.writeNewSize(newSize, newEntries); err != nil {
		return err
	}

	me.lastKey = lastKey
	return me.partialReindex()
}

func (me *SSTable) Reindex() error {
	var (
		newEntries     []SparseMemIndexEntry
		nextChunkStart = me.index.ChunkSize
		numEntries     uint64
		lastKey        []byte
	)
	for entry, err := range me.Entries() {
		if err != nil {
			return err
		}
		numEntries++

		if entry.Location.Offset >= nextChunkStart {
			newEntries = append(newEntries, SparseMemIndexEntry{
				Location: entry.Location,
				Key:      entry.Key,
			})
			nextChunkStart = entry.Location.Offset + me.index.ChunkSize
		}

		lastKey = entry.Key
	}

	me.lastKey = lastKey
	me.index = SparseMemIndex{
		ChunkSize:      me.index.ChunkSize,
		IndexedEntries: newEntries,
	}

	return nil
}

func (me *SSTable) partialReindex() error {
	if len(me.index.IndexedEntries) == 0 {
		return me.Reindex()
	}

	lastIndexEntry := me.index.IndexedEntries[len(me.index.IndexedEntries)-1]
	nextChunkStart := lastIndexEntry.Location.Offset + me.index.ChunkSize

	var (
		lastKey []byte
	)
	for entry, err := range me.EntriesAt(lastIndexEntry.Location) {
		if err != nil {
			return err
		}

		if entry.Location.Offset > nextChunkStart {
			me.index.IndexedEntries = append(me.index.IndexedEntries, SparseMemIndexEntry{
				Key:      entry.Key,
				Location: entry.Location,
			})
			nextChunkStart = entry.Location.Offset + me.index.ChunkSize
		}
		lastKey = entry.Key
	}

	me.lastKey = lastKey
	return nil
}

func (me *SSTable) readBufferAt(offset uint64) *bufio.Reader {
	return bufio.NewReader(util.Ptr(me.fileWrapperAt(offset)))
}

func (me *SSTable) fileWrapperAt(offset uint64) util.FileWrapper {
	return util.NewFileWrapperAt(me.file, offset)
}

func (me *SSTable) truncateToHeader() error {
	return me.file.Truncate(int64(me.header.FileSize))
}

func (me *SSTable) writeNewSize(size, numEntries uint64) error {
	fileWrapper := me.fileWrapperAt(0)
	newHeader := me.header.WithNewSize(size, numEntries)

	if _, err := newHeader.WriteTo(&fileWrapper); err != nil {
		return err
	}

	if err := me.file.Sync(); err != nil {
		return err
	}

	me.header = newHeader
	return nil
}
