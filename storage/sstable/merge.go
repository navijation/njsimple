package sstable

import (
	"bytes"
	"fmt"
	"iter"

	"github.com/navijation/njsimple/util/heap"
)

type MergeTablesArgs struct {
	Srcs []*SSTable
}

// Merge all entries from source tables into dest table
func (me *SSTable) MergeTables(args MergeTablesArgs) error {
	tableMux := newIteratorMux()

	for _, src := range args.Srcs {
		next, stop := iter.Pull2(src.Entries())
		defer stop()

		if err := tableMux.AddIterator(next); err != nil {
			return err
		}
	}

	var nextEntryErr error
	appendErr := me.AppendEntries(func(yield func(KeyValuePair) bool) {
		for {
			nextEntry, hasNext, err := tableMux.NextEntry()
			if !hasNext {
				return
			}
			if err != nil {
				nextEntryErr = err
				return
			}

			if !yield(KeyValuePair{
				Key:       nextEntry.Key,
				Value:     nextEntry.Value,
				IsDeleted: nextEntry.IsDeleted,
			}) {
				nextEntryErr = fmt.Errorf("append aborted early")
				return
			}
		}
	})

	if appendErr != nil {
		return appendErr
	}
	return nextEntryErr
}

type tableMuxEntry struct {
	current     SSTableEntry
	tableNumber int
	nextEntry   func() (SSTableEntry, error, bool)
}

type tableMux struct {
	heap         heap.Heap[tableMuxEntry]
	tableCount   int
	lastKey      []byte
	lastKeyIsSet bool
}

func newIteratorMux() tableMux {
	return tableMux{
		heap: heap.NewHeap(func(a, b tableMuxEntry) int {
			// pick lower keys first, and upon ties pick the later tables first; this ensures
			// later writes win

			bytesComp := bytes.Compare(a.current.Key, b.current.Key)
			if bytesComp != 0 {
				return bytesComp
			}

			return b.tableNumber - a.tableNumber
		}),
	}
}

func (me *tableMux) AddIterator(next func() (SSTableEntry, error, bool)) error {
	sstableEntry, err, exists := next()
	if err != nil {
		return err
	}
	tableNumber := me.tableCount
	me.tableCount++
	if !exists {
		return nil
	}

	entry := tableMuxEntry{
		current:     sstableEntry,
		tableNumber: tableNumber,
		nextEntry:   next,
	}

	me.heap.Push(entry)
	return nil
}

func (me *tableMux) NextEntry() (out SSTableEntry, hasNext bool, _ error) {
	for me.heap.Size() > 0 {
		entry := me.heap.Pop()

		sstableEntry, err, hasNext := entry.nextEntry()
		if err != nil {
			return entry.current, false, err
		}
		if hasNext {
			me.heap.Push(tableMuxEntry{
				current:     sstableEntry,
				tableNumber: entry.tableNumber,
				nextEntry:   entry.nextEntry,
			})
		}

		// don't rewrite keys that were already written
		if bytes.Equal(entry.current.Key, me.lastKey) && me.lastKeyIsSet {
			continue
		}

		me.lastKey = entry.current.Key
		me.lastKeyIsSet = true
		return entry.current, true, nil
	}

	return out, false, nil
}
