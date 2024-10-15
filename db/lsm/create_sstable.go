package lsm

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"

	"github.com/navijation/njsimple/storage/journal"
	"github.com/navijation/njsimple/storage/sstable"
	"github.com/navijation/njsimple/util"
)

func (me *LSMDB) CreateSSTable() error {
	ctx := &dbCtx{}
	if err := me.checkStateErrorSafe(ctx); err != nil {
		return err
	}

	// Unlocking early is not safe because new writeahead log must be created before any
	// subsequent writes are allowed

	ctx.Lock(&me.lock)
	defer ctx.Unlock(&me.lock)

	entry := CreateSSTableEntry{
		SSTableNumber:       me.nextSSTableNumber,
		WriteAheadLogNumber: me.nextWriteAheadLogNumber,
	}

	if err := me.appendEntry(ctx, &entry); err != nil {
		me.stateErr = err
		return err
	}

	me.nextSSTableNumber++
	me.nextWriteAheadLogNumber++

	return me.processCreateSSTableEntry(ctx, entry)
}

// processCreateSSTableEntry creates a new in-memory index and triggers asynchronous creation
// of a new SSTable
func (me *LSMDB) processCreateSSTableEntry(ctx *dbCtx, entry CreateSSTableEntry) error {
	ctx.Lock(&me.lock)
	defer ctx.Unlock(&me.lock)

	if exists, err := util.FileExists(me.sstablePath(entry.SSTableNumber)); err != nil {
		return err
	} else if exists {
		log.Printf("SSTable file %d already exists; skipping", entry.SSTableNumber)
		return nil
	}

	if err := me.createNewWriteaheadLog(ctx, entry); err != nil {
		return err
	}

	// first create new in-memory index, moving old primary to secondary

	entry.index = me.inMemoryIndexes[0]
	me.inMemoryIndexes = slices.Insert(me.inMemoryIndexes, 0, &InMemoryIndex{})

	// now process SSTable creation asynchronously
	select {
	case me.asyncEntryChan <- entry:
	case <-me.done:
		return fmt.Errorf("database was closed")
	}

	return nil
}

func (me *LSMDB) processCreateSSTableEntryAsync(ctx *dbCtx, entry CreateSSTableEntry) error {
	// first create temporary SSTable to store items from in-memory index

	file, err := os.CreateTemp(filepath.Join(me.path, "tmp"), "sstable_")
	if err != nil {
		return err
	}
	_ = os.Remove(file.Name())
	defer os.Remove(file.Name())
	_ = file.Close()

	sstableFile, err := sstable.Open(sstable.OpenArgs{
		Path:           filepath.Join(file.Name()),
		Create:         true,
		IndexChunkSize: me.indexChunkSize,
	})
	if err != nil {
		return err
	}

	// write entries from old in memory index to temporary file

	if err := sstableFile.AppendEntries(func(yield func(sstable.KeyValuePair) bool) {
		for _, kvp := range entry.index.KeyValues {
			if !yield(sstable.KeyValuePair{
				Key:       kvp.Key,
				Value:     kvp.Value,
				IsDeleted: kvp.IsDeleted,
			}) {
				return
			}
		}
	}); err != nil {
		me.stateErr = err
		return err
	}

	// then move the file to the SSTable canonical location

	if err := sstableFile.Rename(me.sstablePath(entry.SSTableNumber)); err != nil {
		return err
	}

	// remove in-memory index and insert new sstable into list

	ctx.Lock(&me.lock)
	me.inMemoryIndexes = me.inMemoryIndexes[:1]
	me.sstables = slices.Insert(me.sstables, 0, &sstableFile)
	ctx.Unlock(&me.lock)

	return nil
}

func (me *LSMDB) createNewWriteaheadLog(ctx *dbCtx, entry CreateSSTableEntry) error {
	ctx.Lock(&me.lock)
	defer ctx.Unlock(&me.lock)

	canonicalPath := me.writeAheadLogPath(entry.WriteAheadLogNumber)

	if exists, err := util.FileExists(canonicalPath); err != nil {
		return err
	} else if exists {
		log.Printf("Writeahead log file %d already exists; skipping", entry.WriteAheadLogNumber)
		return nil
	}

	// first create temporary write-ahead log

	file, err := os.CreateTemp(filepath.Join(me.path, "tmp"), "writeahead_log_")
	if err != nil {
		return err
	}
	_ = os.Remove(file.Name())
	defer os.Remove(file.Name())
	_ = file.Close()

	writeAheadLog, err := journal.Open(journal.OpenArgs{
		Path:   file.Name(),
		Create: true,
	})
	if err != nil {
		me.stateErr = err
		return err
	}

	if err := writeAheadLog.Rename(canonicalPath); err != nil {
		me.stateErr = err
		return err
	}

	me.writeAheadLogs = slices.Insert(me.writeAheadLogs, 0, &writeAheadLog)

	return nil
}
