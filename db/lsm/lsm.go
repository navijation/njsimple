package lsm

import (
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/navijation/njsimple/storage/journal"
	"github.com/navijation/njsimple/storage/keyvaluepair"
	"github.com/navijation/njsimple/storage/sstable"
	"github.com/navijation/njsimple/util"
)

type LSMDB struct {
	// immutable config
	path           string
	indexChunkSize util.Optional[uint64]

	// state tracking
	writeAheadLogs          []*journal.JournalFile
	sstables                []*sstable.SSTable
	inMemoryIndexes         []*InMemoryIndex
	nextSSTableNumber       uint64
	nextWriteAheadLogNumber uint64
	stateErr                error
	isRunning               atomic.Bool

	// concurrency control
	done           chan struct{}
	asyncEntryChan chan any
	wg             sync.WaitGroup
	lock           sync.RWMutex
}

type OpenArgs struct {
	Path           string
	Create         bool
	IndexChunkSize util.Optional[uint64]
}

func Open(args OpenArgs) (out *LSMDB, err error) {
	var (
		writeAheadLogs []*journal.JournalFile
		sstables       []*sstable.SSTable
		maxSSTableNum  uint64
		maxJournalNum  uint64
	)

	out = &LSMDB{
		path: args.Path,
	}

	defer func() {
		if err != nil {
			for _, sstable := range sstables {
				_ = sstable.Close()
			}
			for _, journal := range writeAheadLogs {
				_ = journal.Close()
			}
			if args.Create {
				_ = os.RemoveAll(args.Path)
			}
		}
	}()

	if args.Create {
		// Atomically create DB directory and first writeahead log
		if err := os.Mkdir(args.Path, os.ModeExclusive|0o755); err != nil {
			return out, err
		}
		if tmpJournal, err := journal.Open(journal.OpenArgs{
			Path:    out.writeAheadLogPath(1),
			Create:  true,
			StartAt: 0,
		}); err != nil {
			return out, err
		} else {
			_ = tmpJournal.Close()
		}
	} else {
		// cleanup existing tmp directory
		_ = os.RemoveAll(filepath.Join(args.Path, "tmp"))

	}

	// Create clean tmp directory
	if err := os.Mkdir(filepath.Join(args.Path, "tmp"), 0o755); err != nil {
		return out, err
	}

	directoryEntries, err := os.ReadDir(args.Path)
	if err != nil {
		return out, err
	}

	for _, dirent := range directoryEntries {
		baseName := dirent.Name()
		filename := filepath.Join(args.Path, baseName)
		switch {
		case baseName == "tmp":
			continue

		case dirent.IsDir():
			log.Printf("Unexpected DB directory %q\n", baseName)

		case strings.HasSuffix(baseName, ".sst"):
			if sstableNum, ok := getFileNumber(baseName, "sstable_", ".sst"); !ok {
				log.Printf("Unexpected SSTable file %q\n", baseName)
				continue
			} else {
				maxSSTableNum = max(maxSSTableNum, sstableNum)
			}
			sstableFile, err := sstable.Open(sstable.OpenArgs{
				Path:           filename,
				IndexChunkSize: args.IndexChunkSize,
			})
			if err != nil {
				return out, err
			}
			sstables = append(sstables, &sstableFile)

		case strings.HasSuffix(baseName, ".jrn"):
			if journalNum, ok := getFileNumber(baseName, "writeahead_log_", ".jrn"); !ok {
				log.Printf("Unexpected journal file %q\n", baseName)
				continue
			} else {
				maxJournalNum = max(maxJournalNum, journalNum)
			}
			journalFile, err := journal.Open(journal.OpenArgs{
				Path: filename,
			})
			if err != nil {
				return out, err
			}
			writeAheadLogs = append(writeAheadLogs, &journalFile)

		default:
			log.Printf("Unexpected DB file %q\n", baseName)
		}
	}

	slices.SortFunc(sstables, func(a, b *sstable.SSTable) int {
		number1, _ := getFileNumber(a.Path(), "sstable_", ".sst")
		number2, _ := getFileNumber(b.Path(), "sstable_", ".sst")
		// sort latest first
		return -(int(number1) - int(number2))
	})

	slices.SortFunc(writeAheadLogs, func(a, b *journal.JournalFile) int {
		number1, _ := getFileNumber(a.Path(), "writeahead_log_", ".jrn")
		number2, _ := getFileNumber(b.Path(), "writeahead_log_", ".jrn")
		// sort latest first
		return -(int(number1) - int(number2))
	})

	out = &LSMDB{
		path:           args.Path,
		indexChunkSize: args.IndexChunkSize,

		writeAheadLogs: writeAheadLogs,
		sstables:       sstables,
		// single empty in-memory index
		inMemoryIndexes:         []*InMemoryIndex{{}},
		nextSSTableNumber:       maxSSTableNum + 1,
		nextWriteAheadLogNumber: maxJournalNum + 1,

		// block if >5 async requests have yet to be satisfied
		asyncEntryChan: make(chan any, 5),
		done:           make(chan struct{}),
	}

	return out, nil
}

func (me *LSMDB) Start() error {
	me.runAsyncWorker()
	return me.processWriteAheadLogs(&dbCtx{})
}

func (me *LSMDB) Close() error {
	ctx := &dbCtx{}

	if me.isRunning.Load() {
		close(me.done)
		me.wg.Wait()
	}

	ctx.Lock(&me.lock)
	defer ctx.Unlock(&me.lock)

	for _, log := range me.writeAheadLogs {
		_ = log.Close()
	}

	for _, sstable := range me.sstables {
		_ = sstable.Close()
	}

	return nil
}

func (me *LSMDB) Lookup(key []byte) (out keyvaluepair.KeyValuePair, exists bool, _ error) {
	me.lock.RLock()
	defer me.lock.RUnlock()

	for _, memoryIndex := range me.inMemoryIndexes {
		kvp, exists := memoryIndex.Lookup(key)
		if exists {
			return kvp, true, nil
		}
	}

	for _, sstable := range me.sstables {
		entry, exists, err := sstable.LookupEntry(key)
		if err != nil {
			return out, false, err
		}
		if exists {
			return keyvaluepair.KeyValuePair{
				Key:       key,
				Value:     entry.Value,
				IsDeleted: entry.IsDeleted,
			}, true, nil
		}
	}

	return out, false, nil
}

func (me *LSMDB) processWriteAheadLogs(ctx *dbCtx) error {
	for i := range me.writeAheadLogs {
		// process oldest logs first

		log := me.writeAheadLogs[len(me.writeAheadLogs)-i-1]
		cursor := log.NewCursor(false)
		for {
			entry, hasNext, err := cursor.NextEntry()
			if err != nil {
				return err
			}
			if !hasNext {
				break
			}
			parsed, err := parseJournalEntry(&entry)
			if err != nil {
				return err
			}
			switch parsed := parsed.(type) {
			case CUDKeyValueEntry:
				me.processCUDKeyValueEntry(ctx, parsed)
			case CreateSSTableEntry:
				if err := me.processCreateSSTableEntry(ctx, parsed); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (me *LSMDB) runAsyncWorker() {
	me.wg.Add(1)
	if alreadyRunning := me.isRunning.Swap(true); alreadyRunning {
		me.wg.Done()
		return
	}
	go func() {
		defer func() {
			me.isRunning.Store(false)
			me.wg.Done()
		}()
		ctx := &dbCtx{}
		for {
			select {
			case entry := <-me.asyncEntryChan:
				switch entry := entry.(type) {
				case CUDKeyValueEntry:
					me.processCUDKeyValueEntry(ctx, entry)
				case CreateSSTableEntry:
					for {
						if err := me.processCreateSSTableEntryAsync(ctx, entry); err != nil {
							log.Printf("Failed to create SSTable: %s", err.Error())
						} else {
							break
						}
					}
				}
			case <-me.done:
				return
			}
		}
	}()
}
