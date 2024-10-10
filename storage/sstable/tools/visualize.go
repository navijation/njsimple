package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/navijation/njsimple/storage/sstable"
	"github.com/navijation/njsimple/util"
	"github.com/urfave/cli/v3"
)

func visualizeSSTableFile(ctx context.Context, cmd *cli.Command) error {
	if cmd.Args().Len() != 1 {
		fmt.Printf("%d\n", cmd.Args().Len())
		return errors.New("usage: visualize sstable_path")
	}

	path := cmd.Args().First()

	file, err := sstable.Open(sstable.OpenArgs{
		Path:           path,
		IndexChunkSize: util.Some(cmd.Uint("chunk-size")),
	})
	if err != nil {
		return fmt.Errorf("failed to open %q: %w", path, err)
	}

	defer file.Close()

	return visualizeSSTableFileHelper(&file)
}

func visualizeSSTableFileHelper(file *sstable.SSTable) error {
	header := file.Header()
	fmt.Printf(
		"Header\n"+
			"  ID: %s\n"+
			"  Version: %d\n"+
			"  Size: %d\n"+
			"  Entries: %d\n\n",
		util.UUIDFromBytes(header.ID).String(),
		header.Version,
		header.FileSize,
		header.NumEntries,
	)

	index := file.Index()
	fmt.Printf(
		"Index\n"+
			"  Chunk Size: %d\n"+
			"  Index Entries:\n",
		index.ChunkSize,
	)
	for _, entry := range index.IndexedEntries {
		entryNumber := entry.Location.EntryNumber
		offset := entry.Location.Offset
		fmt.Printf("   - %q -> #%d @%d\n", entry.Key, entryNumber, offset)
	}

	nextIndex := 0

	fmt.Printf("\n" + "Entries:\n")
	for entry, err := range file.Entries() {
		if err != nil {
			return fmt.Errorf("failed to read SSTable entry: %w", err)
		}
		entryNumber := entry.Location.EntryNumber
		offset := entry.Location.Offset
		key := entry.Key
		keySize := entry.KeySize
		value := entry.Value
		valueSize := entry.ValueSize

		if nextIndex < len(index.IndexedEntries) &&
			index.IndexedEntries[nextIndex].Location.EntryNumber == entryNumber {
			fmt.Printf("-----\n")
			nextIndex++
		}
		fmt.Printf("  - #%d @%d: %q (%d bytes) -> %q (%d bytes)\n",
			entryNumber, offset, key, keySize, value, valueSize,
		)
	}

	return nil
}
