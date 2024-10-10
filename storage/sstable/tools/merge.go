package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/navijation/njsimple/storage/sstable"
	"github.com/urfave/cli/v3"
)

func mergeSSTables(_ context.Context, cmd *cli.Command) error {
	if cmd.Args().Len() < 2 {
		fmt.Printf("%d\n", cmd.Args().Len())
		return errors.New("usage: merge dest_path src_path1 [src_path_2 ...]")
	}

	destPath := cmd.Args().Get(0)

	var create bool
	if _, err := os.Stat(destPath); errors.Is(err, os.ErrNotExist) {
		create = true
	}

	file, err := sstable.Open(sstable.OpenArgs{
		Path:   destPath,
		Create: create,
	})
	if err != nil {
		return fmt.Errorf("failed to create %q: %w", destPath, err)
	}

	defer file.Close()

	var srcFiles []*sstable.SSTable
	for i := range cmd.Args().Len() {
		if i != 0 {
			sstable, err := sstable.Open(sstable.OpenArgs{
				Path: cmd.Args().Get(i),
			})
			if err != nil {
				return err
			}
			defer sstable.Close()
			srcFiles = append(srcFiles, &sstable)
		}
	}

	if err := file.MergeTables(sstable.MergeTablesArgs{
		Srcs: srcFiles,
	}); err != nil {
		return err
	}

	return visualizeSSTableFileHelper(&file)
}
