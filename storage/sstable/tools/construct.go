package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/navijation/njsimple/storage/sstable"
	"github.com/urfave/cli/v3"
)

func constructSSTableFile(ctx context.Context, cmd *cli.Command) error {
	if cmd.Args().Len() != 1 {
		fmt.Printf("%d\n", cmd.Args().Len())
		return errors.New("usage: construct sstable_path")
	}

	path := cmd.Args().First()

	var create bool
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		create = true
	}

	file, err := sstable.Open(sstable.OpenArgs{
		Path:   path,
		Create: create,
	})
	if err != nil {
		return fmt.Errorf("failed to create %q: %w", path, err)
	}

	defer file.Close()

	var buildEntryErr error
	err = file.AppendEntries(func(yield func(sstable.KeyValuePair) bool) {
		for {
			reader := bufio.NewReader(os.Stdin)
			line, _, err := reader.ReadLine()
			if err != nil {
				buildEntryErr = err
				return
			}

			fragments := strings.SplitN(string(line), ":", 2)
			if len(fragments) != 2 {
				fmt.Fprintf(os.Stderr, "Entry must be in \"key: value\" format, or \"key:\" format\n")
				continue
			}

			key, value := strings.TrimSpace(fragments[0]), strings.TrimSpace(fragments[1])
			var kvp sstable.KeyValuePair
			if value == "" {
				kvp = sstable.KeyValuePair{Key: []byte(key), IsDeleted: true}
			} else {
				kvp = sstable.KeyValuePair{Key: []byte(key), Value: []byte(value)}
			}

			if !yield(kvp) {
				return
			}
		}
	})
	if err != nil {
		return err
	}
	if buildEntryErr != nil {
		return buildEntryErr
	}

	return nil
}
