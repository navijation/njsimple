package main

import (
	"context"
	"log"
	"os"

	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name:  "sstable_tools",
		Usage: "visualize and manipulate SSTable files",
		Commands: []*cli.Command{
			{
				Name:   "visualize",
				Action: visualizeSSTableFile,
				/*
					Arguments: []cli.Argument{
						&cli.StringArg{
							Name:      "sstable_file",
							UsageText: "path of SSTable file",
							Min:       0,
							Max:       1,
							Config: cli.StringConfig{
								TrimSpace: true,
							},
						},
					},
				*/
				Flags: []cli.Flag{
					&cli.UintFlag{
						Name:        "chunk-size",
						Category:    "",
						DefaultText: "64",
						Value:       64,
						Usage:       "size of indexed chunks",
					},
				},
			},
			{
				Name:   "construct",
				Action: constructSSTableFile,
			},
			{
				Name: "merge",
				Action: mergeSSTables,
			},
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
