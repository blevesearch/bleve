package main

import (
	"github.com/blevesearch/bleve/cmd/bleve/cmd"

	"github.com/spf13/cobra/doc"
)

// you can generate markdown docs by running
//
//   $ go run gendocs.go
//
// this also requires doc sub-package of cobra
// which is not kept in this repo
// you can acquire it by running
//
//   $ gvt restore

func main() {
	cmd.RootCmd.DisableAutoGenTag = true
	doc.GenMarkdownTree(cmd.RootCmd, "./")
}
