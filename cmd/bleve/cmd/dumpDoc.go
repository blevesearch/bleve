//  Copyright (c) 2017 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"

	"github.com/blevesearch/bleve/index/upsidedown"
	"github.com/spf13/cobra"
)

// dumpDocCmd represents the dumpDoc command
var dumpDocCmd = &cobra.Command{
	Use:   "doc [index path] [doc id]",
	Short: "dump only the rows relating to this doc ID",
	Long:  `The doc sub-command of dump will only dump the rows relating to this doc ID.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("must specify docid")
		}

		i, err := idx.Advanced()
		if err != nil {
			return fmt.Errorf("error getting index: %v", err)
		}
		r, err := i.Reader()
		if err != nil {
			return fmt.Errorf("error getting index reader: %v", err)
		}

		dumpChan := r.DumpDoc(args[1])
		for rowOrErr := range dumpChan {
			switch rowOrErr := rowOrErr.(type) {
			case error:
				return fmt.Errorf("error dumping: %v", rowOrErr)
			case upsidedown.UpsideDownCouchRow:
				fmt.Printf("%v\n", rowOrErr)
				fmt.Printf("Key:   % -100x\nValue: % -100x\n\n", rowOrErr.Key(), rowOrErr.Value())
			}
		}
		return nil
	},
}

func init() {
	dumpCmd.AddCommand(dumpDocCmd)
}
