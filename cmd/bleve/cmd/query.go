// Copyright © 2016 Couchbase, Inc.
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
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/spf13/cobra"
)

var limit, skip, repeat int
var explain, highlight, fields bool
var qtype, qfield, sortby string

// queryCmd represents the query command
var queryCmd = &cobra.Command{
	Use:   "query [index path] [query]",
	Short: "queries the index",
	Long:  `The query command will execute a query against the index.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("must specify query")
		}

		query := buildQuery(args)
		for i := 0; i < repeat; i++ {
			req := bleve.NewSearchRequestOptions(query, limit, skip, explain)
			if highlight {
				req.Highlight = bleve.NewHighlightWithStyle("ansi")
			}
			if fields {
				req.Fields = []string{"*"}
			}
			if sortby != "" {
				if strings.Contains(sortby, ",") {
					req.SortBy(strings.Split(sortby, ","))
				} else {
					req.SortBy([]string{sortby})
				}
			}
			res, err := idx.Search(req)
			if err != nil {
				return fmt.Errorf("error running query: %v", err)
			}
			fmt.Println(res)
		}
		return nil
	},
}

func buildQuery(args []string) query.Query {
	var q query.Query
	switch qtype {
	case "prefix":
		pquery := bleve.NewPrefixQuery(strings.Join(args[1:], " "))
		if qfield != "" {
			pquery.SetField(qfield)
		}
		q = pquery
	case "term":
		pquery := bleve.NewTermQuery(strings.Join(args[1:], " "))
		if qfield != "" {
			pquery.SetField(qfield)
		}
		q = pquery
	default:
		// build a search with the provided parameters
		queryString := strings.Join(args[1:], " ")
		q = bleve.NewQueryStringQuery(queryString)
	}
	return q
}

func init() {
	RootCmd.AddCommand(queryCmd)

	queryCmd.Flags().IntVarP(&repeat, "repeat", "r", 1, "Repeat the query this many times.")
	queryCmd.Flags().IntVarP(&limit, "limit", "l", 10, "Limit number of results returned.")
	queryCmd.Flags().IntVarP(&skip, "skip", "s", 0, "Skip the first N results.")
	queryCmd.Flags().BoolVarP(&explain, "explain", "x", false, "Explain the result scoring.")
	queryCmd.Flags().BoolVar(&highlight, "highlight", true, "Highlight matching text in results.")
	queryCmd.Flags().BoolVar(&fields, "fields", false, "Load stored fields.")
	queryCmd.Flags().StringVarP(&qtype, "type", "t", "query_string", "Type of query to run.")
	queryCmd.Flags().StringVarP(&qfield, "field", "f", "", "Restrict query to field, not applicable to query_string queries.")
	queryCmd.Flags().StringVarP(&sortby, "sort-by", "b", "", "Sort by field.")
}
