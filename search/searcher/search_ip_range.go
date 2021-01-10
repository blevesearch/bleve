//  Copyright (c) 2014 Couchbase, Inc.
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

package searcher

import (
	"net"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
)

func maskLen(cidr *net.IPNet) int {
	for i, v := range cidr.Mask {
		if v != 0xff {
			return i
		}
	}
	return len(cidr.Mask)
}

func NewIpRangeSearcher(indexReader index.IndexReader, ipNet *net.IPNet,
	field string, boost float64, options search.SearcherOptions) (
	search.Searcher, error) {
	// find the terms with this prefix
	mLen := maskLen(ipNet)
	fieldDict, err := indexReader.FieldDictPrefix(field, ipNet.IP[0:mLen])
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := fieldDict.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	var terms []string
	tfd, err := fieldDict.Next()
	for err == nil && tfd != nil {
		ip := net.IP(tfd.Term)
		// If the mask length is not an exact number of bytes, we could get some
		// ips from outside the network here.
		if ipNet.Contains(ip) {
			terms = append(terms, tfd.Term)
			if tooManyClauses(len(terms)) {
				return nil, tooManyClausesErr(field, len(terms))
			}
		}
		tfd, err = fieldDict.Next()
	}
	if err != nil {
		return nil, err
	}

	return NewMultiTermSearcher(indexReader, terms, field, boost, options, true)
}
