//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package flexible_go

import (
	"time"

	"github.com/couchbaselabs/bleve/analysis"
)

type FlexibleGoDateTimeParser struct {
	layouts []string
}

func NewFlexibleGoDateTimeParser(layouts []string) *FlexibleGoDateTimeParser {
	return &FlexibleGoDateTimeParser{
		layouts: layouts,
	}
}

func (p *FlexibleGoDateTimeParser) ParseDateTime(input string) (time.Time, error) {
	for _, layout := range p.layouts {
		rv, err := time.Parse(layout, input)
		if err == nil {
			return rv, nil
		}
	}
	return time.Time{}, analysis.INVALID_DATETIME
}
