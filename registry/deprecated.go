//  Copyright (c) 2026 Couchbase, Inc.
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

package registry

import (
	"errors"
	"fmt"
)

// ErrDeprecatedComponent is wrapped by the error returned when an analysis
// component is looked up by a name that used to be registered but has since
// been removed.  Test for it with errors.Is.
//
// Such a name can still appear in the mapping of an index that was created
// while the component was available.  Mapping validation reports it only after
// every other check has passed, so a caller that is opening such an existing
// index may choose to ignore this error, while a caller defining a new mapping
// should reject it.
var ErrDeprecatedComponent = errors.New("deprecated analysis component")

// Names of removed analysis components, per registry kind.  A name is reported
// as deprecated only when nothing is registered under it.
var (
	// the HebMorph based Hebrew analysis components
	deprecatedAnalyzers    = map[string]struct{}{"he": {}}
	deprecatedTokenizers   = map[string]struct{}{"hebrew": {}}
	deprecatedTokenMaps    = map[string]struct{}{"stop_he": {}}
	deprecatedTokenFilters = map[string]struct{}{
		"lemmatizer_he": {},
		"mark_he":       {},
		"niqqud_he":     {},
		"stop_he":       {},
	}
)

func deprecatedError(kind, name string) error {
	return fmt.Errorf("%w: %s '%s' is no longer supported",
		ErrDeprecatedComponent, kind, name)
}

func isDeprecatedError(err error) bool {
	return errors.Is(err, ErrDeprecatedComponent)
}
