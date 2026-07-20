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

package geov2

// pow4Table precomputes all valid powers of 4 for a uint64.
// Array size 32 covers exp 0 through 31.
var pow4Table = [32]uint64{
	1,                   // 4^0
	4,                   // 4^1
	16,                  // 4^2
	64,                  // 4^3
	256,                 // 4^4
	1024,                // 4^5
	4096,                // 4^6
	16384,               // 4^7
	65536,               // 4^8
	262144,              // 4^9
	1048576,             // 4^10
	4194304,             // 4^11
	16777216,            // 4^12
	67108864,            // 4^13
	268435456,           // 4^14
	1073741824,          // 4^15
	4294967296,          // 4^16
	17179869184,         // 4^17
	68719476736,         // 4^18
	274877906944,        // 4^19
	1099511627776,       // 4^20
	4398046511104,       // 4^21
	17592186044416,      // 4^22
	70368744177664,      // 4^23
	281474976710656,     // 4^24
	1125899906842624,    // 4^25
	4503599627370496,    // 4^26
	18014398509481984,   // 4^27
	72057594037927936,   // 4^28
	288230376151711744,  // 4^29
	1152921504606846976, // 4^30
	4611686018427387904, // 4^31
}

// pow4 returns 4^exp quickly using the lookup table.
func pow4(exp uint64) uint64 {
	if exp >= 32 {
		// Handle overflow safely.
		return 0
	}
	return pow4Table[exp]
}

// returns the overlap of query and index cells based on their levels.
// Both levels are assumed to be at most 14 (maxCellLevel in the geo repo's
// region coverer configuration - see geo/geojson/geojson_v2.go), the deepest
// level used across the geoshape_v2 indexing and query cell coverings;
// cells deeper than level 14 are outside this function's contract by design.
func calcScore(queryCellLevel, indexCellLevel uint64) uint64 {
	if indexCellLevel > queryCellLevel {
		return pow4(14 - indexCellLevel)
	} else {
		return pow4(14 - queryCellLevel)
	}
}
