//  Copyright (c) 2018 Couchbase, Inc.
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

package size

import (
	"reflect"
)

func init() {
	var a bool
	SizeOfBool = int(reflect.TypeOf(a).Size())
	var b float32
	SizeOfFloat32 = int(reflect.TypeOf(b).Size())
	var c float64
	SizeOfFloat64 = int(reflect.TypeOf(c).Size())
	var d map[int]int
	SizeOfMap = int(reflect.TypeOf(d).Size())
	var e *int
	SizeOfPtr = int(reflect.TypeOf(e).Size())
	var f []int
	SizeOfSlice = int(reflect.TypeOf(f).Size())
	var g string
	SizeOfString = int(reflect.TypeOf(g).Size())
	var h uint8
	SizeOfUint8 = int(reflect.TypeOf(h).Size())
	var i uint16
	SizeOfUint16 = int(reflect.TypeOf(i).Size())
	var j uint32
	SizeOfUint32 = int(reflect.TypeOf(j).Size())
	var k uint64
	SizeOfUint64 = int(reflect.TypeOf(k).Size())
}

var SizeOfBool int
var SizeOfFloat32 int
var SizeOfFloat64 int
var SizeOfInt int
var SizeOfMap int
var SizeOfPtr int
var SizeOfSlice int
var SizeOfString int
var SizeOfUint8 int
var SizeOfUint16 int
var SizeOfUint32 int
var SizeOfUint64 int
