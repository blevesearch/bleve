//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package firestorm

import (
	"fmt"

	"github.com/golang/protobuf/proto"

	"github.com/blevesearch/bleve/index/store"
)

const Version uint64 = 1

var IncompatibleVersion = fmt.Errorf("incompatible version, %d is supported", Version)

var VersionKey = []byte{'v'}

type VersionRow struct {
	value VersionValue
}

func NewVersionRow(version uint64) *VersionRow {
	rv := VersionRow{}
	rv.value.Version = proto.Uint64(version)
	return &rv
}

func NewVersionRowV(val []byte) (*VersionRow, error) {
	rv := VersionRow{}
	err := rv.value.Unmarshal(val)
	if err != nil {
		return nil, err
	}
	return &rv, nil
}

func (vr *VersionRow) KeySize() int {
	return 1
}

func (vr *VersionRow) KeyTo(buf []byte) (int, error) {
	buf[0] = VersionKey[0]
	return 1, nil
}

func (vr *VersionRow) Key() []byte {
	return VersionKey
}

func (vr *VersionRow) ValueSize() int {
	return vr.value.Size()
}

func (vr *VersionRow) ValueTo(buf []byte) (int, error) {
	return vr.value.MarshalTo(buf)
}

func (vr *VersionRow) Value() []byte {
	buf := make([]byte, vr.ValueSize())
	n, _ := vr.value.MarshalTo(buf)
	return buf[:n]
}

func (vr *VersionRow) Version() uint64 {
	return vr.value.GetVersion()
}

func (f *Firestorm) checkVersion(reader store.KVReader) (newIndex bool, err error) {
	value, err := reader.Get(VersionKey)
	if err != nil {
		return
	}

	if value == nil {
		newIndex = true
		return
	}

	var vr *VersionRow
	vr, err = NewVersionRowV(value)
	if err != nil {
		return
	}

	// assert correct version
	if vr.Version() != Version {
		err = IncompatibleVersion
		return
	}

	return
}

func (f *Firestorm) storeVersion(writer store.KVWriter) error {
	vr := NewVersionRow(Version)
	wb := writer.NewBatch()
	wb.Set(vr.Key(), vr.Value())
	err := writer.ExecuteBatch(wb)
	return err
}
