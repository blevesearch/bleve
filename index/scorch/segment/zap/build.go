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

package zap

import (
	"bufio"
	"math"
	"os"
)

const Version uint32 = 11

const Type string = "zap"

const fieldNotUninverted = math.MaxUint64

// PersistSegmentBase persists SegmentBase in the zap file format.
func PersistSegmentBase(sb *SegmentBase, path string) error {
	flag := os.O_RDWR | os.O_CREATE

	f, err := os.OpenFile(path, flag, 0600)
	if err != nil {
		return err
	}

	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(path)
	}

	br := bufio.NewWriter(f)

	_, err = br.Write(sb.mem)
	if err != nil {
		cleanup()
		return err
	}

	err = persistFooter(sb.numDocs, sb.storedIndexOffset, sb.fieldsIndexOffset, sb.docValueOffset,
		sb.chunkFactor, sb.memCRC, br)
	if err != nil {
		cleanup()
		return err
	}

	err = br.Flush()
	if err != nil {
		cleanup()
		return err
	}

	err = f.Sync()
	if err != nil {
		cleanup()
		return err
	}

	err = f.Close()
	if err != nil {
		cleanup()
		return err
	}

	return nil
}

func persistStoredFieldValues(fieldID int,
	storedFieldValues [][]byte, stf []byte, spf [][]uint64,
	curr int, metaEncode varintEncoder, data []byte) (
	int, []byte, error) {
	for i := 0; i < len(storedFieldValues); i++ {
		// encode field
		_, err := metaEncode(uint64(fieldID))
		if err != nil {
			return 0, nil, err
		}
		// encode type
		_, err = metaEncode(uint64(stf[i]))
		if err != nil {
			return 0, nil, err
		}
		// encode start offset
		_, err = metaEncode(uint64(curr))
		if err != nil {
			return 0, nil, err
		}
		// end len
		_, err = metaEncode(uint64(len(storedFieldValues[i])))
		if err != nil {
			return 0, nil, err
		}
		// encode number of array pos
		_, err = metaEncode(uint64(len(spf[i])))
		if err != nil {
			return 0, nil, err
		}
		// encode all array positions
		for _, pos := range spf[i] {
			_, err = metaEncode(pos)
			if err != nil {
				return 0, nil, err
			}
		}

		data = append(data, storedFieldValues[i]...)
		curr += len(storedFieldValues[i])
	}

	return curr, data, nil
}

func InitSegmentBase(mem []byte, memCRC uint32, chunkFactor uint32,
	fieldsMap map[string]uint16, fieldsInv []string, numDocs uint64,
	storedIndexOffset uint64, fieldsIndexOffset uint64, docValueOffset uint64,
	dictLocs []uint64) (*SegmentBase, error) {
	sb := &SegmentBase{
		mem:               mem,
		memCRC:            memCRC,
		chunkFactor:       chunkFactor,
		fieldsMap:         fieldsMap,
		fieldsInv:         fieldsInv,
		numDocs:           numDocs,
		storedIndexOffset: storedIndexOffset,
		fieldsIndexOffset: fieldsIndexOffset,
		docValueOffset:    docValueOffset,
		dictLocs:          dictLocs,
		fieldDvReaders:    make(map[uint16]*docValueReader),
	}
	sb.updateSize()

	err := sb.loadDvReaders()
	if err != nil {
		return nil, err
	}

	return sb, nil
}
