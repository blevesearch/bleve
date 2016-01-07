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
	"bytes"
	"math/rand"
	"sort"
	"sync"

	"github.com/steveyen/gtreap"
	"github.com/willf/bitset"
)

type Compensator struct {
	inFlightMutex     sync.RWMutex
	maxRead           uint64
	inFlight          *gtreap.Treap
	deletedMutex      sync.RWMutex
	deletedDocNumbers *bitset.BitSet
}

func NewCompensator() *Compensator {
	rv := Compensator{
		inFlight:          gtreap.NewTreap(inFlightItemCompare),
		deletedDocNumbers: bitset.New(1000000),
	}
	return &rv
}

type Snapshot struct {
	maxRead           uint64
	inFlight          *gtreap.Treap
	deletedDocNumbers *bitset.BitSet
}

// returns which doc number is valid
// if none, then 0
func (s *Snapshot) Which(docID []byte, docNumList DocNumberList) uint64 {
	inFlightVal := s.inFlight.Get(&InFlightItem{docID: docID})

	sort.Sort(docNumList) // Descending ordering.

	for _, docNum := range docNumList {
		if docNum > 0 && docNum <= s.maxRead &&
			(inFlightVal == nil || inFlightVal.(*InFlightItem).docNum == docNum) &&
			!s.deletedDocNumbers.Test(uint(docNum)) {
			return docNum
		}
	}
	return 0
}

func (s *Snapshot) Valid(docID []byte, docNum uint64) bool {
	logger.Printf("checking validity of: '%s' - % x - %d", docID, docID, docNum)
	if docNum > s.maxRead {
		return false
	}
	logger.Printf("<= maxRead")
	inFlightVal := s.inFlight.Get(&InFlightItem{docID: docID})
	if inFlightVal != nil && inFlightVal.(*InFlightItem).docNum != docNum {
		return false
	}
	logger.Printf("not in flight")
	if s.deletedDocNumbers.Test(uint(docNum)) {
		return false
	}
	logger.Printf("not deleted")
	return true
}

func (c *Compensator) Mutate(docID []byte, docNum uint64) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	c.inFlight = c.inFlight.Upsert(&InFlightItem{docID: docID, docNum: docNum}, rand.Int())
	if docNum != 0 {
		c.maxRead = docNum
	}
}

func (c *Compensator) MutateBatch(inflightItems []*InFlightItem, lastDocNum uint64) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	for _, item := range inflightItems {
		c.inFlight = c.inFlight.Upsert(item, rand.Int())
	}
	c.maxRead = lastDocNum
}

func (c *Compensator) Migrate(docID []byte, docNum uint64, oldDocNums []uint64) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	c.deletedMutex.Lock()
	defer c.deletedMutex.Unlock()

	// clone deleted doc numbers and mutate
	if len(oldDocNums) > 0 {
		newDeletedDocNumbers := c.deletedDocNumbers.Clone()
		for _, oldDocNum := range oldDocNums {
			newDeletedDocNumbers.Set(uint(oldDocNum))
		}
		// update pointer
		c.deletedDocNumbers = newDeletedDocNumbers
	}

	// remove entry from in-flight if it still has same doc num
	val := c.inFlight.Get(&InFlightItem{docID: docID})
	if val != nil && val.(*InFlightItem).docNum == docNum {
		c.inFlight = c.inFlight.Delete(&InFlightItem{docID: docID})
	}
}

func (c *Compensator) GarbageCollect(docNums []uint64) {
	c.deletedMutex.Lock()
	defer c.deletedMutex.Unlock()

	for _, docNum := range docNums {
		c.deletedDocNumbers.Clear(uint(docNum))
	}
}

func (c *Compensator) Snapshot() *Snapshot {
	c.inFlightMutex.RLock()
	defer c.inFlightMutex.RUnlock()
	c.deletedMutex.RLock()
	defer c.deletedMutex.RUnlock()

	rv := Snapshot{
		maxRead:           c.maxRead,
		inFlight:          c.inFlight,
		deletedDocNumbers: c.deletedDocNumbers,
	}
	return &rv
}

func (c *Compensator) GarbageCount() uint64 {
	return uint64(c.deletedDocNumbers.Count())
}

//**************

type InFlightItem struct {
	docID  []byte
	docNum uint64
}

func inFlightItemCompare(a, b interface{}) int {
	return bytes.Compare(a.(*InFlightItem).docID, b.(*InFlightItem).docID)
}
