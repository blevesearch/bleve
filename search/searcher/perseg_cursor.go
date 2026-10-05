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

package searcher

// docCursor walks the matches of a query in one segment, a document at a
// time, in ascending doc order. A term's cursor is one; the unions and
// intersections below are cursors made of cursors, which is how composite
// per segment searchers nest.
//
// This is the generic machinery: no pruning, every match is visited. The
// algorithms that prune (WAND, MAXSCORE, the block windows of an intersection)
// work on term cursors specifically, and use these as their oracle. A union or
// intersection of terms is read a window of docs at a time by a bufferedUnionCursor
// or bufferedIntersectionCursor; the cursors here are for the rest, and for the
// consumers that seek far (the buffered ones hand over to them then).
//
// A clause is given a cursor by segCursorSeeked, which says how its consumer is going
// to move it, so that it picks the kind that suits.
type docCursor interface {
	// Doc is the doc the cursor is on, noMoreDocs once it has run out.
	Doc() uint32
	// Advance moves to the next match.
	Advance() uint32
	// Seek moves to the first match >= target. It never moves backwards.
	Seek(target uint32) uint32
	// Score is the score of the match the cursor is on. Cursors of a search
	// that has no scores must not be asked.
	Score() float32
	Cost() uint64
	// Offset is what has to be added to a doc number to make it unique across
	// the index.
	Offset() uint64
	Err() error
}

var _ docCursor = (*termCursor)(nil)

// releaser is a docCursor that holds working memory it can give back to a pool
// (a cursor that goes through a window of docs at a time does). Whoever is done
// with a cursor, before it ran out of matches or not, releases it; a cursor that
// isn't released is only garbage collected, which is the cost of an allocation,
// not an error. Releasing twice is harmless; using a cursor after it was is not.
type releaser interface {
	release()
}

// releaseCursor gives back what the cursor holds, if it holds anything.
func releaseCursor(c docCursor) {
	if r, ok := c.(releaser); ok {
		r.release()
	}
}

// unionCursor is the matches of a disjunction: the docs that at least min of
// its cursors are on. The score of a doc is the sum of the scores of the
// cursors on it, times coord: the share of the n clauses of the query that
// match -- n being the clauses of the query, also those that have no match in
// this segment, and so no cursor.
//
// Cursors are kept in the order of the query, and scores are summed in that
// order, so the score of a doc doesn't depend on how the work was done.
type unionCursor struct {
	cursors []docCursor
	n       int
	min     int

	doc     uint32
	matches int
}

func newUnionCursor(cursors []docCursor, n, min int) *unionCursor {
	u := &unionCursor{cursors: cursors, n: n, min: max(min, 1)}
	u.settle()
	return u
}

// settle goes to the first doc, from where the cursors are, that enough of them
// are on.
func (u *unionCursor) settle() {
	for {
		lowest := noMoreDocs
		for _, c := range u.cursors {
			if d := c.Doc(); d < lowest {
				lowest = d
			}
		}
		if lowest == noMoreDocs {
			u.doc, u.matches = noMoreDocs, 0
			return
		}
		matches := 0
		for _, c := range u.cursors {
			if c.Doc() == lowest {
				matches++
			}
		}
		if matches >= u.min {
			u.doc, u.matches = lowest, matches
			return
		}
		for _, c := range u.cursors {
			if c.Doc() == lowest {
				c.Advance()
			}
		}
	}
}

func (u *unionCursor) Doc() uint32 { return u.doc }

func (u *unionCursor) Advance() uint32 {
	if u.doc == noMoreDocs {
		return noMoreDocs
	}
	for _, c := range u.cursors {
		if c.Doc() == u.doc {
			c.Advance()
		}
	}
	u.settle()
	return u.doc
}

func (u *unionCursor) Seek(target uint32) uint32 {
	if u.doc == noMoreDocs || target <= u.doc {
		return u.doc
	}
	for _, c := range u.cursors {
		c.Seek(target)
	}
	u.settle()
	return u.doc
}

func (u *unionCursor) Score() float32 {
	var sum float32
	for _, c := range u.cursors {
		if c.Doc() == u.doc {
			sum += c.Score()
		}
	}
	return disjunctionScore(sum, u.matches, float32(u.n))
}

// release implements releaser: the clauses' cursors may hold memory.
func (u *unionCursor) release() {
	for _, c := range u.cursors {
		releaseCursor(c)
	}
}

func (u *unionCursor) Cost() uint64 {
	var cost uint64
	for _, c := range u.cursors {
		cost += c.Cost()
	}
	return cost
}

func (u *unionCursor) Offset() uint64 { return u.cursors[0].Offset() }

func (u *unionCursor) Err() error {
	for _, c := range u.cursors {
		if err := c.Err(); err != nil {
			return err
		}
	}
	return nil
}

// intersectionCursor is the matches of a conjunction: the docs that all of its
// cursors are on, scored by the sum of their scores, in the order of the
// query. The cursors are walked cheapest first, leapfrogging.
type intersectionCursor struct {
	cursors []docCursor // in the order of the query
	byCost  []docCursor // cheapest first
	doc     uint32
}

func newIntersectionCursor(cursors []docCursor) *intersectionCursor {
	byCost := append([]docCursor(nil), cursors...)
	// insertion sort: there are few of them
	for i := 1; i < len(byCost); i++ {
		for j := i; j > 0 && byCost[j].Cost() < byCost[j-1].Cost(); j-- {
			byCost[j], byCost[j-1] = byCost[j-1], byCost[j]
		}
	}
	x := &intersectionCursor{cursors: cursors, byCost: byCost}
	x.settle(x.byCost[0].Doc())
	return x
}

// settle goes to the first doc >= candidate that every cursor is on
func (x *intersectionCursor) settle(candidate uint32) {
outer:
	for candidate != noMoreDocs {
		for _, c := range x.byCost {
			d := c.Seek(candidate)
			if d != candidate {
				candidate = d
				continue outer
			}
		}
		x.doc = candidate
		return
	}
	x.doc = noMoreDocs
}

func (x *intersectionCursor) Doc() uint32 { return x.doc }

func (x *intersectionCursor) Advance() uint32 {
	if x.doc == noMoreDocs {
		return noMoreDocs
	}
	x.settle(x.byCost[0].Advance())
	return x.doc
}

func (x *intersectionCursor) Seek(target uint32) uint32 {
	if x.doc == noMoreDocs || target <= x.doc {
		return x.doc
	}
	x.settle(target)
	return x.doc
}

func (x *intersectionCursor) Score() float32 {
	var sum float32
	for _, c := range x.cursors {
		sum += c.Score()
	}
	return sum
}

// release implements releaser.
func (x *intersectionCursor) release() {
	for _, c := range x.cursors {
		releaseCursor(c)
	}
}

func (x *intersectionCursor) Cost() uint64 { return x.byCost[0].Cost() }

func (x *intersectionCursor) Offset() uint64 { return x.cursors[0].Offset() }

func (x *intersectionCursor) Err() error {
	for _, c := range x.cursors {
		if err := c.Err(); err != nil {
			return err
		}
	}
	return nil
}

// booleanCursor is the matches of a boolean query: the docs that the required
// clause is on (or, without one, the optional clause), that the excluding
// clause isn't on; scored by the sum of the scores of the required clause and
// the optional one, the latter where it matches too. It is BooleanSearcher's
// semantics:
//
//   - a "should" whose own minimum is 0 only adds to the score of the docs of the
//     must clause; one with a minimum above 0 is required as well;
//   - with no must clause, the docs are those of the should clause.
type booleanCursor struct {
	must, should, mustNot docCursor // nil when there is none
	shouldOptional        bool

	// driver is the clause whose docs are candidates: must, else should
	driver docCursor
	doc    uint32
}

func newBooleanCursor(must, should, mustNot docCursor, shouldOptional bool) *booleanCursor {
	b := &booleanCursor{must: must, should: should, mustNot: mustNot, shouldOptional: shouldOptional}
	b.driver = must
	if b.driver == nil {
		b.driver = should
	}
	b.settle()
	return b
}

// settle goes to the first doc, from where the driver is, that qualifies
func (b *booleanCursor) settle() {
	for {
		d := b.driver.Doc()
		if d == noMoreDocs {
			b.doc = noMoreDocs
			return
		}
		if b.mustNot != nil && b.mustNot.Seek(d) == d {
			b.driver.Advance() // excluded
			continue
		}
		if b.must != nil && b.should != nil && !b.shouldOptional && b.should.Seek(d) != d {
			b.driver.Advance() // the should clause is required, and not here
			continue
		}
		b.doc = d
		return
	}
}

func (b *booleanCursor) Doc() uint32 { return b.doc }

func (b *booleanCursor) Advance() uint32 {
	if b.doc == noMoreDocs {
		return noMoreDocs
	}
	b.driver.Advance()
	b.settle()
	return b.doc
}

func (b *booleanCursor) Seek(target uint32) uint32 {
	if b.doc == noMoreDocs || target <= b.doc {
		return b.doc
	}
	b.driver.Seek(target)
	b.settle()
	return b.doc
}

func (b *booleanCursor) Score() float32 {
	var sum float32
	if b.must != nil {
		sum += b.must.Score()
	}
	if b.should != nil {
		// the optional clause is on the doc, or ahead of it, or behind
		// it: Seek brings it up if it was behind
		if b.must == nil || b.should.Seek(b.doc) == b.doc {
			sum += b.should.Score()
		}
	}
	return sum
}

// release implements releaser.
func (b *booleanCursor) release() {
	for _, c := range []docCursor{b.must, b.should, b.mustNot} {
		if c != nil {
			releaseCursor(c)
		}
	}
}

func (b *booleanCursor) Cost() uint64 { return b.driver.Cost() }

func (b *booleanCursor) Offset() uint64 { return b.driver.Offset() }

func (b *booleanCursor) Err() error {
	for _, c := range []docCursor{b.must, b.should, b.mustNot} {
		if c != nil {
			if err := c.Err(); err != nil {
				return err
			}
		}
	}
	return nil
}
