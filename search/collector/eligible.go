package collector

import (
	"context"
	"time"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

type EligibleCollector struct {
	size     int
	total    uint64
	maxScore float64
	took     time.Duration
	results  search.DocumentMatchCollection

	store collectorStore

	needDocIds   bool
	neededFields []string
	cachedDesc   []bool

	lowestMatchOutsideResults *search.DocumentMatch
	updateFieldVisitor        index.DocValueVisitor
	dvReader                  index.DocValueReader
	searchAfter               *search.DocumentMatch
}

func NewEligibleCollector(size int) *EligibleCollector {
	return newEligibleCollector(size)
}

func newEligibleCollector(size int) *EligibleCollector {
	// No sort order & skip always 0 since this is only to filter eligible docs.
	hc := &EligibleCollector{size: size}

	// comparator is a dummy here
	hc.store = getOptimalCollectorStore(size, 0, func(i, j *search.DocumentMatch) int {
		return 0
	})

	return hc
}

func (hc *EligibleCollector) Collect(ctx context.Context, searcher search.Searcher, reader index.IndexReader) error {
	startTime := time.Now()
	var err error
	var next *search.DocumentMatch

	backingSize := hc.size
	if backingSize > PreAllocSizeSkipCap {
		backingSize = PreAllocSizeSkipCap + 1
	}
	searchContext := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(backingSize+searcher.DocumentMatchPoolSize(), 0),
		Collector:         hc,
		IndexReader:       reader,
	}

	dmHandlerMaker := MakeEligibleDocumentMatchHandler
	if cv := ctx.Value(search.MakeDocumentMatchHandlerKey); cv != nil {
		dmHandlerMaker = cv.(search.MakeDocumentMatchHandler)
	}
	// use the application given builder for making the custom document match
	// handler and perform callbacks/invocations on the newly made handler.
	dmHandler, _, err := dmHandlerMaker(searchContext)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		search.RecordSearchCost(ctx, search.AbortM, 0)
		return ctx.Err()
	default:
		next, err = searcher.Next(searchContext)
	}
	for err == nil && next != nil {
		if hc.total%CheckDoneEvery == 0 {
			select {
			case <-ctx.Done():
				search.RecordSearchCost(ctx, search.AbortM, 0)
				return ctx.Err()
			default:
			}
		}
		hc.total++

		err = dmHandler(next)
		if err != nil {
			break
		}

		next, err = searcher.Next(searchContext)
	}
	if err != nil {
		return err
	}

	// help finalize/flush the results in case
	// of custom document match handlers.
	err = dmHandler(nil)
	if err != nil {
		return err
	}

	// compute search duration
	hc.took = time.Since(startTime)

	// finalize actual results
	err = hc.finalizeResults(reader)
	if err != nil {
		return err
	}
	return nil
}

func (hc *EligibleCollector) finalizeResults(r index.IndexReader) error {
	var err error
	hc.results, err = hc.store.Final(0, func(doc *search.DocumentMatch) error {
		// Adding the results to the store without any modifications since we don't
		// require the external ID of the filtered hits.
		return nil
	})
	return err
}

func (hc *EligibleCollector) Results() search.DocumentMatchCollection {
	return hc.results
}

func (hc *EligibleCollector) Total() uint64 {
	return hc.total
}

// No concept of scoring in the eligible collector.
func (hc *EligibleCollector) MaxScore() float64 {
	return 0
}

func (hc *EligibleCollector) Took() time.Duration {
	return hc.took
}

func (hc *EligibleCollector) SetFacetsBuilder(facetsBuilder *search.FacetsBuilder) {
	// facet unsupported for pre-filtering in KNN search
}

func (hc *EligibleCollector) FacetResults() search.FacetResults {
	// facet unsupported for pre-filtering in KNN search
	return nil
}
