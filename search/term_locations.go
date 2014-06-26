package search

import (
	"sort"
)

type termLocation struct {
	Term  string
	Pos   int
	Start int
	End   int
}

type termLocations []*termLocation

func (t termLocations) Len() int           { return len(t) }
func (t termLocations) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t termLocations) Less(i, j int) bool { return t[i].Start < t[j].Start }

func OrderTermLocations(tlm TermLocationMap) termLocations {
	rv := make(termLocations, 0)
	for term, locations := range tlm {
		for _, location := range locations {
			tl := termLocation{
				Term:  term,
				Pos:   int(location.Pos),
				Start: int(location.Start),
				End:   int(location.End),
			}
			rv = append(rv, &tl)
		}
	}
	sort.Sort(rv)
	return rv
}
