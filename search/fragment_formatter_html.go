package search

import ()

const DEFAULT_HTML_HIGHLIGHT_BEFORE = "<b>"
const DEFAULT_HTML_HIGHLIGHT_AFTER = "</b>"

type HTMLFragmentFormatter struct {
	before string
	after  string
}

func NewHTMLFragmentFormatter() *HTMLFragmentFormatter {
	return &HTMLFragmentFormatter{
		before: DEFAULT_HTML_HIGHLIGHT_BEFORE,
		after:  DEFAULT_HTML_HIGHLIGHT_AFTER,
	}
}

func NewHTMLFragmentFormatterCustom(before, after string) *HTMLFragmentFormatter {
	return &HTMLFragmentFormatter{
		before: before,
		after:  after,
	}
}

func (a *HTMLFragmentFormatter) Format(f *Fragment, tlm TermLocationMap) string {
	orderedTermLocations := OrderTermLocations(tlm)
	rv := ""
	curr := f.start
	for _, termLocation := range orderedTermLocations {
		if termLocation.Start < curr {
			continue
		}
		if termLocation.End > f.end {
			break
		}
		// add the stuff before this location
		rv += string(f.orig[curr:termLocation.Start])
		// add the color
		rv += a.before
		// add the term itself
		rv += string(f.orig[termLocation.Start:termLocation.End])
		// reset the color
		rv += a.after
		// update current
		curr = termLocation.End
	}
	// add any remaining text after the last token
	rv += string(f.orig[curr:f.end])

	return rv
}
