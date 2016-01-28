package search

import (
	"math"
)

func LevenshteinDistance(a, b *string) int {
	la := len(*a)
	lb := len(*b)
	d := make([]int, la+1)
	var lastdiag, olddiag, temp int

	for i := 1; i <= la; i++ {
		d[i] = i
	}
	for i := 1; i <= lb; i++ {
		d[0] = i
		lastdiag = i - 1
		for j := 1; j <= la; j++ {
			olddiag = d[j]
			min := d[j] + 1
			if (d[j-1] + 1) < min {
				min = d[j-1] + 1
			}
			if (*a)[j-1] == (*b)[i-1] {
				temp = 0
			} else {
				temp = 1
			}
			if (lastdiag + temp) < min {
				min = lastdiag + temp
			}
			d[j] = min
			lastdiag = olddiag
		}
	}
	return d[la]
}

// levenshteinDistanceMax same as levenshteinDistance but
// attempts to bail early once we know the distance
// will be greater than max
// in which case the first return val will be the max
// and the second will be true, indicating max was exceeded
func LevenshteinDistanceMax(a, b *string, max int) (int, bool) {
	la := len(*a)
	lb := len(*b)

	ld := int(math.Abs(float64(la - lb)))
	if ld > max {
		return max, true
	}

	d := make([]int, la+1)
	var lastdiag, olddiag, temp int

	for i := 1; i <= la; i++ {
		d[i] = i
	}
	for i := 1; i <= lb; i++ {
		d[0] = i
		lastdiag = i - 1
		rowmin := max + 1
		for j := 1; j <= la; j++ {
			olddiag = d[j]
			min := d[j] + 1
			if (d[j-1] + 1) < min {
				min = d[j-1] + 1
			}
			if (*a)[j-1] == (*b)[i-1] {
				temp = 0
			} else {
				temp = 1
			}
			if (lastdiag + temp) < min {
				min = lastdiag + temp
			}
			if min < rowmin {
				rowmin = min
			}
			d[j] = min

			lastdiag = olddiag
		}
		// after each row if rowmin isn't less than max stop
		if rowmin > max {
			return max, true
		}
	}
	return d[la], false
}
