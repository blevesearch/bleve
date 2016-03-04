package metrics

import (
	"fmt"
	"io"

	"github.com/rcrowley/go-metrics"
)

// NOTE: This is copy & pasted from cbft as otherwise there
// would be an import cycle.

var timerPercentiles = []float64{0.5, 0.75, 0.95, 0.99, 0.999}

func TimerMap(timer metrics.Timer) map[string]interface{} {

	rv := make(map[string]interface{})
	t := timer.Snapshot()
	p := t.Percentiles(timerPercentiles)

	percentiles := make(map[string]interface{})
	percentiles["median"] = p[0]
	percentiles["75%"] = p[1]
	percentiles["95%"] = p[2]
	percentiles["99%"] = p[3]
	percentiles["99.9%"] = p[4]

	rates := make(map[string]interface{})
	rates["1-min"] = t.Rate1()
	rates["5-min"] = t.Rate5()
	rates["15-min"] = t.Rate15()
	rates["mean"] = t.RateMean()

	rv["count"] = t.Count()
	rv["min"] = t.Min()
	rv["max"] = t.Max()
	rv["mean"] = t.Mean()
	rv["stddev"] = t.StdDev()
	rv["percentiles"] = percentiles
	rv["rates"] = rates

	return rv
}

func WriteTimerJSON(w io.Writer, timer metrics.Timer) {
	t := timer.Snapshot()
	p := t.Percentiles(timerPercentiles)

	fmt.Fprintf(w, `{"count":%9d,`, t.Count())
	fmt.Fprintf(w, `"min":%9d,`, t.Min())
	fmt.Fprintf(w, `"max":%9d,`, t.Max())
	fmt.Fprintf(w, `"mean":%12.2f,`, t.Mean())
	fmt.Fprintf(w, `"stddev":%12.2f,`, t.StdDev())
	fmt.Fprintf(w, `"percentiles":{`)
	fmt.Fprintf(w, `"median":%12.2f,`, p[0])
	fmt.Fprintf(w, `"75%%":%12.2f,`, p[1])
	fmt.Fprintf(w, `"95%%":%12.2f,`, p[2])
	fmt.Fprintf(w, `"99%%":%12.2f,`, p[3])
	fmt.Fprintf(w, `"99.9%%":%12.2f},`, p[4])
	fmt.Fprintf(w, `"rates":{`)
	fmt.Fprintf(w, `"1-min":%12.2f,`, t.Rate1())
	fmt.Fprintf(w, `"5-min":%12.2f,`, t.Rate5())
	fmt.Fprintf(w, `"15-min":%12.2f,`, t.Rate15())
	fmt.Fprintf(w, `"mean":%12.2f}}`, t.RateMean())
}

func WriteTimerCSVHeader(w io.Writer, prefix string) {
	fmt.Fprintf(w, "%s-count,", prefix)
	fmt.Fprintf(w, "%s-min,", prefix)
	fmt.Fprintf(w, "%s-max,", prefix)
	fmt.Fprintf(w, "%s-mean,", prefix)
	fmt.Fprintf(w, "%s-stddev,", prefix)
	fmt.Fprintf(w, "%s-percentile-50%%,", prefix)
	fmt.Fprintf(w, "%s-percentile-75%%,", prefix)
	fmt.Fprintf(w, "%s-percentile-95%%,", prefix)
	fmt.Fprintf(w, "%s-percentile-99%%,", prefix)
	fmt.Fprintf(w, "%s-percentile-99.9%%,", prefix)
	fmt.Fprintf(w, "%s-rate-1-min,", prefix)
	fmt.Fprintf(w, "%s-rate-5-min,", prefix)
	fmt.Fprintf(w, "%s-rate-15-min,", prefix)
	fmt.Fprintf(w, "%s-rate-mean", prefix)
}

func WriteTimerCSV(w io.Writer, timer metrics.Timer) {
	t := timer.Snapshot()
	p := t.Percentiles(timerPercentiles)

	fmt.Fprintf(w, `%d,`, t.Count())
	fmt.Fprintf(w, `%d,`, t.Min())
	fmt.Fprintf(w, `%d,`, t.Max())
	fmt.Fprintf(w, `%f,`, t.Mean())
	fmt.Fprintf(w, `%f,`, t.StdDev())
	fmt.Fprintf(w, `%f,`, p[0])
	fmt.Fprintf(w, `%f,`, p[1])
	fmt.Fprintf(w, `%f,`, p[2])
	fmt.Fprintf(w, `%f,`, p[3])
	fmt.Fprintf(w, `%f,`, p[4])
	fmt.Fprintf(w, `%f,`, t.Rate1())
	fmt.Fprintf(w, `%f,`, t.Rate5())
	fmt.Fprintf(w, `%f,`, t.Rate15())
	fmt.Fprintf(w, `%f`, t.RateMean())
}
