package geo

import (
	"math"
	"strconv"
	"strings"
)

type distanceUnit struct {
	conv     float64
	suffixes []string
}

var inch = distanceUnit{0.0254, []string{"in", "inch"}}
var yard = distanceUnit{0.9144, []string{"yd", "yards"}}
var feet = distanceUnit{0.3048, []string{"ft", "feet"}}
var kilom = distanceUnit{1000, []string{"km", "kilometers"}}
var nauticalm = distanceUnit{1852.0, []string{"nm", "nauticalmiles"}}
var millim = distanceUnit{0.001, []string{"mm", "millimeters"}}
var centim = distanceUnit{0.01, []string{"cm", "centimeters"}}
var miles = distanceUnit{1609.344, []string{"mi", "miles"}}
var meters = distanceUnit{1, []string{"m", "meters"}}

var distanceUnits = []*distanceUnit{
	&inch, &yard, &feet, &kilom, &nauticalm, &millim, &centim, &miles, &meters,
}

// ParseDistance attempts to parse a distance, return distance in meters
func ParseDistance(d string) (float64, error) {
	for _, unit := range distanceUnits {
		for _, unitSuffix := range unit.suffixes {
			if strings.HasSuffix(d, unitSuffix) {
				parsedNum, err := strconv.ParseFloat(d[0:len(d)-len(unitSuffix)], 64)
				if err != nil {
					return 0, err
				}
				return parsedNum * unit.conv, nil
			}
		}
	}
	// no unit matched, try assuming meters?
	parsedNum, err := strconv.ParseFloat(d, 64)
	if err != nil {
		return 0, err
	}
	return parsedNum, nil
}

func Haversin(lon1, lat1, lon2, lat2 float64) float64 {
	x1 := lat1 * degreesToRadian
	x2 := lat2 * degreesToRadian
	h1 := 1 - cos(x1-x2)
	h2 := 1 - cos((lon1-lon2)*degreesToRadian)
	h := (h1 + cos(x1)*cos(x2)*h2) / 2
	avgLat := (x1 + x2) / 2
	diameter := earthDiameter(avgLat)

	return diameter * asin(math.Min(1, math.Sqrt(h)))
}
