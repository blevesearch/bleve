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

package geo

import (
	"math"

	"github.com/blevesearch/bleve/numeric"
)

// GeoBits is the number of bits used for a single geo point
// Currently this is 32bits for lon and 32bits for lat
var GeoBits uint = 32

var minLon = -180.0
var minLat = -90.0
var geoTolerance = 1E-6
var lonScale = float64((uint64(0x1)<<GeoBits)-1) / 360.0
var latScale = float64((uint64(0x1)<<GeoBits)-1) / 180.0

// MortonHash computes the morton hash value for the provided geo point
// This point is ordered as lon, lat.
func MortonHash(lon, lat float64) uint64 {
	return numeric.Interleave(scaleLon(lon), scaleLat(lat))
}

func scaleLon(lon float64) uint64 {
	rv := uint64((lon - minLon) * lonScale)
	return rv
}

func scaleLat(lat float64) uint64 {
	rv := uint64((lat - minLat) * latScale)
	return rv
}

// MortonUnhashLon extracts the longitude value from the provided morton hash.
func MortonUnhashLon(hash uint64) float64 {
	return unscaleLon(numeric.Deinterleave(hash))
}

// MortonUnhashLat extracts the latitude value from the provided morton hash.
func MortonUnhashLat(hash uint64) float64 {
	return unscaleLat(numeric.Deinterleave(hash >> 1))
}

func unscaleLon(lon uint64) float64 {
	return (float64(lon) / lonScale) + minLon
}

func unscaleLat(lat uint64) float64 {
	return (float64(lat) / latScale) + minLat
}

// compareGeo will compare two float values and see if they are the same
// taking into consideration a known geo tolerance.
func compareGeo(a, b float64) float64 {
	compare := a - b
	if math.Abs(compare) <= geoTolerance {
		return 0
	}
	return compare
}

// RectIntersects checks whether rectangles a and b intersect
func RectIntersects(aMinX, aMinY, aMaxX, aMaxY, bMinX, bMinY, bMaxX, bMaxY float64) bool {
	return !(aMaxX < bMinX || aMinX > bMaxX || aMaxY < bMinY || aMinY > bMaxY)
}

// RectWithin checks whether box a is within box b
func RectWithin(aMinX, aMinY, aMaxX, aMaxY, bMinX, bMinY, bMaxX, bMaxY float64) bool {
	rv := !(aMinX < bMinX || aMinY < bMinY || aMaxX > bMaxX || aMaxY > bMaxY)
	return rv
}

// BoundingBoxContains checks whether the lon/lat point is within the box
func BoundingBoxContains(lon, lat, minLon, minLat, maxLon, maxLat float64) bool {
	return compareGeo(lon, minLon) >= 0 && compareGeo(lon, maxLon) <= 0 &&
		compareGeo(lat, minLat) >= 0 && compareGeo(lat, maxLat) <= 0
}

// ComputeBoundingBox will compute a bounding box around the provided point
// which surrounds a circle of the provided radius (in meters).
func ComputeBoundingBox(centerLon, centerLat,
	radius float64) (upperLeftLon float64, upperLeftLat float64,
	lowerRightLon float64, lowerRightLat float64) {
	_, tlat := pointFromLonLatBearing(centerLon, centerLat, 0, radius)
	rlon, _ := pointFromLonLatBearing(centerLon, centerLat, 90, radius)
	_, blat := pointFromLonLatBearing(centerLon, centerLat, 180, radius)
	llon, _ := pointFromLonLatBearing(centerLon, centerLat, 270, radius)
	return normalizeLon(llon), normalizeLat(tlat),
		normalizeLon(rlon), normalizeLat(blat)
}

const degreesToRadian = math.Pi / 180
const radiansToDegrees = 180 / math.Pi
const flattening = 1.0 / 298.257223563
const semiMajorAxis = 6378137
const semiMinorAxis = semiMajorAxis * (1.0 - flattening)
const semiMajorAxis2 = semiMajorAxis * semiMajorAxis
const semiMinorAxis2 = semiMinorAxis * semiMinorAxis

// DegreesToRadians converts an angle in degrees to radians
func DegreesToRadians(d float64) float64 {
	return d * degreesToRadian
}

// RadiansToDegrees converts an angle in radians to degress
func RadiansToDegrees(r float64) float64 {
	return r * radiansToDegrees
}

// pointFromLonLatBearing starts that the provide lon,lat
// then moves in the bearing direction (in degrees)
// this move continues for the provided distance (in meters)
// The lon, lat of this destination location is returned.
func pointFromLonLatBearing(lon, lat, bearing,
	dist float64) (float64, float64) {

	alpha1 := DegreesToRadians(bearing)
	cosA1 := math.Cos(alpha1)
	sinA1 := math.Sin(alpha1)
	tanU1 := (1 - flattening) * math.Tan(DegreesToRadians(lat))
	cosU1 := 1 / math.Sqrt(1+tanU1*tanU1)
	sinU1 := tanU1 * cosU1
	sig1 := math.Atan2(tanU1, cosA1)
	sinAlpha := cosU1 * sinA1
	cosSqAlpha := 1 - sinAlpha*sinAlpha
	uSq := cosSqAlpha * (semiMajorAxis2 - semiMinorAxis2) / semiMinorAxis2
	A := 1 + uSq/16384*(4096+uSq*(-768+uSq*(320-175*uSq)))
	B := uSq / 1024 * (256 + uSq*(-128+uSq*(74-47*uSq)))

	sigma := dist / (semiMinorAxis * A)

	cos25SigmaM := math.Cos(2*sig1 + sigma)
	sinSigma := math.Sin(sigma)
	cosSigma := math.Cos(sigma)
	deltaSigma := B * sinSigma * (cos25SigmaM + (B/4)*
		(cosSigma*(-1+2*cos25SigmaM*cos25SigmaM)-(B/6)*cos25SigmaM*
			(-1+4*sinSigma*sinSigma)*(-3+4*cos25SigmaM*cos25SigmaM)))
	sigmaP := sigma
	sigma = dist/(semiMinorAxis*A) + deltaSigma
	for math.Abs(sigma-sigmaP) > 1E-12 {
		cos25SigmaM = math.Cos(2*sig1 + sigma)
		sinSigma = math.Sin(sigma)
		cosSigma = math.Cos(sigma)
		deltaSigma = B * sinSigma * (cos25SigmaM + (B/4)*
			(cosSigma*(-1+2*cos25SigmaM*cos25SigmaM)-(B/6)*cos25SigmaM*
				(-1+4*sinSigma*sinSigma)*(-3+4*cos25SigmaM*cos25SigmaM)))
		sigmaP = sigma
		sigma = dist/(semiMinorAxis*A) + deltaSigma
	}

	tmp := sinU1*sinSigma - cosU1*cosSigma*cosA1
	lat2 := math.Atan2(sinU1*cosSigma+cosU1*sinSigma*cosA1,
		(1-flattening)*math.Sqrt(sinAlpha*sinAlpha+tmp*tmp))
	lamda := math.Atan2(sinSigma*sinA1, cosU1*cosSigma-sinU1*sinSigma*cosA1)
	c := flattening / 16 * cosSqAlpha * (4 + flattening*(4-3*cosSqAlpha))
	lam := lamda - (1-c)*flattening*sinAlpha*
		(sigma+c*sinSigma*(cos25SigmaM+c*cosSigma*(-1+2*cos25SigmaM*cos25SigmaM)))

	rvlon := lon + RadiansToDegrees(lam)
	rvlat := RadiansToDegrees(lat2)

	return rvlon, rvlat
}

// normalizeLon normalizes a longitude value within the -180 to 180 range
func normalizeLon(lonDeg float64) float64 {
	if lonDeg >= -180 && lonDeg <= 180 {
		return lonDeg
	}

	off := math.Mod(lonDeg+180, 360)
	if off < 0 {
		return 180 + off
	} else if off == 0 && lonDeg > 0 {
		return 180
	}
	return -180 + off
}

// normalizeLat normalizes a latitude value within the -90 to 90 range
func normalizeLat(latDeg float64) float64 {
	if latDeg >= -90 && latDeg <= 90 {
		return latDeg
	}
	off := math.Abs(math.Mod(latDeg+90, 360))
	if off <= 180 {
		return off - 90
	}
	return (360 - off) - 90
}
