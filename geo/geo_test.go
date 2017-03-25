package geo

import (
	"math"
	"testing"
)

func TestMortonHashMortonUnhash(t *testing.T) {
	tests := []struct {
		lon float64
		lat float64
	}{
		{-180.0, -90.0},
		{-5, 27.3},
		{0, 0},
		{1.0, 1.0},
		{24.7, -80.4},
		{180.0, 90.0},
	}

	for _, test := range tests {
		hash := MortonHash(test.lon, test.lat)
		lon := MortonUnhashLon(hash)
		lat := MortonUnhashLat(hash)
		if compareGeo(test.lon, lon) != 0 {
			t.Errorf("expected lon %f, got %f, hash %x", test.lon, lon, hash)
		}
		if compareGeo(test.lat, lat) != 0 {
			t.Errorf("expected lat %f, got %f, hash %x", test.lat, lat, hash)
		}
	}
}

func TestScaleLonUnscaleLon(t *testing.T) {
	tests := []struct {
		lon float64
	}{
		{-180.0},
		{0.0},
		{1.0},
		{180.0},
	}

	for _, test := range tests {
		s := scaleLon(test.lon)
		lon := unscaleLon(s)
		if compareGeo(test.lon, lon) != 0 {
			t.Errorf("expected %f, got %f, scaled was %d", test.lon, lon, s)
		}
	}
}

func TestScaleLatUnscaleLat(t *testing.T) {
	tests := []struct {
		lat float64
	}{
		{-90.0},
		{0.0},
		{1.0},
		{90.0},
	}

	for _, test := range tests {
		s := scaleLat(test.lat)
		lat := unscaleLat(s)
		if compareGeo(test.lat, lat) != 0 {
			t.Errorf("expected %.16f, got %.16f, scaled was %d", test.lat, lat, s)
		}
	}
}

func TestComputeBoundingBoxCheckLatitudeAtEquator(t *testing.T) {
	// at the equator 1 degree of latitude is about 110567 meters
	_, upperLeftLat, _, lowerRightLat := ComputeBoundingBox(0, 0, 110567)
	if math.Abs(upperLeftLat-1) > 1E-4 {
		t.Errorf("expected bounding box upper left lat to be almost 1, got %f", upperLeftLat)
	}
	if math.Abs(lowerRightLat+1) > 1E-4 {
		t.Errorf("expected bounding box lower right lat to be almost -1, got %f", lowerRightLat)
	}
}
