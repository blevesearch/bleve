package geo

import (
	"reflect"
	"strings"
)

// ExtractGeoPoint takes an arbitrary interface{} and tries it's best to
// interpret it is as geo point
func ExtractGeoPoint(thing interface{}) (lon, lat float64, success bool) {
	var foundLon, foundLat bool
	// is it a map
	if l, ok := thing.(map[string]interface{}); ok {
		if lval, ok := l["lon"]; ok {
			lon, foundLon = extractNumericVal(lval)
		} else if lval, ok := l["lng"]; ok {
			lon, foundLon = extractNumericVal(lval)
		}
		if lval, ok := l["lat"]; ok {
			lat, foundLat = extractNumericVal(lval)
		}
		return lon, lat, foundLon && foundLat
	}

	// now try reflection on struct fields
	thingVal := reflect.ValueOf(thing)
	thingTyp := thingVal.Type()
	if thingVal.IsValid() && thingVal.Kind() == reflect.Struct {
		for i := 0; i < thingVal.NumField(); i++ {
			field := thingTyp.Field(i)
			fieldName := field.Name
			if strings.HasPrefix(strings.ToLower(fieldName), "lon") {
				if thingVal.Field(i).CanInterface() {
					fieldVal := thingVal.Field(i).Interface()
					lon, foundLon = extractNumericVal(fieldVal)
				}
			}
			if strings.HasPrefix(strings.ToLower(fieldName), "lng") {
				if thingVal.Field(i).CanInterface() {
					fieldVal := thingVal.Field(i).Interface()
					lon, foundLon = extractNumericVal(fieldVal)
				}
			}
			if strings.HasPrefix(strings.ToLower(fieldName), "lat") {
				if thingVal.Field(i).CanInterface() {
					fieldVal := thingVal.Field(i).Interface()
					lat, foundLat = extractNumericVal(fieldVal)
				}
			}
		}
	}

	// last hope, some interfaces
	// lon
	if l, ok := thing.(loner); ok {
		lon = l.Lon()
		foundLon = true
	} else if l, ok := thing.(lnger); ok {
		lon = l.Lng()
		foundLon = true
	}
	// lat
	if l, ok := thing.(later); ok {
		lat = l.Lat()
		foundLat = true
	}

	return lon, lat, foundLon && foundLat
}

// extract numeric value (if possible) and returna s float64
func extractNumericVal(v interface{}) (float64, bool) {
	switch v := v.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	}
	return 0, false
}

// various support interfaces which can be used to find lat/lon
type loner interface {
	Lon() float64
}

type later interface {
	Lat() float64
}

type lnger interface {
	Lng() float64
}
