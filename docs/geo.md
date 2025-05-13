# Geo spatial search

Refer to [geo/README.md](https://github.com/blevesearch/bleve/blob/master/geo/README.md) for more details

# Supported Geo Shapes

## Point

A Point represents a single geographic location defined by a pair of coordinates — usually latitude and longitude. It is the simplest form of geometry and is typically used to mark precise locations such as a city center, a sensor, or a user’s GPS position.

Example Point
```
{
    "type": "Point",
    "coordinates": [75.05687713623047, 22.53539059204079]
}
```

Point supports intersection queries with all other geo shapes and contains queries with only Points and MultiPoints.


## MultiPoint

A MultiPoint is a collection of individual points grouped as a single geometry. Unlike a LineString, the points are not connected. This shape is useful when you need to represent multiple related locations — such as delivery drop-off sites, monitoring stations, or the positions of multiple users — without implying any spatial order or connection between them.

Example MultiPoint
```
{
    "type": "MultiPoint",
    "coordinates": [
        [-115.8343505859375, 38.45789034424927],
        [-115.81237792968749, 38.19502155795575],
        [-120.80017089843749, 36.54053616262899],
        [-120.67932128906249, 36.33725319397006]
    ]
}
```

MultiPoint supports intersection queries with all other geo shapes and contains queries with only Points and MultiPoints.

## LineString

A LineString is an ordered sequence of two or more points connected by straight lines, forming a continuous path. It is commonly used to represent linear features like roads, trails, rivers, or pipelines. The points must be arranged in the order the line should follow, and the shape is not closed (unless explicitly required).

Example LineString
```
{
    "type": "LineString",
    "coordinates": [
        [77.01416015625, 23.0797317624497],
        [78.134765625, 20.385825381874263]
    ]
}
```

LineString supports intersection queries with all other geo shapes and contains queries with only Points and MultiPoints.

## MultiLineString

A MultiLineString is a collection of multiple LineStrings grouped together into one geometry. Each LineString in the set can represent a separate path or segment, and they do not need to be connected. This is useful for representing complex route networks, disjoint road segments, or fragmented boundaries.

Example MultiLineString
```
{
    "type": "MultiLineString",
    "coordinates": [
        [[-118.31726074, 35.250105158], [-117.509765624, 35.3756141]],
        [[-118.6962890, 34.624167789], [-118.317260742, 35.03899204]],
        [[-117.9492187, 35.146862906], [-117.6745605, 34.41144164]]
    ]
}
```

MultiLineString supports intersection queries with all other geo shapes and contains queries with only Points and MultiPoints.

## Polygon

A Polygon defines a two-dimensional surface enclosed by a closed ring of coordinates. It consists of a linear outer boundary and may include one or more inner rings (holes). Polygons are typically used to represent areas such as property boundaries, lakes, administrative regions, or building footprints.

Example Polygon
```
{
    "type": "Polygon",
    "coordinates": [[
        [85.605, 57.207],
        [86.396, 55.998],
        [87.033, 56.716],
        [85.605, 57.207]
    ]]
}
```
Polygons supports intersection and contains queries with all other geo shapes.

#### Note
 * A Polygon intersecting Polygon or MultiPolygon may return arbitrary results when the overlap is only an edge or vertex.

## MultiPolygon

A MultiPolygon is a collection of Polygons grouped as a single geometry. This allows modeling of complex areas made up of multiple disconnected regions or islands, such as archipelagos, districts with non-contiguous zones, or multi-building campuses.

Example MultiPolygon
```
{
    "type": "MultiPolygon",
    "coordinates": [
        [[
            [-73.958, 40.8003],
            [-73.9498, 40.7968],
            [-73.9737, 40.7648],
            [-73.9814, 40.7681],
            [-73.958, 40.8003]
        ]],
        [[
            [-73.958, 40.8003],
            [-73.9498, 40.7968],
            [-73.9737, 40.7648],
            [-73.958, 40.8003]
        ]]
    ]
}
```

MultiPolygons supports intersection and contains queries with all other geo shapes.

#### Note
 * Just as with Polygons, MultiPolgons intersecting Polygons or MultiPolygons may return arbitrary results when the overlap is only an edge or vertex.

## GeometryCollection

A GeometryCollection is a container that can hold multiple geometry types — such as points, lines, and polygons — in a single structure. It provides flexibility for modeling real-world entities that consist of a combination of geometric features, like a campus with multiple buildings (polygons), access roads (lines), and entry points (points).

Example GeometryCollection
```
{
    "type": "GeometryCollection",
    "geometries": [
        {
            "type": "MultiPoint",
            "coordinates": [
                [-73.9580, 40.8003],
                [-73.9498, 40.7968],
                [-73.9737, 40.7648],
                [-73.9814, 40.7681]
            ]
        },
        {
            "type": "MultiLineString",
            "coordinates": [
                [
                    [-73.96943, 40.78519],
                    [-73.96082, 40.78095]
                ],
                [
                    [-73.96415, 40.79229],
                    [-73.95544, 40.78854]
                ],
                [
                    [-73.97162, 40.78205],
                    [-73.96374, 40.77715]
                ],
                [
                    [-73.97880, 40.77247],
                    [-73.97036, 40.76811]
                ]
            ]
        },
        {
            "type" : "Polygon",
            "coordinates" : [
                [
                    [0, 0],
                    [3, 6],
                    [6, 1],
                    [0, 0]
                ],
                [
                    [2, 2],
                    [3, 3],
                    [4, 2],
                    [2, 2]
                ]
            ]
        }
    ]
}
```

GeometryCollections supports intersection and contains queries with all other geo shapes.

## Circle

A Circle defines a circular region using a center point and a radius. It is often used for proximity-based searches, such as finding all points within a certain distance from a location, or for modeling influence zones like cellular coverage areas.


Example Circle
```
{
    "type": "circle",
    "coordinates": [75.05687713623047,22.53539059204079],
    "radius": "1000m"
}
```

Circles supports intersection and contains queries with all other geo shapes.

#### Supported Distances
Distance Units | Suffix
:-------------:| :------------------:
inches         | in or inches
feet           | ft or feet
yards          | yd or yards
miles          | mi or miles
nautical miles | nm or nauticalmiles
millimeters    | mm or millimeters
centimeters    | cm or centimeters
meters         | m or meters
kilometers     | km or kilometers

#### Note
 * If distance suffix is not mentioned, meters is assumed.
 * The case of a clockwise polygon with all of its points within the circle will return a false positive for containment query.

## Envelope

An Envelope is a rectangular bounding box defined its north-west and south-east points. It is commonly used for spatial indexing, querying, or defining the extent of an area without needing the precision of a detailed polygon.

```
{
    "type": "envelope",
    "coordinates": [
        [72.83, 18.979],
        [78.508, 17.4555]
    ]
}
```

Envelopes supports intersection and contains queries with all other geo shapes.

#### Note
 * An envelopes edges will not follow the shortest distance arcs between its edges and will instead follow the latitude and longitude lines.
 * Envelope intersecting queries with LineStrings, MultiLineStrings, Polygons and MultiPolygons implicitly converts the Envelope into a Polygon which changes the curvature of the edges causing inaccurate results for few edge cases.