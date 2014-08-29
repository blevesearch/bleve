#! /bin/sh

echo Running nex...
nex query_syntax.nex
echo Running goyacc...
go tool yacc query_syntax.y
# remove first line which pollutes godocs
tail -n +2 y.go > y.go.new
mv y.go.new y.go
