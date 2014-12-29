#! /bin/sh

echo Running nex...
nex query_string.nex
grep -v 'panic("unreachable")' query_string.nn.go > query_string.nn.go.new
mv query_string.nn.go.new query_string.nn.go
echo Running goyacc...
go tool yacc -o query_string.y.go query_string.y
# remove first line which pollutes godocs
tail -n +2 query_string.y.go > query_string.y.go.new
mv query_string.y.go.new query_string.y.go
# change public Lexer to private lexer
sed -i '' -e 's/Lexer/lexer/g' query_string.nn.go
sed -i '' -e 's/Newlexer/newLexer/g' query_string.nn.go
sed -i '' -e 's/debuglexer/debugLexer/g' query_string.nn.go
