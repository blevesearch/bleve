#! /bin/sh

echo Running nex...
nex query_string.nex
echo Running goyacc...
go tool yacc query_string.y
# remove first line which pollutes godocs
tail -n +2 y.go > y.go.new
mv y.go.new y.go
# change public Lexer to private lexer
sed -i '' -e 's/Lexer/lexer/g' query_string.nn.go
sed -i '' -e 's/Newlexer/newLexer/g' query_string.nn.go
sed -i '' -e 's/debuglexer/debugLexer/g' query_string.nn.go
