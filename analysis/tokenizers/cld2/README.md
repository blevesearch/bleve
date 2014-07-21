# cld2 tokenizer

A bleve tokenizer which passes the input text to the cld2 library.  The library determines what it thinks the language most likely is.  The ISO-639 language code is returned as the single token resulting from the analysis.

# Building

1.  Acquire the source to cld2 in this directory.

        $ svn checkout http://cld2.googlecode.com/svn/trunk/ cld2-read-only

2.  Build cld2

        $ cd cld2-read-only/internal/
        $ ./compile_libs.sh


3.  Put the resulting libraries somewhere your dynamic linker can find.

        $ cp *.so /usr/local/lib

4.  Run the unit tests

        $ cd ../..
        $ go test -v
        === RUN TestCld2Tokenizer
        --- PASS: TestCld2Tokenizer (0.03 seconds)
        PASS
        ok  	github.com/couchbaselabs/bleve/analysis/tokenizers/cld2	0.067s