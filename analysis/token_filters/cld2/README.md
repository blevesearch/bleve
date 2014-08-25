# cld2 token filter

A bleve token filter which passes the text of each token and passes it to the cld2 library.  The library determines what it thinks the language most likely is.  The ISO-639 language code replaces the token term.

In normal usage, you use this with the "single" tokenizer, so there is only one input token.  Further, you should precede it with the "to_lower" filter so that the input term is in all lower-case unicode characters.

# Building

1.  Acquire the source to cld2 in this directory.

        $ svn checkout -r 167 http://cld2.googlecode.com/svn/trunk/ cld2-read-only

2.  Build cld2

	$ ./compile_cld2.sh

3.  Put the resulting libraries somewhere your linker can find.

        $ cp *.a /usr/local/lib

4.  Run the unit tests

        $ go test -v
        === RUN TestCld2Filter
        --- PASS: TestCld2Filter (0.00 seconds)
        PASS
        ok      github.com/couchbaselabs/bleve/analysis/token_filters/cld2      0.033s
