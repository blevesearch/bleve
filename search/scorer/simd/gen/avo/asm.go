//go:build ignore

// Command asm writes kernels_amd64.s: the SSE2 scoring kernels, generated via
// avo (https://github.com/mmcloughlin/avo) instead of hand-picked Plan 9
// registers.
//
// # Regenerating
//
//	go get github.com/mmcloughlin/avo@v0.6.0   # go mod tidy drops this otherwise
//	go run asm.go -out kernels_amd64.s -stubs /dev/null -pkg simd
//
// The go:build ignore tag means `go mod tidy` never sees this file's import
// of avo, so it quietly removes avo from go.mod/go.sum between
// regenerations -- that's the correct, tidy state for bleve's actual
// consumers, but it means the go get above is a real step, not a one-time
// setup.
//
// The real kernels_amd64.go is hand-declared, not avo's own stub: avo's stub
// uses unsafe.Pointer for every argument and skips go:noescape, where the
// checked-in file matches the rest of this package's typed *uint64/*float64
// signatures. Discard the stub avo writes; only kernels_amd64.s is used
// as-is.
package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

// emitBM25 writes the BM25 kernel: two documents' scores per iteration,
// matching simd's package doc comment's operand order and grouping exactly --
// this is what keeps it bit-identical to scorer_term.go's scalar docScore.
func emitBM25() {
	TEXT("bm25Asm", NOSPLIT, "func(freqs *uint64, norms *float64, idf, k1, oneMinusB, b, invAvgDocLength, queryWeight float64, out *float64, numPairs int)")

	freqsPtr := Load(Param("freqs"), GP64())
	normsPtr := Load(Param("norms"), GP64())
	outPtr := Load(Param("out"), GP64())
	numPairs := Load(Param("numPairs"), GP64())

	idf := Load(Param("idf"), XMM())
	UNPCKLPD(idf, idf)
	k1 := Load(Param("k1"), XMM())
	UNPCKLPD(k1, k1)
	oneMinusB := Load(Param("oneMinusB"), XMM())
	UNPCKLPD(oneMinusB, oneMinusB)
	b := Load(Param("b"), XMM())
	UNPCKLPD(b, b)
	invAvgDocLength := Load(Param("invAvgDocLength"), XMM())
	UNPCKLPD(invAvgDocLength, invAvgDocLength)
	queryWeight := Load(Param("queryWeight"), XMM())
	UNPCKLPD(queryWeight, queryWeight)
	ones := XMM()
	one := GP64()
	MOVQ(U64(0x3ff0000000000000), one) // float64(1.0)'s bit pattern, in both lanes
	MOVQ(one, ones)
	UNPCKLPD(ones, ones)

	i := GP64()
	MOVQ(U64(0), i)

	Label("loop")
	CMPQ(i, numPairs)
	JGE(LabelRef("done"))

	// tf = sqrt(freq). CVTSQ2SD is a *signed* convert -- SSE2 has no unsigned
	// 64-bit int-to-double instruction -- which is exact for every freq under
	// 2^63, i.e. every real term frequency (see the package doc comment).
	freq0 := GP64()
	MOVQ(Mem{Base: freqsPtr}, freq0)
	freq1 := GP64()
	MOVQ(Mem{Base: freqsPtr, Disp: 8}, freq1)
	tfv := XMM()
	CVTSQ2SD(freq0, tfv) // tfv[low] = double(freq0)
	tfb := XMM()
	CVTSQ2SD(freq1, tfb) // tfb[low] = double(freq1)
	UNPCKLPD(tfb, tfv)   // tfv = [double(freq0), double(freq1)]
	SQRTPD(tfv, tfv)     // tfv = [sqrt(freq0), sqrt(freq1)]

	normv := XMM()
	MOVUPD(Mem{Base: normsPtr}, normv)

	sq := XMM()
	MOVUPD(normv, sq)
	MULPD(normv, sq) // sq = norm*norm

	fieldLength := XMM()
	MOVUPD(ones, fieldLength)
	DIVPD(sq, fieldLength) // fieldLength = 1/sq

	t1 := XMM()
	MOVUPD(b, t1)
	MULPD(fieldLength, t1) // t1 = b*fieldLength

	t2 := XMM()
	MOVUPD(t1, t2)
	MULPD(invAvgDocLength, t2) // t2 = t1*invAvgDocLength

	inner := XMM()
	MOVUPD(oneMinusB, inner)
	ADDPD(t2, inner) // inner = oneMinusB+t2

	t3 := XMM()
	MOVUPD(k1, t3)
	MULPD(inner, t3) // t3 = k1*inner

	denom := XMM()
	MOVUPD(tfv, denom)
	ADDPD(t3, denom) // denom = tf+t3

	t4 := XMM()
	MOVUPD(tfv, t4)
	MULPD(k1, t4) // t4 = tf*k1

	numer := XMM()
	MOVUPD(idf, numer)
	MULPD(t4, numer) // numer = idf*t4

	score := XMM()
	MOVUPD(numer, score)
	DIVPD(denom, score) // score = numer/denom

	outv := XMM()
	MOVUPD(score, outv)
	MULPD(queryWeight, outv) // out = score*queryWeight

	MOVUPD(outv, Mem{Base: outPtr})

	ADDQ(Imm(16), freqsPtr)
	ADDQ(Imm(16), normsPtr)
	ADDQ(Imm(16), outPtr)
	INCQ(i)
	JMP(LabelRef("loop"))

	Label("done")
	RET()
}

// emitTFIDF writes the plain tf-idf kernel: out = tf*norms*idf*queryWeight,
// two documents per iteration.
func emitTFIDF() {
	TEXT("tfidfAsm", NOSPLIT, "func(freqs *uint64, norms *float64, idf, queryWeight float64, out *float64, numPairs int)")

	freqsPtr := Load(Param("freqs"), GP64())
	normsPtr := Load(Param("norms"), GP64())
	outPtr := Load(Param("out"), GP64())
	numPairs := Load(Param("numPairs"), GP64())

	idf := Load(Param("idf"), XMM())
	UNPCKLPD(idf, idf)
	queryWeight := Load(Param("queryWeight"), XMM())
	UNPCKLPD(queryWeight, queryWeight)

	i := GP64()
	MOVQ(U64(0), i)

	Label("loop")
	CMPQ(i, numPairs)
	JGE(LabelRef("done"))

	freq0 := GP64()
	MOVQ(Mem{Base: freqsPtr}, freq0)
	freq1 := GP64()
	MOVQ(Mem{Base: freqsPtr, Disp: 8}, freq1)
	tfv := XMM()
	CVTSQ2SD(freq0, tfv)
	tfb := XMM()
	CVTSQ2SD(freq1, tfb)
	UNPCKLPD(tfb, tfv)
	SQRTPD(tfv, tfv)

	normv := XMM()
	MOVUPD(Mem{Base: normsPtr}, normv)

	score := XMM()
	MOVUPD(tfv, score)
	MULPD(normv, score) // score = tf*norms
	MULPD(idf, score)   // score = score*idf

	outv := XMM()
	MOVUPD(score, outv)
	MULPD(queryWeight, outv) // out = score*queryWeight

	MOVUPD(outv, Mem{Base: outPtr})

	ADDQ(Imm(16), freqsPtr)
	ADDQ(Imm(16), normsPtr)
	ADDQ(Imm(16), outPtr)
	INCQ(i)
	JMP(LabelRef("loop"))

	Label("done")
	RET()
}

func main() {
	emitBM25()
	emitTFIDF()
	Generate()
}
