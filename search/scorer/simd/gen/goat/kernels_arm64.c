// Hand-written source for goat (https://github.com/gorse-io/goat) to
// transpile into kernels_arm64.s -- see gen.go's doc comment for the
// regeneration command. There is no per-width dispatch table to generate
// here the way bitpack's schedule needs (straight-line arithmetic, not
// bit-shuffling), so this is checked in and compiled directly rather than
// written out by a Go program.
//
// Every step matches simd's package doc comment's operand order and
// grouping exactly -- see kernels_amd64.go's sibling asm.go for the same
// sequence on the SSE2 side.
//
// bm25Asm uses vfmaq_f64 (fused multiply-add) in the two spots
// scorer_term.go's scalar docScore ends up doing the same fusion: Go's
// arm64 backend compiles docScore's `oneMinusB+b*fieldLength*invAvgDocLength`
// and `tf+k1*inner` into FMADDD on its own (confirmed via
// `go build -gcflags=-S`), a single-rounding op that differs from separate
// multiply+add in the last bit. Since docScore is the ground truth
// ScoreBulk must stay bit-identical to (MaxScore's WAND bound has to hold
// against whatever ScoreBulk actually computes), this kernel has to match
// that fusion, not avoid it -- unlike amd64, where default GOAMD64=v1 has
// no FMA3 and docScore compiles to plain MULSD/ADDSD, which is why
// kernels_amd64.go's asm.go deliberately does NOT use FMA.
//
// tf comes from vcvtq_f64_u64 (native unsigned uint64x2_t -> float64x2_t)
// followed by vsqrtq_f64 -- NEON converts unsigned freqs directly, unlike
// SSE2's signed-only CVTSQ2SD (see kernels_amd64.go's sibling asm.go), so
// there's no realistic-freq-magnitude caveat needed on this side at all.
#include <arm_neon.h>
#include <stdint.h>

// Parameter names must match ScoreBulk's Go declaration in kernels_arm64.go
// exactly -- go vet's asmdecl check cross-references the .s file's FP names
// against them. The vector locals below carry a V suffix instead, since C
// won't let a float64x2_t local reuse its originating double parameter's name.
void bm25Asm(const uint64_t *freqs, const double *norms, double idf, double k1,
             double oneMinusB, double b, double invAvgDocLength,
             double queryWeight, double *out, int64_t numPairs) {
  float64x2_t idfV = vdupq_n_f64(idf);
  float64x2_t k1V = vdupq_n_f64(k1);
  float64x2_t oneMinusBV = vdupq_n_f64(oneMinusB);
  float64x2_t bV = vdupq_n_f64(b);
  float64x2_t invAvgDocLengthV = vdupq_n_f64(invAvgDocLength);
  float64x2_t queryWeightV = vdupq_n_f64(queryWeight);
  float64x2_t ones = vdupq_n_f64(1.0);

  for (int64_t i = 0; i < numPairs; i++) {
    uint64x2_t freqv = vld1q_u64(freqs + 2 * i);
    float64x2_t tfv = vsqrtq_f64(vcvtq_f64_u64(freqv));
    float64x2_t normv = vld1q_f64(norms + 2 * i);

    float64x2_t sq = vmulq_f64(normv, normv);
    float64x2_t fieldLength = vdivq_f64(ones, sq);
    float64x2_t t1 = vmulq_f64(bV, fieldLength);
    float64x2_t inner = vfmaq_f64(oneMinusBV, t1, invAvgDocLengthV); // oneMinusB + t1*invAvgDocLength, fused
    float64x2_t denom = vfmaq_f64(tfv, k1V, inner); // tf + k1*inner, fused
    float64x2_t t4 = vmulq_f64(tfv, k1V);
    float64x2_t numer = vmulq_f64(idfV, t4);
    float64x2_t score = vdivq_f64(numer, denom);
    float64x2_t outv = vmulq_f64(score, queryWeightV);

    vst1q_f64(out + 2 * i, outv);
  }
}

void tfidfAsm(const uint64_t *freqs, const double *norms, double idf,
              double queryWeight, double *out, int64_t numPairs) {
  float64x2_t idfV = vdupq_n_f64(idf);
  float64x2_t queryWeightV = vdupq_n_f64(queryWeight);

  for (int64_t i = 0; i < numPairs; i++) {
    uint64x2_t freqv = vld1q_u64(freqs + 2 * i);
    float64x2_t tfv = vsqrtq_f64(vcvtq_f64_u64(freqv));
    float64x2_t normv = vld1q_f64(norms + 2 * i);

    float64x2_t score = vmulq_f64(tfv, normv);
    score = vmulq_f64(score, idfV);
    float64x2_t outv = vmulq_f64(score, queryWeightV);

    vst1q_f64(out + 2 * i, outv);
  }
}
