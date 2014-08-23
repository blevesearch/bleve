#!/bin/bash

SRC="cldutil cldutil_shared compact_lang_det compact_lang_det_hint_code compact_lang_det_impl debug fixunicodevalue generated_entities generated_language generated_ulscript getonescriptspan lang_script offsetmap scoreonescriptspan tote utf8statetable cld_generated_cjk_uni_prop_80 cld2_generated_cjk_compatible cld_generated_cjk_delta_bi_32 generated_distinct_bi_0 cld2_generated_quad0122 cld2_generated_deltaocta0122 cld2_generated_distinctocta0122 cld_generated_score_quad_octa_0122";
OBJ="";
for f in ${SRC}; do
  g++ -c -fPIC -O2 -m64 -o "cld2-read-only/internal/${f}.o" "cld2-read-only/internal/${f}.cc";
  OBJ="${OBJ} cld2-read-only/internal/${f}.o";
done;

ar rcs libcld2_full.a ${OBJ};
