#include "cld2_tokenizer.h"
#include "cld2-read-only/public/compact_lang_det.h"

const char* DetectLang(const char *buffer) {

	bool is_plain_text = true;
  	CLD2::CLDHints cldhints = {NULL, NULL, 0, CLD2::UNKNOWN_LANGUAGE};
  	bool allow_extended_lang = true;
  	int flags = 0;
  	CLD2::Language language3[3];
  	int percent3[3];
  	double normalized_score3[3];
  	CLD2::ResultChunkVector resultchunkvector;
  	int text_bytes;
  	bool is_reliable;

  	CLD2::Language summary_lang = CLD2::UNKNOWN_LANGUAGE;

	summary_lang = CLD2::ExtDetectLanguageSummary(buffer, 
							strlen(buffer),
							is_plain_text,
							&cldhints,
							flags,
							language3,
							percent3,
							normalized_score3,
							&resultchunkvector,
							&text_bytes,
							&is_reliable);

	return CLD2::LanguageCode(summary_lang);
}