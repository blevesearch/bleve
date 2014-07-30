//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
#include <cstddef>
#include <string.h>
#include <stdio.h>
#include <string>
#include "cld2_filter.h"
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