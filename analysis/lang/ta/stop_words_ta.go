package ta

import (
	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const StopName = "stop_ta"

// this content was obtained from:
// lucene-4.7.2/analysis/common/src/resources/org/apache/lucene/analysis/
// ` was changed to ' to allow for literal string

var TamilStopWords = []byte(`
| Combined list of stop words from
| https://github.com/AshokR/TamilNLP/wiki/Stopwords
| https://cls.corpora.uni-leipzig.de/de/tam_community_2017/3.2.1_The%20Most%20Frequent%2050%20Words.html

| An Tamil stop word list. Comments begin with vertical bar. Each stop
| word is at the start of a line.

அங்கு
அங்கே
அடுத்த
அதற்கு
அதனால்
அதன்
அதிக
அதில்
அது
அதே
அதை
அந்த
அந்தக்
அந்தப்
அல்லது
அவரது
அவர்
அவர்கள்
அவள்
அவன்
அவை
அன்று
ஆகிய
ஆகியோர்
ஆகும்
ஆனால்
இங்கு
இங்கே
இடத்தில்
இடம்
இதற்கு
இதனால்
இதனை
இதன்
இதில்
இது
இதை
இந்த
இந்தக்
இந்தத்
இந்தப்
இப்போது
இரு
இருக்கும்
இருந்த
இருந்தது
இருந்து
இல்லை
இவர்
இவை
இன்னும்
உள்ள
உள்ளது
உள்ளன
உன்
எந்த
எல்லாம்
என
எனக்
எனக்கு
எனப்படும்
எனவும்
எனவே
எனினும்
எனும்
என்
என்பது
என்பதை
என்ற
என்று
என்றும்
என்ன
என்னும்
ஏன்
ஒரு
ஒரே
ஓர்
கொண்ட
கொண்டு
கொள்ள
சற்று
சில
சிறு
சேர்ந்த
தவிர
தனது
தன்
தான்
நாம்
நான்
நீ
பல
பலரும்
பல்வேறு
பற்றி
பற்றிய
பிற
பிறகு
பின்
பின்னர்
பெரும்
பேர்
போது
போல
போல்
போன்ற
மட்டுமே
மட்டும்
மற்ற
மற்றும்
மிக
மிகவும்
மீது
முதல்
முறை
மேலும்
மேல்
யார்
வந்த
வந்து
வரும்
வரை
வரையில்
விட
விட்டு
வேண்டும்
வேறு
`)

func TokenMapConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenMap, error) {
	rv := analysis.NewTokenMap()
	err := rv.LoadBytes(TamilStopWords)
	return rv, err
}

func init() {
	registry.RegisterTokenMap(StopName, TokenMapConstructor)
}
