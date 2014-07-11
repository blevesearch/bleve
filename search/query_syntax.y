%{
package search
import "log"

func logDebugGrammar(format string, v ...interface{}) {
	if debugParser {
    	log.Printf(format, v...)
    }
}
%}

%union { 
s string 
n int
f float64}

%token STRING PHRASE PLUS MINUS COLON BOOST LPAREN RPAREN INT STRING

%%

input: 
searchParts {
	logDebugGrammar("INPUT")
};

searchParts:
searchPart searchParts {
	logDebugGrammar("SEARCH PARTS")
}
|
searchPart {
	logDebugGrammar("SEARCH PART")
};

searchPart:
searchPrefix searchBase searchSuffix {
	
};


searchPrefix:
/* empty */ {
}
|
searchMustMustNot {
	
}
;

searchMustMustNot:
PLUS {
	logDebugGrammar("PLUS")
	parsingMust = true
}
|
MINUS {
	logDebugGrammar("MINUS")
	parsingMustNot = true
};

searchBase:
STRING {
	str := $1.s
	logDebugGrammar("STRING - %s", str)
	q := &MatchQuery{
		Match: str,
		Field: parsingDefaultField,
		BoostVal: 1.0,
		Explain: true,
	}
	if parsingMapping[parsingDefaultField] != nil {
		q.Analyzer = parsingMapping[parsingDefaultField].Analyzer
	}
	if parsingMust {
		parsingMustList.Terms = append(parsingMustList.Terms, q)
		parsingMust = false
	} else if parsingMustNot {
		parsingMustNotList.Terms = append(parsingMustNotList.Terms, q)
		parsingMustNot = false
	} else {
		parsingShouldList.Terms = append(parsingShouldList.Terms, q)
	}
}
|
PHRASE {
	phrase := $1.s
	logDebugGrammar("PHRASE - %s", phrase)
	q := &MatchPhraseQuery{
		MatchPhrase: phrase,
		Field: parsingDefaultField,
		BoostVal: 1.0,
		Explain: true,
	}
	if parsingMapping[parsingDefaultField] != nil {
		q.Analyzer = parsingMapping[parsingDefaultField].Analyzer
	}
	if parsingMust {
		parsingMustList.Terms = append(parsingMustList.Terms, q)
		parsingMust = false
	} else if parsingMustNot {
		parsingMustNotList.Terms = append(parsingMustNotList.Terms, q)
		parsingMustNot = false
	} else {
		parsingShouldList.Terms = append(parsingShouldList.Terms, q)
	}
}
|
STRING COLON STRING {
	field := $1.s
	str := $3.s
	logDebugGrammar("FIELD - %s STRING - %s", field, str)
	q := &MatchQuery{
		Match: str,
		Field: field,
		BoostVal: 1.0,
		Explain: true,
	}
	if parsingMapping[field] != nil {
		q.Analyzer = parsingMapping[field].Analyzer
	}
	if parsingMust {
		parsingMustList.Terms = append(parsingMustList.Terms, q)
		parsingMust = false
	} else if parsingMustNot {
		parsingMustNotList.Terms = append(parsingMustNotList.Terms, q)
		parsingMustNot = false
	} else {
		parsingShouldList.Terms = append(parsingShouldList.Terms, q)
	}
}
|
STRING COLON PHRASE {
	field := $1.s
	phrase := $3.s
	logDebugGrammar("FIELD - %s PHRASE - %s", field, phrase)
	q := &MatchPhraseQuery{
		MatchPhrase: phrase,
		Field: field,
		BoostVal: 1.0,
		Explain: true,
	}
	if parsingMapping[field] != nil {
		q.Analyzer = parsingMapping[field].Analyzer
	}
	if parsingMust {
		parsingMustList.Terms = append(parsingMustList.Terms, q)
		parsingMust = false
	} else if parsingMustNot {
		parsingMustNotList.Terms = append(parsingMustNotList.Terms, q)
		parsingMustNot = false
	} else {
		parsingShouldList.Terms = append(parsingShouldList.Terms, q)
	}
};


searchBoost:
BOOST INT {
	boost := $1.n
	logDebugGrammar("BOOST %d", boost)
}

searchSuffix:
/* empty */ {
	
}
|
searchBoost {
	
};