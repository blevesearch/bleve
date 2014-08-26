%{
package bleve
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

%token STRING PHRASE PLUS MINUS COLON BOOST LPAREN RPAREN NUMBER STRING

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
	q := NewMatchQuery(str)
	if parsingMust {
		parsingMustList.AddQuery(q)
		parsingMust = false
	} else if parsingMustNot {
		parsingMustNotList.AddQuery(q)
		parsingMustNot = false
	} else {
		parsingShouldList.AddQuery(q)
	}
	parsingLastQuery = q
}
|
PHRASE {
	phrase := $1.s
	logDebugGrammar("PHRASE - %s", phrase)
	q := NewMatchPhraseQuery(phrase)
	if parsingMust {
		parsingMustList.AddQuery(q)
		parsingMust = false
	} else if parsingMustNot {
		parsingMustNotList.AddQuery(q)
		parsingMustNot = false
	} else {
		parsingShouldList.AddQuery(q)
	}
	parsingLastQuery = q
}
|
STRING COLON STRING {
	field := $1.s
	str := $3.s
	logDebugGrammar("FIELD - %s STRING - %s", field, str)
	q := NewMatchQuery(str).SetField(field)
	if parsingMust {
		parsingMustList.AddQuery(q)
		parsingMust = false
	} else if parsingMustNot {
		parsingMustNotList.AddQuery(q)
		parsingMustNot = false
	} else {
		parsingShouldList.AddQuery(q)
	}
	parsingLastQuery = q
}
|
STRING COLON PHRASE {
	field := $1.s
	phrase := $3.s
	logDebugGrammar("FIELD - %s PHRASE - %s", field, phrase)
	q := NewMatchPhraseQuery(phrase).SetField(field)
	if parsingMust {
		parsingMustList.AddQuery(q)
		parsingMust = false
	} else if parsingMustNot {
		parsingMustNotList.AddQuery(q)
		parsingMustNot = false
	} else {
		parsingShouldList.AddQuery(q)
	}
	parsingLastQuery = q
};


searchBoost:
BOOST NUMBER {
	boost := $2.f
	if parsingLastQuery != nil {
		switch parsingLastQuery := parsingLastQuery.(type) {
		case *MatchQuery:
			parsingLastQuery.SetBoost(boost)
		case *MatchPhraseQuery:
			parsingLastQuery.SetBoost(boost)
		}
	}
	logDebugGrammar("BOOST %f", boost)
};

searchSuffix:
/* empty */ {
	
}
|
searchBoost {
	
};