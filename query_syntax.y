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
	q := NewMatchQuery(str).SetField(parsingDefaultField)
	if parsingMust {
		parsingMustList.AddQuery(q)
		parsingMust = false
	} else if parsingMustNot {
		parsingMustNotList.AddQuery(q)
		parsingMustNot = false
	} else {
		parsingShouldList.AddQuery(q)
	}
}
|
PHRASE {
	phrase := $1.s
	logDebugGrammar("PHRASE - %s", phrase)
	q := NewMatchPhraseQuery(phrase).SetField(parsingDefaultField)
	if parsingMust {
		parsingMustList.AddQuery(q)
		parsingMust = false
	} else if parsingMustNot {
		parsingMustNotList.AddQuery(q)
		parsingMustNot = false
	} else {
		parsingShouldList.AddQuery(q)
	}
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