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

%token tSTRING tPHRASE tPLUS tMINUS tCOLON tBOOST tLPAREN tRPAREN tNUMBER tSTRING tGREATER tLESS tEQUAL

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
tPLUS {
	logDebugGrammar("PLUS")
	parsingMust = true
}
|
tMINUS {
	logDebugGrammar("MINUS")
	parsingMustNot = true
};

searchBase:
tSTRING {
	str := $1.s
	logDebugGrammar("STRING - %s", str)
	q := NewMatchQuery(str)
	addQueryToList(q)
}
|
tNUMBER {
	str := $1.s
	logDebugGrammar("STRING - %s", str)
	q := NewMatchQuery(str)
	addQueryToList(q)
}
|
tPHRASE {
	phrase := $1.s
	logDebugGrammar("PHRASE - %s", phrase)
	q := NewMatchPhraseQuery(phrase)
	addQueryToList(q)
}
|
tSTRING tCOLON tSTRING {
	field := $1.s
	str := $3.s
	logDebugGrammar("FIELD - %s STRING - %s", field, str)
	q := NewMatchQuery(str).SetField(field)
	addQueryToList(q)
}
|
tSTRING tCOLON tNUMBER {
	field := $1.s
	str := $3.s
	logDebugGrammar("FIELD - %s STRING - %s", field, str)
	q := NewMatchQuery(str).SetField(field)
	addQueryToList(q)
}
|
tSTRING tCOLON tPHRASE {
	field := $1.s
	phrase := $3.s
	logDebugGrammar("FIELD - %s PHRASE - %s", field, phrase)
	q := NewMatchPhraseQuery(phrase).SetField(field)
	addQueryToList(q)
}
|
tSTRING tCOLON tGREATER tNUMBER {
	field := $1.s
	min := $4.f
	minInclusive := false
	logDebugGrammar("FIELD - GREATER THAN %f", min)
	q := NewNumericRangeInclusiveQuery(&min, nil, &minInclusive, nil).SetField(field)
	addQueryToList(q)
}
|
tSTRING tCOLON tGREATER tEQUAL tNUMBER {
	field := $1.s
	min := $5.f
	minInclusive := true
	logDebugGrammar("FIELD - GREATER THAN OR EQUAL %f", min)
	q := NewNumericRangeInclusiveQuery(&min, nil, &minInclusive, nil).SetField(field)
	addQueryToList(q)
}
|
tSTRING tCOLON tLESS tNUMBER {
	field := $1.s
	max := $4.f
	maxInclusive := false
	logDebugGrammar("FIELD - LESS THAN %f", max)
	q := NewNumericRangeInclusiveQuery(nil, &max, nil, &maxInclusive).SetField(field)
	addQueryToList(q)
}
|
tSTRING tCOLON tLESS tEQUAL tNUMBER {
	field := $1.s
	max := $5.f
	maxInclusive := true
	logDebugGrammar("FIELD - LESS THAN OR EQUAL %f", max)
	q := NewNumericRangeInclusiveQuery(nil, &max, nil, &maxInclusive).SetField(field)
	addQueryToList(q)
};


searchBoost:
tBOOST tNUMBER {
	boost := $2.f
	if parsingLastQuery != nil {
		parsingLastQuery.SetBoost(boost)
	}
	logDebugGrammar("BOOST %f", boost)
};

searchSuffix:
/* empty */ {
	
}
|
searchBoost {
	
};