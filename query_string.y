%{
package bleve
import "strconv"

func logDebugGrammar(format string, v ...interface{}) {
	if debugParser {
    	logger.Printf(format, v...)
    }
}
%}

%union {
s string
n int
f float64
q Query}

%token tSTRING tPHRASE tPLUS tMINUS tCOLON tBOOST tLPAREN tRPAREN tNUMBER tSTRING tGREATER tLESS
tEQUAL tTILDE tTILDENUMBER tREGEXP tWILD

%type <s>                tSTRING
%type <s>                tWILD
%type <s>                tREGEXP
%type <s>                tPHRASE
%type <s>                tNUMBER
%type <s>                tTILDENUMBER
%type <q>                searchBase
%type <f>                searchSuffix
%type <n>                searchPrefix
%type <n>                searchMustMustNot
%type <f>                searchBoost

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
	query := $2
	query.SetBoost($3)
	switch($1) {
		case queryShould:
			yylex.(*lexerWrapper).query.AddShould(query)
		case queryMust:
			yylex.(*lexerWrapper).query.AddMust(query)
		case queryMustNot:
			yylex.(*lexerWrapper).query.AddMustNot(query)
	}
};


searchPrefix:
/* empty */ {
	$$ = queryShould
}
|
searchMustMustNot {
	$$ = $1
}
;

searchMustMustNot:
tPLUS {
	logDebugGrammar("PLUS")
	$$ = queryMust
}
|
tMINUS {
	logDebugGrammar("MINUS")
	$$ = queryMustNot
};

searchBase:
tSTRING {
	str := $1
	logDebugGrammar("STRING - %s", str)
	q := NewMatchQuery(str)
	$$ = q
}
|
tREGEXP {
	str := $1
	logDebugGrammar("REGEXP - %s", str)
	q := NewRegexpQuery(str)
	$$ = q
}
|
tWILD {
	str := $1
	logDebugGrammar("WILDCARD - %s", str)
	q := NewWildcardQuery(str)
	$$ = q
}
|
tSTRING tTILDE {
	str := $1
	logDebugGrammar("FUZZY STRING - %s", str)
	q := NewMatchQuery(str)
	q.SetFuzziness(1)
	$$ = q
}
|
tSTRING tCOLON tSTRING tTILDE {
	field := $1
	str := $3
	logDebugGrammar("FIELD - %s FUZZY STRING - %s", field, str)
	q := NewMatchQuery(str)
	q.SetFuzziness(1)
	q.SetField(field)
	$$ = q
}
|
tSTRING tTILDENUMBER {
	str := $1
	fuzziness, _ := strconv.ParseFloat($2, 64)
	logDebugGrammar("FUZZY STRING - %s", str)
	q := NewMatchQuery(str)
	q.SetFuzziness(int(fuzziness))
	$$ = q
}
|
tSTRING tCOLON tSTRING tTILDENUMBER {
	field := $1
	str := $3
	fuzziness, _ := strconv.ParseFloat($4, 64)
	logDebugGrammar("FIELD - %s FUZZY-%f STRING - %s", field, fuzziness, str)
	q := NewMatchQuery(str)
	q.SetFuzziness(int(fuzziness))
	q.SetField(field)
	$$ = q
}
|
tSTRING tCOLON tREGEXP {
	field := $1
	str := $3
	logDebugGrammar("FIELD - %s REGEXP - %s", field, str)
	q := NewRegexpQuery(str)
	q.SetField(field)
	$$ = q
}
|
tSTRING tCOLON tWILD {
	field := $1
	str := $3
	logDebugGrammar("FIELD - %s WILD - %s", field, str)
	q := NewWildcardQuery(str)
	q.SetField(field)
	$$ = q
}
|
tNUMBER {
	str := $1
	logDebugGrammar("STRING - %s", str)
	q := NewMatchQuery(str)
	$$ = q
}
|
tPHRASE {
	phrase := $1
	logDebugGrammar("PHRASE - %s", phrase)
	q := NewMatchPhraseQuery(phrase)
	$$ = q
}
|
tSTRING tCOLON tSTRING {
	field := $1
	str := $3
	logDebugGrammar("FIELD - %s STRING - %s", field, str)
	q := NewMatchQuery(str).SetField(field)
	$$ = q
}
|
tSTRING tCOLON tNUMBER {
	field := $1
	str := $3
	logDebugGrammar("FIELD - %s STRING - %s", field, str)
	q := NewMatchQuery(str).SetField(field)
	$$ = q
}
|
tSTRING tCOLON tPHRASE {
	field := $1
	phrase := $3
	logDebugGrammar("FIELD - %s PHRASE - %s", field, phrase)
	q := NewMatchPhraseQuery(phrase).SetField(field)
	$$ = q
}
|
tSTRING tCOLON tGREATER tNUMBER {
	field := $1
	min, _ := strconv.ParseFloat($4, 64)
	minInclusive := false
	logDebugGrammar("FIELD - GREATER THAN %f", min)
	q := NewNumericRangeInclusiveQuery(&min, nil, &minInclusive, nil).SetField(field)
	$$ = q
}
|
tSTRING tCOLON tGREATER tEQUAL tNUMBER {
	field := $1
	min, _ := strconv.ParseFloat($5, 64)
	minInclusive := true
	logDebugGrammar("FIELD - GREATER THAN OR EQUAL %f", min)
	q := NewNumericRangeInclusiveQuery(&min, nil, &minInclusive, nil).SetField(field)
	$$ = q
}
|
tSTRING tCOLON tLESS tNUMBER {
	field := $1
	max, _ := strconv.ParseFloat($4, 64)
	maxInclusive := false
	logDebugGrammar("FIELD - LESS THAN %f", max)
	q := NewNumericRangeInclusiveQuery(nil, &max, nil, &maxInclusive).SetField(field)
	$$ = q
}
|
tSTRING tCOLON tLESS tEQUAL tNUMBER {
	field := $1
	max, _ := strconv.ParseFloat($5, 64)
	maxInclusive := true
	logDebugGrammar("FIELD - LESS THAN OR EQUAL %f", max)
	q := NewNumericRangeInclusiveQuery(nil, &max, nil, &maxInclusive).SetField(field)
	$$ = q
};

searchBoost:
tBOOST tNUMBER {
	boost, _ := strconv.ParseFloat($2, 64)
	$$ = boost
	logDebugGrammar("BOOST %f", boost)
};

searchSuffix:
/* empty */ {
	$$ = 1.0
}
|
searchBoost {

};
