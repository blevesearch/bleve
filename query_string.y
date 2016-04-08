%{
package bleve
import ("strconv"
        "time"
)

// Format 02-01-2015 is used for specifying date without any time. It's a
// reference layout for DD-MM-YYYY date format.
// See here for more: https://golang.org/pkg/time/#pkg-constants
var dateFormats = []string{"02-01-2006", "01-02-2006", time.RFC822,
                           time.UnixDate, time.RFC3339, time.RFC3339Nano}

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
tEQUAL tTILDE tTILDENUMBER

%type <s>                tSTRING
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
tSTRING tCOLON tGREATER tPHRASE {
    field := $1
    minInclusive := false
    phrase := $4

    for _, format := range dateFormats {
        if _, err := time.Parse(format, phrase); err == nil {
            logDebugGrammar("PHRASE - GREATER THAN %f", phrase)
            q := NewDateRangeInclusiveQuery(&phrase, nil, &minInclusive, nil).SetField(field)
            $$ = q
            break
        }
    }
}
|
tSTRING tCOLON tGREATER tEQUAL tPHRASE {
    field := $1
    minInclusive := true
    phrase := $5

    for _, format := range dateFormats {
        if _, err := time.Parse(format, phrase); err == nil {
            logDebugGrammar("PHRASE - GREATER EQUAL THAN %f", phrase)
            q := NewDateRangeInclusiveQuery(&phrase, nil, &minInclusive, nil).SetField(field)
            $$ = q
            break
        }
    }
}
|
tSTRING tCOLON tLESS tPHRASE {
    field := $1
    maxInclusive := false
    phrase := $4

    for _, format := range dateFormats {
        if _, err := time.Parse(format, phrase); err == nil {
            logDebugGrammar("PHRASE - LESS THAN %f", phrase)
            q := NewDateRangeInclusiveQuery(nil, &phrase, nil, &maxInclusive).SetField(field)
            $$ = q
            break
        }
    }
}
|
tSTRING tCOLON tLESS tEQUAL tPHRASE {
    field := $1
    maxInclusive := true
    phrase := $5

    for _, format := range dateFormats {
        if _, err := time.Parse(format, phrase); err == nil {
            logDebugGrammar("PHRASE - LESS THAN %f", phrase)
            q := NewDateRangeInclusiveQuery(nil, &phrase, nil, &maxInclusive).SetField(field)
            $$ = q
            break
        }
    }
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
