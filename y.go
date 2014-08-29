package bleve

import __yyfmt__ "fmt"

//line query_string.y:2
import "log"

func logDebugGrammar(format string, v ...interface{}) {
	if debugParser {
		log.Printf(format, v...)
	}
}

//line query_string.y:12
type yySymType struct {
	yys int
	s   string
	n   int
	f   float64
}

const tSTRING = 57346
const tPHRASE = 57347
const tPLUS = 57348
const tMINUS = 57349
const tCOLON = 57350
const tBOOST = 57351
const tLPAREN = 57352
const tRPAREN = 57353
const tNUMBER = 57354
const tGREATER = 57355
const tLESS = 57356
const tEQUAL = 57357

var yyToknames = []string{
	"tSTRING",
	"tPHRASE",
	"tPLUS",
	"tMINUS",
	"tCOLON",
	"tBOOST",
	"tLPAREN",
	"tRPAREN",
	"tNUMBER",
	"tGREATER",
	"tLESS",
	"tEQUAL",
}
var yyStatenames = []string{}

const yyEofCode = 1
const yyErrCode = 2
const yyMaxDepth = 200

//line yacctab:1
var yyExca = []int{
	-1, 1,
	1, -1,
	-2, 0,
	-1, 3,
	1, 3,
	-2, 5,
}

const yyNprod = 20
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 26

var yyAct = []int{

	17, 18, 23, 21, 26, 24, 22, 25, 15, 19,
	20, 16, 14, 6, 7, 10, 11, 2, 13, 5,
	12, 8, 9, 4, 3, 1,
}
var yyPact = []int{

	7, -1000, -1000, 7, 11, -1000, -1000, -1000, -1000, 3,
	0, -1000, -1000, -1000, -1, -4, -1000, -1000, -1000, -9,
	-10, -1000, -5, -1000, -8, -1000, -1000,
}
var yyPgo = []int{

	0, 25, 17, 24, 23, 22, 20, 19, 18,
}
var yyR1 = []int{

	0, 1, 2, 2, 3, 4, 4, 7, 7, 5,
	5, 5, 5, 5, 5, 5, 5, 8, 6, 6,
}
var yyR2 = []int{

	0, 1, 2, 1, 3, 0, 1, 1, 1, 1,
	1, 3, 3, 4, 5, 4, 5, 2, 0, 1,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -7, 6, 7, -2, -5,
	4, 5, -6, -8, 9, 8, 12, 4, 5, 13,
	14, 12, 15, 12, 15, 12, 12,
}
var yyDef = []int{

	5, -2, 1, -2, 0, 6, 7, 8, 2, 18,
	9, 10, 4, 19, 0, 0, 17, 11, 12, 0,
	0, 13, 0, 15, 0, 14, 16,
}
var yyTok1 = []int{

	1,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15,
}
var yyTok3 = []int{
	0,
}

//line yaccpar:1

/*	parser for yacc output	*/

var yyDebug = 0

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

const yyFlag = -1000

func yyTokname(c int) string {
	// 4 is TOKSTART above
	if c >= 4 && c-4 < len(yyToknames) {
		if yyToknames[c-4] != "" {
			return yyToknames[c-4]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yylex1(lex yyLexer, lval *yySymType) int {
	c := 0
	char := lex.Lex(lval)
	if char <= 0 {
		c = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		c = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			c = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		c = yyTok3[i+0]
		if c == char {
			c = yyTok3[i+1]
			goto out
		}
	}

out:
	if c == 0 {
		c = yyTok2[1] /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(c), uint(char))
	}
	return c
}

func yyParse(yylex yyLexer) int {
	var yyn int
	var yylval yySymType
	var yyVAL yySymType
	yyS := make([]yySymType, yyMaxDepth)

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yychar := -1
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yychar), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = yyPact[yystate]
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yychar < 0 {
		yychar = yylex1(yylex, &yylval)
	}
	yyn += yychar
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yychar { /* valid shift */
		yychar = -1
		yyVAL = yylval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = yyDef[yystate]
	if yyn == -2 {
		if yychar < 0 {
			yychar = yylex1(yylex, &yylval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && yyExca[xi+1] == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = yyExca[xi+0]
			if yyn < 0 || yyn == yychar {
				break
			}
		}
		yyn = yyExca[xi+1]
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error("syntax error")
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yychar))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = yyPact[yyS[yyp].yys] + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = yyAct[yyn] /* simulate a shift of "error" */
					if yyChk[yystate] == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yychar))
			}
			if yychar == yyEofCode {
				goto ret1
			}
			yychar = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= yyR2[yyn]
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = yyR1[yyn]
	yyg := yyPgo[yyn]
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = yyAct[yyg]
	} else {
		yystate = yyAct[yyj]
		if yyChk[yystate] != -yyn {
			yystate = yyAct[yyg]
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 1:
		//line query_string.y:22
		{
			logDebugGrammar("INPUT")
		}
	case 2:
		//line query_string.y:27
		{
			logDebugGrammar("SEARCH PARTS")
		}
	case 3:
		//line query_string.y:31
		{
			logDebugGrammar("SEARCH PART")
		}
	case 4:
		//line query_string.y:36
		{

		}
	case 5:
		//line query_string.y:42
		{
		}
	case 6:
		//line query_string.y:45
		{

		}
	case 7:
		//line query_string.y:51
		{
			logDebugGrammar("PLUS")
			parsingMust = true
		}
	case 8:
		//line query_string.y:56
		{
			logDebugGrammar("MINUS")
			parsingMustNot = true
		}
	case 9:
		//line query_string.y:62
		{
			str := yyS[yypt-0].s
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
	case 10:
		//line query_string.y:78
		{
			phrase := yyS[yypt-0].s
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
	case 11:
		//line query_string.y:94
		{
			field := yyS[yypt-2].s
			str := yyS[yypt-0].s
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
	case 12:
		//line query_string.y:111
		{
			field := yyS[yypt-2].s
			phrase := yyS[yypt-0].s
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
		}
	case 13:
		//line query_string.y:128
		{
			field := yyS[yypt-3].s
			min := yyS[yypt-0].f
			minInclusive := false
			logDebugGrammar("FIELD - GREATER THAN %f", min)
			q := NewNumericRangeInclusiveQuery(&min, nil, &minInclusive, nil).SetField(field)
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
	case 14:
		//line query_string.y:146
		{
			field := yyS[yypt-4].s
			min := yyS[yypt-0].f
			minInclusive := true
			logDebugGrammar("FIELD - GREATER THAN OR EQUAL %f", min)
			q := NewNumericRangeInclusiveQuery(&min, nil, &minInclusive, nil).SetField(field)
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
	case 15:
		//line query_string.y:164
		{
			field := yyS[yypt-3].s
			max := yyS[yypt-0].f
			maxInclusive := false
			logDebugGrammar("FIELD - LESS THAN %f", max)
			q := NewNumericRangeInclusiveQuery(nil, &max, nil, &maxInclusive).SetField(field)
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
	case 16:
		//line query_string.y:182
		{
			field := yyS[yypt-4].s
			max := yyS[yypt-0].f
			maxInclusive := true
			logDebugGrammar("FIELD - LESS THAN OR EQUAL %f", max)
			q := NewNumericRangeInclusiveQuery(nil, &max, nil, &maxInclusive).SetField(field)
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
	case 17:
		//line query_string.y:202
		{
			boost := yyS[yypt-0].f
			if parsingLastQuery != nil {
				switch parsingLastQuery := parsingLastQuery.(type) {
				case *MatchQuery:
					parsingLastQuery.SetBoost(boost)
				case *MatchPhraseQuery:
					parsingLastQuery.SetBoost(boost)
				}
			}
			logDebugGrammar("BOOST %f", boost)
		}
	case 18:
		//line query_string.y:216
		{

		}
	case 19:
		//line query_string.y:220
		{

		}
	}
	goto yystack /* stack new state and value */
}
