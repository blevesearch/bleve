package bleve

import __yyfmt__ "fmt"

//line query_string.y:2
import "log"
import "strconv"

func logDebugGrammar(format string, v ...interface{}) {
	if debugParser {
		log.Printf(format, v...)
	}
}

//line query_string.y:13
type yySymType struct {
	yys int
	s   string
	n   int
	f   float64
	q   Query
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
const tTILDE = 57358
const tTILDENUMBER = 57359

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
	"tTILDE",
	"tTILDENUMBER",
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

const yyNprod = 24
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 30

var yyAct = []int{

	18, 27, 20, 22, 28, 30, 10, 12, 16, 17,
	21, 23, 24, 25, 11, 29, 26, 19, 15, 6,
	7, 2, 3, 1, 14, 8, 5, 4, 13, 9,
}
var yyPact = []int{

	13, -1000, -1000, 13, 2, -1000, -1000, -1000, -1000, 9,
	-8, -1000, -1000, -1000, -1000, 5, -1000, -1000, -2, -1000,
	-1000, -1000, -1000, 1, -11, -1000, 3, -1000, -7, -1000,
	-1000,
}
var yyPgo = []int{

	0, 29, 28, 27, 26, 24, 23, 21, 22,
}
var yyR1 = []int{

	0, 6, 7, 7, 8, 3, 3, 4, 4, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 5, 2, 2,
}
var yyR2 = []int{

	0, 1, 2, 1, 3, 0, 1, 1, 1, 1,
	2, 2, 1, 1, 3, 3, 3, 4, 5, 4,
	5, 2, 0, 1,
}
var yyChk = []int{

	-1000, -6, -7, -8, -3, -4, 6, 7, -7, -1,
	4, 12, 5, -2, -5, 9, 16, 17, 8, 12,
	4, 12, 5, 13, 14, 12, 15, 12, 15, 12,
	12,
}
var yyDef = []int{

	5, -2, 1, -2, 0, 6, 7, 8, 2, 22,
	9, 12, 13, 4, 23, 0, 10, 11, 0, 21,
	14, 15, 16, 0, 0, 17, 0, 19, 0, 18,
	20,
}
var yyTok1 = []int{

	1,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17,
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
		//line query_string.y:35
		{
			logDebugGrammar("INPUT")
		}
	case 2:
		//line query_string.y:40
		{
			logDebugGrammar("SEARCH PARTS")
		}
	case 3:
		//line query_string.y:44
		{
			logDebugGrammar("SEARCH PART")
		}
	case 4:
		//line query_string.y:49
		{
			query := yyS[yypt-1].q
			query.SetBoost(yyS[yypt-0].f)
			switch yyS[yypt-2].n {
			case queryShould:
				yylex.(*lexerWrapper).query.AddShould(query)
			case queryMust:
				yylex.(*lexerWrapper).query.AddMust(query)
			case queryMustNot:
				yylex.(*lexerWrapper).query.AddMustNot(query)
			}
		}
	case 5:
		//line query_string.y:64
		{
			yyVAL.n = queryShould
		}
	case 6:
		//line query_string.y:68
		{
			yyVAL.n = yyS[yypt-0].n
		}
	case 7:
		//line query_string.y:74
		{
			logDebugGrammar("PLUS")
			yyVAL.n = queryMust
		}
	case 8:
		//line query_string.y:79
		{
			logDebugGrammar("MINUS")
			yyVAL.n = queryMustNot
		}
	case 9:
		//line query_string.y:85
		{
			str := yyS[yypt-0].s
			logDebugGrammar("STRING - %s", str)
			q := NewMatchQuery(str)
			yyVAL.q = q
		}
	case 10:
		//line query_string.y:92
		{
			str := yyS[yypt-1].s
			logDebugGrammar("STRING - %s", str)
			q := NewMatchQuery(str)
			q.SetFuzziness(1)
			yyVAL.q = q
		}
	case 11:
		//line query_string.y:100
		{
			str := yyS[yypt-1].s
			fuzziness, _ := strconv.ParseFloat(yyS[yypt-0].s, 64)
			logDebugGrammar("STRING - %s", str)
			q := NewMatchQuery(str)
			q.SetFuzziness(int(fuzziness))
			yyVAL.q = q
		}
	case 12:
		//line query_string.y:109
		{
			str := yyS[yypt-0].s
			logDebugGrammar("STRING - %s", str)
			q := NewMatchQuery(str)
			yyVAL.q = q
		}
	case 13:
		//line query_string.y:116
		{
			phrase := yyS[yypt-0].s
			logDebugGrammar("PHRASE - %s", phrase)
			q := NewMatchPhraseQuery(phrase)
			yyVAL.q = q
		}
	case 14:
		//line query_string.y:123
		{
			field := yyS[yypt-2].s
			str := yyS[yypt-0].s
			logDebugGrammar("FIELD - %s STRING - %s", field, str)
			q := NewMatchQuery(str).SetField(field)
			yyVAL.q = q
		}
	case 15:
		//line query_string.y:131
		{
			field := yyS[yypt-2].s
			str := yyS[yypt-0].s
			logDebugGrammar("FIELD - %s STRING - %s", field, str)
			q := NewMatchQuery(str).SetField(field)
			yyVAL.q = q
		}
	case 16:
		//line query_string.y:139
		{
			field := yyS[yypt-2].s
			phrase := yyS[yypt-0].s
			logDebugGrammar("FIELD - %s PHRASE - %s", field, phrase)
			q := NewMatchPhraseQuery(phrase).SetField(field)
			yyVAL.q = q
		}
	case 17:
		//line query_string.y:147
		{
			field := yyS[yypt-3].s
			min, _ := strconv.ParseFloat(yyS[yypt-0].s, 64)
			minInclusive := false
			logDebugGrammar("FIELD - GREATER THAN %f", min)
			q := NewNumericRangeInclusiveQuery(&min, nil, &minInclusive, nil).SetField(field)
			yyVAL.q = q
		}
	case 18:
		//line query_string.y:156
		{
			field := yyS[yypt-4].s
			min, _ := strconv.ParseFloat(yyS[yypt-0].s, 64)
			minInclusive := true
			logDebugGrammar("FIELD - GREATER THAN OR EQUAL %f", min)
			q := NewNumericRangeInclusiveQuery(&min, nil, &minInclusive, nil).SetField(field)
			yyVAL.q = q
		}
	case 19:
		//line query_string.y:165
		{
			field := yyS[yypt-3].s
			max, _ := strconv.ParseFloat(yyS[yypt-0].s, 64)
			maxInclusive := false
			logDebugGrammar("FIELD - LESS THAN %f", max)
			q := NewNumericRangeInclusiveQuery(nil, &max, nil, &maxInclusive).SetField(field)
			yyVAL.q = q
		}
	case 20:
		//line query_string.y:174
		{
			field := yyS[yypt-4].s
			max, _ := strconv.ParseFloat(yyS[yypt-0].s, 64)
			maxInclusive := true
			logDebugGrammar("FIELD - LESS THAN OR EQUAL %f", max)
			q := NewNumericRangeInclusiveQuery(nil, &max, nil, &maxInclusive).SetField(field)
			yyVAL.q = q
		}
	case 21:
		//line query_string.y:184
		{
			boost, _ := strconv.ParseFloat(yyS[yypt-0].s, 64)
			yyVAL.f = boost
			logDebugGrammar("BOOST %f", boost)
		}
	case 22:
		//line query_string.y:191
		{
			yyVAL.f = 1.0
		}
	case 23:
		//line query_string.y:195
		{

		}
	}
	goto yystack /* stack new state and value */
}
