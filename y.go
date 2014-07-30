//line query_syntax.y:2
package bleve

import __yyfmt__ "fmt"

//line query_syntax.y:2
import "log"

func logDebugGrammar(format string, v ...interface{}) {
	if debugParser {
		log.Printf(format, v...)
	}
}

//line query_syntax.y:12
type yySymType struct {
	yys int
	s   string
	n   int
	f   float64
}

const STRING = 57346
const PHRASE = 57347
const PLUS = 57348
const MINUS = 57349
const COLON = 57350
const BOOST = 57351
const LPAREN = 57352
const RPAREN = 57353
const INT = 57354

var yyToknames = []string{
	"STRING",
	"PHRASE",
	"PLUS",
	"MINUS",
	"COLON",
	"BOOST",
	"LPAREN",
	"RPAREN",
	"INT",
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

const yyNprod = 16
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 18

var yyAct = []int{

	16, 14, 15, 6, 7, 17, 18, 10, 11, 2,
	13, 5, 12, 8, 9, 4, 3, 1,
}
var yyPact = []int{

	-3, -1000, -1000, -3, 3, -1000, -1000, -1000, -1000, -8,
	-6, -1000, -1000, -1000, -12, 1, -1000, -1000, -1000,
}
var yyPgo = []int{

	0, 17, 9, 16, 15, 14, 12, 11, 10,
}
var yyR1 = []int{

	0, 1, 2, 2, 3, 4, 4, 7, 7, 5,
	5, 5, 5, 8, 6, 6,
}
var yyR2 = []int{

	0, 1, 2, 1, 3, 0, 1, 1, 1, 1,
	1, 3, 3, 2, 0, 1,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -7, 6, 7, -2, -5,
	4, 5, -6, -8, 9, 8, 12, 4, 5,
}
var yyDef = []int{

	5, -2, 1, -2, 0, 6, 7, 8, 2, 14,
	9, 10, 4, 15, 0, 0, 13, 11, 12,
}
var yyTok1 = []int{

	1,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12,
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
		//line query_syntax.y:22
		{
			logDebugGrammar("INPUT")
		}
	case 2:
		//line query_syntax.y:27
		{
			logDebugGrammar("SEARCH PARTS")
		}
	case 3:
		//line query_syntax.y:31
		{
			logDebugGrammar("SEARCH PART")
		}
	case 4:
		//line query_syntax.y:36
		{

		}
	case 5:
		//line query_syntax.y:42
		{
		}
	case 6:
		//line query_syntax.y:45
		{

		}
	case 7:
		//line query_syntax.y:51
		{
			logDebugGrammar("PLUS")
			parsingMust = true
		}
	case 8:
		//line query_syntax.y:56
		{
			logDebugGrammar("MINUS")
			parsingMustNot = true
		}
	case 9:
		//line query_syntax.y:62
		{
			str := yyS[yypt-0].s
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
	case 10:
		//line query_syntax.y:77
		{
			phrase := yyS[yypt-0].s
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
	case 11:
		//line query_syntax.y:92
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
		}
	case 12:
		//line query_syntax.y:108
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
		}
	case 13:
		//line query_syntax.y:126
		{
			boost := yyS[yypt-1].n
			logDebugGrammar("BOOST %d", boost)
		}
	case 14:
		//line query_syntax.y:132
		{

		}
	case 15:
		//line query_syntax.y:136
		{

		}
	}
	goto yystack /* stack new state and value */
}
