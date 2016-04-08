package bleve

import (
	"bufio"
	"io"
	"strings"
)

type frame struct {
	i            int
	s            string
	line, column int
}
type lexer struct {
	// The lexer runs in its own goroutine, and communicates via channel 'ch'.
	ch chan frame
	// We record the level of nesting because the action could return, and a
	// subsequent call expects to pick up where it left off. In other words,
	// we're simulating a coroutine.
	// TODO: Support a channel-based variant that compatible with Go's yacc.
	stack []frame
	stale bool

	// The 'l' and 'c' fields were added for
	// https://github.com/wagerlabs/docker/blob/65694e801a7b80930961d70c69cba9f2465459be/buildfile.nex
	// Since then, I introduced the built-in Line() and Column() functions.
	l, c int

	parseResult interface{}

	// The following line makes it easy for scripts to insert fields in the
	// generated code.
	// [NEX_END_OF_LEXER_STRUCT]
}

// newLexerWithInit creates a new lexer object, runs the given callback on it,
// then returns it.
func newLexerWithInit(in io.Reader, initFun func(*lexer)) *lexer {
	type dfa struct {
		acc          []bool           // Accepting states.
		f            []func(rune) int // Transitions.
		startf, endf []int            // Transitions at start and end of input.
		nest         []dfa
	}
	yylex := new(lexer)
	if initFun != nil {
		initFun(yylex)
	}
	yylex.ch = make(chan frame)
	var scan func(in *bufio.Reader, ch chan frame, family []dfa, line, column int)
	scan = func(in *bufio.Reader, ch chan frame, family []dfa, line, column int) {
		// Index of DFA and length of highest-precedence match so far.
		matchi, matchn := 0, -1
		var buf []rune
		n := 0
		checkAccept := func(i int, st int) bool {
			// Higher precedence match? DFAs are run in parallel, so matchn is at most len(buf), hence we may omit the length equality check.
			if family[i].acc[st] && (matchn < n || matchi > i) {
				matchi, matchn = i, n
				return true
			}
			return false
		}
		var state [][2]int
		for i := 0; i < len(family); i++ {
			mark := make([]bool, len(family[i].startf))
			// Every DFA starts at state 0.
			st := 0
			for {
				state = append(state, [2]int{i, st})
				mark[st] = true
				// As we're at the start of input, follow all ^ transitions and append to our list of start states.
				st = family[i].startf[st]
				if -1 == st || mark[st] {
					break
				}
				// We only check for a match after at least one transition.
				checkAccept(i, st)
			}
		}
		atEOF := false
		for {
			if n == len(buf) && !atEOF {
				r, _, err := in.ReadRune()
				switch err {
				case io.EOF:
					atEOF = true
				case nil:
					buf = append(buf, r)
				default:
					panic(err)
				}
			}
			if !atEOF {
				r := buf[n]
				n++
				var nextState [][2]int
				for _, x := range state {
					x[1] = family[x[0]].f[x[1]](r)
					if -1 == x[1] {
						continue
					}
					nextState = append(nextState, x)
					checkAccept(x[0], x[1])
				}
				state = nextState
			} else {
			dollar: // Handle $.
				for _, x := range state {
					mark := make([]bool, len(family[x[0]].endf))
					for {
						mark[x[1]] = true
						x[1] = family[x[0]].endf[x[1]]
						if -1 == x[1] || mark[x[1]] {
							break
						}
						if checkAccept(x[0], x[1]) {
							// Unlike before, we can break off the search. Now that we're at the end, there's no need to maintain the state of each DFA.
							break dollar
						}
					}
				}
				state = nil
			}

			if state == nil {
				lcUpdate := func(r rune) {
					if r == '\n' {
						line++
						column = 0
					} else {
						column++
					}
				}
				// All DFAs stuck. Return last match if it exists, otherwise advance by one rune and restart all DFAs.
				if matchn == -1 {
					if len(buf) == 0 { // This can only happen at the end of input.
						break
					}
					lcUpdate(buf[0])
					buf = buf[1:]
				} else {
					text := string(buf[:matchn])
					buf = buf[matchn:]
					matchn = -1
					ch <- frame{matchi, text, line, column}
					if len(family[matchi].nest) > 0 {
						scan(bufio.NewReader(strings.NewReader(text)), ch, family[matchi].nest, line, column)
					}
					if atEOF {
						break
					}
					for _, r := range text {
						lcUpdate(r)
					}
				}
				n = 0
				for i := 0; i < len(family); i++ {
					state = append(state, [2]int{i, 0})
				}
			}
		}
		ch <- frame{-1, "", line, column}
	}
	go scan(bufio.NewReader(in), yylex.ch, []dfa{
		// \"((\\\")|(\\\\)|(\\\/)|(\\b)|(\\f)|(\\n)|(\\r)|(\\t)|(\\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])|[^\"])*\"
		{[]bool{false, false, true, false, false, true, false, false, false, false, false, false, false, false, false, false, false, false}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 34:
					return 1
				case 47:
					return -1
				case 92:
					return -1
				case 98:
					return -1
				case 102:
					return -1
				case 110:
					return -1
				case 114:
					return -1
				case 116:
					return -1
				case 117:
					return -1
				}
				switch {
				case 48 <= r && r <= 57:
					return -1
				case 65 <= r && r <= 70:
					return -1
				case 97 <= r && r <= 102:
					return -1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 3
				case 102:
					return 3
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 3
				case 65 <= r && r <= 70:
					return 3
				case 97 <= r && r <= 102:
					return 3
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return -1
				case 47:
					return -1
				case 92:
					return -1
				case 98:
					return -1
				case 102:
					return -1
				case 110:
					return -1
				case 114:
					return -1
				case 116:
					return -1
				case 117:
					return -1
				}
				switch {
				case 48 <= r && r <= 57:
					return -1
				case 65 <= r && r <= 70:
					return -1
				case 97 <= r && r <= 102:
					return -1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 3
				case 102:
					return 3
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 3
				case 65 <= r && r <= 70:
					return 3
				case 97 <= r && r <= 102:
					return 3
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 5
				case 47:
					return 6
				case 92:
					return 7
				case 98:
					return 8
				case 102:
					return 9
				case 110:
					return 10
				case 114:
					return 11
				case 116:
					return 12
				case 117:
					return 13
				}
				switch {
				case 48 <= r && r <= 57:
					return 3
				case 65 <= r && r <= 70:
					return 3
				case 97 <= r && r <= 102:
					return 3
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 3
				case 102:
					return 3
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 3
				case 65 <= r && r <= 70:
					return 3
				case 97 <= r && r <= 102:
					return 3
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 3
				case 102:
					return 3
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 3
				case 65 <= r && r <= 70:
					return 3
				case 97 <= r && r <= 102:
					return 3
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 5
				case 47:
					return 6
				case 92:
					return 7
				case 98:
					return 8
				case 102:
					return 9
				case 110:
					return 10
				case 114:
					return 11
				case 116:
					return 12
				case 117:
					return 13
				}
				switch {
				case 48 <= r && r <= 57:
					return 3
				case 65 <= r && r <= 70:
					return 3
				case 97 <= r && r <= 102:
					return 3
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 3
				case 102:
					return 3
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 3
				case 65 <= r && r <= 70:
					return 3
				case 97 <= r && r <= 102:
					return 3
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 3
				case 102:
					return 3
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 3
				case 65 <= r && r <= 70:
					return 3
				case 97 <= r && r <= 102:
					return 3
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 3
				case 102:
					return 3
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 3
				case 65 <= r && r <= 70:
					return 3
				case 97 <= r && r <= 102:
					return 3
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 3
				case 102:
					return 3
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 3
				case 65 <= r && r <= 70:
					return 3
				case 97 <= r && r <= 102:
					return 3
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 3
				case 102:
					return 3
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 3
				case 65 <= r && r <= 70:
					return 3
				case 97 <= r && r <= 102:
					return 3
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 14
				case 102:
					return 14
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 14
				case 65 <= r && r <= 70:
					return 14
				case 97 <= r && r <= 102:
					return 14
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 15
				case 102:
					return 15
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 15
				case 65 <= r && r <= 70:
					return 15
				case 97 <= r && r <= 102:
					return 15
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 16
				case 102:
					return 16
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 16
				case 65 <= r && r <= 70:
					return 16
				case 97 <= r && r <= 102:
					return 16
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 17
				case 102:
					return 17
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 17
				case 65 <= r && r <= 70:
					return 17
				case 97 <= r && r <= 102:
					return 17
				}
				return 3
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 3
				case 102:
					return 3
				case 110:
					return 3
				case 114:
					return 3
				case 116:
					return 3
				case 117:
					return 3
				}
				switch {
				case 48 <= r && r <= 57:
					return 3
				case 65 <= r && r <= 70:
					return 3
				case 97 <= r && r <= 102:
					return 3
				}
				return 3
			},
		}, []int{ /* Start-of-input transitions */ -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, []int{ /* End-of-input transitions */ -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, nil},

		// \/((\\\")|(\\\\)|(\\\/)|(\\b)|(\\f)|(\\n)|(\\r)|(\\t)|(\\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])|[^\/])*\/
		{[]bool{false, false, false, true, false, false, true, false, false, false, false, false, false, false, false, false, false, false}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 34:
					return -1
				case 47:
					return 1
				case 92:
					return -1
				case 98:
					return -1
				case 102:
					return -1
				case 110:
					return -1
				case 114:
					return -1
				case 116:
					return -1
				case 117:
					return -1
				}
				switch {
				case 48 <= r && r <= 57:
					return -1
				case 65 <= r && r <= 70:
					return -1
				case 97 <= r && r <= 102:
					return -1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 2
				case 102:
					return 2
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 2
				case 65 <= r && r <= 70:
					return 2
				case 97 <= r && r <= 102:
					return 2
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 2
				case 102:
					return 2
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 2
				case 65 <= r && r <= 70:
					return 2
				case 97 <= r && r <= 102:
					return 2
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return -1
				case 47:
					return -1
				case 92:
					return -1
				case 98:
					return -1
				case 102:
					return -1
				case 110:
					return -1
				case 114:
					return -1
				case 116:
					return -1
				case 117:
					return -1
				}
				switch {
				case 48 <= r && r <= 57:
					return -1
				case 65 <= r && r <= 70:
					return -1
				case 97 <= r && r <= 102:
					return -1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 34:
					return 5
				case 47:
					return 6
				case 92:
					return 7
				case 98:
					return 8
				case 102:
					return 9
				case 110:
					return 10
				case 114:
					return 11
				case 116:
					return 12
				case 117:
					return 13
				}
				switch {
				case 48 <= r && r <= 57:
					return 2
				case 65 <= r && r <= 70:
					return 2
				case 97 <= r && r <= 102:
					return 2
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 2
				case 102:
					return 2
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 2
				case 65 <= r && r <= 70:
					return 2
				case 97 <= r && r <= 102:
					return 2
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 2
				case 102:
					return 2
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 2
				case 65 <= r && r <= 70:
					return 2
				case 97 <= r && r <= 102:
					return 2
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 5
				case 47:
					return 6
				case 92:
					return 7
				case 98:
					return 8
				case 102:
					return 9
				case 110:
					return 10
				case 114:
					return 11
				case 116:
					return 12
				case 117:
					return 13
				}
				switch {
				case 48 <= r && r <= 57:
					return 2
				case 65 <= r && r <= 70:
					return 2
				case 97 <= r && r <= 102:
					return 2
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 2
				case 102:
					return 2
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 2
				case 65 <= r && r <= 70:
					return 2
				case 97 <= r && r <= 102:
					return 2
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 2
				case 102:
					return 2
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 2
				case 65 <= r && r <= 70:
					return 2
				case 97 <= r && r <= 102:
					return 2
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 2
				case 102:
					return 2
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 2
				case 65 <= r && r <= 70:
					return 2
				case 97 <= r && r <= 102:
					return 2
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 2
				case 102:
					return 2
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 2
				case 65 <= r && r <= 70:
					return 2
				case 97 <= r && r <= 102:
					return 2
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 2
				case 102:
					return 2
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 2
				case 65 <= r && r <= 70:
					return 2
				case 97 <= r && r <= 102:
					return 2
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 14
				case 102:
					return 14
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 14
				case 65 <= r && r <= 70:
					return 14
				case 97 <= r && r <= 102:
					return 14
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 15
				case 102:
					return 15
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 15
				case 65 <= r && r <= 70:
					return 15
				case 97 <= r && r <= 102:
					return 15
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 16
				case 102:
					return 16
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 16
				case 65 <= r && r <= 70:
					return 16
				case 97 <= r && r <= 102:
					return 16
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 17
				case 102:
					return 17
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 17
				case 65 <= r && r <= 70:
					return 17
				case 97 <= r && r <= 102:
					return 17
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 34:
					return 2
				case 47:
					return 3
				case 92:
					return 4
				case 98:
					return 2
				case 102:
					return 2
				case 110:
					return 2
				case 114:
					return 2
				case 116:
					return 2
				case 117:
					return 2
				}
				switch {
				case 48 <= r && r <= 57:
					return 2
				case 65 <= r && r <= 70:
					return 2
				case 97 <= r && r <= 102:
					return 2
				}
				return 2
			},
		}, []int{ /* Start-of-input transitions */ -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, []int{ /* End-of-input transitions */ -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}, nil},

		// \+
		{[]bool{false, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 43:
					return 1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 43:
					return -1
				}
				return -1
			},
		}, []int{ /* Start-of-input transitions */ -1, -1}, []int{ /* End-of-input transitions */ -1, -1}, nil},

		// -
		{[]bool{false, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 45:
					return 1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 45:
					return -1
				}
				return -1
			},
		}, []int{ /* Start-of-input transitions */ -1, -1}, []int{ /* End-of-input transitions */ -1, -1}, nil},

		// :
		{[]bool{false, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 58:
					return 1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 58:
					return -1
				}
				return -1
			},
		}, []int{ /* Start-of-input transitions */ -1, -1}, []int{ /* End-of-input transitions */ -1, -1}, nil},

		// \^
		{[]bool{false, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 94:
					return 1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 94:
					return -1
				}
				return -1
			},
		}, []int{ /* Start-of-input transitions */ -1, -1}, []int{ /* End-of-input transitions */ -1, -1}, nil},

		// \(
		{[]bool{false, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 40:
					return 1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 40:
					return -1
				}
				return -1
			},
		}, []int{ /* Start-of-input transitions */ -1, -1}, []int{ /* End-of-input transitions */ -1, -1}, nil},

		// \)
		{[]bool{false, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 41:
					return 1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 41:
					return -1
				}
				return -1
			},
		}, []int{ /* Start-of-input transitions */ -1, -1}, []int{ /* End-of-input transitions */ -1, -1}, nil},

		// >
		{[]bool{false, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 62:
					return 1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 62:
					return -1
				}
				return -1
			},
		}, []int{ /* Start-of-input transitions */ -1, -1}, []int{ /* End-of-input transitions */ -1, -1}, nil},

		// <
		{[]bool{false, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 60:
					return 1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 60:
					return -1
				}
				return -1
			},
		}, []int{ /* Start-of-input transitions */ -1, -1}, []int{ /* End-of-input transitions */ -1, -1}, nil},

		// =
		{[]bool{false, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 61:
					return 1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 61:
					return -1
				}
				return -1
			},
		}, []int{ /* Start-of-input transitions */ -1, -1}, []int{ /* End-of-input transitions */ -1, -1}, nil},

		// ~([0-9]|[1-9][0-9]*)
		{[]bool{false, false, true, true, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 126:
					return 1
				}
				switch {
				case 48 <= r && r <= 48:
					return -1
				case 49 <= r && r <= 57:
					return -1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 126:
					return -1
				}
				switch {
				case 48 <= r && r <= 48:
					return 2
				case 49 <= r && r <= 57:
					return 3
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 126:
					return -1
				}
				switch {
				case 48 <= r && r <= 48:
					return -1
				case 49 <= r && r <= 57:
					return -1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 126:
					return -1
				}
				switch {
				case 48 <= r && r <= 48:
					return 4
				case 49 <= r && r <= 57:
					return 4
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 126:
					return -1
				}
				switch {
				case 48 <= r && r <= 48:
					return 4
				case 49 <= r && r <= 57:
					return 4
				}
				return -1
			},
		}, []int{ /* Start-of-input transitions */ -1, -1, -1, -1, -1}, []int{ /* End-of-input transitions */ -1, -1, -1, -1, -1}, nil},

		// ~
		{[]bool{false, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 126:
					return 1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 126:
					return -1
				}
				return -1
			},
		}, []int{ /* Start-of-input transitions */ -1, -1}, []int{ /* End-of-input transitions */ -1, -1}, nil},

		// -?([0-9]|[1-9][0-9]*)(\.[0-9][0-9]*)?
		{[]bool{false, false, true, true, false, true, true, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 45:
					return 1
				case 46:
					return -1
				}
				switch {
				case 48 <= r && r <= 48:
					return 2
				case 49 <= r && r <= 57:
					return 3
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 45:
					return -1
				case 46:
					return -1
				}
				switch {
				case 48 <= r && r <= 48:
					return 2
				case 49 <= r && r <= 57:
					return 3
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 45:
					return -1
				case 46:
					return 4
				}
				switch {
				case 48 <= r && r <= 48:
					return -1
				case 49 <= r && r <= 57:
					return -1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 45:
					return -1
				case 46:
					return 4
				}
				switch {
				case 48 <= r && r <= 48:
					return 5
				case 49 <= r && r <= 57:
					return 5
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 45:
					return -1
				case 46:
					return -1
				}
				switch {
				case 48 <= r && r <= 48:
					return 6
				case 49 <= r && r <= 57:
					return 6
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 45:
					return -1
				case 46:
					return 4
				}
				switch {
				case 48 <= r && r <= 48:
					return 5
				case 49 <= r && r <= 57:
					return 5
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 45:
					return -1
				case 46:
					return -1
				}
				switch {
				case 48 <= r && r <= 48:
					return 7
				case 49 <= r && r <= 57:
					return 7
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 45:
					return -1
				case 46:
					return -1
				}
				switch {
				case 48 <= r && r <= 48:
					return 7
				case 49 <= r && r <= 57:
					return 7
				}
				return -1
			},
		}, []int{ /* Start-of-input transitions */ -1, -1, -1, -1, -1, -1, -1, -1}, []int{ /* End-of-input transitions */ -1, -1, -1, -1, -1, -1, -1, -1}, nil},

		// [ \t\n]+
		{[]bool{false, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 9:
					return 1
				case 10:
					return 1
				case 32:
					return 1
				}
				return -1
			},
			func(r rune) int {
				switch r {
				case 9:
					return 1
				case 10:
					return 1
				case 32:
					return 1
				}
				return -1
			},
		}, []int{ /* Start-of-input transitions */ -1, -1}, []int{ /* End-of-input transitions */ -1, -1}, nil},

		// [^\t\n\f\r :^\+\*\?><=~-][^\t\n\f\r :^~\*\?]*
		{[]bool{false, true, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 9:
					return -1
				case 10:
					return -1
				case 12:
					return -1
				case 13:
					return -1
				case 32:
					return -1
				case 42:
					return -1
				case 43:
					return -1
				case 45:
					return -1
				case 58:
					return -1
				case 60:
					return -1
				case 61:
					return -1
				case 62:
					return -1
				case 63:
					return -1
				case 94:
					return -1
				case 126:
					return -1
				}
				return 1
			},
			func(r rune) int {
				switch r {
				case 9:
					return -1
				case 10:
					return -1
				case 12:
					return -1
				case 13:
					return -1
				case 32:
					return -1
				case 42:
					return -1
				case 43:
					return 2
				case 45:
					return 2
				case 58:
					return -1
				case 60:
					return 2
				case 61:
					return 2
				case 62:
					return 2
				case 63:
					return -1
				case 94:
					return -1
				case 126:
					return -1
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 9:
					return -1
				case 10:
					return -1
				case 12:
					return -1
				case 13:
					return -1
				case 32:
					return -1
				case 42:
					return -1
				case 43:
					return 2
				case 45:
					return 2
				case 58:
					return -1
				case 60:
					return 2
				case 61:
					return 2
				case 62:
					return 2
				case 63:
					return -1
				case 94:
					return -1
				case 126:
					return -1
				}
				return 2
			},
		}, []int{ /* Start-of-input transitions */ -1, -1, -1}, []int{ /* End-of-input transitions */ -1, -1, -1}, nil},

		// [^\t\n\f\r :^\+\*\?><=~-][^\t\n\f\r :^~]*
		{[]bool{false, true, true}, []func(rune) int{ // Transitions
			func(r rune) int {
				switch r {
				case 9:
					return -1
				case 10:
					return -1
				case 12:
					return -1
				case 13:
					return -1
				case 32:
					return -1
				case 42:
					return -1
				case 43:
					return -1
				case 45:
					return -1
				case 58:
					return -1
				case 60:
					return -1
				case 61:
					return -1
				case 62:
					return -1
				case 63:
					return -1
				case 94:
					return -1
				case 126:
					return -1
				}
				return 1
			},
			func(r rune) int {
				switch r {
				case 9:
					return -1
				case 10:
					return -1
				case 12:
					return -1
				case 13:
					return -1
				case 32:
					return -1
				case 42:
					return 2
				case 43:
					return 2
				case 45:
					return 2
				case 58:
					return -1
				case 60:
					return 2
				case 61:
					return 2
				case 62:
					return 2
				case 63:
					return 2
				case 94:
					return -1
				case 126:
					return -1
				}
				return 2
			},
			func(r rune) int {
				switch r {
				case 9:
					return -1
				case 10:
					return -1
				case 12:
					return -1
				case 13:
					return -1
				case 32:
					return -1
				case 42:
					return 2
				case 43:
					return 2
				case 45:
					return 2
				case 58:
					return -1
				case 60:
					return 2
				case 61:
					return 2
				case 62:
					return 2
				case 63:
					return 2
				case 94:
					return -1
				case 126:
					return -1
				}
				return 2
			},
		}, []int{ /* Start-of-input transitions */ -1, -1, -1}, []int{ /* End-of-input transitions */ -1, -1, -1}, nil},
	}, 0, 0)
	return yylex
}

func newLexer(in io.Reader) *lexer {
	return newLexerWithInit(in, nil)
}

// Text returns the matched text.
func (yylex *lexer) Text() string {
	return yylex.stack[len(yylex.stack)-1].s
}

// Line returns the current line number.
// The first line is 0.
func (yylex *lexer) Line() int {
	if len(yylex.stack) == 0 {
		return 0
	}
	return yylex.stack[len(yylex.stack)-1].line
}

// Column returns the current column number.
// The first column is 0.
func (yylex *lexer) Column() int {
	if len(yylex.stack) == 0 {
		return 0
	}
	return yylex.stack[len(yylex.stack)-1].column
}

func (yylex *lexer) next(lvl int) int {
	if lvl == len(yylex.stack) {
		l, c := 0, 0
		if lvl > 0 {
			l, c = yylex.stack[lvl-1].line, yylex.stack[lvl-1].column
		}
		yylex.stack = append(yylex.stack, frame{0, "", l, c})
	}
	if lvl == len(yylex.stack)-1 {
		p := &yylex.stack[lvl]
		*p = <-yylex.ch
		yylex.stale = false
	} else {
		yylex.stale = true
	}
	return yylex.stack[lvl].i
}
func (yylex *lexer) pop() {
	yylex.stack = yylex.stack[:len(yylex.stack)-1]
}
func (yylex lexer) Error(e string) {
	panic(e)
}

// Lex runs the lexer. Always returns 0.
// When the -s option is given, this function is not generated;
// instead, the NN_FUN macro runs the lexer.
func (yylex *lexer) Lex(lval *yySymType) int {
OUTER0:
	for {
		switch yylex.next(0) {
		case 0:
			{
				lval.s = yylex.Text()[1 : len(yylex.Text())-1]
				logDebugTokens("PHRASE - %s", lval.s)
				return tPHRASE
			}
		case 1:
			{
				lval.s = yylex.Text()[1 : len(yylex.Text())-1]
				logDebugTokens("REGEXP - %s", lval.s)
				return tREGEXP
			}
		case 2:
			{
				logDebugTokens("PLUS")
				return tPLUS
			}
		case 3:
			{
				logDebugTokens("MINUS")
				return tMINUS
			}
		case 4:
			{
				logDebugTokens("COLON")
				return tCOLON
			}
		case 5:
			{
				logDebugTokens("BOOST")
				return tBOOST
			}
		case 6:
			{
				logDebugTokens("LPAREN")
				return tLPAREN
			}
		case 7:
			{
				logDebugTokens("RPAREN")
				return tRPAREN
			}
		case 8:
			{
				logDebugTokens("GREATER")
				return tGREATER
			}
		case 9:
			{
				logDebugTokens("LESS")
				return tLESS
			}
		case 10:
			{
				logDebugTokens("EQUAL")
				return tEQUAL
			}
		case 11:
			{
				lval.s = yylex.Text()[1:]
				logDebugTokens("TILDENUMBER - %s", lval.s)
				return tTILDENUMBER
			}
		case 12:
			{
				logDebugTokens("TILDE")
				return tTILDE
			}
		case 13:
			{
				lval.s = yylex.Text()
				logDebugTokens("NUMBER - %s", lval.s)
				return tNUMBER
			}
		case 14:
			{
				logDebugTokens("WHITESPACE (count=%d)", len(yylex.Text())) /* eat up whitespace */
			}
		case 15:
			{
				lval.s = yylex.Text()
				logDebugTokens("STRING - %s", lval.s)
				return tSTRING
			}
		case 16:
			{
				lval.s = yylex.Text()
				logDebugTokens("WILD - %s", lval.s)
				return tWILD
			}
		default:
			break OUTER0
		}
		continue
	}
	yylex.pop()

	return 0
}
func logDebugTokens(format string, v ...interface{}) {
	if debugLexer {
		logger.Printf(format, v...)
	}
}
