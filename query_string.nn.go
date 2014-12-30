package bleve
import ("bufio";"io";"strings")
type dfa struct {
  acc []bool
  f []func(rune) int
  id int
}
type family struct {
  a []dfa
  endcase int
}
var a0 [15]dfa
var a []family
func init() {
a = make([]family, 1)
{
var acc [18]bool
var fun [18]func(rune) int
fun[6] = func(r rune) int {
  switch(r) {
  case 34: return 5
  case 92: return 6
  case 47: return 7
  case 114: return 8
  case 98: return 9
  case 102: return 10
  case 110: return 11
  case 116: return 12
  case 117: return 13
  default:
    switch {
    case 48 <= r && r <= 57: return 3
    case 65 <= r && r <= 70: return 3
    case 97 <= r && r <= 102: return 3
    default: return 3
    }
  }
}
fun[7] = func(r rune) int {
  switch(r) {
  case 34: return 4
  case 92: return 2
  case 47: return 3
  case 114: return 3
  case 98: return 3
  case 102: return 3
  case 110: return 3
  case 116: return 3
  case 117: return 3
  default:
    switch {
    case 48 <= r && r <= 57: return 3
    case 65 <= r && r <= 70: return 3
    case 97 <= r && r <= 102: return 3
    default: return 3
    }
  }
}
fun[8] = func(r rune) int {
  switch(r) {
  case 114: return 3
  case 34: return 4
  case 92: return 2
  case 47: return 3
  case 116: return 3
  case 117: return 3
  case 98: return 3
  case 102: return 3
  case 110: return 3
  default:
    switch {
    case 48 <= r && r <= 57: return 3
    case 65 <= r && r <= 70: return 3
    case 97 <= r && r <= 102: return 3
    default: return 3
    }
  }
}
fun[9] = func(r rune) int {
  switch(r) {
  case 98: return 3
  case 102: return 3
  case 110: return 3
  case 116: return 3
  case 117: return 3
  case 34: return 4
  case 92: return 2
  case 47: return 3
  case 114: return 3
  default:
    switch {
    case 48 <= r && r <= 57: return 3
    case 65 <= r && r <= 70: return 3
    case 97 <= r && r <= 102: return 3
    default: return 3
    }
  }
}
fun[12] = func(r rune) int {
  switch(r) {
  case 98: return 3
  case 102: return 3
  case 110: return 3
  case 116: return 3
  case 117: return 3
  case 34: return 4
  case 92: return 2
  case 47: return 3
  case 114: return 3
  default:
    switch {
    case 48 <= r && r <= 57: return 3
    case 65 <= r && r <= 70: return 3
    case 97 <= r && r <= 102: return 3
    default: return 3
    }
  }
}
fun[14] = func(r rune) int {
  switch(r) {
  case 98: return 15
  case 102: return 15
  case 110: return 3
  case 116: return 3
  case 117: return 3
  case 34: return 4
  case 92: return 2
  case 47: return 3
  case 114: return 3
  default:
    switch {
    case 48 <= r && r <= 57: return 15
    case 65 <= r && r <= 70: return 15
    case 97 <= r && r <= 102: return 15
    default: return 3
    }
  }
}
fun[3] = func(r rune) int {
  switch(r) {
  case 102: return 3
  case 110: return 3
  case 116: return 3
  case 117: return 3
  case 98: return 3
  case 92: return 2
  case 47: return 3
  case 114: return 3
  case 34: return 4
  default:
    switch {
    case 48 <= r && r <= 57: return 3
    case 65 <= r && r <= 70: return 3
    case 97 <= r && r <= 102: return 3
    default: return 3
    }
  }
}
fun[0] = func(r rune) int {
  switch(r) {
  case 98: return -1
  case 102: return -1
  case 110: return -1
  case 116: return -1
  case 117: return -1
  case 34: return 1
  case 92: return -1
  case 47: return -1
  case 114: return -1
  default:
    switch {
    case 48 <= r && r <= 57: return -1
    case 65 <= r && r <= 70: return -1
    case 97 <= r && r <= 102: return -1
    default: return -1
    }
  }
}
fun[1] = func(r rune) int {
  switch(r) {
  case 92: return 2
  case 47: return 3
  case 114: return 3
  case 34: return 4
  case 102: return 3
  case 110: return 3
  case 116: return 3
  case 117: return 3
  case 98: return 3
  default:
    switch {
    case 48 <= r && r <= 57: return 3
    case 65 <= r && r <= 70: return 3
    case 97 <= r && r <= 102: return 3
    default: return 3
    }
  }
}
acc[4] = true
fun[4] = func(r rune) int {
  switch(r) {
  case 34: return -1
  case 92: return -1
  case 47: return -1
  case 114: return -1
  case 117: return -1
  case 98: return -1
  case 102: return -1
  case 110: return -1
  case 116: return -1
  default:
    switch {
    case 48 <= r && r <= 57: return -1
    case 65 <= r && r <= 70: return -1
    case 97 <= r && r <= 102: return -1
    default: return -1
    }
  }
}
fun[13] = func(r rune) int {
  switch(r) {
  case 116: return 3
  case 117: return 3
  case 98: return 14
  case 102: return 14
  case 110: return 3
  case 114: return 3
  case 34: return 4
  case 92: return 2
  case 47: return 3
  default:
    switch {
    case 48 <= r && r <= 57: return 14
    case 65 <= r && r <= 70: return 14
    case 97 <= r && r <= 102: return 14
    default: return 3
    }
  }
}
fun[15] = func(r rune) int {
  switch(r) {
  case 114: return 3
  case 34: return 4
  case 92: return 2
  case 47: return 3
  case 116: return 3
  case 117: return 3
  case 98: return 16
  case 102: return 16
  case 110: return 3
  default:
    switch {
    case 48 <= r && r <= 57: return 16
    case 65 <= r && r <= 70: return 16
    case 97 <= r && r <= 102: return 16
    default: return 3
    }
  }
}
fun[17] = func(r rune) int {
  switch(r) {
  case 34: return 4
  case 92: return 2
  case 47: return 3
  case 114: return 3
  case 98: return 3
  case 102: return 3
  case 110: return 3
  case 116: return 3
  case 117: return 3
  default:
    switch {
    case 48 <= r && r <= 57: return 3
    case 65 <= r && r <= 70: return 3
    case 97 <= r && r <= 102: return 3
    default: return 3
    }
  }
}
fun[16] = func(r rune) int {
  switch(r) {
  case 34: return 4
  case 92: return 2
  case 47: return 3
  case 114: return 3
  case 98: return 17
  case 102: return 17
  case 110: return 3
  case 116: return 3
  case 117: return 3
  default:
    switch {
    case 48 <= r && r <= 57: return 17
    case 65 <= r && r <= 70: return 17
    case 97 <= r && r <= 102: return 17
    default: return 3
    }
  }
}
acc[5] = true
fun[5] = func(r rune) int {
  switch(r) {
  case 47: return 3
  case 114: return 3
  case 34: return 4
  case 92: return 2
  case 110: return 3
  case 116: return 3
  case 117: return 3
  case 98: return 3
  case 102: return 3
  default:
    switch {
    case 48 <= r && r <= 57: return 3
    case 65 <= r && r <= 70: return 3
    case 97 <= r && r <= 102: return 3
    default: return 3
    }
  }
}
fun[10] = func(r rune) int {
  switch(r) {
  case 116: return 3
  case 117: return 3
  case 98: return 3
  case 102: return 3
  case 110: return 3
  case 114: return 3
  case 34: return 4
  case 92: return 2
  case 47: return 3
  default:
    switch {
    case 48 <= r && r <= 57: return 3
    case 65 <= r && r <= 70: return 3
    case 97 <= r && r <= 102: return 3
    default: return 3
    }
  }
}
fun[11] = func(r rune) int {
  switch(r) {
  case 34: return 4
  case 92: return 2
  case 47: return 3
  case 114: return 3
  case 98: return 3
  case 102: return 3
  case 110: return 3
  case 116: return 3
  case 117: return 3
  default:
    switch {
    case 48 <= r && r <= 57: return 3
    case 65 <= r && r <= 70: return 3
    case 97 <= r && r <= 102: return 3
    default: return 3
    }
  }
}
fun[2] = func(r rune) int {
  switch(r) {
  case 34: return 5
  case 92: return 6
  case 47: return 7
  case 114: return 8
  case 98: return 9
  case 102: return 10
  case 110: return 11
  case 116: return 12
  case 117: return 13
  default:
    switch {
    case 48 <= r && r <= 57: return 3
    case 65 <= r && r <= 70: return 3
    case 97 <= r && r <= 102: return 3
    default: return 3
    }
  }
}
a0[0].acc = acc[:]
a0[0].f = fun[:]
a0[0].id = 0
}
{
var acc [2]bool
var fun [2]func(rune) int
fun[0] = func(r rune) int {
  switch(r) {
  case 43: return 1
  default:
    switch {
    default: return -1
    }
  }
}
acc[1] = true
fun[1] = func(r rune) int {
  switch(r) {
  case 43: return -1
  default:
    switch {
    default: return -1
    }
  }
}
a0[1].acc = acc[:]
a0[1].f = fun[:]
a0[1].id = 1
}
{
var acc [2]bool
var fun [2]func(rune) int
acc[1] = true
fun[1] = func(r rune) int {
  switch(r) {
  case 45: return -1
  default:
    switch {
    default: return -1
    }
  }
}
fun[0] = func(r rune) int {
  switch(r) {
  case 45: return 1
  default:
    switch {
    default: return -1
    }
  }
}
a0[2].acc = acc[:]
a0[2].f = fun[:]
a0[2].id = 2
}
{
var acc [2]bool
var fun [2]func(rune) int
fun[0] = func(r rune) int {
  switch(r) {
  case 58: return 1
  default:
    switch {
    default: return -1
    }
  }
}
acc[1] = true
fun[1] = func(r rune) int {
  switch(r) {
  case 58: return -1
  default:
    switch {
    default: return -1
    }
  }
}
a0[3].acc = acc[:]
a0[3].f = fun[:]
a0[3].id = 3
}
{
var acc [2]bool
var fun [2]func(rune) int
fun[0] = func(r rune) int {
  switch(r) {
  case 94: return 1
  default:
    switch {
    default: return -1
    }
  }
}
acc[1] = true
fun[1] = func(r rune) int {
  switch(r) {
  case 94: return -1
  default:
    switch {
    default: return -1
    }
  }
}
a0[4].acc = acc[:]
a0[4].f = fun[:]
a0[4].id = 4
}
{
var acc [2]bool
var fun [2]func(rune) int
fun[0] = func(r rune) int {
  switch(r) {
  case 40: return 1
  default:
    switch {
    default: return -1
    }
  }
}
acc[1] = true
fun[1] = func(r rune) int {
  switch(r) {
  case 40: return -1
  default:
    switch {
    default: return -1
    }
  }
}
a0[5].acc = acc[:]
a0[5].f = fun[:]
a0[5].id = 5
}
{
var acc [2]bool
var fun [2]func(rune) int
fun[0] = func(r rune) int {
  switch(r) {
  case 41: return 1
  default:
    switch {
    default: return -1
    }
  }
}
acc[1] = true
fun[1] = func(r rune) int {
  switch(r) {
  case 41: return -1
  default:
    switch {
    default: return -1
    }
  }
}
a0[6].acc = acc[:]
a0[6].f = fun[:]
a0[6].id = 6
}
{
var acc [2]bool
var fun [2]func(rune) int
acc[1] = true
fun[1] = func(r rune) int {
  switch(r) {
  case 62: return -1
  default:
    switch {
    default: return -1
    }
  }
}
fun[0] = func(r rune) int {
  switch(r) {
  case 62: return 1
  default:
    switch {
    default: return -1
    }
  }
}
a0[7].acc = acc[:]
a0[7].f = fun[:]
a0[7].id = 7
}
{
var acc [2]bool
var fun [2]func(rune) int
fun[0] = func(r rune) int {
  switch(r) {
  case 60: return 1
  default:
    switch {
    default: return -1
    }
  }
}
acc[1] = true
fun[1] = func(r rune) int {
  switch(r) {
  case 60: return -1
  default:
    switch {
    default: return -1
    }
  }
}
a0[8].acc = acc[:]
a0[8].f = fun[:]
a0[8].id = 8
}
{
var acc [2]bool
var fun [2]func(rune) int
fun[0] = func(r rune) int {
  switch(r) {
  case 61: return 1
  default:
    switch {
    default: return -1
    }
  }
}
acc[1] = true
fun[1] = func(r rune) int {
  switch(r) {
  case 61: return -1
  default:
    switch {
    default: return -1
    }
  }
}
a0[9].acc = acc[:]
a0[9].f = fun[:]
a0[9].id = 9
}
{
var acc [5]bool
var fun [5]func(rune) int
fun[0] = func(r rune) int {
  switch(r) {
  case 126: return 1
  default:
    switch {
    case 48 <= r && r <= 48: return -1
    case 49 <= r && r <= 57: return -1
    default: return -1
    }
  }
}
fun[1] = func(r rune) int {
  switch(r) {
  case 126: return -1
  default:
    switch {
    case 48 <= r && r <= 48: return 2
    case 49 <= r && r <= 57: return 3
    default: return -1
    }
  }
}
acc[2] = true
fun[2] = func(r rune) int {
  switch(r) {
  case 126: return -1
  default:
    switch {
    case 48 <= r && r <= 48: return -1
    case 49 <= r && r <= 57: return -1
    default: return -1
    }
  }
}
acc[3] = true
fun[3] = func(r rune) int {
  switch(r) {
  case 126: return -1
  default:
    switch {
    case 48 <= r && r <= 48: return 4
    case 49 <= r && r <= 57: return 4
    default: return -1
    }
  }
}
acc[4] = true
fun[4] = func(r rune) int {
  switch(r) {
  case 126: return -1
  default:
    switch {
    case 48 <= r && r <= 48: return 4
    case 49 <= r && r <= 57: return 4
    default: return -1
    }
  }
}
a0[10].acc = acc[:]
a0[10].f = fun[:]
a0[10].id = 10
}
{
var acc [2]bool
var fun [2]func(rune) int
fun[0] = func(r rune) int {
  switch(r) {
  case 126: return 1
  default:
    switch {
    default: return -1
    }
  }
}
acc[1] = true
fun[1] = func(r rune) int {
  switch(r) {
  case 126: return -1
  default:
    switch {
    default: return -1
    }
  }
}
a0[11].acc = acc[:]
a0[11].f = fun[:]
a0[11].id = 11
}
{
var acc [8]bool
var fun [8]func(rune) int
fun[4] = func(r rune) int {
  switch(r) {
  case 45: return -1
  case 46: return -1
  default:
    switch {
    case 48 <= r && r <= 48: return 6
    case 49 <= r && r <= 57: return 6
    default: return -1
    }
  }
}
acc[5] = true
fun[5] = func(r rune) int {
  switch(r) {
  case 45: return -1
  case 46: return 4
  default:
    switch {
    case 48 <= r && r <= 48: return 5
    case 49 <= r && r <= 57: return 5
    default: return -1
    }
  }
}
acc[6] = true
fun[6] = func(r rune) int {
  switch(r) {
  case 45: return -1
  case 46: return -1
  default:
    switch {
    case 48 <= r && r <= 48: return 7
    case 49 <= r && r <= 57: return 7
    default: return -1
    }
  }
}
fun[0] = func(r rune) int {
  switch(r) {
  case 46: return -1
  case 45: return 1
  default:
    switch {
    case 48 <= r && r <= 48: return 2
    case 49 <= r && r <= 57: return 3
    default: return -1
    }
  }
}
fun[1] = func(r rune) int {
  switch(r) {
  case 45: return -1
  case 46: return -1
  default:
    switch {
    case 48 <= r && r <= 48: return 2
    case 49 <= r && r <= 57: return 3
    default: return -1
    }
  }
}
acc[2] = true
fun[2] = func(r rune) int {
  switch(r) {
  case 45: return -1
  case 46: return 4
  default:
    switch {
    case 48 <= r && r <= 48: return -1
    case 49 <= r && r <= 57: return -1
    default: return -1
    }
  }
}
acc[3] = true
fun[3] = func(r rune) int {
  switch(r) {
  case 45: return -1
  case 46: return 4
  default:
    switch {
    case 48 <= r && r <= 48: return 5
    case 49 <= r && r <= 57: return 5
    default: return -1
    }
  }
}
acc[7] = true
fun[7] = func(r rune) int {
  switch(r) {
  case 45: return -1
  case 46: return -1
  default:
    switch {
    case 48 <= r && r <= 48: return 7
    case 49 <= r && r <= 57: return 7
    default: return -1
    }
  }
}
a0[12].acc = acc[:]
a0[12].f = fun[:]
a0[12].id = 12
}
{
var acc [2]bool
var fun [2]func(rune) int
fun[0] = func(r rune) int {
  switch(r) {
  case 10: return 1
  case 32: return 1
  case 9: return 1
  default:
    switch {
    default: return -1
    }
  }
}
acc[1] = true
fun[1] = func(r rune) int {
  switch(r) {
  case 32: return 1
  case 9: return 1
  case 10: return 1
  default:
    switch {
    default: return -1
    }
  }
}
a0[13].acc = acc[:]
a0[13].f = fun[:]
a0[13].id = 13
}
{
var acc [3]bool
var fun [3]func(rune) int
fun[0] = func(r rune) int {
  switch(r) {
  case 13: return -1
  case 32: return -1
  case 58: return -1
  case 94: return -1
  case 45: return -1
  case 62: return -1
  case 9: return -1
  case 10: return -1
  case 126: return -1
  case 60: return -1
  case 61: return -1
  case 12: return -1
  case 43: return -1
  default:
    switch {
    default: return 1
    }
  }
}
acc[1] = true
fun[1] = func(r rune) int {
  switch(r) {
  case 12: return -1
  case 43: return 2
  case 94: return -1
  case 45: return 2
  case 62: return 2
  case 9: return -1
  case 10: return -1
  case 13: return -1
  case 32: return -1
  case 58: return -1
  case 60: return 2
  case 61: return 2
  case 126: return -1
  default:
    switch {
    default: return 2
    }
  }
}
acc[2] = true
fun[2] = func(r rune) int {
  switch(r) {
  case 94: return -1
  case 45: return 2
  case 62: return 2
  case 9: return -1
  case 10: return -1
  case 13: return -1
  case 32: return -1
  case 58: return -1
  case 60: return 2
  case 61: return 2
  case 126: return -1
  case 12: return -1
  case 43: return 2
  default:
    switch {
    default: return 2
    }
  }
}
a0[14].acc = acc[:]
a0[14].f = fun[:]
a0[14].id = 14
}
a[0].endcase = 15
a[0].a = a0[:]
}
func getAction(c *frame) int {
  if -1 == c.match { return -1 }
  c.action = c.fam.a[c.match].id
  c.match = -1
  return c.action
}
type frame struct {
  atEOF bool
  action, match, matchn, n int
  buf []rune
  text string
  in *bufio.Reader
  state []int
  fam family
}
func newFrame(in *bufio.Reader, index int) *frame {
  f := new(frame)
  f.buf = make([]rune, 0, 128)
  f.in = in
  f.match = -1
  f.fam = a[index]
  f.state = make([]int, len(f.fam.a))
  return f
}
type lexer []*frame
func newLexer(in io.Reader) lexer {
  stack := make([]*frame, 0, 4)
  stack = append(stack, newFrame(bufio.NewReader(in), 0))
  return stack
}
func (stack lexer) isDone() bool {
  return 1 == len(stack) && stack[0].atEOF
}
func (stack lexer) nextAction() int {
  c := stack[len(stack) - 1]
  for {
    if c.atEOF { return c.fam.endcase }
    if c.n == len(c.buf) {
      r,_,er := c.in.ReadRune()
      switch er {
      case nil: c.buf = append(c.buf, r)
      case io.EOF:
	c.atEOF = true
	if c.n > 0 {
	  c.text = string(c.buf)
	  return getAction(c)
	}
	return c.fam.endcase
      default: panic(er.Error())
      }
    }
    jammed := true
    r := c.buf[c.n]
    for i, x := range c.fam.a {
      if -1 == c.state[i] { continue }
      c.state[i] = x.f[c.state[i]](r)
      if -1 == c.state[i] { continue }
      jammed = false
      if x.acc[c.state[i]] {
	if -1 == c.match || c.matchn < c.n+1 || c.match > i {
	  c.match = i
	  c.matchn = c.n+1
	}
      }
    }
    if jammed {
      a := getAction(c)
      if -1 == a { c.matchn = c.n + 1 }
      c.n = 0
      for i, _ := range c.state { c.state[i] = 0 }
      c.text = string(c.buf[:c.matchn])
      copy(c.buf, c.buf[c.matchn:])
      c.buf = c.buf[:len(c.buf) - c.matchn]
      return a
    }
    c.n++
  }
}
func (stack lexer) push(index int) lexer {
  c := stack[len(stack) - 1]
  return append(stack,
      newFrame(bufio.NewReader(strings.NewReader(c.text)), index))
}
func (stack lexer) pop() lexer {
  return stack[:len(stack) - 1]
}
func (stack lexer) Text() string {
  c := stack[len(stack) - 1]
  return c.text
}
func (yylex lexer) Error(e string) {
  panic(e)
}
func (yylex lexer) Lex(lval *yySymType) int {
  for !yylex.isDone() {
    switch yylex.nextAction() {
    case -1:
    case 0:  //\"((\\\")|(\\\\)|(\\\/)|(\\b)|(\\f)|(\\n)|(\\r)|(\\t)|(\\u[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])|[^\"])*\"/
{ 
                    lval.s = yylex.Text()[1:len(yylex.Text())-1]
                    logDebugTokens("PHRASE - %s", lval.s);
                    return tPHRASE 
              }
    case 1:  //\+/
{ logDebugTokens("PLUS"); return tPLUS }
    case 2:  //-/
{ logDebugTokens("MINUS"); return tMINUS }
    case 3:  //:/
{ logDebugTokens("COLON"); return tCOLON }
    case 4:  //^/
{ logDebugTokens("BOOST"); return tBOOST }
    case 5:  //\(/
{ logDebugTokens("LPAREN"); return tLPAREN }
    case 6:  //\)/
{ logDebugTokens("RPAREN"); return tRPAREN }
    case 7:  //>/
{ logDebugTokens("GREATER"); return tGREATER }
    case 8:  //</
{ logDebugTokens("LESS"); return tLESS }
    case 9:  //=/
{ logDebugTokens("EQUAL"); return tEQUAL }
    case 10:  //~([0-9]|[1-9][0-9]*)/
{
                    lval.s = yylex.Text()[1:]
                    logDebugTokens("TILDENUMBER - %s", lval.s);
                    return tTILDENUMBER
                  }
    case 11:  //~/
{ logDebugTokens("TILDE"); return tTILDE }
    case 12:  //-?([0-9]|[1-9][0-9]*)(\.[0-9][0-9]*)?/
{ 
                    lval.s = yylex.Text()
                    logDebugTokens("NUMBER - %s", lval.s);
                    return tNUMBER
                  }
    case 13:  //[ \t\n]+/
{ logDebugTokens("WHITESPACE (count=%d)", len(yylex.Text())) /* eat up whitespace */ }
    case 14:  //[^\t\n\f\r :^\+\-><=~][^\t\n\f\r :^~]*/
{
                    lval.s = yylex.Text()
                    logDebugTokens("STRING - %s", lval.s);
                    return tSTRING 
                  }
    case 15:  ///
// [END]
    }
  }
  return 0
}
func logDebugTokens(format string, v ...interface{}) {
    if debugLexer {
        logger.Printf(format, v...)
    }
}
