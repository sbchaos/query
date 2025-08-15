package query_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sbchaos/query"
)

func TestIdent_String(t *testing.T) {
	AssertExprStringer(t, &query.Ident{Name: "foo"}, `"foo"`)
	AssertExprStringer(t, &query.Ident{Name: "foo \" bar"}, `"foo "" bar"`)
}

func TestParenExpr_String(t *testing.T) {
	AssertExprStringer(t, &query.ParenExpr{X: &query.NullLit{}}, `(NULL)`)
}

func TestUnaryExpr_String(t *testing.T) {
	AssertExprStringer(t, &query.UnaryExpr{Op: query.PLUS, X: &query.NumberLit{Value: "100"}}, `+100`)
	AssertExprStringer(t, &query.UnaryExpr{Op: query.MINUS, X: &query.NumberLit{Value: "100"}}, `-100`)
	AssertExprStringer(t, &query.UnaryExpr{Op: query.NOT, X: &query.NumberLit{Value: "100"}}, `NOT 100`)
	AssertNodeStringerPanic(t, &query.UnaryExpr{X: &query.NumberLit{Value: "100"}}, `query.UnaryExpr.String(): invalid op ILLEGAL`)
}

func TestBinaryExpr_String(t *testing.T) {
	AssertExprStringer(t, &query.BinaryExpr{Op: query.PLUS, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 + 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.MINUS, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 - 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.STAR, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 * 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.SLASH, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 / 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.REM, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 % 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.CONCAT, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 || 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.LSHIFT, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 << 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.RSHIFT, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 >> 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.BITAND, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 & 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.BITOR, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 | 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.LT, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 < 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.LE, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 <= 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.GT, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 > 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.GE, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 >= 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.EQ, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 = 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.NE, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 != 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.IS, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 IS 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.ISNOT, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 IS NOT 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.IN, X: &query.NumberLit{Value: "1"}, Y: &query.ExprList{Exprs: []query.Expr{&query.NumberLit{Value: "2"}}}}, `1 IN (2)`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.NOTIN, X: &query.NumberLit{Value: "1"}, Y: &query.ExprList{Exprs: []query.Expr{&query.NumberLit{Value: "2"}}}}, `1 NOT IN (2)`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.LIKE, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 LIKE 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.NOTLIKE, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 NOT LIKE 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.GLOB, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 GLOB 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.NOTGLOB, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 NOT GLOB 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.MATCH, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 MATCH 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.NOTMATCH, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 NOT MATCH 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.REGEXP, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 REGEXP 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.NOTREGEXP, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 NOT REGEXP 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.AND, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 AND 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.OR, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 OR 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.JSON_EXTRACT_JSON, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 -> 2`)
	AssertExprStringer(t, &query.BinaryExpr{Op: query.JSON_EXTRACT_SQL, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 ->> 2`)
	AssertNodeStringerPanic(t, &query.BinaryExpr{}, `query.BinaryExpr.String(): invalid op ILLEGAL`)
}

func TestExprList_String(t *testing.T) {
	AssertExprStringer(t, &query.ExprList{Exprs: []query.Expr{&query.NullLit{}}}, `(NULL)`)
	AssertExprStringer(t, &query.ExprList{Exprs: []query.Expr{&query.NullLit{}, &query.NullLit{}}}, `(NULL, NULL)`)
}

func TestParser_ParseExpr(t *testing.T) {
	t.Run("Ident", func(t *testing.T) {
		AssertParseExpr(t, `fooBAR_123'`, &query.Ident{NamePos: pos(0), Name: `fooBAR_123`})
	})
	t.Run("StringLit", func(t *testing.T) {
		AssertParseExpr(t, `'foo bar'`, &query.StringLit{ValuePos: pos(0), Value: `foo bar`})
	})
	t.Run("BlobLit", func(t *testing.T) {
		AssertParseExpr(t, `x'0123'`, &query.BlobLit{ValuePos: pos(0), Value: `0123`})
	})
	t.Run("Integer", func(t *testing.T) {
		AssertParseExpr(t, `123`, &query.NumberLit{ValuePos: pos(0), Value: `123`})
	})
	t.Run("Float", func(t *testing.T) {
		AssertParseExpr(t, `123.456`, &query.NumberLit{ValuePos: pos(0), Value: `123.456`})
	})
	t.Run("Null", func(t *testing.T) {
		AssertParseExpr(t, `NULL`, &query.NullLit{Pos: pos(0)})
	})
	t.Run("Bool", func(t *testing.T) {
		AssertParseExpr(t, `true`, &query.BoolLit{ValuePos: pos(0), Value: true})
		AssertParseExpr(t, `false`, &query.BoolLit{ValuePos: pos(0), Value: false})
	})
	t.Run("UnaryExpr", func(t *testing.T) {
		AssertParseExpr(t, `-123`, &query.UnaryExpr{OpPos: pos(0), Op: query.MINUS, X: &query.NumberLit{ValuePos: pos(1), Value: `123`}})
		AssertParseExpr(t, `NOT foo`, &query.UnaryExpr{OpPos: pos(0), Op: query.NOT, X: &query.Ident{NamePos: pos(4), Name: "foo"}})
		AssertParseExpr(t, `~1`, &query.UnaryExpr{OpPos: pos(0), Op: query.BITNOT, X: &query.NumberLit{ValuePos: pos(1), Value: "1"}})
		AssertParseExprError(t, `-`, `1:1: expected expression, found 'EOF'`)
	})

	t.Run("BinaryExpr", func(t *testing.T) {
		AssertParseExpr(t, `1 + 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.PLUS,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 - 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.MINUS,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 * 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.STAR,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 / 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.SLASH,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 % 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.REM,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 || 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.CONCAT,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 << 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.LSHIFT,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 >> 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.RSHIFT,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 & 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.BITAND,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 | 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.BITOR,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 < 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.LT,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 <= 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.LE,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 > 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.GT,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 >= 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.GE,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 = 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.EQ,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 != 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.NE,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `(1 + 2)'`, &query.ParenExpr{
			Lparen: pos(0),
			X: &query.BinaryExpr{
				X:     &query.NumberLit{ValuePos: pos(1), Value: "1"},
				OpPos: pos(3), Op: query.PLUS,
				Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
			},
			Rparen: pos(6),
		})
		AssertParseExprError(t, `(`, `1:1: expected expression, found 'EOF'`)
		AssertParseExpr(t, `1 IS 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.IS,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 IS NOT 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.ISNOT,
			Y: &query.NumberLit{ValuePos: pos(9), Value: "2"},
		})
		AssertParseExpr(t, `1 LIKE 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.LIKE,
			Y: &query.NumberLit{ValuePos: pos(7), Value: "2"},
		})
		AssertParseExpr(t, `1 NOT LIKE 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.NOTLIKE,
			Y: &query.NumberLit{ValuePos: pos(11), Value: "2"},
		})
		AssertParseExpr(t, `1 GLOB 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.GLOB,
			Y: &query.NumberLit{ValuePos: pos(7), Value: "2"},
		})
		AssertParseExpr(t, `1 NOT GLOB 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.NOTGLOB,
			Y: &query.NumberLit{ValuePos: pos(11), Value: "2"},
		})
		AssertParseExpr(t, `1 REGEXP 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.REGEXP,
			Y: &query.NumberLit{ValuePos: pos(9), Value: "2"},
		})
		AssertParseExpr(t, `1 NOT REGEXP 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.NOTREGEXP,
			Y: &query.NumberLit{ValuePos: pos(13), Value: "2"},
		})
		AssertParseExpr(t, `1 MATCH 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.MATCH,
			Y: &query.NumberLit{ValuePos: pos(8), Value: "2"},
		})
		AssertParseExpr(t, `1 NOT MATCH 2'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.NOTMATCH,
			Y: &query.NumberLit{ValuePos: pos(12), Value: "2"},
		})
		AssertParseExprError(t, `1 NOT TABLE`, `1:7: expected IN, LIKE, GLOB, REGEXP, MATCH, BETWEEN, IS/NOT NULL, found 'TABLE'`)
		AssertParseExpr(t, `1 IN (2, 3)'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.IN,
			Y: &query.ExprList{
				Lparen: pos(5),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(6), Value: "2"},
					&query.NumberLit{ValuePos: pos(9), Value: "3"},
				},
				Rparen: pos(10),
			},
		})
		AssertParseExpr(t, `1 NOT IN (2, 3)'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.NOTIN,
			Y: &query.ExprList{
				Lparen: pos(9),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(10), Value: "2"},
					&query.NumberLit{ValuePos: pos(13), Value: "3"},
				},
				Rparen: pos(14),
			},
		})
		AssertParseExprError(t, `1 IN 2`, `1:6: expected left paren, found 2`)
		AssertParseExprError(t, `1 IN (`, `1:6: expected expression, found 'EOF'`)
		AssertParseExprError(t, `1 IN (2 3`, `1:9: expected comma or right paren, found 3`)
		AssertParseExpr(t, `1 -> 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.JSON_EXTRACT_JSON,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 ->> 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.JSON_EXTRACT_SQL,
			Y: &query.NumberLit{ValuePos: pos(6), Value: "2"},
		})
		AssertParseExprError(t, `1 BETWEEN`, `1:9: expected expression, found 'EOF'`)
		AssertParseExprError(t, `1 BETWEEN 2`, `1:11: expected range expression, found 'EOF'`)
		AssertParseExprError(t, `1 BETWEEN 2 + 3`, `1:15: expected range expression, found 'EOF'`)
		AssertParseExprError(t, `1 + `, `1:4: expected expression, found 'EOF'`)
	})
}

// AssertParseExpr asserts the value of the first parse of s.
func AssertParseExpr(tb testing.TB, str string, want query.Expr) {
	tb.Helper()

	exp, err := query.ParseExprString(str)
	assert.NoError(tb, err)

	// Check if it will work, or we need to convert to string first
	assert.Equal(tb, want, exp)
}

// AssertParseExprError asserts s parses to a given error string.
func AssertParseExprError(tb testing.TB, s string, want string) {
	tb.Helper()

	_, err := query.ParseExprString(s)
	assert.EqualError(tb, err, want)
}

func AssertNodeStringerPanic(tb testing.TB, node query.Node, msg string) {
	tb.Helper()

	var r interface{}
	func() {
		defer func() { r = recover() }()
		_ = node.String()
	}()

	assert.NotNil(tb, r)
	assert.Contains(tb, r, msg)
}

func pos(offset int) query.Pos {
	return query.Pos{Offset: offset, Line: 1, Column: offset + 1}
}
