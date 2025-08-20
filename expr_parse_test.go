package query_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sbchaos/query"
)

func TestIdent_String(t *testing.T) {
	AssertExprStringer(t, &query.Ident{Name: "foo", Tok: query.QIDENT}, `"foo"`)
	AssertExprStringer(t, &query.Ident{Name: "foo \" bar", Tok: query.QIDENT}, `"foo "" bar"`)
}

func TestMultiPartIdent_String(t *testing.T) {
	AssertExprStringer(t, &query.MultiPartIdent{
		First:  &query.Ident{Name: "project"},
		Second: &query.Ident{Name: "schema"},
		Name:   &query.Ident{Name: "job_table"},
	}, `project.schema.job_table`)
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
	AssertExprStringer(t, &query.BinaryExpr{Op: query.EQN, X: &query.NumberLit{Value: "1"}, Y: &query.NumberLit{Value: "2"}}, `1 <=> 2`)
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

func TestCastExpr_String(t *testing.T) {
	AssertExprStringer(t, &query.CastExpr{X: &query.NumberLit{Value: "1"}, Type: &query.Type{Name: &query.Ident{Name: "INTEGER"}}}, `CAST(1 AS INTEGER)`)
}

func TestExprList_String(t *testing.T) {
	AssertExprStringer(t, &query.ExprList{Exprs: []query.Expr{&query.NullLit{}}}, `(NULL)`)
	AssertExprStringer(t, &query.ExprList{Exprs: []query.Expr{&query.NullLit{}, &query.NullLit{}}}, `(NULL, NULL)`)
}

func TestQualifiedRef_String(t *testing.T) {
	AssertExprStringer(t, &query.QualifiedRef{
		Name: &query.MultiPartIdent{
			First: &query.Ident{Name: "tbl", Tok: query.QIDENT},
			Name:  &query.Ident{Name: "col", Tok: query.QIDENT}}}, `"tbl"."col"`)
	AssertExprStringer(t, &query.QualifiedRef{
		Name: &query.MultiPartIdent{
			Name: &query.Ident{Name: "tbl", Tok: query.QIDENT},
		},
		Star: pos(0)}, `"tbl".*`)
}

func TestCall_String(t *testing.T) {
	AssertExprStringer(t, &query.Call{Name: &query.MultiPartIdent{Name: &query.Ident{Name: "foo", Tok: query.IDENT}}}, `foo()`)
	AssertExprStringer(t, &query.Call{Name: &query.MultiPartIdent{Name: &query.Ident{Name: "foo", Tok: query.IDENT}}, Star: pos(0)}, `foo(*)`)

	AssertExprStringer(t, &query.Call{
		Name:     &query.MultiPartIdent{Name: &query.Ident{Name: "foo", Tok: query.IDENT}},
		Distinct: pos(0),
		Args: []query.Expr{
			&query.NullLit{},
			&query.NullLit{},
		},
	}, `foo(DISTINCT NULL, NULL)`)
}

func TestParser_ParseExpr(t *testing.T) {
	t.Run("Ident", func(t *testing.T) {
		AssertParseExpr(t, `fooBAR_123'`, &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(0), Name: `fooBAR_123`, Tok: query.IDENT}})
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
		AssertParseExpr(t, `NOT foo`, &query.UnaryExpr{OpPos: pos(0), Op: query.NOT, X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(4), Name: "foo", Tok: query.IDENT}}})
		AssertParseExpr(t, `~1`, &query.UnaryExpr{OpPos: pos(0), Op: query.BITNOT, X: &query.NumberLit{ValuePos: pos(1), Value: "1"}})
		AssertParseExprError(t, `-`, `1:1: expected expression, found 'EOF'`)
	})
	t.Run("MultiPartIdent", func(t *testing.T) {
		AssertParseExpr(t, `"tbl"."col"`, &query.MultiPartIdent{
			First: &query.Ident{NamePos: pos(0), Name: "tbl", Tok: query.QIDENT},
			Dot1:  pos(5),
			Name:  &query.Ident{NamePos: pos(6), Name: "col", Tok: query.QIDENT},
		})
		AssertParseExpr(t, `proj.schema.my_table`, &query.MultiPartIdent{
			First:  &query.Ident{NamePos: pos(0), Name: "proj", Tok: query.IDENT},
			Dot1:   pos(4),
			Second: &query.Ident{NamePos: pos(5), Name: "schema", Tok: query.IDENT},
			Dot2:   pos(11),
			Name:   &query.Ident{NamePos: pos(12), Name: "my_table", Tok: query.IDENT},
		})
		AssertParseExpr(t, "`proj`.schema.my_table", &query.MultiPartIdent{
			First:  &query.Ident{NamePos: pos(0), Name: "proj", Tok: query.TSTRING},
			Dot1:   pos(6),
			Second: &query.Ident{NamePos: pos(7), Name: "schema", Tok: query.IDENT},
			Dot2:   pos(13),
			Name:   &query.Ident{NamePos: pos(14), Name: "my_table", Tok: query.IDENT},
		})
		AssertParseExpr(t, "`proj`.`schema`.`my_table`", &query.MultiPartIdent{
			First:  &query.Ident{NamePos: pos(0), Name: "proj", Tok: query.TSTRING},
			Dot1:   pos(6),
			Second: &query.Ident{NamePos: pos(7), Name: "schema", Tok: query.TSTRING},
			Dot2:   pos(15),
			Name:   &query.Ident{NamePos: pos(16), Name: "my_table", Tok: query.TSTRING},
		})
	})
	t.Run("QualifiedRef", func(t *testing.T) {
		AssertParseExpr(t, `tbl.*`, &query.QualifiedRef{
			Name: &query.MultiPartIdent{
				Name: &query.Ident{
					NamePos: pos(0),
					Name:    "tbl",
					Tok:     query.IDENT,
				},
			},
			Dot:  pos(3),
			Star: pos(4),
		})
		AssertParseExprError(t, `tbl.`, `1:4: expected identifier, found 'EOF'`)
	})

	t.Run("BinaryExpr", func(t *testing.T) {
		AssertParseExpr(t, `1 + 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.PLUS,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 - 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.MINUS,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 * 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.STAR,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 / 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.SLASH,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 % 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.REM,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 || 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.CONCAT,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 << 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.LSHIFT,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 >> 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.RSHIFT,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 & 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.BITAND,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 | 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.BITOR,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 < 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.LT,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 <= 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.LE,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 <=> 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.EQN,
			Y: &query.NumberLit{ValuePos: pos(6), Value: "2"},
		})
		AssertParseExpr(t, `1 > 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.GT,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 >= 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.GE,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `1 = 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.EQ,
			Y: &query.NumberLit{ValuePos: pos(4), Value: "2"},
		})
		AssertParseExpr(t, `1 != 2`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.NE,
			Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
		})
		AssertParseExpr(t, `(1 + 2)`, &query.ParenExpr{
			Lparen: pos(0),
			X: &query.BinaryExpr{
				X:     &query.NumberLit{ValuePos: pos(1), Value: "1"},
				OpPos: pos(3), Op: query.PLUS,
				Y: &query.NumberLit{ValuePos: pos(5), Value: "2"},
			},
			Rparen: pos(6),
		})
		AssertParseExpr(t, `{{ .Count }} != 2`, &query.BinaryExpr{
			X:     &query.TemplateStr{TmplPos: pos(1), Template: " .Count "},
			OpPos: pos(13), Op: query.NE,
			Y: &query.NumberLit{ValuePos: pos(16), Value: "2"},
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
		AssertParseExpr(t, `1 BETWEEN 2 AND 3'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.BETWEEN,
			Y: &query.Range{
				X:   &query.NumberLit{ValuePos: pos(10), Value: "2"},
				And: pos(12),
				Y:   &query.NumberLit{ValuePos: pos(16), Value: "3"},
			},
		})
		AssertParseExpr(t, `1 NOT BETWEEN 2 AND 3'`, &query.BinaryExpr{
			X:     &query.NumberLit{ValuePos: pos(0), Value: "1"},
			OpPos: pos(2), Op: query.NOTBETWEEN,
			Y: &query.Range{
				X:   &query.NumberLit{ValuePos: pos(14), Value: "2"},
				And: pos(16),
				Y:   &query.NumberLit{ValuePos: pos(20), Value: "3"},
			},
		})
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
	t.Run("Call", func(t *testing.T) {
		AssertParseExpr(t, `sum()`, &query.Call{
			Name: &query.MultiPartIdent{
				Name: &query.Ident{
					NamePos: pos(0),
					Name:    "sum",
					Tok:     query.IDENT,
				},
			},
			Lparen: pos(3),
			Rparen: pos(4),
		})
		AssertParseExpr(t, `project.default.sum()`, &query.Call{
			Name: &query.MultiPartIdent{
				First:  &query.Ident{NamePos: pos(0), Name: "project", Tok: query.IDENT},
				Dot1:   pos(7),
				Second: &query.Ident{NamePos: pos(8), Name: "default", Tok: query.IDENT},
				Dot2:   pos(15),
				Name:   &query.Ident{NamePos: pos(16), Name: "sum", Tok: query.IDENT},
			},
			Lparen: pos(19),
			Rparen: pos(20),
		})
		AssertParseExpr(t, `sum(*)`, &query.Call{
			Name: &query.MultiPartIdent{
				Name: &query.Ident{NamePos: pos(0), Name: "sum", Tok: query.IDENT},
			},
			Lparen: pos(3),
			Star:   pos(4),
			Rparen: pos(5),
		})
		AssertParseExpr(t, `sum(foo, 123)`, &query.Call{
			Name: &query.MultiPartIdent{
				Name: &query.Ident{NamePos: pos(0), Name: "sum", Tok: query.IDENT},
			},
			Lparen: pos(3),
			Args: []query.Expr{
				&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(4), Name: "foo", Tok: query.IDENT}},
				&query.NumberLit{ValuePos: pos(9), Value: "123"},
			},
			Rparen: pos(12),
		})
		AssertParseExpr(t, `sum(distinct 'foo')`, &query.Call{
			Name: &query.MultiPartIdent{
				Name: &query.Ident{NamePos: pos(0), Name: "sum", Tok: query.IDENT},
			},
			Lparen:   pos(3),
			Distinct: pos(4),
			Args: []query.Expr{
				&query.StringLit{ValuePos: pos(13), Value: "foo"},
			},
			Rparen: pos(18),
		})
		AssertParseExpr(t, `sum(1, sum(2, 3))`, &query.Call{
			Name: &query.MultiPartIdent{
				Name: &query.Ident{NamePos: pos(0), Name: "sum", Tok: query.IDENT},
			},
			Lparen: pos(3),
			Args: []query.Expr{
				&query.NumberLit{ValuePos: pos(4), Value: "1"},
				&query.Call{
					Name: &query.MultiPartIdent{
						Name: &query.Ident{NamePos: pos(7), Name: "sum", Tok: query.IDENT},
					},
					Lparen: pos(10),
					Args: []query.Expr{
						&query.NumberLit{ValuePos: pos(11), Value: "2"},
						&query.NumberLit{ValuePos: pos(14), Value: "3"},
					},
					Rparen: pos(15),
				},
			},
			Rparen: pos(16),
		})
		AssertParseExpr(t, `sum(sum(1,2), sum(3, 4))`, &query.Call{
			Name: &query.MultiPartIdent{
				Name: &query.Ident{NamePos: pos(0), Name: "sum", Tok: query.IDENT},
			},
			Lparen: pos(3),
			Args: []query.Expr{
				&query.Call{
					Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(4), Name: "sum", Tok: query.IDENT}},
					Lparen: pos(7),
					Args: []query.Expr{
						&query.NumberLit{ValuePos: pos(8), Value: "1"},
						&query.NumberLit{ValuePos: pos(10), Value: "2"},
					},
					Rparen: pos(11),
				}, &query.Call{
					Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "sum", Tok: query.IDENT}},
					Lparen: pos(17),
					Args: []query.Expr{
						&query.NumberLit{ValuePos: pos(18), Value: "3"},
						&query.NumberLit{ValuePos: pos(21), Value: "4"},
					},
					Rparen: pos(22),
				},
			},
			Rparen: pos(23),
		})
		AssertParseExprError(t, `sum(`, `1:4: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum(*`, `1:5: expected right paren, found 'EOF'`)
		AssertParseExprError(t, `sum(foo foo`, `1:9: expected comma or right paren, found foo`)
	})
	t.Run("Case", func(t *testing.T) {
		AssertParseExpr(t, `CASE 1 WHEN 2 THEN 3 WHEN 4 THEN 5 ELSE 6 END`, &query.CaseExpr{
			Case:    pos(0),
			Operand: &query.NumberLit{ValuePos: pos(5), Value: "1"},
			Blocks: []*query.CaseBlock{
				{
					When:      pos(7),
					Condition: &query.NumberLit{ValuePos: pos(12), Value: "2"},
					Then:      pos(14),
					Body:      &query.NumberLit{ValuePos: pos(19), Value: "3"},
				},
				{
					When:      pos(21),
					Condition: &query.NumberLit{ValuePos: pos(26), Value: "4"},
					Then:      pos(28),
					Body:      &query.NumberLit{ValuePos: pos(33), Value: "5"},
				},
			},
			Else:     pos(35),
			ElseExpr: &query.NumberLit{ValuePos: pos(40), Value: "6"},
			End:      pos(42),
		})
		AssertParseExpr(t, `CASE WHEN 1 THEN 2 END`, &query.CaseExpr{
			Case: pos(0),
			Blocks: []*query.CaseBlock{
				{
					When:      pos(5),
					Condition: &query.NumberLit{ValuePos: pos(10), Value: "1"},
					Then:      pos(12),
					Body:      &query.NumberLit{ValuePos: pos(17), Value: "2"},
				},
			},
			End: pos(19),
		})
		AssertParseExpr(t, `CASE WHEN 1 IS NULL THEN 2 END`, &query.CaseExpr{
			Case: pos(0),
			Blocks: []*query.CaseBlock{
				{
					When: pos(5),
					Condition: &query.Null{
						X:     &query.NumberLit{ValuePos: pos(10), Value: "1"},
						Op:    query.ISNULL,
						OpPos: pos(12),
					},
					Then: pos(20),
					Body: &query.NumberLit{ValuePos: pos(25), Value: "2"},
				},
			},
			End: pos(27),
		})
		AssertParseExprError(t, `CASE`, `1:4: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE 1`, `1:6: expected WHEN, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN`, `1:9: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1`, `1:11: expected THEN, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN`, `1:16: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN 2`, `1:18: expected WHEN, ELSE or END, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN 2 ELSE`, `1:23: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN 2 ELSE 3`, `1:25: expected END, found 'EOF'`)
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
