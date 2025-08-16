package query_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sbchaos/query"
)

func TestParser_ParseStatement(t *testing.T) {
	t.Run("ErrNoStatement", func(t *testing.T) {
		AssertParseStatementError(t, `123`, `1:1: expected statement, found 123`)
	})

	t.Run("Select", func(t *testing.T) {
		AssertParseStatement(t, `SELECT 5678`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.NumberLit{ValuePos: pos(7), Value: "5678"},
				},
			},
		})

		AssertParseStatement(t, `SELECT 1 NOT NULL`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.Null{
						X:     &query.NumberLit{ValuePos: pos(7), Value: "1"},
						OpPos: pos(9),
						Op:    query.NOTNULL,
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT 1 NOTNULL`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.Null{
						X:     &query.NumberLit{ValuePos: pos(7), Value: "1"},
						OpPos: pos(9),
						Op:    query.NOTNULL,
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT 1 IS NULL`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.Null{
						X:     &query.NumberLit{ValuePos: pos(7), Value: "1"},
						OpPos: pos(9),
						Op:    query.ISNULL,
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT 1 ISNULL`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.Null{
						X:     &query.NumberLit{ValuePos: pos(7), Value: "1"},
						OpPos: pos(9),
						Op:    query.ISNULL,
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT 1 IS NULL AND false`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.BinaryExpr{
						X: &query.Null{
							X:     &query.NumberLit{ValuePos: pos(7), Value: "1"},
							OpPos: pos(9),
							Op:    query.ISNULL,
						},
						OpPos: pos(17),
						Op:    query.AND,
						Y:     &query.BoolLit{ValuePos: pos(21), Value: false},
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT * FROM tbl`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.QualifiedTableName{
				Name: &query.Ident{NamePos: pos(14), Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT * FROM main.tbl;`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.QualifiedTableName{
				Schema: &query.Ident{NamePos: pos(14), Name: "main"},
				Dot:    pos(18),
				Name:   &query.Ident{NamePos: pos(19), Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT DISTINCT * FROM tbl`, &query.SelectStatement{
			Select:   pos(0),
			Distinct: pos(7),
			Columns: []*query.ResultColumn{
				{Star: pos(16)},
			},
			From: pos(18),
			Source: &query.QualifiedTableName{
				Name: &query.Ident{NamePos: pos(23), Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT ALL * FROM tbl`, &query.SelectStatement{
			Select: pos(0),
			All:    pos(7),
			Columns: []*query.ResultColumn{
				{Star: pos(11)},
			},
			From: pos(13),
			Source: &query.QualifiedTableName{
				Name: &query.Ident{NamePos: pos(18), Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT * FROM tbl tbl2`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.QualifiedTableName{
				Name:  &query.Ident{NamePos: pos(14), Name: "tbl"},
				Alias: &query.Ident{NamePos: pos(18), Name: "tbl2"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl AS tbl2`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.QualifiedTableName{
				Name:  &query.Ident{NamePos: pos(14), Name: "tbl"},
				As:    pos(18),
				Alias: &query.Ident{NamePos: pos(21), Name: "tbl2"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM main.tbl AS tbl2`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.QualifiedTableName{
				Schema: &query.Ident{NamePos: pos(14), Name: "main"},
				Dot:    pos(18),
				Name:   &query.Ident{NamePos: pos(19), Name: "tbl"},
				As:     pos(23),
				Alias:  &query.Ident{NamePos: pos(26), Name: "tbl2"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl INDEXED BY idx`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.QualifiedTableName{
				Name:      &query.Ident{NamePos: pos(14), Name: "tbl"},
				Indexed:   pos(18),
				IndexedBy: pos(26),
				Index:     &query.Ident{NamePos: pos(29), Name: "idx"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl NOT INDEXED`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.QualifiedTableName{
				Name:       &query.Ident{NamePos: pos(14), Name: "tbl"},
				Not:        pos(18),
				NotIndexed: pos(22),
			},
		})

		AssertParseStatement(t, `SELECT * FROM (SELECT *) AS tbl`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.ParenSource{
				Lparen: pos(14),
				X: &query.SelectStatement{
					Select: pos(15),
					Columns: []*query.ResultColumn{
						{Star: pos(22)},
					},
				},
				Rparen: pos(23),
				As:     pos(25),
				Alias:  &query.Ident{NamePos: pos(28), Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT * FROM (VALUES (NULL))`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.ParenSource{
				Lparen: pos(14),
				X: &query.SelectStatement{
					Values: pos(15),
					ValueLists: []*query.ExprList{
						{
							Lparen: pos(22),
							Exprs: []query.Expr{
								&query.NullLit{Pos: pos(23)},
							},
							Rparen: pos(27),
						},
					},
				},
				Rparen: pos(28),
			},
		})
		AssertParseStatement(t, `SELECT * FROM ( t ) a`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.ParenSource{
				Lparen: pos(14),
				X: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(16), Name: "t"},
				},
				Rparen: pos(18),
				Alias:  &query.Ident{NamePos: pos(20), Name: "a"},
			},
		})

		AssertParseStatement(t, `SELECT * FROM foo, bar`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.JoinClause{
				X: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &query.JoinOperator{Comma: pos(17)},
				Y: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(19), Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo JOIN bar`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.JoinClause{
				X: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &query.JoinOperator{Join: pos(18)},
				Y: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(23), Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo NATURAL JOIN bar`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.JoinClause{
				X: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &query.JoinOperator{Natural: pos(18), Join: pos(26)},
				Y: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(31), Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo INNER JOIN bar ON true`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.JoinClause{
				X: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &query.JoinOperator{Inner: pos(18), Join: pos(24)},
				Y: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(29), Name: "bar"},
				},
				Constraint: &query.OnConstraint{
					On: pos(33),
					X:  &query.BoolLit{ValuePos: pos(36), Value: true},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo LEFT JOIN bar USING (x, y)`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.JoinClause{
				X: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &query.JoinOperator{Left: pos(18), Join: pos(23)},
				Y: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(28), Name: "bar"},
				},
				Constraint: &query.UsingConstraint{
					Using:  pos(32),
					Lparen: pos(38),
					Columns: []*query.Ident{
						{NamePos: pos(39), Name: "x"},
						{NamePos: pos(42), Name: "y"},
					},
					Rparen: pos(43),
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM X INNER JOIN Y ON true INNER JOIN Z ON false`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.JoinClause{
				X: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(14), Name: "X"},
				},
				Operator: &query.JoinOperator{Inner: pos(16), Join: pos(22)},
				Y: &query.JoinClause{
					X: &query.QualifiedTableName{
						Name: &query.Ident{NamePos: pos(27), Name: "Y"},
					},
					Operator: &query.JoinOperator{Inner: pos(37), Join: pos(43)},
					Y: &query.QualifiedTableName{
						Name: &query.Ident{NamePos: pos(48), Name: "Z"},
					},
					Constraint: &query.OnConstraint{
						On: pos(50),
						X:  &query.BoolLit{ValuePos: pos(53), Value: false},
					},
				},
				Constraint: &query.OnConstraint{
					On: pos(29),
					X:  &query.BoolLit{ValuePos: pos(32), Value: true},
				},
			},
		})

		AssertParseStatement(t, `SELECT * FROM foo LEFT OUTER JOIN bar`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.JoinClause{
				X: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &query.JoinOperator{Left: pos(18), Outer: pos(23), Join: pos(29)},
				Y: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(34), Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo CROSS JOIN bar`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.JoinClause{
				X: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(14), Name: "foo"},
				},
				Operator: &query.JoinOperator{Cross: pos(18), Join: pos(24)},
				Y: &query.QualifiedTableName{
					Name: &query.Ident{NamePos: pos(29), Name: "bar"},
				},
			},
		})

		AssertParseStatement(t, `WITH cte (foo, bar) AS (SELECT baz), xxx AS (SELECT yyy) SELECT bat`, &query.SelectStatement{
			WithClause: &query.WithClause{
				With: pos(0),
				CTEs: []*query.CTE{
					{
						TableName:     &query.Ident{NamePos: pos(5), Name: "cte"},
						ColumnsLparen: pos(9),
						Columns: []*query.Ident{
							{NamePos: pos(10), Name: "foo"},
							{NamePos: pos(15), Name: "bar"},
						},
						ColumnsRparen: pos(18),
						As:            pos(20),
						SelectLparen:  pos(23),
						Select: &query.SelectStatement{
							Select: pos(24),
							Columns: []*query.ResultColumn{
								{Expr: &query.Ident{NamePos: pos(31), Name: "baz"}},
							},
						},
						SelectRparen: pos(34),
					},
					{
						TableName:    &query.Ident{NamePos: pos(37), Name: "xxx"},
						As:           pos(41),
						SelectLparen: pos(44),
						Select: &query.SelectStatement{
							Select: pos(45),
							Columns: []*query.ResultColumn{
								{Expr: &query.Ident{NamePos: pos(52), Name: "yyy"}},
							},
						},
						SelectRparen: pos(55),
					},
				},
			},
			Select: pos(57),
			Columns: []*query.ResultColumn{
				{Expr: &query.Ident{NamePos: pos(64), Name: "bat"}},
			},
		})
		AssertParseStatement(t, `WITH RECURSIVE cte AS (SELECT foo) SELECT bar`, &query.SelectStatement{
			WithClause: &query.WithClause{
				With:      pos(0),
				Recursive: pos(5),
				CTEs: []*query.CTE{
					{
						TableName:    &query.Ident{NamePos: pos(15), Name: "cte"},
						As:           pos(19),
						SelectLparen: pos(22),
						Select: &query.SelectStatement{
							Select: pos(23),
							Columns: []*query.ResultColumn{
								{Expr: &query.Ident{NamePos: pos(30), Name: "foo"}},
							},
						},
						SelectRparen: pos(33),
					},
				},
			},
			Select: pos(35),
			Columns: []*query.ResultColumn{
				{Expr: &query.Ident{NamePos: pos(42), Name: "bar"}},
			},
		})

		AssertParseStatement(t, `SELECT * WHERE true`, &query.SelectStatement{
			Select:    pos(0),
			Columns:   []*query.ResultColumn{{Star: pos(7)}},
			Where:     pos(9),
			WhereExpr: &query.BoolLit{ValuePos: pos(15), Value: true},
		})

		AssertParseStatement(t, `SELECT 1 WHERE true AND true`, &query.SelectStatement{
			Select:  pos(0),
			Columns: []*query.ResultColumn{{Expr: &query.NumberLit{ValuePos: pos(7), Value: "1"}}},
			Where:   pos(9),
			WhereExpr: &query.BinaryExpr{
				X:     &query.BoolLit{ValuePos: pos(15), Value: true},
				OpPos: pos(20),
				Op:    query.AND,
				Y:     &query.BoolLit{ValuePos: pos(24), Value: true},
			},
		})

		AssertParseStatement(t, `SELECT * GROUP BY foo, bar`, &query.SelectStatement{
			Select:  pos(0),
			Columns: []*query.ResultColumn{{Star: pos(7)}},
			Group:   pos(9),
			GroupBy: pos(15),
			GroupByExprs: []query.Expr{
				&query.Ident{NamePos: pos(18), Name: "foo"},
				&query.Ident{NamePos: pos(23), Name: "bar"},
			},
		})
		AssertParseStatement(t, `SELECT * GROUP BY foo HAVING true`, &query.SelectStatement{
			Select:  pos(0),
			Columns: []*query.ResultColumn{{Star: pos(7)}},
			Group:   pos(9),
			GroupBy: pos(15),
			GroupByExprs: []query.Expr{
				&query.Ident{NamePos: pos(18), Name: "foo"},
			},
			Having:     pos(22),
			HavingExpr: &query.BoolLit{ValuePos: pos(29), Value: true},
		})
		AssertParseStatement(t, `SELECT * WINDOW win1 AS (), win2 AS ()`, &query.SelectStatement{
			Select:  pos(0),
			Columns: []*query.ResultColumn{{Star: pos(7)}},
			Window:  pos(9),
			Windows: []*query.Window{
				{
					Name: &query.Ident{NamePos: pos(16), Name: "win1"},
					As:   pos(21),
					Definition: &query.WindowDefinition{
						Lparen: pos(24),
						Rparen: pos(25),
					},
				},
				{
					Name: &query.Ident{NamePos: pos(28), Name: "win2"},
					As:   pos(33),
					Definition: &query.WindowDefinition{
						Lparen: pos(36),
						Rparen: pos(37),
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT * ORDER BY foo ASC, bar DESC`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			Order:   pos(9),
			OrderBy: pos(15),
			OrderingTerms: []*query.OrderingTerm{
				{X: &query.Ident{NamePos: pos(18), Name: "foo"}, Asc: pos(22)},
				{X: &query.Ident{NamePos: pos(27), Name: "bar"}, Desc: pos(31)},
			},
		})

		AssertParseStatement(t, `SELECT * LIMIT 1`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			Limit:     pos(9),
			LimitExpr: &query.NumberLit{ValuePos: pos(15), Value: "1"},
		})
		AssertParseStatement(t, `SELECT * LIMIT 1 OFFSET 2`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			Limit:      pos(9),
			LimitExpr:  &query.NumberLit{ValuePos: pos(15), Value: "1"},
			Offset:     pos(17),
			OffsetExpr: &query.NumberLit{ValuePos: pos(24), Value: "2"},
		})
		AssertParseStatement(t, `SELECT * LIMIT 1, 2`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			Limit:       pos(9),
			LimitExpr:   &query.NumberLit{ValuePos: pos(15), Value: "1"},
			OffsetComma: pos(16),
			OffsetExpr:  &query.NumberLit{ValuePos: pos(18), Value: "2"},
		})
		AssertParseStatement(t, `SELECT shop_uuid, price_range FROM merchant_price WHERE sale_date = '{{ .DSTART | Date }}'`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Expr: &query.Ident{NamePos: pos(7), Name: "shop_uuid"}},
				{Expr: &query.Ident{NamePos: pos(18), Name: "price_range"}},
			},
			From: pos(30),
			Source: &query.QualifiedTableName{
				Name: &query.Ident{NamePos: pos(35), Name: "merchant_price"},
			},
			Where: pos(50),
			WhereExpr: &query.BinaryExpr{
				X:     &query.Ident{NamePos: pos(56), Name: "sale_date"},
				OpPos: pos(66),
				Op:    query.EQ,
				Y:     &query.StringLit{ValuePos: pos(68), Value: "{{ .DSTART | Date }}"},
			},
		})
		AssertParseStatement(t, `SELECT * UNION SELECT * ORDER BY foo`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			Union: pos(9),
			Compound: &query.SelectStatement{
				Select: pos(15),
				Columns: []*query.ResultColumn{
					{Star: pos(22)},
				},
			},
			Order:   pos(24),
			OrderBy: pos(30),
			OrderingTerms: []*query.OrderingTerm{
				{X: &query.Ident{NamePos: pos(33), Name: "foo"}},
			},
		})
		AssertParseStatement(t, `SELECT * UNION ALL SELECT *`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			Union:    pos(9),
			UnionAll: pos(15),
			Compound: &query.SelectStatement{
				Select: pos(19),
				Columns: []*query.ResultColumn{
					{Star: pos(26)},
				},
			},
		})
		AssertParseStatement(t, `SELECT * INTERSECT SELECT *`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			Intersect: pos(9),
			Compound: &query.SelectStatement{
				Select: pos(19),
				Columns: []*query.ResultColumn{
					{Star: pos(26)},
				},
			},
		})
		AssertParseStatement(t, `SELECT * EXCEPT SELECT *`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			Except: pos(9),
			Compound: &query.SelectStatement{
				Select: pos(16),
				Columns: []*query.ResultColumn{
					{Star: pos(23)},
				},
			},
		})

		AssertParseStatement(t, `VALUES (1, 2), (3, 4)`, &query.SelectStatement{
			Values: pos(0),
			ValueLists: []*query.ExprList{
				{
					Lparen: pos(7),
					Exprs: []query.Expr{
						&query.NumberLit{ValuePos: pos(8), Value: "1"},
						&query.NumberLit{ValuePos: pos(11), Value: "2"},
					},
					Rparen: pos(12),
				},
				{
					Lparen: pos(15),
					Exprs: []query.Expr{
						&query.NumberLit{ValuePos: pos(16), Value: "3"},
						&query.NumberLit{ValuePos: pos(19), Value: "4"},
					},
					Rparen: pos(20),
				},
			},
		})

		AssertParseStatement(t, `SELECT rowid FROM foo`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.Ident{
						NamePos: pos(7),
						Name:    "rowid",
					},
				},
			},
			From: pos(13),
			Source: &query.QualifiedTableName{
				Name: &query.Ident{
					NamePos: pos(18),
					Name:    "foo",
				},
			},
		})

		AssertParseStatement(t, `SELECT rowid FROM foo ORDER BY rowid`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.Ident{
						NamePos: pos(7),
						Name:    "rowid",
					},
				},
			},
			From: pos(13),
			Source: &query.QualifiedTableName{
				Name: &query.Ident{
					NamePos: pos(18),
					Name:    "foo",
				},
			},
			Order:   pos(22),
			OrderBy: pos(28),
			OrderingTerms: []*query.OrderingTerm{
				{
					X: &query.Ident{
						NamePos: pos(31),
						Name:    "rowid",
					},
				},
			},
		})

		AssertParseStatement(t, `SELECT CURRENT_TIMESTAMP FROM foo`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.Ident{
						NamePos: pos(7),
						Name:    "CURRENT_TIMESTAMP",
					},
				},
			},
			From: pos(25),
			Source: &query.QualifiedTableName{
				Name: &query.Ident{
					NamePos: pos(30),
					Name:    "foo",
				},
			},
		})

		AssertParseStatement(t, `SELECT * FROM generate_series(1,3)`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Star: pos(7),
				},
			},
			From: pos(9),
			Source: &query.QualifiedTableFunctionName{
				Name: &query.Ident{
					NamePos: pos(14),
					Name:    "generate_series",
				},
				Lparen: pos(29),
				Args: []query.Expr{
					&query.NumberLit{
						ValuePos: pos(30),
						Value:    "1",
					},
					&query.NumberLit{
						ValuePos: pos(32),
						Value:    "3",
					},
				},
				Rparen: pos(33),
			},
		})

		AssertParseStatementError(t, `WITH `, `1:5: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte`, `1:8: expected AS, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte (`, `1:10: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte (foo`, `1:13: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte (foo)`, `1:14: expected AS, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte AS`, `1:11: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte AS (`, `1:13: expected SELECT or VALUES, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte AS (SELECT foo`, `1:23: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `WITH cte AS (SELECT foo)`, `1:24: expected SELECT, VALUES, INSERT, REPLACE, UPDATE, or DELETE, found 'EOF'`)
		AssertParseStatementError(t, `SELECT `, `1:7: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT 1+`, `1:9: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo,`, `1:11: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo AS`, `1:13: expected column alias, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo FROM`, `1:15: expected table name or left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo FROM foo INDEXED`, `1:27: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo FROM foo INDEXED BY`, `1:30: expected index name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo FROM foo NOT`, `1:23: expected INDEXED, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo INNER`, `1:23: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo CROSS`, `1:23: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo NATURAL`, `1:25: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo LEFT`, `1:22: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo LEFT OUTER`, `1:28: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo,`, `1:18: expected table name or left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar ON`, `1:29: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING`, `1:32: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING (`, `1:34: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING (x`, `1:35: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING (x,`, `1:36: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (`, `1:15: expected table name or left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM ((`, `1:16: expected table name or left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (SELECT`, `1:21: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (tbl`, `1:18: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (SELECT *) AS`, `1:27: expected table alias, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo AS`, `1:20: expected table alias, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo WHERE`, `1:16: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * GROUP`, `1:14: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * GROUP BY`, `1:17: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * GROUP BY foo bar`, `1:23: expected semicolon or EOF, found bar`)
		AssertParseStatementError(t, `SELECT * GROUP BY foo HAVING`, `1:28: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW`, `1:15: expected window name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW win1`, `1:20: expected AS, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW win1 AS`, `1:23: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW win1 AS (`, `1:25: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * WINDOW win1 AS () win2`, `1:28: expected semicolon or EOF, found win2`)
		AssertParseStatementError(t, `SELECT * ORDER`, `1:14: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * ORDER BY`, `1:17: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * ORDER BY 1,`, `1:20: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * LIMIT`, `1:14: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * LIMIT 1,`, `1:17: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * LIMIT 1 OFFSET`, `1:23: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `VALUES`, `1:6: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `VALUES (`, `1:8: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `VALUES (1`, `1:9: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `VALUES (1,`, `1:10: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * UNION`, `1:14: expected SELECT or VALUES, found 'EOF'`)
	})
	t.Run("Set", func(t *testing.T) {
		AssertParseStatement(t, `set odps.sql.submit.mode=script;`, &query.SetStatement{
			Set:   pos(0),
			Key:   "odps.sql.submit.mode",
			Equal: pos(24),
			Value: "script",
		})
		AssertParseStatement(t, `set odps.sql.groupby.orderby.position.alias=true;`, &query.SetStatement{
			Set:   pos(0),
			Key:   "odps.sql.groupby.orderby.position.alias",
			Equal: pos(43),
			Value: "true",
		})
	})
	t.Run("Variable", func(t *testing.T) {
		AssertParseStatement(t, `@start_date Date;`, &query.DeclarationStatement{
			Name: &query.Ident{Name: "@start_date", NamePos: pos(0)},
			Type: &query.Ident{NamePos: pos(12), Name: "Date"},
		})
		AssertParseStatement(t, `@start_date := '{{ .DSTART | Date }}';`, &query.DeclarationStatement{
			Name:  &query.Ident{Name: "@start_date", NamePos: pos(0)},
			Value: &query.StringLit{ValuePos: pos(15), Value: "{{ .DSTART | Date }}"},
		})
		AssertParseStatement(t, `@modified_timestamp := CURRENT_TIMESTAMP();`, &query.DeclarationStatement{
			Name: &query.Ident{Name: "@modified_timestamp", NamePos: pos(0)},
			Value: &query.Call{
				Name:   &query.Ident{NamePos: pos(23), Name: "CURRENT_TIMESTAMP"},
				Lparen: pos(40),
				Rparen: pos(41),
			},
		})
		AssertParseStatement(t, `@tmp := SELECT data_date, shop_id FROM shop;`, &query.DeclarationStatement{
			Name: &query.Ident{Name: "@tmp", NamePos: pos(0)},
			Value: query.SelectExpr{
				SelectStatement: &query.SelectStatement{
					Select: pos(8),
					Columns: []*query.ResultColumn{
						{
							Expr: &query.Ident{
								NamePos: pos(15),
								Name:    "data_date",
							},
						},
						{
							Expr: &query.Ident{
								NamePos: pos(26),
								Name:    "shop_id",
							},
						},
					},
					From: pos(34),
					Source: &query.QualifiedTableName{
						Name: &query.Ident{NamePos: pos(39), Name: "shop"},
					},
				},
			},
		})
	})
}

// AssertParseStatementError asserts s parses to a given error string.
func AssertParseStatementError(tb testing.TB, s string, want string) {
	tb.Helper()
	_, err := query.NewParser(strings.NewReader(s)).ParseStatement()

	assert.Error(tb, err)
	assert.ErrorContains(tb, err, want)
}

// AssertParseStatement asserts the value of the first parse of s.
func AssertParseStatement(tb testing.TB, s string, want query.Statement) {
	tb.Helper()
	stmt, err := query.NewParser(strings.NewReader(s)).ParseStatement()

	assert.NoError(tb, err)
	assert.Equal(tb, stmt, want)
}
