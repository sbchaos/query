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
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "tbl", Tok: query.IDENT}},
			},
		})

		AssertParseStatement(t, `SELECT * FROM main.tbl;`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{
					First: &query.Ident{NamePos: pos(14), Name: "main", Tok: query.IDENT},
					Dot1:  pos(18),
					Name:  &query.Ident{NamePos: pos(19), Name: "tbl", Tok: query.IDENT}},
			},
		})

		AssertParseStatement(t, `SELECT tbl.ab.struct1.part1 FROM tbl;`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Expr: &query.MultiPartIdent{
					First:  &query.Ident{NamePos: pos(7), Name: "tbl", Tok: query.IDENT},
					Dot1:   pos(10),
					Second: &query.Ident{NamePos: pos(11), Name: "ab", Tok: query.IDENT},
					Dot2:   pos(13),
					Third:  &query.Ident{NamePos: pos(14), Name: "struct1", Tok: query.IDENT},
					Dot3:   pos(21),
					Name:   &query.Ident{NamePos: pos(22), Name: "part1", Tok: query.IDENT},
				}},
			},
			From: pos(28),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(33), Name: "tbl", Tok: query.IDENT}},
			},
		})
		AssertParseStatement(t, `SELECT 10 AS t1, CONCAT('\'', NVL(c1, NULL)) AS t2 FROM tbl1`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr:  &query.NumberLit{ValuePos: pos(7), Value: "10"},
					As:    pos(10),
					Alias: &query.Ident{Name: "t1", NamePos: pos(13), Tok: query.IDENT},
				},
				{
					Expr: &query.Call{
						Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(17), Name: "CONCAT", Tok: query.IDENT}},
						Lparen: pos(23),
						Rparen: pos(43),
						Args: []*query.Params{
							{X: &query.StringLit{ValuePos: pos(24), Value: "'"}},
							{X: &query.Call{
								Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(30), Name: "NVL", Tok: query.IDENT}},
								Lparen: pos(33),
								Rparen: pos(42),
								Args: []*query.Params{
									{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(34), Name: "c1", Tok: query.IDENT}}},
									{X: &query.NullLit{Pos: pos(38)}},
								},
							}},
						},
					},
					As:    pos(45),
					Alias: &query.Ident{NamePos: pos(48), Name: "t2", Tok: query.IDENT},
				},
			},
			From: pos(51),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(56), Name: "tbl1", Tok: query.IDENT}},
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
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(23), Name: "tbl", Tok: query.IDENT}},
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
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(18), Name: "tbl", Tok: query.IDENT}},
			},
		})

		AssertParseStatement(t, `SELECT * FROM tbl tbl2`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.QualifiedTableName{
				Name:  &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "tbl", Tok: query.IDENT}},
				Alias: &query.Ident{NamePos: pos(18), Name: "tbl2", Tok: query.IDENT},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl AS tbl2`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.QualifiedTableName{
				Name:  &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "tbl", Tok: query.IDENT}},
				As:    pos(18),
				Alias: &query.Ident{NamePos: pos(21), Name: "tbl2", Tok: query.IDENT},
			},
		})
		AssertParseStatement(t, `SELECT name, 'm' AS period_type, EXTRACT(DAY FROM LAST_DAY(purchase_date)) AS day_count FROM @monthly`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Expr: &query.MultiPartIdent{
					Name: &query.Ident{NamePos: pos(7), Name: "name", Tok: query.IDENT},
				}},
				{Expr: &query.StringLit{ValuePos: pos(13), Value: "m"},
					As:    pos(17),
					Alias: &query.Ident{NamePos: pos(20), Name: "period_type", Tok: query.IDENT}},
				{Expr: &query.Call{
					Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(33), Name: "EXTRACT", Tok: query.IDENT}},
					Lparen: pos(40),
					Rparen: pos(73),
					Args: []*query.Params{
						{X: &query.Call{
							Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(41), Name: "DAY FROM LAST_DAY", Tok: query.IDENT}},
							Lparen: pos(58),
							Rparen: pos(72),
							Args: []*query.Params{
								{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(59), Name: "purchase_date", Tok: query.IDENT}}},
							},
						}},
					},
				},
					As:    pos(75),
					Alias: &query.Ident{NamePos: pos(78), Name: "day_count", Tok: query.IDENT},
				},
			},
			From: pos(88),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(93), Name: "@monthly", Tok: query.BIND}},
			},
		})
		AssertParseStatement(t, `SELECT * FROM main.tbl AS tbl2`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{
					First: &query.Ident{NamePos: pos(14), Name: "main", Tok: query.IDENT},
					Dot1:  pos(18),
					Name:  &query.Ident{NamePos: pos(19), Name: "tbl", Tok: query.IDENT}},
				As:    pos(23),
				Alias: &query.Ident{NamePos: pos(26), Name: "tbl2", Tok: query.IDENT},
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
				Alias:  &query.Ident{NamePos: pos(28), Name: "tbl", Tok: query.IDENT},
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
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(16), Name: "t", Tok: query.IDENT}},
				},
				Rparen: pos(18),
				Alias:  &query.Ident{NamePos: pos(20), Name: "a", Tok: query.IDENT},
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
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "foo", Tok: query.IDENT}},
				},
				Operator: &query.JoinOperator{Comma: pos(17)},
				Y: &query.QualifiedTableName{
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(19), Name: "bar", Tok: query.IDENT}},
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
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "foo", Tok: query.IDENT}},
				},
				Operator: &query.JoinOperator{Join: pos(18)},
				Y: &query.QualifiedTableName{
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(23), Name: "bar", Tok: query.IDENT}},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo FULL JOIN bar`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.JoinClause{
				X: &query.QualifiedTableName{
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "foo", Tok: query.IDENT}},
				},
				Operator: &query.JoinOperator{Full: pos(18), Join: pos(23)},
				Y: &query.QualifiedTableName{
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(28), Name: "bar", Tok: query.IDENT}},
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
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "foo", Tok: query.IDENT}},
				},
				Operator: &query.JoinOperator{Inner: pos(18), Join: pos(24)},
				Y: &query.QualifiedTableName{
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(29), Name: "bar", Tok: query.IDENT}},
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
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "foo", Tok: query.IDENT}},
				},
				Operator: &query.JoinOperator{Left: pos(18), Join: pos(23)},
				Y: &query.QualifiedTableName{
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(28), Name: "bar", Tok: query.IDENT}},
				},
				Constraint: &query.UsingConstraint{
					Using:  pos(32),
					Lparen: pos(38),
					Columns: []*query.Ident{
						{NamePos: pos(39), Name: "x", Tok: query.IDENT},
						{NamePos: pos(42), Name: "y", Tok: query.IDENT},
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
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "X", Tok: query.IDENT}},
				},
				Operator: &query.JoinOperator{Inner: pos(16), Join: pos(22)},
				Y: &query.JoinClause{
					X: &query.QualifiedTableName{
						Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(27), Name: "Y", Tok: query.IDENT}},
					},
					Operator: &query.JoinOperator{Inner: pos(37), Join: pos(43)},
					Y: &query.QualifiedTableName{
						Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(48), Name: "Z", Tok: query.IDENT}},
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
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "foo", Tok: query.IDENT}},
				},
				Operator: &query.JoinOperator{Left: pos(18), Outer: pos(23), Join: pos(29)},
				Y: &query.QualifiedTableName{
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(34), Name: "bar", Tok: query.IDENT}},
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
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "foo", Tok: query.IDENT}},
				},
				Operator: &query.JoinOperator{Cross: pos(18), Join: pos(24)},
				Y: &query.QualifiedTableName{
					Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(29), Name: "bar", Tok: query.IDENT}},
				},
			},
		})

		AssertParseStatement(t, `WITH cte (foo, bar) AS (SELECT baz), xxx AS (SELECT yyy) SELECT bat`, &query.SelectStatement{
			WithClause: &query.WithClause{
				With: pos(0),
				CTEs: []*query.CTE{
					{
						TableName:     &query.Ident{NamePos: pos(5), Name: "cte", Tok: query.IDENT},
						ColumnsLparen: pos(9),
						Columns: []*query.Ident{
							{NamePos: pos(10), Name: "foo", Tok: query.IDENT},
							{NamePos: pos(15), Name: "bar", Tok: query.IDENT},
						},
						ColumnsRparen: pos(18),
						As:            pos(20),
						SelectLparen:  pos(23),
						Select: &query.SelectStatement{
							Select: pos(24),
							Columns: []*query.ResultColumn{
								{Expr: &query.MultiPartIdent{
									Name: &query.Ident{NamePos: pos(31), Name: "baz", Tok: query.IDENT}},
								},
							},
						},
						SelectRparen: pos(34),
					},
					{
						TableName:    &query.Ident{NamePos: pos(37), Name: "xxx", Tok: query.IDENT},
						As:           pos(41),
						SelectLparen: pos(44),
						Select: &query.SelectStatement{
							Select: pos(45),
							Columns: []*query.ResultColumn{
								{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(52), Name: "yyy", Tok: query.IDENT}}},
							},
						},
						SelectRparen: pos(55),
					},
				},
			},
			Select: pos(57),
			Columns: []*query.ResultColumn{
				{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(64), Name: "bat", Tok: query.IDENT}}},
			},
		})
		AssertParseStatement(t, `WITH RECURSIVE cte AS (SELECT foo) SELECT bar`, &query.SelectStatement{
			WithClause: &query.WithClause{
				With:      pos(0),
				Recursive: pos(5),
				CTEs: []*query.CTE{
					{
						TableName:    &query.Ident{NamePos: pos(15), Name: "cte", Tok: query.IDENT},
						As:           pos(19),
						SelectLparen: pos(22),
						Select: &query.SelectStatement{
							Select: pos(23),
							Columns: []*query.ResultColumn{
								{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(30), Name: "foo", Tok: query.IDENT}}},
							},
						},
						SelectRparen: pos(33),
					},
				},
			},
			Select: pos(35),
			Columns: []*query.ResultColumn{
				{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(42), Name: "bar", Tok: query.IDENT}}},
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
		AssertParseStatement(t, `Select * FROM abc WHERE a.sell_date BETWEEN DATEADD(@end_date, -13, 'dd') AND @end_date OR a.sell_date = DATEADD(@end_date, -1, 'yyyy')`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Star: pos(7)},
			},
			From: pos(9),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "abc", Tok: query.IDENT}},
			},
			Where: pos(18),
			WhereExpr: &query.BinaryExpr{
				X: &query.BinaryExpr{
					X: &query.MultiPartIdent{
						First: &query.Ident{NamePos: pos(24), Name: "a", Tok: query.IDENT},
						Dot1:  pos(25),
						Name:  &query.Ident{NamePos: pos(26), Name: "sell_date", Tok: query.IDENT},
					},
					OpPos: pos(36),
					Op:    query.BETWEEN,
					Y: &query.Range{
						X: &query.Call{
							Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(44), Name: "DATEADD", Tok: query.IDENT}},
							Lparen: pos(51),
							Rparen: pos(72),
							Args: []*query.Params{
								{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(52), Name: "@end_date", Tok: query.BIND}}},
								{X: &query.UnaryExpr{OpPos: pos(63), Op: query.MINUS, X: &query.NumberLit{ValuePos: pos(64), Value: "13"}}},
								{X: &query.StringLit{ValuePos: pos(68), Value: "dd"}},
							},
						},
						And: pos(74),
						Y:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(78), Name: "@end_date", Tok: query.BIND}},
					},
				},
				OpPos: pos(88),
				Op:    query.OR,
				Y: &query.BinaryExpr{
					X: &query.MultiPartIdent{
						First: &query.Ident{NamePos: pos(91), Name: "a", Tok: query.IDENT},
						Dot1:  pos(92),
						Name:  &query.Ident{NamePos: pos(93), Name: "sell_date", Tok: query.IDENT},
					},
					OpPos: pos(103),
					Op:    query.EQ,
					Y: &query.Call{
						Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(105), Name: "DATEADD", Tok: query.IDENT}},
						Lparen: pos(112),
						Rparen: pos(134),
						Args: []*query.Params{
							{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(113), Name: "@end_date", Tok: query.BIND}}},
							{X: &query.UnaryExpr{
								OpPos: pos(124),
								Op:    query.MINUS,
								X:     &query.NumberLit{ValuePos: pos(125), Value: "1"},
							}},
							{X: &query.StringLit{ValuePos: pos(128), Value: "yyyy"}},
						},
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl1 WHERE event_date BETWEEN to_date('2025-06-01')-1 AND CURRENT_DATE()`, &query.SelectStatement{
			Select:  pos(0),
			Columns: []*query.ResultColumn{{Star: pos(7)}},
			From:    pos(9),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{Name: "tbl1", NamePos: pos(14), Tok: query.IDENT}},
			},
			Where: pos(19),
			WhereExpr: &query.BinaryExpr{
				X:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(25), Name: "event_date", Tok: query.IDENT}},
				Op:    query.BETWEEN,
				OpPos: pos(36),
				Y: &query.Range{
					X: &query.BinaryExpr{
						X: &query.Call{
							Name:   &query.MultiPartIdent{Name: &query.Ident{Name: "to_date", NamePos: pos(44), Tok: query.IDENT}},
							Lparen: pos(51),
							Rparen: pos(64),
							Args: []*query.Params{
								{
									X: &query.StringLit{Value: "2025-06-01", ValuePos: pos(52)},
								},
							},
						},
						Op:    query.MINUS,
						OpPos: pos(65),
						Y:     &query.NumberLit{Value: "1", ValuePos: pos(66)},
					},
					And: pos(68),
					Y: &query.Call{
						Name:   &query.MultiPartIdent{Name: &query.Ident{Name: "CURRENT_DATE", NamePos: pos(72), Tok: query.CURRENT_DATE}},
						Lparen: pos(84),
						Rparen: pos(85),
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl1 WHERE name rlike 'done'`, &query.SelectStatement{
			Select:  pos(0),
			Columns: []*query.ResultColumn{{Star: pos(7)}},
			From:    pos(9),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "tbl1", Tok: query.IDENT}},
			},
			Where: pos(19),
			WhereExpr: &query.BinaryExpr{
				X:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(25), Name: "name", Tok: query.IDENT}},
				Op:    query.RLIKE,
				OpPos: pos(30),
				Y:     &query.StringLit{Value: "done", ValuePos: pos(36)},
			},
		})
		AssertParseStatement(t, `SELECT * FROM dt WHERE true AND effective_timestamp <= CAST(dstart AS TIMESTAMP)`, &query.SelectStatement{
			Select:  pos(0),
			Columns: []*query.ResultColumn{{Star: pos(7)}},
			From:    pos(9),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "dt", Tok: query.IDENT}},
			},
			Where: pos(17),
			WhereExpr: &query.BinaryExpr{
				X:     &query.BoolLit{ValuePos: pos(23), Value: true},
				OpPos: pos(28),
				Op:    query.AND,
				Y: &query.BinaryExpr{
					X:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(32), Name: "effective_timestamp", Tok: query.IDENT}},
					OpPos: pos(52),
					Op:    query.LE,
					Y: &query.CastExpr{
						Cast:   pos(55),
						Lparen: pos(59),
						Rparen: pos(79),
						X:      &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(60), Name: "dstart", Tok: query.IDENT}},
						As:     pos(67),
						Type: &query.Type{
							Name: &query.Ident{NamePos: pos(70), Name: "TIMESTAMP"},
						},
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT DATE '{{ .DSTART | Date }}' AS dstart FROM dt `, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.MultiPartIdent{
						Name: &query.Ident{NamePos: pos(7), Name: "DATE '{{ .DSTART | Date }}'", Tok: query.DATE},
					},
					As:    pos(35),
					Alias: &query.Ident{NamePos: pos(38), Name: "dstart", Tok: query.IDENT},
				},
			},
			From: pos(45),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(50), Name: "dt", Tok: query.IDENT}},
			},
		})
		AssertParseStatement(t, `Select * from abc LATERAL VIEW EXPLODE(filters) _T2 AS f LATERAL VIEW EXPLODE(_T2.f.actions) _T3 AS ap`, &query.SelectStatement{
			Select:  pos(0),
			Columns: []*query.ResultColumn{{Star: pos(7)}},
			From:    pos(9),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "abc", Tok: query.IDENT}},
				LateralViews: []*query.LateralView{
					{
						Lateral: pos(18),
						View:    pos(26),
						Udtf: &query.Call{
							Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(31), Name: "EXPLODE", Tok: query.IDENT}},
							Lparen: pos(38),
							Rparen: pos(46),
							Args: []*query.Params{
								{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(39), Name: "filters", Tok: query.IDENT}}},
							},
						},
						TableAlias: &query.Ident{NamePos: pos(48), Name: "_T2", Tok: query.IDENT},
						As:         pos(52),
						ColAlias: []*query.Ident{
							{NamePos: pos(55), Name: "f", Tok: query.IDENT},
						},
					},
					{
						Lateral: pos(57),
						View:    pos(65),
						Udtf: &query.Call{
							Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(70), Name: "EXPLODE", Tok: query.IDENT}},
							Lparen: pos(77),
							Rparen: pos(91),
							Args: []*query.Params{
								{X: &query.MultiPartIdent{
									First:  &query.Ident{NamePos: pos(78), Name: "_T2", Tok: query.IDENT},
									Dot1:   pos(81),
									Second: &query.Ident{NamePos: pos(82), Name: "f", Tok: query.IDENT},
									Dot2:   pos(83),
									Name:   &query.Ident{NamePos: pos(84), Name: "actions", Tok: query.IDENT},
								}},
							},
						},
						TableAlias: &query.Ident{NamePos: pos(93), Name: "_T3", Tok: query.IDENT},
						As:         pos(97),
						ColAlias: []*query.Ident{
							{NamePos: pos(100), Name: "ap", Tok: query.IDENT},
						},
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT SPLIT(a.link.url, "/")[SAFE_OFFSET(3)] FROM a`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{{
				Expr: &query.IndexExpr{
					X: &query.Call{
						Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(7), Name: "SPLIT", Tok: query.IDENT}},
						Lparen: pos(12),
						Rparen: pos(28),
						Args: []*query.Params{
							{
								X: &query.MultiPartIdent{
									First:  &query.Ident{NamePos: pos(13), Name: "a", Tok: query.IDENT},
									Dot1:   pos(14),
									Second: &query.Ident{NamePos: pos(15), Name: "link", Tok: query.IDENT},
									Dot2:   pos(19),
									Name:   &query.Ident{NamePos: pos(20), Name: "url", Tok: query.IDENT},
								},
							},
							{
								X: &query.MultiPartIdent{
									Name: &query.Ident{NamePos: pos(25), Name: "/", Tok: query.QIDENT},
								},
							},
						},
					},
					LBrack: pos(29),
					RBrack: pos(44),
					Call: &query.Call{
						Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(30), Name: "SAFE_OFFSET", Tok: query.IDENT}},
						Lparen: pos(41),
						Rparen: pos(43),
						Args: []*query.Params{
							{
								X: &query.NumberLit{ValuePos: pos(42), Value: "3"},
							},
						},
					},
				},
			}},
			From: pos(46),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{Name: "a", NamePos: pos(51), Tok: query.IDENT}},
			},
		})
		AssertParseStatement(t, `SELECT a  FROM tbl1 c1 LEFT OUTER JOIN tbl2 c2 ON c1.id = c2.id AND b < b1 LATERAL VIEW JSON_EXPLODE(JSON_PARSE(c1.json)) _T1 AS elem, raw_metadata`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(7), Name: "a", Tok: query.IDENT}}},
			},
			From: pos(10),
			Source: &query.JoinClause{
				X: &query.QualifiedTableName{
					Name:  &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(15), Name: "tbl1", Tok: query.IDENT}},
					Alias: &query.Ident{NamePos: pos(20), Name: "c1", Tok: query.IDENT},
					LateralViews: []*query.LateralView{
						{
							Lateral: pos(75),
							View:    pos(83),
							Udtf: &query.Call{
								Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(88), Name: "JSON_EXPLODE", Tok: query.IDENT}},
								Lparen: pos(100),
								Args: []*query.Params{
									{X: &query.Call{
										Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(101), Name: "JSON_PARSE", Tok: query.IDENT}},
										Lparen: pos(111),
										Rparen: pos(119),
										Args: []*query.Params{
											{X: &query.MultiPartIdent{
												First: &query.Ident{NamePos: pos(112), Name: "c1", Tok: query.IDENT},
												Dot1:  pos(114),
												Name:  &query.Ident{NamePos: pos(115), Name: "json", Tok: query.IDENT}},
											},
										},
									}},
								},
								Rparen: pos(120),
							},
							TableAlias: &query.Ident{NamePos: pos(122), Name: "_T1", Tok: query.IDENT},
							As:         pos(126),
							ColAlias: []*query.Ident{
								{NamePos: pos(135), Name: "raw_metadata", Tok: query.IDENT},
							},
						},
					},
				},
				Operator: &query.JoinOperator{
					Left:  pos(23),
					Outer: pos(28),
					Join:  pos(34),
				},
				Y: &query.QualifiedTableName{
					Name:  &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(39), Name: "tbl2", Tok: query.IDENT}},
					Alias: &query.Ident{NamePos: pos(44), Name: "c2", Tok: query.IDENT},
				},
				Constraint: &query.OnConstraint{
					On: pos(47),
					X: &query.BinaryExpr{
						X: &query.BinaryExpr{
							X: &query.MultiPartIdent{
								First: &query.Ident{NamePos: pos(50), Name: "c1", Tok: query.IDENT},
								Dot1:  pos(52),
								Name:  &query.Ident{NamePos: pos(53), Name: "id", Tok: query.IDENT}},
							OpPos: pos(56),
							Op:    query.EQ,
							Y: &query.MultiPartIdent{
								First: &query.Ident{NamePos: pos(58), Name: "c2", Tok: query.IDENT},
								Dot1:  pos(60),
								Name:  &query.Ident{NamePos: pos(61), Name: "id", Tok: query.IDENT}},
						},
						Op:    query.AND,
						OpPos: pos(64),
						Y: &query.BinaryExpr{
							X:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(68), Name: "b", Tok: query.IDENT}},
							Op:    query.LT,
							OpPos: pos(70),
							Y:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(72), Name: "b1", Tok: query.IDENT}},
						},
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT * GROUP BY foo, bar`, &query.SelectStatement{
			Select:  pos(0),
			Columns: []*query.ResultColumn{{Star: pos(7)}},
			Group:   pos(9),
			GroupBy: pos(15),
			GroupByExprs: []query.Expr{
				&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(18), Name: "foo", Tok: query.IDENT}},
				&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(23), Name: "bar", Tok: query.IDENT}},
			},
		})
		AssertParseStatement(t, `SELECT * GROUP BY ALL`, &query.SelectStatement{
			Select:     pos(0),
			Columns:    []*query.ResultColumn{{Star: pos(7)}},
			Group:      pos(9),
			GroupBy:    pos(15),
			GroupByAll: pos(18),
		})
		AssertParseStatement(t, `Select * FROM cols GROUP BY GROUPING SETS ((a, b, a.c), (a, b, d))`, &query.SelectStatement{
			Select:      pos(0),
			Columns:     []*query.ResultColumn{{Star: pos(7)}},
			From:        pos(9),
			Source:      &query.QualifiedTableName{Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "cols", Tok: query.IDENT}}},
			Group:       pos(19),
			GroupBy:     pos(25),
			Grouping:    pos(28),
			GroupingSet: pos(37),
			GroupingExpr: &query.ExprList{
				Lparen: pos(42),
				Rparen: pos(65),
				Exprs: []query.Expr{
					&query.ExprList{
						Lparen: pos(43),
						Rparen: pos(53),
						Exprs: []query.Expr{
							&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(44), Name: "a", Tok: query.IDENT}},
							&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(47), Name: "b", Tok: query.IDENT}},
							&query.MultiPartIdent{
								First: &query.Ident{NamePos: pos(50), Name: "a", Tok: query.IDENT},
								Dot1:  pos(51),
								Name:  &query.Ident{NamePos: pos(52), Name: "c", Tok: query.IDENT}},
						},
					},
					&query.ExprList{
						Lparen: pos(56),
						Rparen: pos(64),
						Exprs: []query.Expr{
							&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(57), Name: "a", Tok: query.IDENT}},
							&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(60), Name: "b", Tok: query.IDENT}},
							&query.MultiPartIdent{
								Name: &query.Ident{NamePos: pos(63), Name: "d", Tok: query.IDENT}},
						},
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT * GROUP BY foo HAVING true`, &query.SelectStatement{
			Select:  pos(0),
			Columns: []*query.ResultColumn{{Star: pos(7)}},
			Group:   pos(9),
			GroupBy: pos(15),
			GroupByExprs: []query.Expr{
				&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(18), Name: "foo", Tok: query.IDENT}},
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
					Name: &query.Ident{NamePos: pos(16), Name: "win1", Tok: query.IDENT},
					As:   pos(21),
					Definition: &query.WindowDefinition{
						Lparen: pos(24),
						Rparen: pos(25),
					},
				},
				{
					Name: &query.Ident{NamePos: pos(28), Name: "win2", Tok: query.IDENT},
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
				{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(18), Name: "foo", Tok: query.IDENT}}, Asc: pos(22)},
				{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(27), Name: "bar", Tok: query.IDENT}}, Desc: pos(31)},
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
				{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(7), Name: "shop_uuid", Tok: query.IDENT}}},
				{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(18), Name: "price_range", Tok: query.IDENT}}},
			},
			From: pos(30),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(35), Name: "merchant_price", Tok: query.IDENT}},
			},
			Where: pos(50),
			WhereExpr: &query.BinaryExpr{
				X:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(56), Name: "sale_date", Tok: query.IDENT}},
				OpPos: pos(66),
				Op:    query.EQ,
				Y:     &query.StringLit{ValuePos: pos(68), Value: "{{ .DSTART | Date }}"},
			},
		})
		AssertParseStatement(t, `SELECT a, b, c, FROM price`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(7), Name: "a", Tok: query.IDENT}}},
				{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(10), Name: "b", Tok: query.IDENT}}},
				{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(13), Name: "c", Tok: query.IDENT}}},
			},
			From: pos(16),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(21), Name: "price", Tok: query.IDENT}},
			},
		})
		AssertParseStatement(t, `SELECT a, b FROM @price`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(7), Name: "a", Tok: query.IDENT}}},
				{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(10), Name: "b", Tok: query.IDENT}}},
			},
			From: pos(12),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(17), Name: "@price", Tok: query.BIND}},
			},
		})
		AssertParseStatement(t, `with date_ref as( select date('2023-03-01') ds, date_sub(current_date, interval 1 day) de, ) select * from date_ref`, &query.SelectStatement{
			WithClause: &query.WithClause{
				With: pos(0),
				CTEs: []*query.CTE{
					{
						TableName:    &query.Ident{Name: "date_ref", NamePos: pos(5), Tok: query.IDENT},
						As:           pos(14),
						SelectLparen: pos(16),
						SelectRparen: pos(91),
						Select: &query.SelectStatement{
							Select: pos(18),
							Columns: []*query.ResultColumn{
								{Expr: &query.Call{
									Name:   &query.MultiPartIdent{Name: &query.Ident{Name: "date", NamePos: pos(25), Tok: query.DATE}},
									Lparen: pos(29),
									Rparen: pos(42),
									Args:   []*query.Params{{X: &query.StringLit{ValuePos: pos(30), Value: "2023-03-01"}}},
								},
									Alias: &query.Ident{NamePos: pos(44), Name: "ds", Tok: query.IDENT},
								},
								{
									Expr: &query.Call{
										Name:   &query.MultiPartIdent{Name: &query.Ident{Name: "date_sub", NamePos: pos(48), Tok: query.IDENT}},
										Lparen: pos(56),
										Args: []*query.Params{
											{X: &query.MultiPartIdent{Name: &query.Ident{Name: "current_date", NamePos: pos(57), Tok: query.CURRENT_DATE}}},
											{X: &query.IntervalLit{Interval: pos(71), Value: "1", Unit: "day"}},
										},
										Rparen: pos(85),
									},
									Alias: &query.Ident{NamePos: pos(87), Name: "de", Tok: query.IDENT},
								},
							},
						},
					},
				},
			},
			Select: pos(93),
			Columns: []*query.ResultColumn{
				{Star: pos(100)},
			},
			From: pos(102),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(107), Name: "date_ref", Tok: query.IDENT}},
			},
		})
		AssertParseStatement(t, `SELECT * FROM users u WHERE NOT EXISTS (SELECT * FROM orders o WHERE o.user_id = u.user_id);`, &query.SelectStatement{
			Select:  pos(0),
			From:    pos(9),
			Columns: []*query.ResultColumn{{Star: pos(7)}},
			Source: &query.QualifiedTableName{
				Name:  &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(14), Name: "users", Tok: query.IDENT}},
				Alias: &query.Ident{NamePos: pos(20), Name: "u", Tok: query.IDENT},
			},
			Where: pos(22),
			WhereExpr: &query.Exists{
				Not:    pos(28),
				Exists: pos(32),
				Lparen: pos(39),
				Rparen: pos(90),
				Select: &query.SelectStatement{
					Select:  pos(40),
					Columns: []*query.ResultColumn{{Star: pos(47)}},
					From:    pos(49),
					Source: &query.QualifiedTableName{
						Name:  &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(54), Name: "orders", Tok: query.IDENT}},
						Alias: &query.Ident{NamePos: pos(61), Name: "o", Tok: query.IDENT},
					},
					Where: pos(63),
					WhereExpr: &query.BinaryExpr{
						X: &query.MultiPartIdent{
							First: &query.Ident{NamePos: pos(69), Name: "o", Tok: query.IDENT},
							Dot1:  pos(70),
							Name:  &query.Ident{NamePos: pos(71), Name: "user_id", Tok: query.IDENT}},
						Op:    query.EQ,
						OpPos: pos(79),
						Y: &query.MultiPartIdent{
							First: &query.Ident{NamePos: pos(81), Name: "u", Tok: query.IDENT},
							Dot1:  pos(82),
							Name:  &query.Ident{NamePos: pos(83), Name: "user_id", Tok: query.IDENT},
						},
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT CONCAT(LEFT(a.b, 5), CAST(RIGHT(a.b, 2) AS INT)) FROM a`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.Call{
						Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(7), Name: "CONCAT", Tok: query.IDENT}},
						Lparen: pos(13),
						Rparen: pos(54),
						Args: []*query.Params{
							{
								X: &query.Call{
									Name: &query.MultiPartIdent{
										Name: &query.Ident{NamePos: pos(14), Name: "LEFT", Tok: query.LEFT}},
									Args: []*query.Params{
										{X: &query.MultiPartIdent{
											First: &query.Ident{NamePos: pos(19), Name: "a", Tok: query.IDENT},
											Dot1:  pos(20),
											Name:  &query.Ident{NamePos: pos(21), Name: "b", Tok: query.IDENT}}},
										{X: &query.NumberLit{ValuePos: pos(24), Value: "5"}},
									},
									Lparen: pos(18),
									Rparen: pos(25),
								},
							},
							{
								X: &query.CastExpr{
									Cast: pos(28),
									X: &query.Call{
										Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(33), Name: "RIGHT", Tok: query.RIGHT}},
										Args: []*query.Params{
											{X: &query.MultiPartIdent{
												First: &query.Ident{NamePos: pos(39), Name: "a", Tok: query.IDENT},
												Dot1:  pos(40),
												Name:  &query.Ident{NamePos: pos(41), Name: "b", Tok: query.IDENT}}},
											{X: &query.NumberLit{ValuePos: pos(44), Value: "2"}},
										},
										Lparen: pos(38),
										Rparen: pos(45),
									},
									As:     pos(47),
									Type:   &query.Type{Name: &query.Ident{NamePos: pos(50), Name: "INT"}},
									Lparen: pos(32),
									Rparen: pos(53),
								},
							},
						},
					},
				},
			},
			From: pos(56),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(61), Name: "a", Tok: query.IDENT}},
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
				{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(33), Name: "foo", Tok: query.IDENT}}},
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
		AssertParseStatement(t, `SELECT a FROM abc UNION DISTINCT SELECT DISTINCT b FROM bcd`, &query.SelectStatement{
			Select: pos(0),
			From:   pos(9),
			Columns: []*query.ResultColumn{
				{Expr: &query.MultiPartIdent{Name: &query.Ident{Name: "a", NamePos: pos(7), Tok: query.IDENT}}},
			},
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{
					Name: &query.Ident{NamePos: pos(14), Name: "abc", Tok: query.IDENT},
				},
			},
			Union:     pos(18),
			UnionDist: pos(24),
			Compound: &query.SelectStatement{
				Select:   pos(33),
				Distinct: pos(40),
				Columns: []*query.ResultColumn{
					{Expr: &query.MultiPartIdent{Name: &query.Ident{Name: "b", NamePos: pos(49), Tok: query.IDENT}}},
				},
				From: pos(51),
				Source: &query.QualifiedTableName{
					Name: &query.MultiPartIdent{Name: &query.Ident{Name: "bcd", NamePos: pos(56), Tok: query.IDENT}},
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
		AssertParseStatement(t, `SELECT a.* EXCEPT(price, place), b.price FROM tbl1 a JOIN tbl2 b ON a.id = b.id`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.QualifiedRef{
						Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(7), Name: "a", Tok: query.IDENT}},
						Dot:  pos(8),
						Star: pos(9),
					},
					Except: pos(11),
					ExceptCol: &query.ExprList{
						Lparen: pos(17),
						Exprs: []query.Expr{
							&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(18), Name: "price", Tok: query.IDENT}},
							&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(25), Name: "place", Tok: query.IDENT}},
						},
						Rparen: pos(30),
					},
				},
				{
					Expr: &query.MultiPartIdent{
						First: &query.Ident{NamePos: pos(33), Name: "b", Tok: query.IDENT},
						Dot1:  pos(34),
						Name:  &query.Ident{NamePos: pos(35), Name: "price", Tok: query.IDENT},
					},
				},
			},
			From: pos(41),
			Source: &query.JoinClause{
				X: &query.QualifiedTableName{
					Name:  &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(46), Name: "tbl1", Tok: query.IDENT}},
					Alias: &query.Ident{NamePos: pos(51), Name: "a", Tok: query.IDENT},
				},
				Operator: &query.JoinOperator{
					Join: pos(53),
				},
				Y: &query.QualifiedTableName{
					Name:  &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(58), Name: "tbl2", Tok: query.IDENT}},
					Alias: &query.Ident{NamePos: pos(63), Name: "b", Tok: query.IDENT},
				},
				Constraint: &query.OnConstraint{
					On: pos(65),
					X: &query.BinaryExpr{
						X: &query.MultiPartIdent{
							First: &query.Ident{NamePos: pos(68), Name: "a", Tok: query.IDENT},
							Dot1:  pos(69),
							Name:  &query.Ident{NamePos: pos(70), Name: "id", Tok: query.IDENT}},
						Op:    query.EQ,
						OpPos: pos(73),
						Y: &query.MultiPartIdent{
							First: &query.Ident{NamePos: pos(75), Name: "b", Tok: query.IDENT},
							Dot1:  pos(76),
							Name:  &query.Ident{NamePos: pos(77), Name: "id", Tok: query.IDENT}},
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT a as a1, DATETIME(event_timestamp) as TIMESTAMP FROM tbl1`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr:  &query.MultiPartIdent{Name: &query.Ident{Name: "a", NamePos: pos(7), Tok: query.IDENT}},
					As:    pos(9),
					Alias: &query.Ident{NamePos: pos(12), Name: "a1", Tok: query.IDENT},
				},
				{
					Expr: &query.Call{
						Name: &query.MultiPartIdent{Name: &query.Ident{Name: "DATETIME", NamePos: pos(16), Tok: query.IDENT}},
						Args: []*query.Params{
							{
								X: &query.MultiPartIdent{Name: &query.Ident{Name: "event_timestamp", NamePos: pos(25), Tok: query.IDENT}},
							},
						},
						Lparen: pos(24),
						Rparen: pos(40),
					},
					As: pos(42),
					Type: &query.Type{
						Name: &query.Ident{NamePos: pos(45), Name: "TIMESTAMP"},
					},
				},
			},
			From: pos(55),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(60), Name: "tbl1", Tok: query.IDENT}},
			},
		})
		AssertParseStatement(t, `SELECT array_agg(STRUCT(*)) WITHIN GROUP (ORDER BY a1 DESC limit 1)[0] AS col1 FROM tbl1`, &query.SelectStatement{
			Select: pos(0),
			From:   pos(79),
			Columns: []*query.ResultColumn{{
				Expr: &query.Call{
					Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(7), Name: "array_agg", Tok: query.IDENT}},
					Lparen: pos(16),
					Rparen: pos(26),
					Args: []*query.Params{
						{X: &query.Call{
							Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(17), Name: "STRUCT", Tok: query.IDENT}},
							Lparen: pos(23),
							Rparen: pos(25),
							Star:   pos(24),
						}},
					},
				},
				As:    pos(71),
				Alias: &query.Ident{Name: "col1", NamePos: pos(74), Tok: query.IDENT},
				Within: &query.Within{
					Within:       pos(28),
					Group:        pos(35),
					GroupLparen:  pos(41),
					GroupRparen:  pos(66),
					GroupOrder:   pos(42),
					GroupOrderBy: pos(48),
					OrderingTerm: &query.OrderingTerm{
						X:    &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(51), Name: "a1", Tok: query.IDENT}},
						Desc: pos(54),
					},
					GroupLimit:     pos(59),
					GroupLimitExpr: &query.NumberLit{ValuePos: pos(65), Value: "1"},
					Index:          &query.NumberLit{ValuePos: pos(68), Value: "0"},
				},
			}},
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(84), Name: "tbl1", Tok: query.IDENT}},
			},
		})
		AssertParseStatement(t, `SELECT a1, STRING_AGG(DISTINCT a2, ",") WITHIN GROUP (ORDER BY a2 asc) col1 FROM tbl1`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(7), Name: "a1", Tok: query.IDENT}},
				},
				{
					Expr: &query.Call{
						Name:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(11), Name: "STRING_AGG", Tok: query.IDENT}},
						Lparen:   pos(21),
						Rparen:   pos(38),
						Distinct: pos(22),
						Args: []*query.Params{
							{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(31), Name: "a2", Tok: query.IDENT}}},
							{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(35), Name: ",", Tok: query.QIDENT}}},
						},
					},
					Alias: &query.Ident{Name: "col1", NamePos: pos(71), Tok: query.IDENT},
					Within: &query.Within{
						Within:       pos(40),
						Group:        pos(47),
						GroupLparen:  pos(53),
						GroupRparen:  pos(69),
						GroupOrder:   pos(54),
						GroupOrderBy: pos(60),
						OrderingTerm: &query.OrderingTerm{
							X:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(63), Name: "a2", Tok: query.IDENT}},
							Asc: pos(66),
						},
					},
				}},
			From: pos(76),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(81), Name: "tbl1", Tok: query.IDENT}},
			},
		})
		AssertParseStatement(t, `SELECT DISTINCT a1, FN(a2 AS STRING) AS col2 FROM tbl1`, &query.SelectStatement{
			Select:   pos(0),
			Distinct: pos(7),
			Columns: []*query.ResultColumn{
				{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(16), Name: "a1", Tok: query.IDENT}}},
				{Expr: &query.Call{
					Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(20), Name: "FN", Tok: query.IDENT}},
					Lparen: pos(22),
					Rparen: pos(35),
					Args: []*query.Params{{
						X:  &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(23), Name: "a2", Tok: query.IDENT}},
						As: pos(26),
						Type: &query.Type{
							Name: &query.Ident{NamePos: pos(29), Name: "STRING"},
						},
					}},
				},
					As:    pos(37),
					Alias: &query.Ident{NamePos: pos(40), Name: "col2", Tok: query.IDENT},
				},
			},
			From: pos(45),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(50), Name: "tbl1", Tok: query.IDENT}},
			},
		})
		AssertParseStatement(t, `SELECT * EXCEPT (col1, col2), bcd FROM tbl1`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{{
				Star:   pos(7),
				Except: pos(9),
				ExceptCol: &query.ExprList{
					Lparen: pos(16),
					Rparen: pos(27),
					Exprs: []query.Expr{
						&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(17), Name: "col1", Tok: query.IDENT}},
						&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(23), Name: "col2", Tok: query.IDENT}},
					},
				},
			},
				{
					Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(30), Name: "bcd", Tok: query.IDENT}},
				},
			},
			From: pos(34),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(39), Name: "tbl1", Tok: query.IDENT}},
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
					Expr: &query.MultiPartIdent{
						Name: &query.Ident{NamePos: pos(7), Name: "rowid", Tok: query.ROWID}},
				},
			},
			From: pos(13),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{
					NamePos: pos(18),
					Name:    "foo",
					Tok:     query.IDENT,
				}},
			},
		})
		AssertParseStatement(t, `SELECT * FROM {{.TASK__DESTINATION_TABLE_ID }}`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Star: pos(7),
				},
			},
			From: pos(9),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{
					NamePos: pos(15),
					Name:    ".TASK__DESTINATION_TABLE_ID ",
					Tok:     query.TMPL,
				}},
			},
		})
		AssertParseStatement(t, `SELECT a, IF(GROUPING(b.c) = 1,'All',b.d) AS g1 FROM b`, &query.SelectStatement{
			//
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(7), Name: "a", Tok: query.IDENT}}},
				{Expr: &query.Call{
					Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(10), Name: "IF", Tok: query.IF}},
					Lparen: pos(12),
					Rparen: pos(40),
					Args: []*query.Params{
						{X: &query.BinaryExpr{
							X: &query.Call{
								Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(13), Name: "GROUPING", Tok: query.IDENT}},
								Lparen: pos(21),
								Rparen: pos(25),
								Args: []*query.Params{
									{X: &query.MultiPartIdent{
										First: &query.Ident{NamePos: pos(22), Name: "b", Tok: query.IDENT},
										Dot1:  pos(23),
										Name:  &query.Ident{NamePos: pos(24), Name: "c", Tok: query.IDENT}}},
								},
							},
							OpPos: pos(27),
							Op:    query.EQ,
							Y:     &query.NumberLit{ValuePos: pos(29), Value: "1"},
						}},
						{X: &query.StringLit{ValuePos: pos(31), Value: "All"}},
						{X: &query.MultiPartIdent{
							First: &query.Ident{NamePos: pos(37), Name: "b", Tok: query.IDENT},
							Dot1:  pos(38),
							Name:  &query.Ident{NamePos: pos(39), Name: "d", Tok: query.IDENT}}},
					},
				},
					As: pos(42), Alias: &query.Ident{NamePos: pos(45), Name: "g1", Tok: query.IDENT}},
			},
			From: pos(48),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(53), Name: "b", Tok: query.IDENT}},
			},
		})

		AssertParseStatement(t, `SELECT rowid FROM foo ORDER BY rowid`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.MultiPartIdent{
						Name: &query.Ident{NamePos: pos(7), Name: "rowid", Tok: query.ROWID}},
				},
			},
			From: pos(13),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{
					NamePos: pos(18),
					Name:    "foo",
					Tok:     query.IDENT,
				}},
			},
			Order:   pos(22),
			OrderBy: pos(28),
			OrderingTerms: []*query.OrderingTerm{
				{
					X: &query.MultiPartIdent{
						Name: &query.Ident{NamePos: pos(31), Name: "rowid", Tok: query.ROWID}},
				},
			},
		})

		AssertParseStatement(t, `SELECT CURRENT_TIMESTAMP FROM foo`, &query.SelectStatement{
			Select: pos(0),
			Columns: []*query.ResultColumn{
				{
					Expr: &query.MultiPartIdent{
						Name: &query.Ident{NamePos: pos(7), Name: "CURRENT_TIMESTAMP", Tok: query.CURRENT_TIMESTAMP}},
				},
			},
			From: pos(25),
			Source: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{
					NamePos: pos(30),
					Name:    "foo",
					Tok:     query.IDENT,
				}},
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
					Tok:     query.IDENT,
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
		//AssertParseStatement(t, `SELECT * FROM (WITH shop AS (SELECT * FROM business))`, &query.SelectStatement{
		//	Select: pos(0),
		//	Columns: []*query.ResultColumn{
		//		{
		//			Star: pos(7),
		//		},
		//	},
		//	From: pos(9),
		//	Source: &query.ParenSource{
		//		Lparen: pos(29),
		//		//Name: &query.Ident{
		//		//	NamePos: pos(14),
		//		//	Name:    "generate_series",
		//		//},
		//		//Args: []query.Expr{
		//		//	&query.NumberLit{
		//		//		ValuePos: pos(30),
		//		//		Value:    "1",
		//		//	},
		//		//	&query.NumberLit{
		//		//		ValuePos: pos(32),
		//		//		Value:    "3",
		//		//	},
		//		//},
		//		Rparen: pos(33),
		//	},
		//})

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
