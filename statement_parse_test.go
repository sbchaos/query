package query_test

import (
	"testing"

	"github.com/sbchaos/query"
)

func TestParser_ParseStatement2(t *testing.T) {
	t.Run("DEBUG", func(t *testing.T) {
	})

	t.Run("Script", func(t *testing.T) {

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
			Name: &query.Ident{Name: "@start_date", NamePos: pos(0), Tok: query.BIND},
			Type: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "Date", Tok: query.DATE}},
		})
		AssertParseStatement(t, `@start_date := '{{ .DSTART | Date }}';`, &query.DeclarationStatement{
			Name:  &query.Ident{Name: "@start_date", NamePos: pos(0), Tok: query.BIND},
			Value: &query.StringLit{ValuePos: pos(15), Value: "{{ .DSTART | Date }}"},
		})
		AssertParseStatement(t, `@start_date := DATE '{{ .DSTART | Date }}';`, &query.DeclarationStatement{
			Name:  &query.Ident{Name: "@start_date", NamePos: pos(0), Tok: query.BIND},
			Value: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(15), Name: "DATE '{{ .DSTART | Date }}'", Tok: query.DATE}},
		})
		AssertParseStatement(t, `@start_date := TO_DATE('{{ .DSTART | Date }}');`, &query.DeclarationStatement{
			Name: &query.Ident{Name: "@start_date", NamePos: pos(0), Tok: query.BIND},
			Value: &query.Call{
				Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(15), Name: "TO_DATE", Tok: query.IDENT}},
				Lparen: pos(22),
				Rparen: pos(45),
				Args: []query.Expr{
					&query.StringLit{ValuePos: pos(23), Value: "{{ .DSTART | Date }}"},
				},
			},
		})
		AssertParseStatement(t, `@modified_timestamp := CURRENT_TIMESTAMP();`, &query.DeclarationStatement{
			Name: &query.Ident{Name: "@modified_timestamp", NamePos: pos(0), Tok: query.BIND},
			Value: &query.Call{
				Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(23), Name: "CURRENT_TIMESTAMP", Tok: query.CURRENT_TIMESTAMP}},
				Lparen: pos(40),
				Rparen: pos(41),
			},
		})
		AssertParseStatement(t, `@tmp := SELECT data_date, shop_id FROM shop;`, &query.DeclarationStatement{
			Name: &query.Ident{Name: "@tmp", NamePos: pos(0), Tok: query.BIND},
			Value: query.SelectExpr{
				SelectStatement: &query.SelectStatement{
					Select: pos(8),
					Columns: []*query.ResultColumn{
						{
							Expr: &query.MultiPartIdent{
								Name: &query.Ident{
									NamePos: pos(15),
									Name:    "data_date",
									Tok:     query.IDENT,
								},
							},
						},
						{
							Expr: &query.MultiPartIdent{
								Name: &query.Ident{
									NamePos: pos(26),
									Name:    "shop_id",
									Tok:     query.IDENT,
								},
							},
						},
					},
					From: pos(34),
					Source: &query.QualifiedTableName{
						Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(39), Name: "shop", Tok: query.IDENT}},
					},
				},
			},
		})
		AssertParseStatement(t, `@end_ts := TIMESTAMP(@end_date) + INTERVAL 17 HOUR - INTERVAL 1 SECOND;`, &query.DeclarationStatement{
			Name: &query.Ident{Name: "@end_ts", NamePos: pos(0), Tok: query.BIND},
			Value: &query.BinaryExpr{
				X: &query.BinaryExpr{
					X: &query.Call{
						Name: &query.MultiPartIdent{Name: &query.Ident{
							NamePos: pos(11),
							Name:    "TIMESTAMP",
							Tok:     query.TIMESTAMP,
						}},
						Lparen: pos(20),
						Args: []query.Expr{
							&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(21), Name: "@end_date", Tok: query.BIND}},
						},
						Rparen: pos(30),
					},
					OpPos: pos(32),
					Op:    query.PLUS,
					Y:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(34), Name: "INTERVAL 17 HOUR", Tok: query.IDENT}},
				},
				OpPos: pos(51),
				Op:    query.MINUS,
				Y:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(53), Name: "INTERVAL 1 SECOND", Tok: query.IDENT}},
			},
		})
	})

	t.Run("Insert", func(t *testing.T) {
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (1, 2)`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x", Tok: query.IDENT},
				{NamePos: pos(20), Name: "y", Tok: query.IDENT},
			},
			ColumnsRparen: pos(21),
			Values:        pos(23),
			ValueLists: []*query.ExprList{{
				Lparen: pos(30),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(31), Value: "1"},
					&query.NumberLit{ValuePos: pos(34), Value: "2"},
				},
				Rparen: pos(35),
			}},
		})

		AssertParseStatement(t, `INSERT INTO TABLE tbl (x, y) VALUES (1, 2)`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			TablePos:      pos(12),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(18), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(22),
			Columns: []*query.Ident{
				{NamePos: pos(23), Name: "x", Tok: query.IDENT},
				{NamePos: pos(26), Name: "y", Tok: query.IDENT},
			},
			ColumnsRparen: pos(27),
			Values:        pos(29),
			ValueLists: []*query.ExprList{{
				Lparen: pos(36),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(37), Value: "1"},
					&query.NumberLit{ValuePos: pos(40), Value: "2"},
				},
				Rparen: pos(41),
			}},
		})

		AssertParseStatement(t, `INSERT INTO proj.sch.tbl (x, y) VALUES (@foo, @bar)`, &query.InsertStatement{
			Insert: pos(0),
			Into:   pos(7),
			Table: &query.MultiPartIdent{
				First:  &query.Ident{NamePos: pos(12), Name: "proj", Tok: query.IDENT},
				Dot1:   pos(16),
				Second: &query.Ident{NamePos: pos(17), Name: "sch", Tok: query.IDENT},
				Dot2:   pos(20),
				Name:   &query.Ident{NamePos: pos(21), Name: "tbl", Tok: query.IDENT},
			},
			ColumnsLparen: pos(25),
			Columns: []*query.Ident{
				{NamePos: pos(26), Name: "x", Tok: query.IDENT},
				{NamePos: pos(29), Name: "y", Tok: query.IDENT},
			},
			ColumnsRparen: pos(30),
			Values:        pos(32),
			ValueLists: []*query.ExprList{{
				Lparen: pos(39),
				Exprs: []query.Expr{
					&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(40), Name: "@foo", Tok: query.BIND}},
					&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(46), Name: "@bar", Tok: query.BIND}},
				},
				Rparen: pos(50),
			}},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (1, random())`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x", Tok: query.IDENT},
				{NamePos: pos(20), Name: "y", Tok: query.IDENT},
			},
			ColumnsRparen: pos(21),
			Values:        pos(23),
			ValueLists: []*query.ExprList{{
				Lparen: pos(30),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(31), Value: "1"},
					&query.Call{
						Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(34), Name: "random", Tok: query.IDENT}},
						Lparen: pos(40),
						Rparen: pos(41),
					},
				},
				Rparen: pos(42),
			}},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (1, abs(random()))`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x", Tok: query.IDENT},
				{NamePos: pos(20), Name: "y", Tok: query.IDENT},
			},
			ColumnsRparen: pos(21),
			Values:        pos(23),
			ValueLists: []*query.ExprList{{
				Lparen: pos(30),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(31), Value: "1"},
					&query.Call{
						Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(34), Name: "abs", Tok: query.IDENT}},
						Lparen: pos(37),
						Rparen: pos(46),
						Args: []query.Expr{
							&query.Call{
								Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(38), Name: "random", Tok: query.IDENT}},
								Lparen: pos(44),
								Rparen: pos(45),
							},
						},
					},
				},
				Rparen: pos(47),
			}},
		})
		AssertParseStatement(t, `REPLACE INTO tbl (x, y) VALUES (1, 2), (3, 4)`, &query.InsertStatement{
			Replace:       pos(0),
			Into:          pos(8),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(13), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(17),
			Columns: []*query.Ident{
				{NamePos: pos(18), Name: "x", Tok: query.IDENT},
				{NamePos: pos(21), Name: "y", Tok: query.IDENT},
			},
			ColumnsRparen: pos(22),
			Values:        pos(24),
			ValueLists: []*query.ExprList{
				{
					Lparen: pos(31),
					Exprs: []query.Expr{
						&query.NumberLit{ValuePos: pos(32), Value: "1"},
						&query.NumberLit{ValuePos: pos(35), Value: "2"},
					},
					Rparen: pos(36),
				},
				{
					Lparen: pos(39),
					Exprs: []query.Expr{
						&query.NumberLit{ValuePos: pos(40), Value: "3"},
						&query.NumberLit{ValuePos: pos(43), Value: "4"},
					},
					Rparen: pos(44),
				},
			},
		})
		/*
				AssertParseStatement(t, `WITH cte (foo) AS (SELECT bar) INSERT INTO tbl VALUES (1)`, &query.InsertStatement{
					WithClause: &query.WithClause{
						With: pos(0),
						CTEs: []*query.CTE{{
							TableName:     &query.Ident{NamePos: pos(5), Name: "cte"},
							ColumnsLparen: pos(9),
							Columns: []*query.Ident{
								{NamePos: pos(10), Name: "foo"},
							},
							ColumnsRparen: pos(13),
							As:            pos(15),
							SelectLparen:  pos(18),
							Select: &query.SelectStatement{
								Select: pos(19),
								Columns: []*query.ResultColumn{
									{Expr: &query.Ident{NamePos: pos(26), Name: "bar"}},
								},
							},
							SelectRparen: pos(29),
						}},
					},
					Insert: pos(31),
					Into:   pos(38),
					Table:  &query.Ident{NamePos: pos(43), Name: "tbl"},
					Values: pos(47),
					ValueLists: []*query.ExprList{{
						Lparen: pos(54),
						Exprs: []query.Expr{
							&query.NumberLit{ValuePos: pos(55), Value: "1"},
						},
						Rparen: pos(56),
					}},
				})


			AssertParseStatement(t, `WITH cte (foo) AS (SELECT bar) INSERT INTO tbl VALUES (1)`, &query.InsertStatement{
				WithClause: &query.WithClause{
					With: pos(0),
					CTEs: []*query.CTE{{
						TableName:     &query.Ident{NamePos: pos(5), Name: "cte"},
						ColumnsLparen: pos(9),
						Columns: []*query.Ident{
							{NamePos: pos(10), Name: "foo"},
						},
						ColumnsRparen: pos(13),
						As:            pos(15),
						SelectLparen:  pos(18),
						Select: &query.SelectStatement{
							Select: pos(19),
							Columns: []*query.ResultColumn{
								{Expr: &query.Ident{NamePos: pos(26), Name: "bar"}},
							},
						},
						SelectRparen: pos(29),
					}},
				},
				Insert: pos(31),
				Into:   pos(38),
				Table:  &query.Ident{NamePos: pos(43), Name: "tbl"},
				Values: pos(47),
				ValueLists: []*query.ExprList{{
					Lparen: pos(54),
					Exprs: []query.Expr{
						&query.NumberLit{ValuePos: pos(55), Value: "1"},
					},
					Rparen: pos(56),
				}},
			})
		*/
		AssertParseStatement(t, `INSERT INTO tbl (x) SELECT y`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x", Tok: query.IDENT},
			},
			ColumnsRparen: pos(18),
			Select: &query.SelectStatement{
				Select: pos(20),
				Columns: []*query.ResultColumn{
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(27), Name: "y", Tok: query.IDENT}}},
				},
			},
		})

		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (y ASC, z DESC) DO NOTHING`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x", Tok: query.IDENT},
			},
			ColumnsRparen: pos(18),
			Values:        pos(20),
			ValueLists: []*query.ExprList{{
				Lparen: pos(27),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(28), Value: "1"},
				},
				Rparen: pos(29),
			}},
			UpsertClause: &query.UpsertClause{
				On:         pos(31),
				OnConflict: pos(34),
				Lparen:     pos(43),
				Columns: []*query.IndexedColumn{
					{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(44), Name: "y", Tok: query.IDENT}}, Asc: pos(46)},
					{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(51), Name: "z", Tok: query.IDENT}}, Desc: pos(53)},
				},
				Rparen:    pos(57),
				Do:        pos(59),
				DoNothing: pos(62),
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING *`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x", Tok: query.IDENT},
			},
			ColumnsRparen: pos(18),
			Values:        pos(20),
			ValueLists: []*query.ExprList{{
				Lparen: pos(27),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(28), Value: "1"},
				},
				Rparen: pos(29),
			}},
			ReturningClause: &query.ReturningClause{
				Returning: pos(31),
				Columns:   []*query.ResultColumn{{Star: pos(41)}},
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING x`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x", Tok: query.IDENT},
			},
			ColumnsRparen: pos(18),
			Values:        pos(20),
			ValueLists: []*query.ExprList{{
				Lparen: pos(27),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(28), Value: "1"},
				},
				Rparen: pos(29),
			}},
			ReturningClause: &query.ReturningClause{
				Returning: pos(31),
				Columns: []*query.ResultColumn{
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(41), Name: "x", Tok: query.IDENT}}},
				},
			},
		})
		AssertParseStatement(t, `INSERT OVERWRITE tbl (x) VALUES (1) RETURNING x`, &query.InsertStatement{
			Insert:        pos(0),
			Overwrite:     pos(7),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(17), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(21),
			Columns: []*query.Ident{
				{NamePos: pos(22), Name: "x", Tok: query.IDENT},
			},
			ColumnsRparen: pos(23),
			Values:        pos(25),
			ValueLists: []*query.ExprList{{
				Lparen: pos(32),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(33), Value: "1"},
				},
				Rparen: pos(34),
			}},
			ReturningClause: &query.ReturningClause{
				Returning: pos(36),
				Columns: []*query.ResultColumn{
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(46), Name: "x", Tok: query.IDENT}}},
				},
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING x AS y`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x", Tok: query.IDENT},
			},
			ColumnsRparen: pos(18),
			Values:        pos(20),
			ValueLists: []*query.ExprList{{
				Lparen: pos(27),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(28), Value: "1"},
				},
				Rparen: pos(29),
			}},
			ReturningClause: &query.ReturningClause{
				Returning: pos(31),
				Columns: []*query.ResultColumn{
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(41), Name: "x", Tok: query.IDENT}}, As: pos(43), Alias: &query.Ident{NamePos: pos(46), Name: "y", Tok: query.IDENT}},
				},
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING x,y`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x", Tok: query.IDENT},
			},
			ColumnsRparen: pos(18),
			Values:        pos(20),
			ValueLists: []*query.ExprList{{
				Lparen: pos(27),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(28), Value: "1"},
				},
				Rparen: pos(29),
			}},
			ReturningClause: &query.ReturningClause{
				Returning: pos(31),
				Columns: []*query.ResultColumn{
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(41), Name: "x", Tok: query.IDENT}}},
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(43), Name: "y", Tok: query.IDENT}}},
				},
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING x,y*2`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x", Tok: query.IDENT},
			},
			ColumnsRparen: pos(18),
			Values:        pos(20),
			ValueLists: []*query.ExprList{{
				Lparen: pos(27),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(28), Value: "1"},
				},
				Rparen: pos(29),
			}},
			ReturningClause: &query.ReturningClause{
				Returning: pos(31),
				Columns: []*query.ResultColumn{
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(41), Name: "x", Tok: query.IDENT}}},
					{
						Expr: &query.BinaryExpr{
							X:  &query.MultiPartIdent{Name: &query.Ident{Name: "y", NamePos: pos(43), Tok: query.IDENT}},
							Op: query.STAR, OpPos: pos(44),
							Y: &query.NumberLit{Value: "2", ValuePos: pos(45)},
						},
					},
				},
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (y) WHERE true DO UPDATE SET foo = 1, (bar, baz) = 2 WHERE false`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x", Tok: query.IDENT},
			},
			ColumnsRparen: pos(18),
			Values:        pos(20),
			ValueLists: []*query.ExprList{{
				Lparen: pos(27),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(28), Value: "1"},
				},
				Rparen: pos(29),
			}},
			UpsertClause: &query.UpsertClause{
				On:         pos(31),
				OnConflict: pos(34),
				Lparen:     pos(43),
				Columns: []*query.IndexedColumn{
					{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(44), Name: "y", Tok: query.IDENT}}},
				},
				Rparen:      pos(45),
				Where:       pos(47),
				WhereExpr:   &query.BoolLit{ValuePos: pos(53), Value: true},
				Do:          pos(58),
				DoUpdate:    pos(61),
				DoUpdateSet: pos(68),
				Assignments: []*query.Assignment{
					{
						Columns: []*query.MultiPartIdent{{Name: &query.Ident{NamePos: pos(72), Name: "foo", Tok: query.IDENT}}},
						Eq:      pos(76),
						Expr:    &query.NumberLit{ValuePos: pos(78), Value: "1"},
					},
					{
						Lparen: pos(81),
						Columns: []*query.MultiPartIdent{
							{Name: &query.Ident{NamePos: pos(82), Name: "bar", Tok: query.IDENT}},
							{Name: &query.Ident{NamePos: pos(87), Name: "baz", Tok: query.IDENT}},
						},
						Rparen: pos(90),
						Eq:     pos(92),
						Expr:   &query.NumberLit{ValuePos: pos(94), Value: "2"},
					},
				},
				UpdateWhere:     pos(96),
				UpdateWhereExpr: &query.BoolLit{ValuePos: pos(102), Value: false},
			},
		})

		AssertParseStatementError(t, `INSERT`, `1:6: expected INTO or OVERWRITE, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO`, `1:11: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl AS`, `1:18: expected alias, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl `, `1:16: expected VALUES, SELECT, or DEFAULT VALUES, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (`, `1:17: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x`, `1:18: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x)`, `1:19: expected VALUES, SELECT, or DEFAULT VALUES, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES`, `1:26: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (`, `1:28: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1`, `1:29: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) SELECT`, `1:26: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) RETURNING`, `1:40: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON`, `1:33: expected CONFLICT, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (`, `1:44: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x`, `1:45: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) WHERE`, `1:52: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x)`, `1:46: expected DO, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO`, `1:49: expected NOTHING or UPDATE SET, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE`, `1:56: expected SET, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET foo`, `1:64: expected =, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET foo =`, `1:66: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET foo = 1 WHERE`, `1:74: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET (`, `1:62: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET (foo`, `1:65: expected comma or right paren, found 'EOF'`)
	})

	t.Run("Delete", func(t *testing.T) {
		AssertParseStatement(t, `DELETE FROM tbl`, &query.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			},
		})
		AssertParseStatement(t, `DELETE FROM tbl WHERE x = 1`, &query.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			},
			Where: pos(16),
			WhereExpr: &query.BinaryExpr{
				X:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(22), Name: "x", Tok: query.IDENT}},
				OpPos: pos(24), Op: query.EQ,
				Y: &query.NumberLit{ValuePos: pos(26), Value: "1"},
			},
		})
		AssertParseStatement(t, `DELETE FROM tbl WHERE x = 1 RETURNING x`, &query.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			},
			Where: pos(16),
			WhereExpr: &query.BinaryExpr{
				X:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(22), Name: "x", Tok: query.IDENT}},
				OpPos: pos(24), Op: query.EQ,
				Y: &query.NumberLit{ValuePos: pos(26), Value: "1"},
			},
			ReturningClause: &query.ReturningClause{
				Returning: pos(28),
				Columns:   []*query.ResultColumn{{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(38), Name: "x", Tok: query.IDENT}}}},
			},
		})
		AssertParseStatement(t, `WITH cte (x) AS (SELECT y) DELETE FROM tbl`, &query.DeleteStatement{
			WithClause: &query.WithClause{
				With: pos(0),
				CTEs: []*query.CTE{
					{
						TableName:     &query.Ident{NamePos: pos(5), Name: "cte", Tok: query.IDENT},
						ColumnsLparen: pos(9),
						Columns: []*query.Ident{
							{NamePos: pos(10), Name: "x", Tok: query.IDENT},
						},
						ColumnsRparen: pos(11),
						As:            pos(13),
						SelectLparen:  pos(16),
						Select: &query.SelectStatement{
							Select: pos(17),
							Columns: []*query.ResultColumn{
								{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(24), Name: "y", Tok: query.IDENT}}},
							},
						},
						SelectRparen: pos(25),
					},
				},
			},
			Delete: pos(27),
			From:   pos(34),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(39), Name: "tbl", Tok: query.IDENT}},
			},
		})
		AssertParseStatement(t, `DELETE FROM tbl ORDER BY x, y LIMIT 1 OFFSET 2`, &query.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			},
			Order:   pos(16),
			OrderBy: pos(22),
			OrderingTerms: []*query.OrderingTerm{
				{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(25), Name: "x", Tok: query.IDENT}}},
				{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(28), Name: "y", Tok: query.IDENT}}},
			},
			Limit:      pos(30),
			LimitExpr:  &query.NumberLit{ValuePos: pos(36), Value: "1"},
			Offset:     pos(38),
			OffsetExpr: &query.NumberLit{ValuePos: pos(45), Value: "2"},
		})
		AssertParseStatement(t, `DELETE FROM tbl LIMIT 1`, &query.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			},
			Limit:     pos(16),
			LimitExpr: &query.NumberLit{ValuePos: pos(22), Value: "1"},
		})
		AssertParseStatement(t, `DELETE FROM tbl LIMIT 1, 2`, &query.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl", Tok: query.IDENT}},
			},
			Limit:       pos(16),
			LimitExpr:   &query.NumberLit{ValuePos: pos(22), Value: "1"},
			OffsetComma: pos(23),
			OffsetExpr:  &query.NumberLit{ValuePos: pos(25), Value: "2"},
		})

		AssertParseStatement(t, `DELETE FROM tbl1 WHERE id IN (SELECT tbl1_id FROM tbl2 WHERE foo = 'bar')`, &query.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl1", Tok: query.IDENT}},
			},
			Where: pos(17),
			WhereExpr: &query.BinaryExpr{
				X:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(23), Name: "id", Tok: query.IDENT}},
				OpPos: pos(26),
				Op:    query.IN,
				Y: &query.ExprList{
					Lparen: pos(29),
					Exprs: []query.Expr{query.SelectExpr{
						SelectStatement: &query.SelectStatement{
							Select: pos(30),
							Columns: []*query.ResultColumn{
								{
									Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(37), Name: "tbl1_id", Tok: query.IDENT}},
								},
							},
							From: pos(45),
							Source: &query.QualifiedTableName{
								Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(50), Name: "tbl2", Tok: query.IDENT}},
							},
							Where: pos(55),
							WhereExpr: &query.BinaryExpr{
								X:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(61), Name: "foo", Tok: query.IDENT}},
								OpPos: pos(65),
								Op:    query.EQ,
								Y:     &query.StringLit{ValuePos: pos(67), Value: "bar"},
							},
						},
					}},
					Rparen: pos(72),
				},
			},
		})

		AssertParseStatementError(t, `DELETE`, `1:6: expected FROM, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM`, `1:11: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl WHERE`, `1:21: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl ORDER `, `1:22: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl ORDER BY`, `1:24: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl ORDER BY x`, `1:26: expected LIMIT, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl LIMIT`, `1:21: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl LIMIT 1,`, `1:24: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl LIMIT 1 OFFSET`, `1:30: expected expression, found 'EOF'`)
	})

	t.Run("DropTable", func(t *testing.T) {
		AssertParseStatement(t, `DROP TABLE vw`, &query.DropTableStatement{
			Drop:  pos(0),
			Table: pos(5),
			Name:  &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(11), Name: "vw", Tok: query.IDENT}},
		})
		AssertParseStatement(t, `DROP TABLE proj.sch.tbl`, &query.DropTableStatement{
			Drop:  pos(0),
			Table: pos(5),
			Name: &query.MultiPartIdent{
				First:  &query.Ident{NamePos: pos(11), Name: "proj", Tok: query.IDENT},
				Dot1:   pos(15),
				Second: &query.Ident{NamePos: pos(16), Name: "sch", Tok: query.IDENT},
				Dot2:   pos(19),
				Name:   &query.Ident{NamePos: pos(20), Name: "tbl", Tok: query.IDENT},
			},
		})
		AssertParseStatement(t, `DROP TABLE IF EXISTS vw`, &query.DropTableStatement{
			Drop:     pos(0),
			Table:    pos(5),
			If:       pos(11),
			IfExists: pos(14),
			Name:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(21), Name: "vw", Tok: query.IDENT}},
		})
		AssertParseStatementError(t, `DROP TABLE`, `1:10: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `DROP TABLE IF`, `1:13: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `DROP TABLE IF EXISTS`, `1:20: expected table name, found 'EOF'`)
	})

	t.Run("CreateTable", func(t *testing.T) {
		AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT, col2 DECIMAL(10,5))`, &query.CreateTableStatement{
			Create: pos(0),
			Table:  pos(7),
			Name: &query.MultiPartIdent{Name: &query.Ident{
				Name:    "tbl",
				NamePos: pos(13),
				Tok:     query.IDENT,
			}},
			Lparen: pos(17),
			Columns: []*query.ColumnDefinition{
				{
					Name: &query.Ident{NamePos: pos(18), Name: "col1", Tok: query.IDENT},
					Type: &query.Type{
						Name: &query.Ident{NamePos: pos(23), Name: "TEXT", Tok: query.IDENT},
					},
				},
				{
					Name: &query.Ident{NamePos: pos(29), Name: "col2", Tok: query.IDENT},
					Type: &query.Type{
						Name:      &query.Ident{NamePos: pos(34), Name: "DECIMAL", Tok: query.IDENT},
						Lparen:    pos(41),
						Precision: &query.NumberLit{ValuePos: pos(42), Value: "10"},
						Scale:     &query.NumberLit{ValuePos: pos(45), Value: "5"},
						Rparen:    pos(46),
					},
				},
			},
			Rparen: pos(47),
		})

		// No column type
		AssertParseStatement(t, `CREATE TABLE tbl (col1, col2)`, &query.CreateTableStatement{
			Create: pos(0),
			Table:  pos(7),
			Name: &query.MultiPartIdent{Name: &query.Ident{
				Name:    "tbl",
				NamePos: pos(13),
				Tok:     query.IDENT,
			}},
			Lparen: pos(17),
			Columns: []*query.ColumnDefinition{
				{
					Name: &query.Ident{NamePos: pos(18), Name: "col1", Tok: query.IDENT},
				},
				{
					Name: &query.Ident{NamePos: pos(24), Name: "col2", Tok: query.IDENT},
				},
			},
			Rparen: pos(28),
		})

		// Column name as a bare keyword
		AssertParseStatement(t, `CREATE TABLE tbl (key)`, &query.CreateTableStatement{
			Create: pos(0),
			Table:  pos(7),
			Name: &query.MultiPartIdent{Name: &query.Ident{
				Name:    "tbl",
				NamePos: pos(13),
				Tok:     query.IDENT,
			}},
			Lparen: pos(17),
			Columns: []*query.ColumnDefinition{
				{
					Name: &query.Ident{NamePos: pos(18), Name: "key", Tok: query.IDENT},
				},
			},
			Rparen: pos(21),
		})

		// With comments
		AssertParseStatement(t, "CREATE TABLE tbl ( -- comment\n\tcol1 TEXT, -- comment\n\t  col2 TEXT)", &query.CreateTableStatement{
			Create: pos(0),
			Table:  pos(7),
			Name: &query.MultiPartIdent{Name: &query.Ident{
				Name:    "tbl",
				NamePos: pos(13),
				Tok:     query.IDENT,
			}},
			Lparen: pos(17),
			Columns: []*query.ColumnDefinition{
				{
					Name: &query.Ident{NamePos: query.Pos{Offset: 31, Line: 2, Column: 2}, Name: "col1", Tok: query.IDENT},
					Type: &query.Type{
						Name: &query.Ident{NamePos: query.Pos{Offset: 36, Line: 2, Column: 7}, Name: "TEXT", Tok: query.IDENT},
					},
				},
				{
					Name: &query.Ident{NamePos: query.Pos{Offset: 56, Line: 3, Column: 4}, Name: "col2", Tok: query.IDENT},
					Type: &query.Type{
						Name: &query.Ident{NamePos: query.Pos{Offset: 61, Line: 3, Column: 9}, Name: "TEXT", Tok: query.IDENT},
					},
				},
			},
			Rparen: query.Pos{Offset: 65, Line: 3, Column: 13},
		})

		AssertParseStatementError(t, `CREATE TABLE`, `1:12: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl `, `1:17: expected AS or left paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (`, `1:18: expected column name, CONSTRAINT, or right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 TEXT`, `1:27: expected column name, CONSTRAINT, or right paren, found 'EOF'`)

		AssertParseStatement(t, `CREATE TABLE IF NOT EXISTS tbl (col1 TEXT)`, &query.CreateTableStatement{
			Create:      pos(0),
			Table:       pos(7),
			If:          pos(13),
			IfNot:       pos(16),
			IfNotExists: pos(20),
			Name: &query.MultiPartIdent{Name: &query.Ident{
				Name:    "tbl",
				NamePos: pos(27),
				Tok:     query.IDENT,
			}},
			Lparen: pos(31),
			Columns: []*query.ColumnDefinition{
				{
					Name: &query.Ident{NamePos: pos(32), Name: "col1", Tok: query.IDENT},
					Type: &query.Type{
						Name: &query.Ident{NamePos: pos(37), Name: "TEXT", Tok: query.IDENT},
					},
				},
			},
			Rparen: pos(41),
		})

		AssertParseStatement(t, `CREATE TABLE tbl (col1 TEXT, ts DATETIME)`, &query.CreateTableStatement{
			Create: pos(0),
			Table:  pos(7),
			Name: &query.MultiPartIdent{Name: &query.Ident{
				Name:    "tbl",
				NamePos: pos(13),
				Tok:     query.IDENT,
			}},
			Lparen: pos(17),
			Columns: []*query.ColumnDefinition{
				{
					Name: &query.Ident{
						NamePos: pos(18),
						Name:    "col1",
						Tok:     query.IDENT,
					},
					Type: &query.Type{
						Name: &query.Ident{
							NamePos: pos(23),
							Name:    "TEXT",
							Tok:     query.IDENT,
						},
					},
				},
				{
					Name: &query.Ident{
						NamePos: pos(29),
						Name:    "ts",
						Tok:     query.IDENT,
					},
					Type: &query.Type{
						Name: &query.Ident{
							NamePos: pos(32),
							Name:    "DATETIME",
							Tok:     query.IDENT,
						},
					},
				},
			},
			Rparen: pos(40),
		})

		AssertParseStatement(t, "CREATE TABLE t (c1 CHARACTER VARYING, c2 UUID, c3 TIMESTAMP)", &query.CreateTableStatement{
			Create: pos(0),
			Table:  pos(7),
			Name: &query.MultiPartIdent{Name: &query.Ident{
				NamePos: pos(13),
				Name:    "t",
				Tok:     query.IDENT,
			}},
			Lparen: pos(15),
			Columns: []*query.ColumnDefinition{
				{
					Name: &query.Ident{
						NamePos: pos(16),
						Name:    "c1",
						Tok:     query.IDENT,
					},
					Type: &query.Type{
						Name: &query.Ident{
							NamePos: pos(19),
							Name:    "CHARACTER VARYING",
							Tok:     query.IDENT,
						},
					},
				},
				{
					Name: &query.Ident{
						NamePos: pos(38),
						Name:    "c2",
						Tok:     query.IDENT,
					},
					Type: &query.Type{
						Name: &query.Ident{
							NamePos: pos(41),
							Name:    "UUID",
							Tok:     query.IDENT,
						},
					},
				},
				{
					Name: &query.Ident{
						NamePos: pos(47),
						Name:    "c3",
						Tok:     query.IDENT,
					},
					Type: &query.Type{
						Name: &query.Ident{
							NamePos: pos(50),
							Name:    "TIMESTAMP",
							Tok:     query.TIMESTAMP,
						},
					},
				},
			},
			Rparen: pos(59),
		})

		AssertParseStatement(t, "CREATE TABLE t (c1 NULL)", &query.CreateTableStatement{
			Create: pos(0),
			Table:  pos(7),
			Name: &query.MultiPartIdent{Name: &query.Ident{
				NamePos: pos(13),
				Name:    "t",
				Tok:     query.IDENT,
			}},
			Lparen: pos(15),
			Columns: []*query.ColumnDefinition{
				{
					Name: &query.Ident{
						NamePos: pos(16),
						Name:    "c1",
						Tok:     query.IDENT,
					},
					Type: &query.Type{
						Name: &query.Ident{
							NamePos: pos(19),
							Name:    "NULL",
						},
					},
				},
			},
			Rparen: pos(23),
		})

		AssertParseStatementError(t, `CREATE TABLE IF`, `1:15: expected NOT, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE IF NOT`, `1:19: expected EXISTS, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1`, `1:22: expected column name, CONSTRAINT, or right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 DECIMAL(`, `1:31: expected precision, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 DECIMAL(-12,`, `1:35: expected scale, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 DECIMAL(1,2`, `1:34: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl (col1 DECIMAL(1`, `1:32: expected right paren, found 'EOF'`)

		AssertParseStatement(t, `CREATE TABLE tbl AS SELECT foo`, &query.CreateTableStatement{
			Create: pos(0),
			Table:  pos(7),
			Name: &query.MultiPartIdent{Name: &query.Ident{
				Name:    "tbl",
				NamePos: pos(13),
				Tok:     query.IDENT,
			}},
			As: pos(17),
			Select: &query.SelectStatement{
				Select: pos(20),
				Columns: []*query.ResultColumn{
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(27), Name: "foo", Tok: query.IDENT}}},
				},
			},
		})
		AssertParseStatement(t, `CREATE TABLE tbl AS WITH cte (x) AS (SELECT y) SELECT foo`, &query.CreateTableStatement{
			Create: pos(0),
			Table:  pos(7),
			Name: &query.MultiPartIdent{Name: &query.Ident{
				Name:    "tbl",
				NamePos: pos(13),
				Tok:     query.IDENT,
			}},
			As: pos(17),
			Select: &query.SelectStatement{
				WithClause: &query.WithClause{
					With: pos(20),
					CTEs: []*query.CTE{
						{
							TableName:     &query.Ident{NamePos: pos(25), Name: "cte", Tok: query.IDENT},
							ColumnsLparen: pos(29),
							Columns: []*query.Ident{
								{NamePos: pos(30), Name: "x", Tok: query.IDENT},
							},
							ColumnsRparen: pos(31),
							As:            pos(33),
							SelectLparen:  pos(36),
							Select: &query.SelectStatement{
								Select: pos(37),
								Columns: []*query.ResultColumn{
									{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(44), Name: "y", Tok: query.IDENT}}},
								},
							},
							SelectRparen: pos(45),
						},
					},
				},
				Select: pos(47),
				Columns: []*query.ResultColumn{
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(54), Name: "foo", Tok: query.IDENT}}},
				},
			},
		})
		AssertParseStatementError(t, `CREATE TABLE tbl AS`, `1:19: expected SELECT or VALUES, found 'EOF'`)
		AssertParseStatementError(t, `CREATE TABLE tbl AS WITH`, `1:24: expected table name, found 'EOF'`)

		t.Run("WithComment", func(t *testing.T) {
			t.Run("SingleLine", func(t *testing.T) {
				AssertParseStatement(t, "CREATE TABLE tbl\n\t-- test one two\n\t(col1 TEXT)", &query.CreateTableStatement{
					Create: pos(0),
					Table:  pos(7),
					Name:   &query.MultiPartIdent{Name: &query.Ident{Name: "tbl", Tok: query.IDENT, NamePos: query.Pos{Offset: 13, Line: 1, Column: 14}}},
					Lparen: query.Pos{Offset: 35, Line: 3, Column: 2},
					Columns: []*query.ColumnDefinition{
						{
							Name: &query.Ident{Name: "col1", NamePos: query.Pos{Offset: 36, Line: 3, Column: 3}, Tok: query.IDENT},
							Type: &query.Type{
								Name: &query.Ident{Name: "TEXT", NamePos: query.Pos{Offset: 41, Line: 3, Column: 8}, Tok: query.IDENT},
							},
						},
					},
					Rparen: query.Pos{Offset: 45, Line: 3, Column: 12},
				})
			})
			t.Run("MultiLine", func(t *testing.T) {
				AssertParseStatement(t, "CREATE TABLE tbl\n\t/* test one\ntwo */ (col1 TEXT)", &query.CreateTableStatement{
					Create: pos(0),
					Table:  pos(7),
					Name:   &query.MultiPartIdent{Name: &query.Ident{Name: "tbl", Tok: query.IDENT, NamePos: query.Pos{Offset: 13, Line: 1, Column: 14}}},
					Lparen: query.Pos{Offset: 37, Line: 3, Column: 8},
					Columns: []*query.ColumnDefinition{
						{
							Name: &query.Ident{Name: "col1", Tok: query.IDENT, NamePos: query.Pos{Offset: 38, Line: 3, Column: 9}},
							Type: &query.Type{
								Name: &query.Ident{Name: "TEXT", Tok: query.IDENT, NamePos: query.Pos{Offset: 43, Line: 3, Column: 14}},
							},
						},
					},
					Rparen: query.Pos{Offset: 47, Line: 3, Column: 18},
				})
			})
		})
	})

	t.Run("MergeStatement", func(t *testing.T) {
		AssertParseStatement(t, `MERGE INTO tbl1 target_table USING source_tbl src ON target_table.id = src.id
WHEN MATCHED THEN UPDATE SET target_table.place = src.place
WHEN NOT MATCHED THEN INSERT (id, place) VALUES (src.id, src.place);`, &query.MergeStatement{
			Merge: pos(0),
			Into:  pos(6),
			Target: &query.QualifiedTableName{
				Name:  &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(11), Name: "tbl1", Tok: query.IDENT}},
				Alias: &query.Ident{NamePos: pos(16), Name: "target_table", Tok: query.IDENT},
			},
			Using: pos(29),
			Source: &query.QualifiedTableName{
				Name:  &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(35), Name: "source_tbl", Tok: query.IDENT}},
				Alias: &query.Ident{NamePos: pos(46), Name: "src", Tok: query.IDENT},
			},
			On: pos(50),
			OnExpr: &query.BinaryExpr{
				X: &query.MultiPartIdent{
					First: &query.Ident{NamePos: pos(53), Name: "target_table", Tok: query.IDENT},
					Dot1:  pos(65),
					Name:  &query.Ident{NamePos: pos(66), Name: "id", Tok: query.IDENT}},
				Y: &query.MultiPartIdent{
					First: &query.Ident{NamePos: pos(71), Name: "src", Tok: query.IDENT},
					Dot1:  pos(74),
					Name:  &query.Ident{NamePos: pos(75), Name: "id", Tok: query.IDENT}},
				Op:    query.EQ,
				OpPos: pos(69),
			},
			Matched: []*query.MatchedCondition{
				{
					When:      query.Pos{Offset: 78, Line: 2, Column: 1},
					Matched:   query.Pos{Offset: 83, Line: 2, Column: 6},
					Then:      query.Pos{Offset: 91, Line: 2, Column: 14},
					Update:    query.Pos{Offset: 96, Line: 2, Column: 19},
					UpdateSet: query.Pos{Offset: 103, Line: 2, Column: 26},
					Assignments: []*query.Assignment{
						{
							Columns: []*query.MultiPartIdent{
								{
									First: &query.Ident{
										NamePos: query.Pos{Offset: 107, Line: 2, Column: 30}, Name: "target_table", Tok: query.IDENT},
									Dot1: query.Pos{Offset: 119, Line: 2, Column: 42},
									Name: &query.Ident{NamePos: query.Pos{Offset: 120, Line: 2, Column: 43}, Name: "place", Tok: query.IDENT},
								},
							},
							Eq: query.Pos{Offset: 126, Line: 2, Column: 49},
							Expr: &query.MultiPartIdent{
								First: &query.Ident{NamePos: query.Pos{Offset: 128, Line: 2, Column: 51}, Name: "src", Tok: query.IDENT},
								Dot1:  query.Pos{Offset: 131, Line: 2, Column: 54},
								Name:  &query.Ident{NamePos: query.Pos{Offset: 132, Line: 2, Column: 55}, Name: "place", Tok: query.IDENT},
							},
						},
					},
				},
				{
					When:    query.Pos{Offset: 138, Line: 3, Column: 1},
					Not:     query.Pos{Offset: 143, Line: 3, Column: 6},
					Matched: query.Pos{Offset: 147, Line: 3, Column: 10},
					Then:    query.Pos{Offset: 155, Line: 3, Column: 18},
					Insert:  query.Pos{Offset: 160, Line: 3, Column: 23},
					Values:  query.Pos{Offset: 179, Line: 3, Column: 42},
					ColList: &query.ExprList{
						Lparen: query.Pos{Offset: 167, Line: 3, Column: 30},
						Rparen: query.Pos{Offset: 177, Line: 3, Column: 40},
						Exprs: []query.Expr{
							&query.MultiPartIdent{
								Name: &query.Ident{NamePos: query.Pos{Offset: 168, Line: 3, Column: 31}, Name: "id", Tok: query.IDENT},
							},
							&query.MultiPartIdent{
								Name: &query.Ident{NamePos: query.Pos{Offset: 172, Line: 3, Column: 35}, Name: "place", Tok: query.IDENT},
							},
						},
					},
					ValueLists: &query.ExprList{
						Lparen: query.Pos{Offset: 186, Line: 3, Column: 49},
						Rparen: query.Pos{Offset: 204, Line: 3, Column: 67},
						Exprs: []query.Expr{
							&query.MultiPartIdent{
								First: &query.Ident{NamePos: query.Pos{Offset: 187, Line: 3, Column: 50}, Name: "src", Tok: query.IDENT},
								Dot1:  query.Pos{Offset: 190, Line: 3, Column: 53},
								Name:  &query.Ident{NamePos: query.Pos{Offset: 191, Line: 3, Column: 54}, Name: "id", Tok: query.IDENT},
							},
							&query.MultiPartIdent{
								First: &query.Ident{NamePos: query.Pos{Offset: 195, Line: 3, Column: 58}, Name: "src", Tok: query.IDENT},
								Dot1:  query.Pos{Offset: 198, Line: 3, Column: 61},
								Name:  &query.Ident{NamePos: query.Pos{Offset: 199, Line: 3, Column: 62}, Name: "place", Tok: query.IDENT},
							},
						},
					},
				},
			},
		})
	})
}
