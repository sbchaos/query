package query_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sbchaos/query"
)

func TestParser_ParseStatement2(t *testing.T) {
	t.Run("DEBUG", func(t *testing.T) {
		t.Run("SetStatement1", func(t *testing.T) {
			input := `
		;-- script-mode
		set odps.sql.allow.fullscan=true
		;
		set odps.sql.bigquery.compatible=true
		;
		set odps.sql.allow.cartesian=true
		;
		set odps.sql.groupby.orderby.position.alias=true
		;
		set odps.sql.type.system.odps2=true
		;
`
			stmt, err := query.NewParser(strings.NewReader(input)).ParseStatements()

			assert.NoError(t, err)
			assert.Equal(t, 5, len(stmt))
		})
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
			Name: &query.Ident{Name: "@start_date", NamePos: pos(0), Bind: true},
			Type: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "Date"}},
		})
		AssertParseStatement(t, `@start_date := '{{ .DSTART | Date }}';`, &query.DeclarationStatement{
			Name:  &query.Ident{Name: "@start_date", NamePos: pos(0), Bind: true},
			Value: &query.StringLit{ValuePos: pos(15), Value: "{{ .DSTART | Date }}"},
		})
		AssertParseStatement(t, `@start_date := DATE '{{ .DSTART | Date }}';`, &query.DeclarationStatement{
			Name:  &query.Ident{Name: "@start_date", NamePos: pos(0), Bind: true},
			Type:  &query.MultiPartIdent{Name: &query.Ident{Name: "DATE", NamePos: pos(15)}},
			Value: &query.StringLit{ValuePos: pos(20), Value: "{{ .DSTART | Date }}"},
		})
		AssertParseStatement(t, `@start_date := TO_DATE('{{ .DSTART | Date }}');`, &query.DeclarationStatement{
			Name: &query.Ident{Name: "@start_date", NamePos: pos(0), Bind: true},
			Value: &query.Call{
				Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(15), Name: "TO_DATE"}},
				Lparen: pos(22),
				Rparen: pos(45),
				Args: []query.Expr{
					&query.StringLit{ValuePos: pos(23), Value: "{{ .DSTART | Date }}"},
				},
			},
		})
		AssertParseStatement(t, `@modified_timestamp := CURRENT_TIMESTAMP();`, &query.DeclarationStatement{
			Name: &query.Ident{Name: "@modified_timestamp", NamePos: pos(0), Bind: true},
			Value: &query.Call{
				Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(23), Name: "CURRENT_TIMESTAMP"}},
				Lparen: pos(40),
				Rparen: pos(41),
			},
		})
		AssertParseStatement(t, `@tmp := SELECT data_date, shop_id FROM shop;`, &query.DeclarationStatement{
			Name: &query.Ident{Name: "@tmp", NamePos: pos(0), Bind: true},
			Value: query.SelectExpr{
				SelectStatement: &query.SelectStatement{
					Select: pos(8),
					Columns: []*query.ResultColumn{
						{
							Expr: &query.MultiPartIdent{
								Name: &query.Ident{
									NamePos: pos(15),
									Name:    "data_date",
								},
							},
						},
						{
							Expr: &query.MultiPartIdent{
								Name: &query.Ident{
									NamePos: pos(26),
									Name:    "shop_id",
								},
							},
						},
					},
					From: pos(34),
					Source: &query.QualifiedTableName{
						Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(39), Name: "shop"}},
					},
				},
			},
		})
	})

	t.Run("Insert", func(t *testing.T) {
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (1, 2)`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x"},
				{NamePos: pos(20), Name: "y"},
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

		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (@foo, @bar)`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x"},
				{NamePos: pos(20), Name: "y"},
			},
			ColumnsRparen: pos(21),
			Values:        pos(23),
			ValueLists: []*query.ExprList{{
				Lparen: pos(30),
				Exprs: []query.Expr{
					&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(31), Name: "@foo", Bind: true}},
					&query.MultiPartIdent{Name: &query.Ident{NamePos: pos(37), Name: "@bar", Bind: true}},
				},
				Rparen: pos(41),
			}},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (1, random())`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x"},
				{NamePos: pos(20), Name: "y"},
			},
			ColumnsRparen: pos(21),
			Values:        pos(23),
			ValueLists: []*query.ExprList{{
				Lparen: pos(30),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(31), Value: "1"},
					&query.Call{
						Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(34), Name: "random"}},
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
			Table:         &query.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x"},
				{NamePos: pos(20), Name: "y"},
			},
			ColumnsRparen: pos(21),
			Values:        pos(23),
			ValueLists: []*query.ExprList{{
				Lparen: pos(30),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(31), Value: "1"},
					&query.Call{
						Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(34), Name: "abs"}},
						Lparen: pos(37),
						Rparen: pos(46),
						Args: []query.Expr{
							&query.Call{
								Name:   &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(38), Name: "random"}},
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
			Table:         &query.Ident{NamePos: pos(13), Name: "tbl"},
			ColumnsLparen: pos(17),
			Columns: []*query.Ident{
				{NamePos: pos(18), Name: "x"},
				{NamePos: pos(21), Name: "y"},
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

		AssertParseStatement(t, `INSERT OR REPLACE INTO tbl (x) VALUES (1)`, &query.InsertStatement{
			Insert:          pos(0),
			InsertOr:        pos(7),
			InsertOrReplace: pos(10),
			Into:            pos(18),
			Table:           &query.Ident{NamePos: pos(23), Name: "tbl"},
			ColumnsLparen:   pos(27),
			Columns: []*query.Ident{
				{NamePos: pos(28), Name: "x"},
			},
			ColumnsRparen: pos(29),
			Values:        pos(31),
			ValueLists: []*query.ExprList{{
				Lparen: pos(38),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(39), Value: "1"},
				},
				Rparen: pos(40),
			}},
		})
		AssertParseStatement(t, `INSERT OR ROLLBACK INTO tbl (x) VALUES (1)`, &query.InsertStatement{
			Insert:           pos(0),
			InsertOr:         pos(7),
			InsertOrRollback: pos(10),
			Into:             pos(19),
			Table:            &query.Ident{NamePos: pos(24), Name: "tbl"},
			ColumnsLparen:    pos(28),
			Columns: []*query.Ident{
				{NamePos: pos(29), Name: "x"},
			},
			ColumnsRparen: pos(30),
			Values:        pos(32),
			ValueLists: []*query.ExprList{{
				Lparen: pos(39),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(40), Value: "1"},
				},
				Rparen: pos(41),
			}},
		})
		AssertParseStatement(t, `INSERT OR ABORT INTO tbl (x) VALUES (1)`, &query.InsertStatement{
			Insert:        pos(0),
			InsertOr:      pos(7),
			InsertOrAbort: pos(10),
			Into:          pos(16),
			Table:         &query.Ident{NamePos: pos(21), Name: "tbl"},
			ColumnsLparen: pos(25),
			Columns: []*query.Ident{
				{NamePos: pos(26), Name: "x"},
			},
			ColumnsRparen: pos(27),
			Values:        pos(29),
			ValueLists: []*query.ExprList{{
				Lparen: pos(36),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(37), Value: "1"},
				},
				Rparen: pos(38),
			}},
		})
		AssertParseStatement(t, `INSERT OR FAIL INTO tbl VALUES (1)`, &query.InsertStatement{
			Insert:       pos(0),
			InsertOr:     pos(7),
			InsertOrFail: pos(10),
			Into:         pos(15),
			Table:        &query.Ident{NamePos: pos(20), Name: "tbl"},
			Values:       pos(24),
			ValueLists: []*query.ExprList{{
				Lparen: pos(31),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(32), Value: "1"},
				},
				Rparen: pos(33),
			}},
		})
		AssertParseStatement(t, `INSERT OR IGNORE INTO tbl AS tbl2 VALUES (1)`, &query.InsertStatement{
			Insert:         pos(0),
			InsertOr:       pos(7),
			InsertOrIgnore: pos(10),
			Into:           pos(17),
			Table:          &query.Ident{NamePos: pos(22), Name: "tbl"},
			As:             pos(26),
			Alias:          &query.Ident{NamePos: pos(29), Name: "tbl2"},
			Values:         pos(34),
			ValueLists: []*query.ExprList{{
				Lparen: pos(41),
				Exprs: []query.Expr{
					&query.NumberLit{ValuePos: pos(42), Value: "1"},
				},
				Rparen: pos(43),
			}},
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
			Table:         &query.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x"},
			},
			ColumnsRparen: pos(18),
			Select: &query.SelectStatement{
				Select: pos(20),
				Columns: []*query.ResultColumn{
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(27), Name: "y"}}},
				},
			},
		})

		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (y ASC, z DESC) DO NOTHING`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x"},
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
					{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(44), Name: "y"}}, Asc: pos(46)},
					{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(51), Name: "z"}}, Desc: pos(53)},
				},
				Rparen:    pos(57),
				Do:        pos(59),
				DoNothing: pos(62),
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING *`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x"},
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
			Table:         &query.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x"},
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
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(41), Name: "x"}}},
				},
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING x AS y`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x"},
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
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(41), Name: "x"}}, As: pos(43), Alias: &query.Ident{NamePos: pos(46), Name: "y"}},
				},
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING x,y`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x"},
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
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(41), Name: "x"}}},
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(43), Name: "y"}}},
				},
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) RETURNING x,y*2`, &query.InsertStatement{
			Insert:        pos(0),
			Into:          pos(7),
			Table:         &query.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x"},
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
					{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(41), Name: "x"}}},
					{
						Expr: &query.BinaryExpr{
							X:  &query.MultiPartIdent{Name: &query.Ident{Name: "y", NamePos: pos(43)}},
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
			Table:         &query.Ident{NamePos: pos(12), Name: "tbl"},
			ColumnsLparen: pos(16),
			Columns: []*query.Ident{
				{NamePos: pos(17), Name: "x"},
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
					{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(44), Name: "y"}}},
				},
				Rparen:      pos(45),
				Where:       pos(47),
				WhereExpr:   &query.BoolLit{ValuePos: pos(53), Value: true},
				Do:          pos(58),
				DoUpdate:    pos(61),
				DoUpdateSet: pos(68),
				Assignments: []*query.Assignment{
					{
						Columns: []*query.Ident{
							{NamePos: pos(72), Name: "foo"},
						},
						Eq:   pos(76),
						Expr: &query.NumberLit{ValuePos: pos(78), Value: "1"},
					},
					{
						Lparen: pos(81),
						Columns: []*query.Ident{
							{NamePos: pos(82), Name: "bar"},
							{NamePos: pos(87), Name: "baz"},
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

		AssertParseStatementError(t, `INSERT`, `1:6: expected INTO, found 'EOF'`)
		AssertParseStatementError(t, `INSERT OR`, `1:9: expected ROLLBACK, REPLACE, ABORT, FAIL, or IGNORE, found 'EOF'`)
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
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET (`, `1:62: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET (foo`, `1:65: expected comma or right paren, found 'EOF'`)
	})

	t.Run("Delete", func(t *testing.T) {
		AssertParseStatement(t, `DELETE FROM tbl`, &query.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl"}},
			},
		})
		AssertParseStatement(t, `DELETE FROM tbl WHERE x = 1`, &query.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl"}},
			},
			Where: pos(16),
			WhereExpr: &query.BinaryExpr{
				X:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(22), Name: "x"}},
				OpPos: pos(24), Op: query.EQ,
				Y: &query.NumberLit{ValuePos: pos(26), Value: "1"},
			},
		})
		AssertParseStatement(t, `DELETE FROM tbl WHERE x = 1 RETURNING x`, &query.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl"}},
			},
			Where: pos(16),
			WhereExpr: &query.BinaryExpr{
				X:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(22), Name: "x"}},
				OpPos: pos(24), Op: query.EQ,
				Y: &query.NumberLit{ValuePos: pos(26), Value: "1"},
			},
			ReturningClause: &query.ReturningClause{
				Returning: pos(28),
				Columns:   []*query.ResultColumn{{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(38), Name: "x"}}}},
			},
		})
		AssertParseStatement(t, `WITH cte (x) AS (SELECT y) DELETE FROM tbl`, &query.DeleteStatement{
			WithClause: &query.WithClause{
				With: pos(0),
				CTEs: []*query.CTE{
					{
						TableName:     &query.Ident{NamePos: pos(5), Name: "cte"},
						ColumnsLparen: pos(9),
						Columns: []*query.Ident{
							{NamePos: pos(10), Name: "x"},
						},
						ColumnsRparen: pos(11),
						As:            pos(13),
						SelectLparen:  pos(16),
						Select: &query.SelectStatement{
							Select: pos(17),
							Columns: []*query.ResultColumn{
								{Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(24), Name: "y"}}},
							},
						},
						SelectRparen: pos(25),
					},
				},
			},
			Delete: pos(27),
			From:   pos(34),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(39), Name: "tbl"}},
			},
		})
		AssertParseStatement(t, `DELETE FROM tbl ORDER BY x, y LIMIT 1 OFFSET 2`, &query.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl"}},
			},
			Order:   pos(16),
			OrderBy: pos(22),
			OrderingTerms: []*query.OrderingTerm{
				{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(25), Name: "x"}}},
				{X: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(28), Name: "y"}}},
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
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl"}},
			},
			Limit:     pos(16),
			LimitExpr: &query.NumberLit{ValuePos: pos(22), Value: "1"},
		})
		AssertParseStatement(t, `DELETE FROM tbl LIMIT 1, 2`, &query.DeleteStatement{
			Delete: pos(0),
			From:   pos(7),
			Table: &query.QualifiedTableName{
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl"}},
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
				Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(12), Name: "tbl1"}},
			},
			Where: pos(17),
			WhereExpr: &query.BinaryExpr{
				X:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(23), Name: "id"}},
				OpPos: pos(26),
				Op:    query.IN,
				Y: &query.ExprList{
					Lparen: pos(29),
					Exprs: []query.Expr{query.SelectExpr{
						SelectStatement: &query.SelectStatement{
							Select: pos(30),
							Columns: []*query.ResultColumn{
								{
									Expr: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(37), Name: "tbl1_id"}},
								},
							},
							From: pos(45),
							Source: &query.QualifiedTableName{
								Name: &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(50), Name: "tbl2"}},
							},
							Where: pos(55),
							WhereExpr: &query.BinaryExpr{
								X:     &query.MultiPartIdent{Name: &query.Ident{NamePos: pos(61), Name: "foo"}},
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
}
