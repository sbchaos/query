package query

import (
	"errors"
	"io"
)

var EmptyStmt = Error{Pos: Pos{}, Msg: "empty statement"}

func (p *Parser) ParseStatements() ([]Statement, error) {
	var stmts []Statement
	for {
		stmt, err := p.ParseStatement()
		if err != nil {
			if err == io.EOF {
				return stmts, nil
			} else if errors.Is(err, EmptyStmt) {
				continue
			}
			return nil, err
		}
		stmts = append(stmts, stmt)
	}
}

func (p *Parser) ParseStatement() (stmt Statement, err error) {
	switch tok := p.peek(); tok {
	case EOF:
		return nil, io.EOF
	case SEMI:
		p.scan()
		return nil, EmptyStmt
	default:
		if stmt, err = p.parseNonExplainStatement(); err != nil {
			return stmt, err
		}
	}

	// Read trailing semicolon or end of file.
	if tok := p.peek(); tok != EOF && tok != SEMI {
		return stmt, p.errorExpected(p.pos, p.tok, "semicolon or EOF")
	}
	p.scan()

	return stmt, nil
}

// parseStmt parses all statement types.
func (p *Parser) parseNonExplainStatement() (Statement, error) {
	switch p.peek() {
	case BIND:
		return p.parseDeclarationStatement()
	case SET:
		return p.parseSetStatement()
	case SELECT, VALUES:
		return p.parseSelectStatement(false, nil)
	case INSERT, REPLACE:
		return p.parseInsertStatement(nil)
	case DELETE:
		return p.parseDeleteStatement(nil)
	case WITH:
		return p.parseWithStatement()
	default:
		return nil, p.errorExpected(p.pos, p.tok, "statement")
	}
}

func (p *Parser) parseDeclarationStatement() (Statement, error) {
	pos, tok, val := p.scan()
	n1 := &Ident{Name: val, NamePos: pos, Bind: tok == BIND}
	var t1 Expr
	var v1 Expr

	if p.peek() == ASSIGN { // should be :=
		p.scan()

		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}

		if p.peek() != SEMI {
			expr2, err := p.ParseExpr()
			if err != nil {
				return nil, err
			}
			t1 = expr
			v1 = expr2
		} else {
			v1 = expr
		}
	} else {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		t1 = expr
	}

	return &DeclarationStatement{Name: n1, Type: t1, Value: v1}, nil
}

func (p *Parser) parseSetStatement() (Statement, error) {
	assert(p.peek() == SET)
	var set SetStatement

	set.Set, _, _ = p.scan()
	key := ""
	for {
		_, tok, val := p.scan()
		if tok == IDENT {
			key += val
		} else if tok == DOT {
			key += "."
		}

		if p.peek() == EQ {
			break
		}
	}
	set.Key = key
	set.Equal, _, _ = p.scan()
	_, _, val := p.scan()
	set.Value = val

	return &set, nil
}

func (p *Parser) parseInsertStatement(withClause *WithClause) (_ *InsertStatement, err error) {
	assert(p.peek() == INSERT || p.peek() == REPLACE)

	var stmt InsertStatement
	stmt.WithClause = withClause

	if p.peek() == INSERT {
		stmt.Insert, _, _ = p.scan()
	} else {
		stmt.Replace, _, _ = p.scan()
	}

	if p.peek() == INTO {
		stmt.Into, _, _ = p.scan()
	} else if p.peek() == OVERWRITE {
		stmt.Overwrite, _, _ = p.scan()
	} else {
		return &stmt, p.errorExpected(p.pos, p.tok, "INTO or OVERWRITE")
	}

	// Parse table name & optional alias.
	if stmt.Table, err = p.parseIdent("table name"); err != nil {
		return &stmt, err
	}
	if p.peek() == AS {
		stmt.As, _, _ = p.scan()
		if stmt.Alias, err = p.parseIdent("alias"); err != nil {
			return &stmt, err
		}
	}

	// Parse optional column list.
	if p.peek() == LP {
		stmt.ColumnsLparen, _, _ = p.scan()
		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &stmt, err
			}
			stmt.Columns = append(stmt.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		stmt.ColumnsRparen, _, _ = p.scan()
	}

	switch p.peek() {
	case VALUES:
		stmt.Values, _, _ = p.scan()
		for {
			var list ExprList
			if p.peek() != LP {
				return &stmt, p.errorExpected(p.pos, p.tok, "left paren")
			}
			list.Lparen, _, _ = p.scan()

			for {
				expr, err := p.ParseExpr()
				if err != nil {
					return &stmt, err
				}
				list.Exprs = append(list.Exprs, expr)

				if p.peek() == RP {
					break
				} else if p.peek() != COMMA {
					return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
				}
				p.scan()
			}
			list.Rparen, _, _ = p.scan()
			stmt.ValueLists = append(stmt.ValueLists, &list)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}
	case SELECT:
		if stmt.Select, err = p.parseSelectStatement(false, nil); err != nil {
			return &stmt, err
		}
	default:
		return &stmt, p.errorExpected(p.pos, p.tok, "VALUES, SELECT, or DEFAULT VALUES")
	}

	// Parse optional upsert clause.
	if p.peek() == ON {
		if stmt.UpsertClause, err = p.parseUpsertClause(); err != nil {
			return &stmt, err
		}
	}

	// Parse optional RETURNING clause.
	if p.peek() == RETURNING {
		if stmt.ReturningClause, err = p.parseReturningClause(); err != nil {
			return &stmt, err
		}
	}

	return &stmt, nil
}

func (p *Parser) parseUpsertClause() (_ *UpsertClause, err error) {
	assert(p.peek() == ON)

	var clause UpsertClause

	// Parse "ON CONFLICT"
	clause.On, _, _ = p.scan()
	if p.peek() != CONFLICT {
		return &clause, p.errorExpected(p.pos, p.tok, "CONFLICT")
	}
	clause.OnConflict, _, _ = p.scan()

	// Parse optional indexed column list & WHERE conditional.
	if p.peek() == LP {
		clause.Lparen, _, _ = p.scan()
		for {
			col, err := p.parseIndexedColumn()
			if err != nil {
				return &clause, err
			}
			clause.Columns = append(clause.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &clause, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		clause.Rparen, _, _ = p.scan()

		if p.peek() == WHERE {
			clause.Where, _, _ = p.scan()
			if clause.WhereExpr, err = p.ParseExpr(); err != nil {
				return &clause, err
			}
		}
	}

	// Parse "DO NOTHING" or "DO UPDATE SET".
	if p.peek() != DO {
		return &clause, p.errorExpected(p.pos, p.tok, "DO")
	}
	clause.Do, _, _ = p.scan()

	// If next token is NOTHING, then read it and exit immediately.
	if p.peek() == NOTHING {
		clause.DoNothing, _, _ = p.scan()
		return &clause, nil
	} else if p.peek() != UPDATE {
		return &clause, p.errorExpected(p.pos, p.tok, "NOTHING or UPDATE SET")
	}

	// Otherwise parse "UPDATE SET"
	clause.DoUpdate, _, _ = p.scan()
	if p.peek() != SET {
		return &clause, p.errorExpected(p.pos, p.tok, "SET")
	}
	clause.DoUpdateSet, _, _ = p.scan()

	// Parse list of assignments.
	for {
		assignment, err := p.parseAssignment()
		if err != nil {
			return &clause, err
		}
		clause.Assignments = append(clause.Assignments, assignment)

		if p.peek() != COMMA {
			break
		}
		p.scan()
	}

	// Parse WHERE after DO UPDATE SET.
	if p.peek() == WHERE {
		clause.UpdateWhere, _, _ = p.scan()
		if clause.UpdateWhereExpr, err = p.ParseExpr(); err != nil {
			return &clause, err
		}
	}

	return &clause, nil
}

func (p *Parser) parseReturningClause() (_ *ReturningClause, err error) {
	assert(p.peek() == RETURNING)

	var clause ReturningClause

	clause.Returning, _, _ = p.scan()
	// Parse result columns.
	for {
		col, err := p.parseResultColumn()
		if err != nil {
			return &clause, err
		}
		clause.Columns = append(clause.Columns, col)

		if p.peek() != COMMA {
			break
		}
		p.scan()
	}

	return &clause, nil
}

func (p *Parser) parseIndexedColumn() (_ *IndexedColumn, err error) {
	var col IndexedColumn
	if col.X, err = p.ParseExpr(); err != nil {
		return &col, err
	}

	if p.peek() == COLLATE {
		col.Collate, _, _ = p.scan()
		if col.Collation, err = p.parseIdent("collation name"); err != nil {
			return &col, err
		}
	}

	if p.peek() == ASC {
		col.Asc, _, _ = p.scan()
	} else if p.peek() == DESC {
		col.Desc, _, _ = p.scan()
	}
	return &col, nil
}

func (p *Parser) parseDeleteStatement(withClause *WithClause) (_ *DeleteStatement, err error) {
	assert(p.peek() == DELETE)

	var stmt DeleteStatement
	stmt.WithClause = withClause

	// Parse "DELETE FROM tbl"
	stmt.Delete, _, _ = p.scan()
	if p.peek() != FROM {
		return &stmt, p.errorExpected(p.pos, p.tok, "FROM")
	}
	stmt.From, _, _ = p.scan()
	if !isIdentToken(p.peek()) {
		return nil, p.errorExpected(p.pos, p.tok, "table name")
	}
	ident, _ := p.parseIdent("table name")
	if stmt.Table, err = p.parseQualifiedTableName(ident, true, true, true, true); err != nil {
		return &stmt, err
	}

	// Parse WHERE clause.
	if p.peek() == WHERE {
		stmt.Where, _, _ = p.scan()
		if stmt.WhereExpr, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}
	}

	// Parse ORDER BY clause. This differs from the SELECT parsing in that
	// if an ORDER BY is specified then the LIMIT is required.
	if p.peek() == ORDER || p.peek() == LIMIT {
		if p.peek() == ORDER {
			stmt.Order, _, _ = p.scan()
			if p.peek() != BY {
				return &stmt, p.errorExpected(p.pos, p.tok, "BY")
			}
			stmt.OrderBy, _, _ = p.scan()

			for {
				term, err := p.parseOrderingTerm()
				if err != nil {
					return &stmt, err
				}
				stmt.OrderingTerms = append(stmt.OrderingTerms, term)

				if p.peek() != COMMA {
					break
				}
				p.scan()
			}
		}

		// Parse LIMIT/OFFSET clause.
		if p.peek() != LIMIT {
			return &stmt, p.errorExpected(p.pos, p.tok, "LIMIT")
		}
		stmt.Limit, _, _ = p.scan()
		if stmt.LimitExpr, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}

		if tok := p.peek(); tok == OFFSET || tok == COMMA {
			if tok == OFFSET {
				stmt.Offset, _, _ = p.scan()
			} else {
				stmt.OffsetComma, _, _ = p.scan()
			}
			if stmt.OffsetExpr, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
		}
	}

	// Parse optional RETURNING clause.
	if p.peek() == RETURNING {
		if stmt.ReturningClause, err = p.parseReturningClause(); err != nil {
			return &stmt, err
		}
	}

	return &stmt, nil
}

func (p *Parser) parseAssignment() (_ *Assignment, err error) {
	var assignment Assignment

	// Parse either a single column (IDENT) or a column list (LP IDENT COMMA IDENT RP)
	if isIdentToken(p.peek()) {
		col, _ := p.parseIdent("column name")
		assignment.Columns = []*Ident{col}
	} else if p.peek() == LP {
		assignment.Lparen, _, _ = p.scan()
		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &assignment, err
			}
			assignment.Columns = append(assignment.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &assignment, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		assignment.Rparen, _, _ = p.scan()
	} else {
		return &assignment, p.errorExpected(p.pos, p.tok, "column name or column list")
	}

	if p.peek() != EQ {
		return &assignment, p.errorExpected(p.pos, p.tok, "=")
	}
	assignment.Eq, _, _ = p.scan()

	if assignment.Expr, err = p.ParseExpr(); err != nil {
		return &assignment, err
	}

	return &assignment, nil
}
