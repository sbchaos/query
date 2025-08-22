package query

// parseSelectStatement parses a SELECT statement.
// If compounded is true, WITH, ORDER BY, & LIMIT/OFFSET are skipped.
func (p *Parser) parseSelectStatement(compounded bool, withClause *WithClause) (_ *SelectStatement, err error) {
	var stmt SelectStatement
	stmt.WithClause = withClause

	// Parse optional "WITH [RECURSIVE} cte, cte..."
	// This is only called here if this method is called directly. Generic
	// statement parsing will parse the WITH clause and pass it in instead.
	if !compounded && stmt.WithClause == nil && p.peek() == WITH {
		if stmt.WithClause, err = p.parseWithClause(); err != nil {
			return &stmt, err
		}
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
		stmt.Select, _, _ = p.scan()

		// Parse optional "DISTINCT" or "ALL".
		if tok := p.peek(); tok == DISTINCT {
			stmt.Distinct, _, _ = p.scan()
		} else if tok == ALL {
			stmt.All, _, _ = p.scan()
		}

		// Parse result columns.
		for {
			col, err := p.parseResultColumn()
			if err != nil {
				return &stmt, err
			}
			stmt.Columns = append(stmt.Columns, col)

			if p.peek() != COMMA {
				break
			}
			p.scan()
			if p.peek() == FROM {
				break
			}
		}

		// Parse FROM clause.
		if p.peek() == FROM {
			stmt.From, _, _ = p.scan()
			if stmt.Source, err = p.parseSource(); err != nil {
				return &stmt, err
			}
		}

		// Parse WHERE clause.
		if p.peek() == WHERE {
			stmt.Where, _, _ = p.scan()
			if stmt.WhereExpr, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
		}

		// Parse GROUP BY/HAVING clause.
		if p.peek() == GROUP {
			stmt.Group, _, _ = p.scan()
			if p.peek() != BY {
				return &stmt, p.errorExpected(p.pos, p.tok, "BY")
			}
			stmt.GroupBy, _, _ = p.scan()

			if p.peek() == ALL {
				stmt.GroupByAll, _, _ = p.scan()
			} else if p.peek() == GROUPING {
				stmt.Grouping, _, _ = p.scan()
				if p.peek() != SETS {
					return &stmt, p.errorExpected(p.pos, p.tok, "SETS")
				}
				stmt.GroupingSet, _, _ = p.scan()

				expr, err := p.ParseExpr()
				if err != nil {
					return &stmt, err
				}
				stmt.GroupingExpr = expr
			} else {
				for {
					expr, err := p.ParseExpr()
					if err != nil {
						return &stmt, err
					}
					stmt.GroupByExprs = append(stmt.GroupByExprs, expr)

					if p.peek() != COMMA {
						break
					}
					p.scan()
				}
			}

			// Parse optional HAVING clause.
			if p.peek() == HAVING {
				stmt.Having, _, _ = p.scan()
				if stmt.HavingExpr, err = p.ParseExpr(); err != nil {
					return &stmt, err
				}
			}
		}

		// Parse optional QUALIFY clause
		if p.peek() == QUALIFY {
			stmt.Qualify, _, _ = p.scan()
			if stmt.QualifyExpr, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
		}

		// Parse WINDOW clause.
		if p.peek() == WINDOW {
			stmt.Window, _, _ = p.scan()

			for {
				var window Window
				if window.Name, err = p.parseIdent("window name"); err != nil {
					return &stmt, err
				}

				if p.peek() != AS {
					return &stmt, p.errorExpected(p.pos, p.tok, "AS")
				}
				window.As, _, _ = p.scan()

				if window.Definition, err = p.parseWindowDefinition(); err != nil {
					return &stmt, err
				}

				stmt.Windows = append(stmt.Windows, &window)

				if p.peek() != COMMA {
					break
				}
				p.scan()
			}
		}
	default:
		return &stmt, p.errorExpected(p.pos, p.tok, "SELECT or VALUES")
	}

	// Optionally compound additional SELECT/VALUES.
	switch tok := p.peek(); tok {
	case UNION, INTERSECT:
		if tok == UNION {
			stmt.Union, _, _ = p.scan()
			if p.peek() == ALL {
				stmt.UnionAll, _, _ = p.scan()
			}
		} else if tok == INTERSECT {
			stmt.Intersect, _, _ = p.scan()
		}

		if stmt.Compound, err = p.parseSelectStatement(true, nil); err != nil {
			return &stmt, err
		}
	}

	// Parse ORDER BY clause.
	if !compounded && p.peek() == ORDER {
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
	// The offset is optional. Can be specified with COMMA or OFFSET.
	// e.g. "LIMIT 1 OFFSET 2" or "LIMIT 1, 2"
	if !compounded && p.peek() == LIMIT {
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

	return &stmt, nil
}

func (p *Parser) parseResultColumn() (_ *ResultColumn, err error) {
	var col ResultColumn

	// An initial "*" returns all columns.
	if p.peek() == STAR {
		col.Star, _, _ = p.scan()

		if p.peek() == EXCEPT {
			col.Except, _, _ = p.scan()
			expr, err := p.ParseExpr()
			if err != nil {
				return &col, err
			}
			col.ExceptCol = expr
		}

		return &col, nil
	}

	// Next can be either "EXPR [[AS] column-alias]" or "IDENT DOT STAR".
	// We need read the next element as an expression and then determine what next.
	if col.Expr, err = p.ParseExpr(); err != nil {
		return &col, err
	}

	// If we have a qualified ref w/ a star, don't allow an alias.
	//if ref, ok := col.Expr.(*QualifiedRef); ok && ref.Star.IsValid() {
	//	return &col, nil
	//}

	// If "AS" is next, the alias must follow.
	// Otherwise it can optionally be an IDENT alias.
	if p.peek() == AS {
		col.As, _, _ = p.scan()
		if !isIdentToken(p.peek()) {
			return &col, p.errorExpected(p.pos, p.tok, "column alias")
		}
		col.Alias, _ = p.parseIdent("column alias")
	} else if isIdentToken(p.peek()) {
		col.Alias, _ = p.parseIdent("column alias")
	}

	return &col, nil
}

func (p *Parser) parseOrderingTerm() (_ *OrderingTerm, err error) {
	var term OrderingTerm
	if term.X, err = p.ParseExpr(); err != nil {
		return &term, err
	}

	//// Parse optional "COLLATE"
	//if p.peek() == COLLATE {
	//	if term.Collation, err = p.parseCollationClause(); err != nil {
	//		return &term, err
	//	}
	//}

	// Parse optional sort direction ("ASC" or "DESC")
	switch p.peek() {
	case ASC:
		term.Asc, _, _ = p.scan()
	case DESC:
		term.Desc, _, _ = p.scan()
	}

	// Parse optional "NULLS FIRST" or "NULLS LAST"
	if p.peek() == NULLS {
		term.Nulls, _, _ = p.scan()
		switch p.peek() {
		case FIRST:
			term.NullsFirst, _, _ = p.scan()
		case LAST:
			term.NullsLast, _, _ = p.scan()
		default:
			return &term, p.errorExpected(p.pos, p.tok, "FIRST or LAST")
		}
	}

	return &term, nil
}

func (p *Parser) parseSource() (source Source, err error) {
	source, err = p.parseUnarySource()
	if err != nil {
		return source, err
	}

	for {
		// Exit immediately if not part of a join operator.
		switch p.peek() {
		case COMMA, NATURAL, FULL, LEFT, INNER, CROSS, JOIN:
		default:
			return source, nil
		}

		// Parse join operator.
		operator, err := p.parseJoinOperator()
		if err != nil {
			return source, err
		}
		y, err := p.parseUnarySource()
		if err != nil {
			return source, err
		}
		constraint, err := p.parseJoinConstraint()
		if err != nil {
			return source, err
		}

		// Rewrite last source to nest next join on right side.
		if lhs, ok := source.(*JoinClause); ok {
			source = &JoinClause{
				X:        lhs.X,
				Operator: lhs.Operator,
				Y: &JoinClause{
					X:          lhs.Y,
					Operator:   operator,
					Y:          y,
					Constraint: constraint,
				},
				Constraint: lhs.Constraint,
			}
		} else {
			source = &JoinClause{X: source, Operator: operator, Y: y, Constraint: constraint}
		}
	}
}

// parseUnarySource parses a qualified table name, table function name, or subquery but not a JOIN.
func (p *Parser) parseUnarySource() (source Source, err error) {
	switch p.peek() {
	case LP:
		return p.parseParenSource()
	case IDENT, QIDENT, TSTRING, BIND, TMPL:
		return p.parseQualifiedTable(true, true)
	case VALUES:
		return p.parseSelectStatement(false, nil)
	default:
		return nil, p.errorExpected(p.pos, p.tok, "table name or left paren")
	}
}

func (p *Parser) parseJoinOperator() (*JoinOperator, error) {
	var op JoinOperator

	// Handle single comma join.
	if p.peek() == COMMA {
		op.Comma, _, _ = p.scan()
		return &op, nil
	}

	if p.peek() == NATURAL {
		op.Natural, _, _ = p.scan()
	}

	// Parse "LEFT", "LEFT OUTER", "INNER", or "CROSS"
	switch p.peek() {
	case LEFT:
		op.Left, _, _ = p.scan()
		if p.peek() == OUTER {
			op.Outer, _, _ = p.scan()
		}
	case INNER:
		op.Inner, _, _ = p.scan()
	case CROSS:
		op.Cross, _, _ = p.scan()
	case FULL:
		op.Full, _, _ = p.scan()
		if p.peek() == OUTER {
			op.Outer, _, _ = p.scan()
		}
	}

	// Parse final JOIN.
	if p.peek() != JOIN {
		return &op, p.errorExpected(p.pos, p.tok, "JOIN")
	}
	op.Join, _, _ = p.scan()

	return &op, nil
}

func (p *Parser) parseJoinConstraint() (JoinConstraint, error) {
	switch p.peek() {
	case ON:
		return p.parseOnConstraint()
	case USING:
		return p.parseUsingConstraint()
	default:
		return nil, nil
	}
}

func (p *Parser) parseOnConstraint() (_ *OnConstraint, err error) {
	assert(p.peek() == ON)

	var con OnConstraint
	con.On, _, _ = p.scan()
	if con.X, err = p.ParseExpr(); err != nil {
		return &con, err
	}
	return &con, nil
}

func (p *Parser) parseUsingConstraint() (*UsingConstraint, error) {
	assert(p.peek() == USING)

	var con UsingConstraint
	con.Using, _, _ = p.scan()

	if p.peek() != LP {
		return &con, p.errorExpected(p.pos, p.tok, "left paren")
	}
	con.Lparen, _, _ = p.scan()

	for {
		col, err := p.parseIdent("column name")
		if err != nil {
			return &con, err
		}
		con.Columns = append(con.Columns, col)

		if p.peek() == RP {
			break
		} else if p.peek() != COMMA {
			return &con, p.errorExpected(p.pos, p.tok, "comma or right paren")
		}
		p.scan()
	}
	con.Rparen, _, _ = p.scan()

	return &con, nil
}

func (p *Parser) parseParenSource() (_ *ParenSource, err error) {
	assert(p.peek() == LP)

	var source ParenSource
	source.Lparen, _, _ = p.scan()

	var withClause *WithClause
	if p.peek() == WITH {
		withClause, err = p.parseWithClause()
		if err != nil {
			return nil, err
		}
	}
	if p.peek() == SELECT {
		if source.X, err = p.parseSelectStatement(false, withClause); err != nil {
			return &source, err
		}
	} else {
		if withClause != nil {
			return &source, p.errorExpected(p.pos, p.tok, "suspicious with clause")
		}
		if source.X, err = p.parseSource(); err != nil {
			return &source, err
		}
	}

	if p.peek() != RP {
		return nil, p.errorExpected(p.pos, p.tok, "right paren")
	}
	source.Rparen, _, _ = p.scan()

	if p.peek() == AS || isIdentToken(p.peek()) {
		if p.peek() == AS {
			source.As, _, _ = p.scan()
		}
		if source.Alias, err = p.parseIdent("table alias"); err != nil {
			return &source, err
		}
	}

	return &source, nil
}

func (p *Parser) parseQualifiedTable(aliasOK, indexedOK bool) (_ Source, err error) {
	if !isIdentToken(p.peek()) {
		return nil, p.errorExpected(p.pos, p.tok, "table name")
	}
	ident, _ := p.parseIdent("table name")
	if p.peek() == LP {
		return p.parseQualifiedTableFunctionName(ident)
	}
	return p.parseQualifiedTableName(ident, aliasOK)
}

func (p *Parser) parseQualifiedTableName(ident *Ident, aliasOK bool) (_ *QualifiedTableName, err error) {
	var tbl QualifiedTableName
	mIdent, dotPos := p.parseMultiIdent(ident)
	if dotPos.IsValid() {
		return nil, &Error{Pos: p.pos, Msg: "Found extra . in input"}
	}
	tbl.Name = mIdent

	// Parse optional table alias ("AS alias" or just "alias").
	if tok := p.peek(); tok == AS || isIdentToken(tok) {
		if !aliasOK {
			return &tbl, p.errorExpected(p.pos, p.tok, "unqualified table name")
		}
		if p.peek() == AS {
			tbl.As, _, _ = p.scan()
		}
		if tbl.Alias, err = p.parseIdent("table alias"); err != nil {
			return &tbl, err
		}
	}

	for p.peek() == LATERAL {
		view, err := p.parseLateralView()
		if err != nil {
			return &tbl, err
		}
		tbl.LateralViews = append(tbl.LateralViews, view)
	}

	return &tbl, nil
}

func (p *Parser) parseLateralView() (*LateralView, error) {
	var lv LateralView
	p1, _, _ := p.scan()
	lv.Lateral = p1

	p2, t2, _ := p.scan()
	if t2 != VIEW {
		return &lv, p.errorExpected(p.pos, p.tok, "lateral view")
	}
	lv.View = p2

	if p.peek() == OUTER {
		lv.Outer, _, _ = p.scan()
	}

	expr, err := p.parseOperand()
	if err != nil {
		return &lv, err
	}

	c1, ok := expr.(*Call)
	if !ok {
		return &lv, p.errorExpected(p.pos, p.tok, "lateral view udf call")
	}
	lv.Udtf = c1

	if !isExprIdentToken(p.peek()) {
		return &lv, p.errorExpected(p.pos, p.tok, "lateral view TableAlias")
	}
	p2, t2, lit2 := p.scan()
	lv.TableAlias = &Ident{Name: lit2, NamePos: p2, Tok: t2}

	if p.peek() != AS {
		return &lv, p.errorExpected(p.pos, p.tok, "lateral view AS")
	}
	lv.As, _, _ = p.scan()

	for {
		var idents []*Ident
		if !isExprIdentToken(p.peek()) {
			return &lv, p.errorExpected(p.pos, p.tok, "lateral view TableAlias")
		}
		p3, t3, lit3 := p.scan()
		idents = append(idents, &Ident{Name: lit3, NamePos: p3, Tok: t3})

		if p.peek() == COMMA {
			p.scan()
		} else {
			lv.ColAlias = idents
			break
		}
	}

	return &lv, nil
}

func (p *Parser) parseQualifiedTableFunctionName(ident *Ident) (_ *QualifiedTableFunctionName, err error) {
	assert(p.peek() == LP)

	var tbl QualifiedTableFunctionName
	tbl.Name = ident

	tbl.Lparen, _, _ = p.scan()
	for {
		expr, err := p.ParseExpr()
		if err != nil {
			return &tbl, err
		}
		tbl.Args = append(tbl.Args, expr)

		if p.peek() == RP {
			break
		} else if p.peek() != COMMA {
			return &tbl, p.errorExpected(p.pos, p.tok, "comma or right paren")
		}
		p.scan()
	}
	tbl.Rparen, _, _ = p.scan()

	// Parse optional table alias ("AS alias" or just "alias").
	if tok := p.peek(); tok == AS || isIdentToken(tok) {
		if p.peek() == AS {
			tbl.As, _, _ = p.scan()
		}
		if tbl.Alias, err = p.parseIdent("table function alias"); err != nil {
			return &tbl, err
		}
	}

	return &tbl, nil
}

func (p *Parser) parseWithClause() (*WithClause, error) {
	assert(p.peek() == WITH)

	var clause WithClause
	clause.With, _, _ = p.scan()
	if p.peek() == RECURSIVE {
		clause.Recursive, _, _ = p.scan()
	}

	// Parse comma-delimited list of common table expressions (CTE).
	for {
		cte, err := p.parseCTE()
		if err != nil {
			return &clause, err
		}
		clause.CTEs = append(clause.CTEs, cte)

		if p.peek() != COMMA {
			break
		}
		p.scan()
	}
	return &clause, nil
}

func (p *Parser) parseCTE() (_ *CTE, err error) {
	var cte CTE
	if cte.TableName, err = p.parseIdent("table name"); err != nil {
		return &cte, err
	}

	// Parse optional column list.
	if p.peek() == LP {
		cte.ColumnsLparen, _, _ = p.scan()

		for {
			column, err := p.parseIdent("column name")
			if err != nil {
				return &cte, err
			}
			cte.Columns = append(cte.Columns, column)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return nil, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.scan()
		}
		cte.ColumnsRparen, _, _ = p.scan()
	}

	if p.peek() != AS {
		return nil, p.errorExpected(p.pos, p.tok, "AS")
	}
	cte.As, _, _ = p.scan()

	// Parse select statement.
	if p.peek() != LP {
		return nil, p.errorExpected(p.pos, p.tok, "left paren")
	}
	cte.SelectLparen, _, _ = p.scan()

	if cte.Select, err = p.parseSelectStatement(false, nil); err != nil {
		return &cte, err
	}

	if p.peek() != RP {
		return nil, p.errorExpected(p.pos, p.tok, "right paren")
	}
	cte.SelectRparen, _, _ = p.scan()

	return &cte, nil
}

func (p *Parser) parseOverClause() (_ *OverClause, err error) {
	assert(p.peek() == OVER)

	var clause OverClause
	clause.Over, _, _ = p.scan()

	// If specifying a window name, read it and exit.
	if isIdentToken(p.peek()) {
		pos, tok, lit := p.scan()
		clause.Name = &Ident{Name: lit, NamePos: pos, Tok: tok}
		return &clause, nil
	}

	if clause.Definition, err = p.parseWindowDefinition(); err != nil {
		return &clause, err
	}
	return &clause, nil
}

func (p *Parser) parseWindowDefinition() (_ *WindowDefinition, err error) {
	var def WindowDefinition

	// Otherwise parse the window definition.
	if p.peek() != LP {
		return &def, p.errorExpected(p.pos, p.tok, "left paren")
	}
	def.Lparen, _, _ = p.scan()

	// Read base window name.
	if isIdentToken(p.peek()) {
		pos, tok, lit := p.scan()
		def.Base = &Ident{Name: lit, NamePos: pos, Tok: tok}
	}

	// Parse "PARTITION BY expr, expr..."
	if p.peek() == PARTITION {
		def.Partition, _, _ = p.scan()
		if p.peek() != BY {
			return &def, p.errorExpected(p.pos, p.tok, "BY")
		}
		def.PartitionBy, _, _ = p.scan()

		for {
			partition, err := p.ParseExpr()
			if err != nil {
				return &def, err
			}
			def.Partitions = append(def.Partitions, partition)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}
	}

	// Parse "ORDER BY ordering-term, ordering-term..."
	if p.peek() == ORDER {
		def.Order, _, _ = p.scan()
		if p.peek() != BY {
			return &def, p.errorExpected(p.pos, p.tok, "BY")
		}
		def.OrderBy, _, _ = p.scan()

		for {
			term, err := p.parseOrderingTerm()
			if err != nil {
				return &def, err
			}
			def.OrderingTerms = append(def.OrderingTerms, term)

			if p.peek() != COMMA {
				break
			}
			p.scan()
		}
	}

	// Parse final rparen.
	if p.peek() != RP {
		return &def, p.errorExpected(p.pos, p.tok, "right paren")
	}
	def.Rparen, _, _ = p.scan()

	return &def, nil
}

// parseWithStatement is called only from parseNonExplainStatement as we don't
// know what kind of statement we'll have after the CTEs (e.g. SELECT, INSERT, etc).
func (p *Parser) parseWithStatement() (Statement, error) {
	withClause, err := p.parseWithClause()
	if err != nil {
		return nil, err
	}

	switch p.peek() {
	case SELECT, VALUES:
		return p.parseSelectStatement(false, withClause)
	case INSERT, REPLACE:
		return p.parseInsertStatement(withClause)
	case DELETE:
		return p.parseDeleteStatement(withClause)
	default:
		return nil, p.errorExpected(p.pos, p.tok, "SELECT, VALUES, INSERT, REPLACE, UPDATE, or DELETE")
	}
}
