package query

func (p *Parser) parseParenExpr() (Expr, error) {
	lparen, _, _ := p.scan()

	// Parse the first expression
	x, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	// If there's no comma after the first expression, treat it as a normal parenthesized expression
	if p.peek() != COMMA {
		rparen, _, _ := p.scan()
		return &ParenExpr{Lparen: lparen, X: x, Rparen: rparen}, nil
	}

	// If there's a comma, we're dealing with an expression list
	var list ExprList
	list.Lparen = lparen
	list.Exprs = append(list.Exprs, x)

	for p.peek() == COMMA {
		p.scan() // consume the comma

		expr, err := p.ParseExpr()
		if err != nil {
			return &list, err
		}
		list.Exprs = append(list.Exprs, expr)
	}

	if p.peek() != RP {
		return &list, p.errorExpected(p.pos, p.tok, "right paren")
	}
	list.Rparen, _, _ = p.scan()

	return &list, nil
}

func (p *Parser) parseCastExpr() (_ *CastExpr, err error) {
	assert(p.peek() == CAST)

	var expr CastExpr
	expr.Cast, _, _ = p.scan()

	if p.peek() != LP {
		return &expr, p.errorExpected(p.pos, p.tok, "left paren")
	}
	expr.Lparen, _, _ = p.scan()

	if expr.X, err = p.ParseExpr(); err != nil {
		return &expr, err
	}

	if p.peek() != AS {
		return &expr, p.errorExpected(p.pos, p.tok, "AS")
	}
	expr.As, _, _ = p.scan()

	if expr.Type, err = p.parseType(); err != nil {
		return &expr, err
	}

	if p.peek() != RP {
		return &expr, p.errorExpected(p.pos, p.tok, "right paren")
	}
	expr.Rparen, _, _ = p.scan()
	return &expr, nil
}

func (p *Parser) parseIdent(desc string) (*Ident, error) {
	pos, tok, lit := p.scan()
	switch tok {
	case IDENT, QIDENT, TSTRING, BIND, TMPL:
		return &Ident{Name: lit, NamePos: pos, Tok: tok}, nil
	// Special Cases
	case DATE, TIMESTAMP:
		return &Ident{Name: lit, NamePos: pos, Tok: tok}, nil
	case NULL:
		return &Ident{Name: lit, NamePos: pos}, nil
	default:
		if isBareToken(tok) {
			return &Ident{Name: lit, NamePos: pos, Tok: tok}, nil
		}
		return nil, p.errorExpected(pos, tok, desc)
	}
}

func (p *Parser) parseType() (_ *Type, err error) {
	var typ Type
	for {
		tok := p.peek()
		if tok != IDENT && tok != NULL && tok != DATE && tok != TIMESTAMP {
			break
		}
		typeName, err := p.parseIdent("type name")
		if err != nil {
			return &typ, err
		}
		if typ.Name == nil {
			typ.Name = typeName
		} else {
			typ.Name.Name += " " + typeName.Name
		}
	}

	if typ.Name == nil {
		return &typ, p.errorExpected(p.pos, p.tok, "type name")
	}

	// Optionally parse precision & scale.
	if p.peek() == LP {
		typ.Lparen, _, _ = p.scan()
		if typ.Precision, err = p.parseSignedNumber("precision"); err != nil {
			return &typ, err
		}

		if p.peek() == COMMA {
			p.scan()
			if typ.Scale, err = p.parseSignedNumber("scale"); err != nil {
				return &typ, err
			}
		}

		if p.peek() != RP {
			return nil, p.errorExpected(p.pos, p.tok, "right paren")
		}
		typ.Rparen, _, _ = p.scan()
	}

	return &typ, nil
}

func (p *Parser) ParseExpr() (expr Expr, err error) {
	return p.parseBinaryExpr(LowestPrec + 1)
}

func (p *Parser) parseOperand() (expr Expr, err error) {
	pos, tok, lit := p.scan()
	switch {
	case isExprIdentToken(tok):
		ident, err := p.handleSpecialCases(pos, tok, lit)
		if err != nil {
			ident = &Ident{Name: lit, NamePos: pos, Tok: tok}
		}
		return p.parseIdentifier(ident)
	case tok == STRING:
		return &StringLit{ValuePos: pos, Value: lit}, nil
	case tok == TMPL:
		return &TemplateStr{TmplPos: pos, Template: lit}, nil
	case tok == BLOB:
		return &BlobLit{ValuePos: pos, Value: lit}, nil
	case tok == FLOAT, tok == INTEGER:
		return &NumberLit{ValuePos: pos, Value: lit}, nil
	case tok == NULL:
		return &NullLit{Pos: pos}, nil
	case tok == TRUE, tok == FALSE:
		return &BoolLit{ValuePos: pos, Value: tok == TRUE}, nil
	case tok == BIND:
		return &Ident{NamePos: pos, Name: lit, Tok: tok}, nil
	case tok == PLUS, tok == MINUS, tok == BITNOT:
		expr, err = p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{OpPos: pos, Op: tok, X: expr}, nil
	case tok == LP:
		p.unscan()
		return p.parseParenExpr()
	case tok == CASE:
		p.unscan()
		return p.parseCaseExpr()
	case tok == CAST:
		p.unscan()
		return p.parseCastExpr()
	case tok == NOT:
		expr, err = p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{OpPos: pos, Op: tok, X: expr}, nil
	case tok == WITH:
		p.unscan()
		return p.parseWithExpr()
	case tok == SELECT:
		p.unscan()
		selectStmt, err := p.parseSelectStatement(false, nil)
		return SelectExpr{selectStmt}, err
	default:
		return nil, p.errorExpected(p.pos, p.tok, "expression")
	}
}

func (p *Parser) handleSpecialCases(pos Pos, tok Token, lit string) (*Ident, error) {
	if tok == IDENT && (lit == "DAY" || lit == "MONTH" || lit == "YEAR") {
		strName := lit
		if p.peek() != FROM {
			return nil, p.errorExpected(p.pos, p.tok, "FROM")
		}
		p.scan()
		strName = strName + " FROM "

		_, t1, lit1 := p.scan()
		if t1 == IDENT {
			strName += lit1
		}
		if p.peek() == LP {
			return &Ident{Name: strName, NamePos: pos, Tok: tok}, nil
		}
	}

	if tok == IDENT && lit == "INTERVAL" {
		strName := lit
		if p.peek() != INTEGER {
			return nil, p.errorExpected(p.pos, p.tok, "integer")
		}
		_, _, lit2 := p.scan()
		strName = strName + " " + lit2

		_, _, lit3 := p.scan()
		strName = strName + " " + lit3
		return &Ident{Name: strName, NamePos: pos, Tok: tok}, nil
	}

	if tok == GROUPING {
		return &Ident{NamePos: pos, Name: "GROUPING", Tok: IDENT}, nil
	}

	if tok == DATE || tok == TIMESTAMP {
		strName := lit
		peek := p.peek()
		if isExprIdentToken(peek) || peek == STRING {
			_, _, lit2 := p.scan()
			strName = strName + " '" + lit2 + "'"
		}
		return &Ident{Name: strName, NamePos: pos, Tok: tok}, nil
	}

	return nil, &Error{Pos: pos}
}

func (p *Parser) parseWithExpr() (*SelectExpr, error) {
	withClause, err := p.parseWithClause()
	if err != nil {
		return nil, err
	}

	switch p.peek() {
	case SELECT, VALUES:
		selectStmt, err := p.parseSelectStatement(false, withClause)
		if err != nil {
			return nil, err
		}
		return &SelectExpr{selectStmt}, nil
	default:
		return nil, p.errorExpected(p.pos, p.tok, "SELECT after With")
	}
}

func (p *Parser) parseMultiIdent(ident *Ident) (*MultiPartIdent, Pos) {
	emptyPos := Pos{}
	if p.peek() != DOT {
		return &MultiPartIdent{
			Name: ident,
		}, emptyPos
	}
	dot1, _, _ := p.scan()
	if !isIdentToken(p.peek()) {
		return &MultiPartIdent{Name: ident}, dot1
	}
	// Next ident
	pos, tok, lit := p.scan()
	ident2 := &Ident{Name: lit, NamePos: pos, Tok: tok}

	if p.peek() != DOT {
		return &MultiPartIdent{First: ident, Dot1: dot1, Name: ident2}, emptyPos
	}

	dot2, _, _ := p.scan()
	if !isIdentToken(p.peek()) {
		return &MultiPartIdent{First: ident, Dot1: dot1, Name: ident2}, dot2
	}

	// Next ident
	pos2, tok2, lit2 := p.scan()
	ident3 := &Ident{Name: lit2, NamePos: pos2, Tok: tok2}

	return &MultiPartIdent{
		First:  ident,
		Dot1:   dot1,
		Second: ident2,
		Dot2:   dot2,
		Name:   ident3,
	}, emptyPos
}

func (p *Parser) parseIdentifier(ident *Ident) (Expr, error) {
	mIdent, dotPos := p.parseMultiIdent(ident)

	if p.peek() == DOT {
		if dotPos.IsValid() {
			return mIdent, &Error{Pos: p.pos, Msg: "Found .. in input string"}
		} else {
			dotPos, _, _ = p.scan()
		}
	}

	if dotPos.IsValid() {
		var star Pos
		if p.peek() == STAR {
			star, _, _ = p.scan()
			return &QualifiedRef{Name: mIdent, Dot: dotPos, Star: star}, nil
		}
		return nil, p.errorExpected(p.pos, p.tok, "identifier")
	} else if p.peek() == LP {
		return p.parseCall(mIdent)
	}
	return mIdent, nil
}

func (p *Parser) parseBinaryExpr(prec1 int) (expr Expr, err error) {
	x, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	for {
		if p.peek().Precedence() < prec1 {
			return x, nil
		}

		pos, op, err := p.scanBinaryOp()
		if err != nil {
			return nil, err
		}

		switch op {
		case NOTNULL, ISNULL:
			x = &Null{X: x, OpPos: pos, Op: op}
		case IN, NOTIN:

			y, err := p.parseExprList()
			if err != nil {
				return x, err
			}
			x = &BinaryExpr{X: x, OpPos: pos, Op: op, Y: y}

		case BETWEEN, NOTBETWEEN:
			rng, err := p.parseBetweenExpr()
			if err != nil {
				return nil, err
			}
			x = &BinaryExpr{
				X:     x,
				OpPos: pos,
				Op:    op,
				Y:     rng,
			}

		default:
			y, err := p.parseBinaryExpr(op.Precedence() + 1)
			if err != nil {
				return nil, err
			}
			x = &BinaryExpr{X: x, OpPos: pos, Op: op, Y: y}
		}
	}
}

func (p *Parser) parseBetweenExpr() (expr *Range, err error) {
	x1, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	if p.peek() != AND {
		return nil, p.errorExpected(p.pos, p.tok, "AND for BETWEEN")
	}
	and, _, _ := p.scan()

	y, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	return &Range{
		X:   x1,
		And: and,
		Y:   y,
	}, nil
}

func (p *Parser) parseExprList() (_ *ExprList, err error) {
	var list ExprList
	if p.peek() != LP {
		return &list, p.errorExpected(p.pos, p.tok, "left paren")
	}
	list.Lparen, _, _ = p.scan()

	for p.peek() != RP {
		x, err := p.ParseExpr()
		if err != nil {
			return &list, err
		}
		list.Exprs = append(list.Exprs, x)

		if p.peek() == RP {
			break
		} else if p.peek() != COMMA {
			return &list, p.errorExpected(p.pos, p.tok, "comma or right paren")
		}
		p.scan()
	}

	list.Rparen, _, _ = p.scan()

	return &list, nil
}

func (p *Parser) parseCall(name *MultiPartIdent) (*Call, error) {
	assert(p.peek() == LP)

	var expr Call
	var err error
	expr.Name = name
	expr.Lparen, _, _ = p.scan()

	// Parse argument list: either "*" or "[DISTINCT] expr, expr..."
	if p.peek() == STAR {
		expr.Star, _, _ = p.scan()
	} else {
		if p.peek() == DISTINCT {
			expr.Distinct, _, _ = p.scan()
		}
		for p.peek() != RP {
			arg, err := p.ParseExpr()
			if err != nil {
				return &expr, err
			}

			expr.Args = append(expr.Args, arg)

			if tok := p.peek(); tok == COMMA {
				p.scan()
			} else if tok != RP {
				return &expr, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}

		}
	}

	if p.peek() != RP {
		return &expr, p.errorExpected(p.pos, p.tok, "right paren")
	}
	expr.Rparen, _, _ = p.scan()

	// Parse optional over clause.
	if p.peek() == OVER {
		if expr.Over, err = p.parseOverClause(); err != nil {
			return &expr, err
		}
	}

	return &expr, nil
}

func (p *Parser) parseCaseExpr() (_ *CaseExpr, err error) {
	assert(p.peek() == CASE)

	var expr CaseExpr
	expr.Case, _, _ = p.scan()

	// Parse optional expression if WHEN is not next.
	if p.peek() != WHEN {
		if expr.Operand, err = p.ParseExpr(); err != nil {
			return &expr, err
		}
	}

	// Parse one or more WHEN/THEN pairs.
	for {
		var blk CaseBlock
		if p.peek() != WHEN {
			return &expr, p.errorExpected(p.pos, p.tok, "WHEN")
		}
		blk.When, _, _ = p.scan()

		if blk.Condition, err = p.ParseExpr(); err != nil {
			return &expr, err
		}

		if p.peek() != THEN {
			return &expr, p.errorExpected(p.pos, p.tok, "THEN")
		}
		blk.Then, _, _ = p.scan()

		if blk.Body, err = p.ParseExpr(); err != nil {
			return &expr, err
		}

		expr.Blocks = append(expr.Blocks, &blk)

		if tok := p.peek(); tok == ELSE || tok == END {
			break
		} else if tok != WHEN {
			return &expr, p.errorExpected(p.pos, p.tok, "WHEN, ELSE or END")
		}
	}

	// Parse optional ELSE block.
	if p.peek() == ELSE {
		expr.Else, _, _ = p.scan()
		if expr.ElseExpr, err = p.ParseExpr(); err != nil {
			return &expr, err
		}
	}

	if p.peek() != END {
		return &expr, p.errorExpected(p.pos, p.tok, "END")
	}
	expr.End, _, _ = p.scan()

	return &expr, nil
}
