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

func (p *Parser) parseIdent(desc string) (*Ident, error) {
	pos, tok, lit := p.scan()
	switch tok {
	case IDENT, QIDENT:
		return &Ident{Name: lit, NamePos: pos, Quoted: tok == QIDENT}, nil
	case NULL:
		return &Ident{Name: lit, NamePos: pos}, nil
	default:
		if isBareToken(tok) {
			return &Ident{Name: lit, NamePos: pos}, nil
		}
		return nil, p.errorExpected(pos, tok, desc)
	}
}

func (p *Parser) ParseExpr() (expr Expr, err error) {
	return p.parseBinaryExpr(LowestPrec + 1)
}

func (p *Parser) parseOperand() (expr Expr, err error) {
	pos, tok, lit := p.scan()
	switch {
	case isExprIdentToken(tok):
		ident := &Ident{Name: lit, NamePos: pos, Quoted: tok == QIDENT}
		return ident, nil
	case tok == STRING:
		return &StringLit{ValuePos: pos, Value: lit}, nil
	case tok == BLOB:
		return &BlobLit{ValuePos: pos, Value: lit}, nil
	case tok == FLOAT, tok == INTEGER:
		return &NumberLit{ValuePos: pos, Value: lit}, nil
	case tok == NULL:
		return &NullLit{Pos: pos}, nil
	case tok == TRUE, tok == FALSE:
		return &BoolLit{ValuePos: pos, Value: tok == TRUE}, nil
	case tok == PLUS, tok == MINUS, tok == BITNOT:
		expr, err = p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{OpPos: pos, Op: tok, X: expr}, nil
	case tok == LP:
		p.unscan()
		return p.parseParenExpr()
	case tok == NOT:
		expr, err = p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{OpPos: pos, Op: tok, X: expr}, nil
	default:
		return nil, p.errorExpected(p.pos, p.tok, "expression")
	}
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
			// Parsing the expression should yield a binary expression with AND op.
			// However, we don't want to conflate the boolean AND and the ranged AND
			// so we convert the expression to a Range.
			if rng, err := p.parseBinaryExpr(LowestPrec + 1); err != nil {
				return x, err
			} else if rng, ok := rng.(*BinaryExpr); !ok || rng.Op != AND {
				return x, p.errorExpected(p.pos, p.tok, "range expression")
			} else {
				x = &BinaryExpr{
					X:     x,
					OpPos: pos,
					Op:    op,
					//Y:     &Range{X: rng.X, And: rng.OpPos, Y: rng.Y},
				}
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
