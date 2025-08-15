package query

func (p *Parser) parseSignedNumber(desc string) (*NumberLit, error) {
	pos, tok, lit := p.scan()

	// Prepend "+" or "-" to the next number value.
	if tok == PLUS || tok == MINUS {
		prefix := lit
		_, tok, lit = p.scan()
		lit = prefix + lit
	}

	switch tok {
	case FLOAT, INTEGER:
		return &NumberLit{ValuePos: pos, Value: lit}, nil
	default:
		return nil, p.errorExpected(p.pos, p.tok, desc)
	}
}

func (p *Parser) mustParseLiteral() Expr {
	assert(isLiteralToken(p.tok))
	pos, tok, lit := p.scan()
	switch tok {
	case STRING:
		return &StringLit{ValuePos: pos, Value: lit}
	case CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP:
		return &TimestampLit{ValuePos: pos, Value: lit}
	case BLOB:
		return &BlobLit{ValuePos: pos, Value: lit}
	case FLOAT, INTEGER:
		return &NumberLit{ValuePos: pos, Value: lit}
	case TRUE, FALSE:
		return &BoolLit{ValuePos: pos, Value: tok == TRUE}
	default:
		assert(tok == NULL)
		return &NullLit{Pos: pos}
	}
}

// isLiteralToken returns true if token represents a literal value.
func isLiteralToken(tok Token) bool {
	switch tok {
	case FLOAT, INTEGER, STRING, BLOB, TRUE, FALSE, NULL,
		CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP:
		return true
	default:
		return false
	}
}
