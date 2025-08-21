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
