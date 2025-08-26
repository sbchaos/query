package query

import (
	"io"
	"strings"
)

// Parser represents a Query parser.
type Parser struct {
	s *Scanner

	pos  Pos    // current position
	tok  Token  // current token
	lit  string // current literal value
	full bool   // buffer full
}

// NewParser returns a new instance of Parser that reads from r.
func NewParser(r io.RuneReader) *Parser {
	return &Parser{
		s: NewScanner(r),
	}
}

// ParseExprString parses s into an expression. Returns nil if s is blank.
func ParseExprString(s string) (Expr, error) {
	if s == "" {
		return nil, nil
	}
	return NewParser(strings.NewReader(s)).ParseExpr()
}

// MustParseExprString parses s into an expression. Panic on error.
func MustParseExprString(s string) Expr {
	expr, err := ParseExprString(s)
	if err != nil {
		panic(err)
	}
	return expr
}

func (p *Parser) scan() (Pos, Token, string) {
	if p.full {
		p.full = false
		return p.pos, p.tok, p.lit
	}

	// Continue scanning until we find a non-comment token.
	for {
		if pos, tok, lit := p.s.Scan(); tok != COMMENT {
			p.pos, p.tok, p.lit = pos, tok, lit
			return p.pos, p.tok, p.lit
		}
	}
}

func (p *Parser) scanUntil(cond Condition, escape rune) (Pos, string, error) {
	pos, str, err := p.s.ScanUntil(cond, escape)
	if err != nil {
		return pos, str, err
	}

	p.full = false
	p.scan()
	p.full = true
	return pos, str, nil
}

// scanBinaryOp performs a scan but combines multi-word operations into a single token.
func (p *Parser) scanBinaryOp() (Pos, Token, error) {
	pos, tok, _ := p.scan()
	switch tok {
	case IS:
		if p.peek() == NOT {
			p.scan()
			return pos, ISNOT, nil
		} else if p.peek() == NULL {
			p.scan()
			return pos, ISNULL, nil
		}
		return pos, IS, nil
	case NOT:
		switch p.peek() {
		case IN:
			p.scan()
			return pos, NOTIN, nil
		case LIKE:
			p.scan()
			return pos, NOTLIKE, nil
		case GLOB:
			p.scan()
			return pos, NOTGLOB, nil
		case REGEXP:
			p.scan()
			return pos, NOTREGEXP, nil
		case MATCH:
			p.scan()
			return pos, NOTMATCH, nil
		case BETWEEN:
			p.scan()
			return pos, NOTBETWEEN, nil
		case NULL:
			p.scan()
			return pos, NOTNULL, nil
		default:
			return pos, tok, p.errorExpected(p.pos, p.tok, "IN, LIKE, GLOB, REGEXP, MATCH, BETWEEN, IS/NOT NULL")
		}
	default:
		return pos, tok, nil
	}
}

func (p *Parser) peek() Token {
	if !p.full {
		p.scan()
		p.unscan()
	}
	return p.tok
}

func (p *Parser) peekScan() (Pos, Token, string) { // nolint
	if !p.full {
		p.scan()
		p.unscan()
	}
	return p.pos, p.tok, p.lit
}

func (p *Parser) unscan() {
	assert(!p.full)
	p.full = true
}

func (p *Parser) errorExpected(pos Pos, tok Token, msg string) error {
	msg = "expected " + msg
	if pos == p.pos {
		if p.tok.IsLiteral() {
			msg += ", found " + p.lit
		} else {
			msg += ", found '" + p.tok.String() + "'"
		}
	}
	return &Error{Pos: pos, Msg: msg}
}

// Error represents a parse error.
type Error struct {
	Pos Pos
	Msg string
}

// Error implements the error interface.
func (e Error) Error() string {
	if e.Pos.IsValid() {
		return e.Pos.String() + ": " + e.Msg
	}
	return e.Msg
}
