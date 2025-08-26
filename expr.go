package query

import (
	"bytes"
	"fmt"
)

type Node interface {
	node()
	fmt.Stringer
}

type Expr interface {
	Node
	expr()
}

func (*BinaryExpr) node()     {}
func (*Call) node()           {}
func (*CaseBlock) node()      {}
func (*CaseExpr) node()       {}
func (*CastExpr) node()       {}
func (*Null) node()           {}
func (*ExprList) node()       {}
func (*Ident) node()          {}
func (*MultiPartIdent) node() {}
func (*ParenExpr) node()      {}
func (*Range) node()          {}
func (*QualifiedRef) node()   {}
func (*UnaryExpr) node()      {}
func (*IndexExpr) node()      {}
func (SelectExpr) node()      {}

// Expression Types
func (*BinaryExpr) expr()     {}
func (*Call) expr()           {}
func (*CastExpr) expr()       {}
func (*CaseExpr) expr()       {}
func (*Null) expr()           {}
func (*ExprList) expr()       {}
func (*Ident) expr()          {}
func (*MultiPartIdent) expr() {}
func (*ParenExpr) expr()      {}
func (*Range) expr()          {}
func (*QualifiedRef) expr()   {}
func (*UnaryExpr) expr()      {}
func (*IndexExpr) expr()      {}
func (SelectExpr) expr()      {}

type BinaryExpr struct {
	X     Expr  `json:"x"`
	OpPos Pos   `json:"op_pos"`
	Op    Token `json:"op"`
	Y     Expr  `json:"y"`
}

// String returns the string representation of the expression.
func (expr *BinaryExpr) String() string {
	switch expr.Op {
	case PLUS:
		return expr.X.String() + " + " + expr.Y.String()
	case MINUS:
		return expr.X.String() + " - " + expr.Y.String()
	case STAR:
		return expr.X.String() + " * " + expr.Y.String()
	case SLASH:
		return expr.X.String() + " / " + expr.Y.String()
	case REM:
		return expr.X.String() + " % " + expr.Y.String()
	case CONCAT:
		return expr.X.String() + " || " + expr.Y.String()
	case BETWEEN:
		return expr.X.String() + " BETWEEN " + expr.Y.String()
	case NOTBETWEEN:
		return expr.X.String() + " NOT BETWEEN " + expr.Y.String()
	case LSHIFT:
		return expr.X.String() + " << " + expr.Y.String()
	case RSHIFT:
		return expr.X.String() + " >> " + expr.Y.String()
	case BITAND:
		return expr.X.String() + " & " + expr.Y.String()
	case BITOR:
		return expr.X.String() + " | " + expr.Y.String()
	case LT:
		return expr.X.String() + " < " + expr.Y.String()
	case LE:
		return expr.X.String() + " <= " + expr.Y.String()
	case EQN:
		return expr.X.String() + " <=> " + expr.Y.String()
	case GT:
		return expr.X.String() + " > " + expr.Y.String()
	case GE:
		return expr.X.String() + " >= " + expr.Y.String()
	case EQ:
		return expr.X.String() + " = " + expr.Y.String()
	case NE:
		return expr.X.String() + " != " + expr.Y.String()
	case JSON_EXTRACT_JSON:
		return expr.X.String() + " -> " + expr.Y.String()
	case JSON_EXTRACT_SQL:
		return expr.X.String() + " ->> " + expr.Y.String()
	case IS:
		return expr.X.String() + " IS " + expr.Y.String()
	case ISNOT:
		return expr.X.String() + " IS NOT " + expr.Y.String()
	case IN:
		return expr.X.String() + " IN " + expr.Y.String()
	case NOTIN:
		return expr.X.String() + " NOT IN " + expr.Y.String()
	case LIKE:
		return expr.X.String() + " LIKE " + expr.Y.String()
	case NOTLIKE:
		return expr.X.String() + " NOT LIKE " + expr.Y.String()
	case GLOB:
		return expr.X.String() + " GLOB " + expr.Y.String()
	case NOTGLOB:
		return expr.X.String() + " NOT GLOB " + expr.Y.String()
	case MATCH:
		return expr.X.String() + " MATCH " + expr.Y.String()
	case NOTMATCH:
		return expr.X.String() + " NOT MATCH " + expr.Y.String()
	case REGEXP:
		return expr.X.String() + " REGEXP " + expr.Y.String()
	case NOTREGEXP:
		return expr.X.String() + " NOT REGEXP " + expr.Y.String()
	case AND:
		return expr.X.String() + " AND " + expr.Y.String()
	case OR:
		return expr.X.String() + " OR " + expr.Y.String()
	default:
		panic(fmt.Sprintf("query.BinaryExpr.String(): invalid op %s", expr.Op))
	}
}

type Params struct {
	X    Expr  `json:"x"`
	As   Pos   `json:"as"`
	Type *Type `json:"type"`
}

func (expr *Params) String() string {
	x := expr.X.String()
	if expr.As.IsValid() {
		return x + " AS " + expr.Type.String()
	}
	return x
}

type Call struct {
	Name     *MultiPartIdent `json:"name"`
	Lparen   Pos             `json:"lparen"`
	Star     Pos             `json:"star"`
	Distinct Pos             `json:"distinct"`
	Args     []*Params       `json:"args"`
	Rparen   Pos             `json:"rparen"`
	Over     *OverClause     `json:"over"`
}

// String returns the string representation of the expression.
func (c *Call) String() string {
	var buf bytes.Buffer
	buf.WriteString(c.Name.String())
	buf.WriteString("(")
	if c.Star.IsValid() {
		buf.WriteString("*")
	} else {
		if c.Distinct.IsValid() {
			buf.WriteString("DISTINCT")
			if len(c.Args) != 0 {
				buf.WriteString(" ")
			}
		}
		for i, arg := range c.Args {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(arg.String())
		}
	}
	buf.WriteString(")")

	return buf.String()
}

type CastExpr struct {
	Cast   Pos   `json:"cast"`
	Lparen Pos   `json:"lparen"`
	X      Expr  `json:"x"`
	As     Pos   `json:"as"`
	Type   *Type `json:"type"`
	Rparen Pos   `json:"rparen"`
}

// String returns the string representation of the expression.
func (expr *CastExpr) String() string {
	return fmt.Sprintf("CAST(%s AS %s)", expr.X.String(), expr.Type.String())
}

type Type struct {
	Name      *Ident     `json:"name"`
	Lparen    Pos        `json:"lparen"`
	Precision *NumberLit `json:"precision"`
	Scale     *NumberLit `json:"scale"`
	Rparen    Pos        `json:"rparen"`
}

// String returns the string representation of the type.
func (t *Type) String() string {
	if t.Precision != nil && t.Scale != nil {
		return fmt.Sprintf("%s(%s,%s)", t.Name.Name, t.Precision.String(), t.Scale.String())
	} else if t.Precision != nil {
		return fmt.Sprintf("%s(%s)", t.Name.Name, t.Precision.String())
	}
	return t.Name.Name
}

type CaseExpr struct {
	Case     Pos          `json:"case"`
	Operand  Expr         `json:"operand"`
	Blocks   []*CaseBlock `json:"blocks"`
	Else     Pos          `json:"else"`
	ElseExpr Expr         `json:"else_expr"`
	End      Pos          `json:"end"`
}

// String returns the string representation of the expression.
func (expr *CaseExpr) String() string {
	var buf bytes.Buffer
	buf.WriteString("CASE")
	if expr.Operand != nil {
		buf.WriteString(" ")
		buf.WriteString(expr.Operand.String())
	}
	for _, blk := range expr.Blocks {
		buf.WriteString(" ")
		buf.WriteString(blk.String())
	}
	if expr.ElseExpr != nil {
		buf.WriteString(" ELSE ")
		buf.WriteString(expr.ElseExpr.String())
	}
	buf.WriteString(" END")
	return buf.String()
}

type CaseBlock struct {
	When      Pos  `json:"when"`
	Condition Expr `json:"condition"`
	Then      Pos  `json:"then"`
	Body      Expr `json:"body"`
}

// String returns the string representation of the block.
func (b *CaseBlock) String() string {
	return fmt.Sprintf("WHEN %s THEN %s", b.Condition.String(), b.Body.String())
}

type Null struct {
	X     Expr  `json:"x"`
	Op    Token `json:"op"`
	OpPos Pos   `json:"op_pos"`
}

// String returns the string representation of the expression.
func (expr *Null) String() string {
	var buf bytes.Buffer

	buf.WriteString(expr.X.String())
	if expr.Op == ISNULL {
		buf.WriteString(" IS NULL")
	} else {
		buf.WriteString(" NOT NULL")
	}

	return buf.String()
}

type ExprList struct {
	Lparen Pos    `json:"lparen"`
	Exprs  []Expr `json:"exprs"`
	Rparen Pos    `json:"rparen"`
}

// String returns the string representation of the expression.
func (l *ExprList) String() string {
	var buf bytes.Buffer
	buf.WriteString("(")
	for i, expr := range l.Exprs {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(expr.String())
	}
	buf.WriteString(")")
	return buf.String()
}

type MultiPartIdent struct {
	First  *Ident `json:"first"`
	Dot1   Pos    `json:"dot1"`
	Second *Ident `json:"second"`
	Dot2   Pos    `json:"dot2"`
	Third  *Ident `json:"third"`
	Dot3   Pos    `json:"dot3"`
	Name   *Ident `json:"name"`
}

// String returns the string representation of the expression.
func (m *MultiPartIdent) String() string {
	var buf bytes.Buffer
	if m.First != nil {
		buf.WriteString(m.First.String())
		buf.WriteString(".")
	}
	if m.Second != nil {
		buf.WriteString(m.Second.String())
		buf.WriteString(".")
	}
	if m.Third != nil {
		buf.WriteString(m.Third.String())
		buf.WriteString(".")
	}
	buf.WriteString(m.Name.String())
	return buf.String()
}

// MIdentName returns the name of ident. Returns a blank string if ident is nil.
func MIdentName(ident *MultiPartIdent) string {
	if ident == nil {
		return ""
	}
	return ident.String()
}

type Ident struct {
	NamePos Pos    `json:"name_pos"`
	Name    string `json:"name"`
	Tok     Token  `json:"tok"`
}

// String returns the string representation of the expression.
func (i *Ident) String() string {
	switch i.Tok {
	case IDENT, BIND:
		return i.Name
	case QIDENT:
		return `"` + i.Name + `"`
	case STRING:
		return "'" + i.Name + "'"
	case TSTRING:
		return "`" + i.Name + "`"
	case TMPL:
		return `{{ ` + i.Name + ` }}`
	default:
		return i.Name
	}
}

// IdentName returns the name of ident. Returns a blank string if ident is nil.
func IdentName(ident *Ident) string {
	if ident == nil {
		return ""
	}
	return ident.Name
}

type ParenExpr struct {
	Lparen Pos  `json:"lparen"`
	X      Expr `json:"x"`
	Rparen Pos  `json:"rparen"`
}

// String returns the string representation of the expression.
func (expr *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", expr.X.String())
}

type Range struct {
	X   Expr `json:"x"`
	And Pos  `json:"and"`
	Y   Expr `json:"y"`
}

// String returns the string representation of the expression.
func (r *Range) String() string {
	return fmt.Sprintf("%s AND %s", r.X.String(), r.Y.String())
}

type QualifiedRef struct {
	Name *MultiPartIdent `json:"name"`
	Dot  Pos             `json:"dot"`
	Star Pos             `json:"star"`
}

// String returns the string representation of the expression.
func (r *QualifiedRef) String() string {
	if r.Star.IsValid() {
		return fmt.Sprintf("%s.*", r.Name.String())
	}
	return r.Name.String()
}

type UnaryExpr struct {
	OpPos Pos   `json:"op_pos"`
	Op    Token `json:"op"`
	X     Expr  `json:"x"`
}

// String returns the string representation of the expression.
func (expr *UnaryExpr) String() string {
	switch expr.Op {
	case PLUS:
		return "+" + expr.X.String()
	case MINUS:
		return "-" + expr.X.String()
	case NOT:
		return "NOT " + expr.X.String()
	case BITNOT:
		return "~" + expr.X.String()
	default:
		panic(fmt.Sprintf("query.UnaryExpr.String(): invalid op %s", expr.Op))
	}
}

// SelectExpr represents a SELECT statement inside an expression.
type SelectExpr struct {
	*SelectStatement
}

type IndexExpr struct {
	X      Expr       `json:"x"`
	LBrack Pos        `json:"lbrack"`
	Index  *NumberLit `json:"index"`
	Call   *Call      `json:"call"`
	RBrack Pos        `json:"rbrack"`
}

func (expr *IndexExpr) String() string {
	if expr.Call != nil {
		return expr.X.String() + "[" + expr.Call.String() + "]"
	}
	return expr.X.String() + "[" + expr.Index.String() + "]"
}
