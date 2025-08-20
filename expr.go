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
func (SelectExpr) expr()      {}

// CloneExpr returns a deep copy expr.
func CloneExpr(expr Expr) Expr {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *BinaryExpr:
		return expr.Clone()
	case *BlobLit:
		return expr.Clone()
	case *BoolLit:
		return expr.Clone()
	case *Call:
		return expr.Clone()
	case *CaseExpr:
		return expr.Clone()
	case *CastExpr:
		return expr.Clone()
	case *Null:
		return expr.Clone()
	case *ExprList:
		return expr.Clone()
	case *Ident:
		return expr.Clone()
	case *MultiPartIdent:
		return expr.Clone()
	case *NullLit:
		return expr.Clone()
	case *NumberLit:
		return expr.Clone()
	case *ParenExpr:
		return expr.Clone()
	case *Range:
		return expr.Clone()
	case *QualifiedRef:
		return expr.Clone()
	case *StringLit:
		return expr.Clone()
	case *TimestampLit:
		return expr.Clone()
	case *UnaryExpr:
		return expr.Clone()
	case SelectExpr:
		return expr.Clone()
	default:
		panic(fmt.Sprintf("invalid expr type: %T", expr))
	}
}

func cloneExprs(a []Expr) []Expr {
	if a == nil {
		return nil
	}
	other := make([]Expr, len(a))
	for i := range a {
		other[i] = CloneExpr(a[i])
	}
	return other
}

type BinaryExpr struct {
	X     Expr  // lhs
	OpPos Pos   // position of Op
	Op    Token // operator
	Y     Expr  // rhs
}

// Clone returns a deep copy of expr.
func (expr *BinaryExpr) Clone() *BinaryExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.X = CloneExpr(expr.X)
	other.Y = CloneExpr(expr.Y)
	return &other
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

type Call struct {
	Name     *MultiPartIdent // function name
	Lparen   Pos             // position of left paren
	Star     Pos             // position of *
	Distinct Pos             // position of DISTINCT keyword
	Args     []Expr          // argument list
	Rparen   Pos             // position of right paren
	Over     *OverClause     // over clause
}

// Clone returns a deep copy of c.
func (c *Call) Clone() *Call {
	if c == nil {
		return nil
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Args = cloneExprs(c.Args)
	return &other
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
	Cast   Pos   // position of CAST keyword
	Lparen Pos   // position of left paren
	X      Expr  // target expression
	As     Pos   // position of AS keyword
	Type   *Type // cast type
	Rparen Pos   // position of right paren
}

// Clone returns a deep copy of expr.
func (expr *CastExpr) Clone() *CastExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.X = CloneExpr(expr.X)
	other.Type = expr.Type.Clone()
	return &other
}

// String returns the string representation of the expression.
func (expr *CastExpr) String() string {
	return fmt.Sprintf("CAST(%s AS %s)", expr.X.String(), expr.Type.String())
}

type Type struct {
	Name      *Ident     // type name
	Lparen    Pos        // position of left paren (optional)
	Precision *NumberLit // precision (optional)
	Scale     *NumberLit // scale (optional)
	Rparen    Pos        // position of right paren (optional)
}

// Clone returns a deep copy of t.
func (t *Type) Clone() *Type {
	if t == nil {
		return nil
	}
	other := *t
	other.Name = t.Name.Clone()
	other.Precision = t.Precision.Clone()
	other.Scale = t.Scale.Clone()
	return &other
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
	Case     Pos          // position of CASE keyword
	Operand  Expr         // optional condition after the CASE keyword
	Blocks   []*CaseBlock // list of WHEN/THEN pairs
	Else     Pos          // position of ELSE keyword
	ElseExpr Expr         // expression used by default case
	End      Pos          // position of END keyword
}

// Clone returns a deep copy of expr.
func (expr *CaseExpr) Clone() *CaseExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.Operand = CloneExpr(expr.Operand)
	other.Blocks = cloneCaseBlocks(expr.Blocks)
	other.ElseExpr = CloneExpr(expr.ElseExpr)
	return &other
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
	When      Pos  // position of WHEN keyword
	Condition Expr // block condition
	Then      Pos  // position of THEN keyword
	Body      Expr // result expression
}

// Clone returns a deep copy of blk.
func (b *CaseBlock) Clone() *CaseBlock {
	if b == nil {
		return nil
	}
	other := *b
	other.Condition = CloneExpr(b.Condition)
	other.Body = CloneExpr(b.Body)
	return &other
}

func cloneCaseBlocks(a []*CaseBlock) []*CaseBlock {
	if a == nil {
		return nil
	}
	other := make([]*CaseBlock, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the block.
func (b *CaseBlock) String() string {
	return fmt.Sprintf("WHEN %s THEN %s", b.Condition.String(), b.Body.String())
}

type Null struct {
	X     Expr  // expression being checked for null
	Op    Token // IS or NOT token
	OpPos Pos   // position of NOT NULL postfix operation
}

// Clone returns a deep copy of expr.
func (expr *Null) Clone() *Null {
	if expr == nil {
		return nil
	}
	other := *expr
	other.X = CloneExpr(expr.X)
	return &other
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
	Lparen Pos    // position of left paren
	Exprs  []Expr // list of expressions
	Rparen Pos    // position of right paren
}

// Clone returns a deep copy of l.
func (l *ExprList) Clone() *ExprList {
	if l == nil {
		return nil
	}
	other := *l
	other.Exprs = cloneExprs(l.Exprs)
	return &other
}

func cloneExprLists(a []*ExprList) []*ExprList {
	if a == nil {
		return nil
	}
	other := make([]*ExprList, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
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
	First  *Ident // first part eg project
	Dot1   Pos    // dot after first segment
	Second *Ident // Second Segment (Optional)
	Dot2   Pos    // position of dot after 2nd
	Name   *Ident // table name
}

func (m *MultiPartIdent) Clone() *MultiPartIdent {
	if m == nil {
		return nil
	}
	other := *m
	other.First = m.First.Clone()
	other.Second = m.Second.Clone()
	other.Name = m.Name.Clone()
	return &other
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
	NamePos Pos    // identifier position
	Name    string // identifier name
	Tok     Token  // Token type - BIND, IDENT, TMPL
}

// Clone returns a deep copy of i.
func (i *Ident) Clone() *Ident {
	if i == nil {
		return nil
	}
	other := *i
	return &other
}

func cloneIdents(a []*Ident) []*Ident {
	if a == nil {
		return nil
	}
	other := make([]*Ident, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
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
	Lparen Pos  // position of left paren
	X      Expr // parenthesized expression
	Rparen Pos  // position of right paren
}

// Clone returns a deep copy of expr.
func (expr *ParenExpr) Clone() *ParenExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.X = CloneExpr(expr.X)
	return &other
}

// String returns the string representation of the expression.
func (expr *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", expr.X.String())
}

type Range struct {
	X   Expr // lhs expression
	And Pos  // position of AND keyword
	Y   Expr // rhs expression
}

// Clone returns a deep copy of r.
func (r *Range) Clone() *Range {
	if r == nil {
		return nil
	}
	other := *r
	other.X = CloneExpr(r.X)
	other.Y = CloneExpr(r.Y)
	return &other
}

// String returns the string representation of the expression.
func (r *Range) String() string {
	return fmt.Sprintf("%s AND %s", r.X.String(), r.Y.String())
}

type QualifiedRef struct {
	Name *MultiPartIdent // table name
	Dot  Pos             // position of dot for *
	Star Pos             // position of * (result column only)
}

// Clone returns a deep copy of r.
func (r *QualifiedRef) Clone() *QualifiedRef {
	if r == nil {
		return nil
	}
	other := *r
	other.Name = r.Name.Clone()
	return &other
}

// String returns the string representation of the expression.
func (r *QualifiedRef) String() string {
	if r.Star.IsValid() {
		return fmt.Sprintf("%s.*", r.Name.String())
	}
	return r.Name.String()
}

type UnaryExpr struct {
	OpPos Pos   // operation position
	Op    Token // operation
	X     Expr  // target expression
}

// Clone returns a deep copy of expr.
func (expr *UnaryExpr) Clone() *UnaryExpr {
	if expr == nil {
		return nil
	}
	other := *expr
	other.X = CloneExpr(expr.X)
	return &other
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

// Clone returns a deep copy of expr.
func (expr SelectExpr) Clone() SelectExpr {
	return SelectExpr{expr.SelectStatement.Clone()}
}
