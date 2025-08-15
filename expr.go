package query

import (
	"bytes"
	"fmt"
	"strings"
)

type Node interface {
	node()
	fmt.Stringer
}

type Expr interface {
	Node
	expr()
}

func (*BinaryExpr) node() {}
func (*Null) node()       {}
func (*ExprList) node()   {}
func (*Ident) node()      {}
func (*ParenExpr) node()  {}
func (*UnaryExpr) node()  {}

// Expression Types
func (*BinaryExpr) expr() {}
func (*Null) expr()       {}
func (*ExprList) expr()   {}
func (*Ident) expr()      {}
func (*ParenExpr) expr()  {}
func (*UnaryExpr) expr()  {}

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
	case *Null:
		return expr.Clone()
	case *ExprList:
		return expr.Clone()
	case *Ident:
		return expr.Clone()
	case *NullLit:
		return expr.Clone()
	case *NumberLit:
		return expr.Clone()
	case *ParenExpr:
		return expr.Clone()
	case *StringLit:
		return expr.Clone()
	case *TimestampLit:
		return expr.Clone()
	case *UnaryExpr:
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

type Ident struct {
	NamePos Pos    // identifier position
	Name    string // identifier name
	Quoted  bool   // true if double quoted
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
	return `"` + strings.Replace(i.Name, `"`, `""`, -1) + `"`
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
