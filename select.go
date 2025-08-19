package query

import (
	"bytes"
	"fmt"
)

func (*JoinClause) node()                 {}
func (*ParenSource) node()                {}
func (*QualifiedTableName) node()         {}
func (*QualifiedTableFunctionName) node() {}
func (*SelectStatement) node()            {}
func (*OnConstraint) node()               {}
func (*UsingConstraint) node()            {}
func (*SetStatement) node()               {}
func (*DeclarationStatement) node()       {}

type Statement interface {
	Node
	stmt()
}

func (*SelectStatement) stmt()      {}
func (*SetStatement) stmt()         {}
func (*DeclarationStatement) stmt() {}

// CloneStatement returns a deep copy stmt.
func CloneStatement(stmt Statement) Statement {
	if stmt == nil {
		return nil
	}

	switch stmt := stmt.(type) {
	case *SelectStatement:
		return stmt.Clone()
	default:
		panic(fmt.Sprintf("invalid statement type: %T", stmt))
	}
}

type SelectStatement struct {
	WithClause *WithClause // clause containing CTEs

	Values     Pos         // position of VALUES keyword
	ValueLists []*ExprList // lists of lists of values

	Select   Pos             // position of SELECT keyword
	Distinct Pos             // position of DISTINCT keyword
	All      Pos             // position of ALL keyword
	Columns  []*ResultColumn // list of result columns in the SELECT clause

	From   Pos    // position of FROM keyword
	Source Source // chain of tables & subqueries in FROM clause

	Where     Pos  // position of WHERE keyword
	WhereExpr Expr // condition for WHERE clause

	Group        Pos    // position of GROUP keyword
	GroupBy      Pos    // position of BY keyword after GROUP
	GroupByAll   Pos    // positon of ALL keyword after GROUP BY
	GroupByExprs []Expr // group by expression list
	Having       Pos    // position of HAVING keyword
	HavingExpr   Expr   // HAVING expression

	Window  Pos       // position of WINDOW keyword
	Windows []*Window // window list

	Union     Pos              // position of UNION keyword
	UnionAll  Pos              // position of ALL keyword after UNION
	Intersect Pos              // position of INTERSECT keyword
	Except    Pos              // position of EXCEPT keyword
	Compound  *SelectStatement // compounded SELECT statement

	Order         Pos             // position of ORDER keyword
	OrderBy       Pos             // position of BY keyword after ORDER
	OrderingTerms []*OrderingTerm // terms of ORDER BY clause

	Limit       Pos  // position of LIMIT keyword
	LimitExpr   Expr // limit expression
	Offset      Pos  // position of OFFSET keyword
	OffsetComma Pos  // position of COMMA (instead of OFFSET)
	OffsetExpr  Expr // offset expression
}

// Clone returns a deep copy of s.
func (s *SelectStatement) Clone() *SelectStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.WithClause = s.WithClause.Clone()
	other.ValueLists = cloneExprLists(s.ValueLists)
	other.Columns = cloneResultColumns(s.Columns)
	other.Source = CloneSource(s.Source)
	other.WhereExpr = CloneExpr(s.WhereExpr)
	other.GroupByExprs = cloneExprs(s.GroupByExprs)
	other.HavingExpr = CloneExpr(s.HavingExpr)
	other.Windows = cloneWindows(s.Windows)
	other.Compound = s.Compound.Clone()
	other.OrderingTerms = cloneOrderingTerms(s.OrderingTerms)
	other.LimitExpr = CloneExpr(s.LimitExpr)
	other.OffsetExpr = CloneExpr(s.OffsetExpr)
	return &other
}

// String returns the string representation of the statement.
func (s *SelectStatement) String() string {
	var buf bytes.Buffer
	if s.WithClause != nil {
		buf.WriteString(s.WithClause.String())
		buf.WriteString(" ")
	}

	if len(s.ValueLists) > 0 {
		buf.WriteString("VALUES ")
		for i, exprs := range s.ValueLists {
			if i != 0 {
				buf.WriteString(", ")
			}

			buf.WriteString("(")
			for j, expr := range exprs.Exprs {
				if j != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(expr.String())
			}
			buf.WriteString(")")
		}
	} else {
		buf.WriteString("SELECT ")
		if s.Distinct.IsValid() {
			buf.WriteString("DISTINCT ")
		} else if s.All.IsValid() {
			buf.WriteString("ALL ")
		}

		for i, col := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}

		if s.Source != nil {
			fmt.Fprintf(&buf, " FROM %s", s.Source.String())
		}

		if s.WhereExpr != nil {
			fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
		}

		if len(s.GroupByExprs) != 0 || s.GroupByAll.IsValid() {
			buf.WriteString(" GROUP BY ")
			if s.GroupBy.IsValid() {
				buf.WriteString("ALL")
			} else {
				for i, expr := range s.GroupByExprs {
					if i != 0 {
						buf.WriteString(", ")
					}
					buf.WriteString(expr.String())
				}
			}

			if s.HavingExpr != nil {
				fmt.Fprintf(&buf, " HAVING %s", s.HavingExpr.String())
			}
		}

		if len(s.Windows) != 0 {
			buf.WriteString(" WINDOW ")
			for i, window := range s.Windows {
				if i != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(window.String())
			}
		}
	}

	// Write compound operator.
	if s.Compound != nil {
		switch {
		case s.Union.IsValid():
			buf.WriteString(" UNION")
			if s.UnionAll.IsValid() {
				buf.WriteString(" ALL")
			}
		case s.Intersect.IsValid():
			buf.WriteString(" INTERSECT")
		case s.Except.IsValid():
			buf.WriteString(" EXCEPT")
		}

		fmt.Fprintf(&buf, " %s", s.Compound.String())
	}

	// Write ORDER BY.
	if len(s.OrderingTerms) != 0 {
		buf.WriteString(" ORDER BY ")
		for i, term := range s.OrderingTerms {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(term.String())
		}
	}

	// Write LIMIT/OFFSET.
	if s.LimitExpr != nil {
		fmt.Fprintf(&buf, " LIMIT %s", s.LimitExpr.String())
		if s.OffsetExpr != nil {
			fmt.Fprintf(&buf, " OFFSET %s", s.OffsetExpr.String())
		}
	}

	return buf.String()
}

type WithClause struct {
	With      Pos    // position of WITH keyword
	Recursive Pos    // position of RECURSIVE keyword
	CTEs      []*CTE // common table expressions
}

// Clone returns a deep copy of c.
func (c *WithClause) Clone() *WithClause {
	if c == nil {
		return nil
	}
	other := *c
	other.CTEs = cloneCTEs(c.CTEs)
	return &other
}

// String returns the string representation of the clause.
func (c *WithClause) String() string {
	var buf bytes.Buffer
	buf.WriteString("WITH ")
	if c.Recursive.IsValid() {
		buf.WriteString("RECURSIVE ")
	}

	for i, cte := range c.CTEs {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(cte.String())
	}

	return buf.String()
}

// CTE represents an AST node for a common table expression.
type CTE struct {
	TableName     *Ident           // table name
	ColumnsLparen Pos              // position of column list left paren
	Columns       []*Ident         // optional column list
	ColumnsRparen Pos              // position of column list right paren
	As            Pos              // position of AS keyword
	SelectLparen  Pos              // position of select left paren
	Select        *SelectStatement // select statement
	SelectRparen  Pos              // position of select right paren
}

// Clone returns a deep copy of cte.
func (cte *CTE) Clone() *CTE {
	if cte == nil {
		return nil
	}
	other := *cte
	other.TableName = cte.TableName.Clone()
	other.Columns = cloneIdents(cte.Columns)
	other.Select = cte.Select.Clone()
	return &other
}

func cloneCTEs(a []*CTE) []*CTE {
	if a == nil {
		return nil
	}
	other := make([]*CTE, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the CTE.
func (cte *CTE) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s", cte.TableName.String())

	if len(cte.Columns) != 0 {
		buf.WriteString(" (")
		for i, col := range cte.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	fmt.Fprintf(&buf, " AS (%s)", cte.Select.String())

	return buf.String()
}

type ResultColumn struct {
	Star  Pos    // position of *
	Expr  Expr   // column expression (may be "tbl.*")
	As    Pos    // position of AS keyword
	Alias *Ident // alias name
}

// Clone returns a deep copy of c.
func (c *ResultColumn) Clone() *ResultColumn {
	if c == nil {
		return nil
	}
	other := *c
	other.Expr = CloneExpr(c.Expr)
	other.Alias = c.Alias.Clone()
	return &other
}

func cloneResultColumns(a []*ResultColumn) []*ResultColumn {
	if a == nil {
		return nil
	}
	other := make([]*ResultColumn, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the column.
func (c *ResultColumn) String() string {
	if c.Star.IsValid() {
		return "*"
	} else if c.Alias != nil {
		return fmt.Sprintf("%s AS %s", c.Expr.String(), c.Alias.String())
	}
	return c.Expr.String()
}

// Source represents a table or subquery.
type Source interface {
	Node
	source()
}

func (*JoinClause) source()                 {}
func (*ParenSource) source()                {}
func (*QualifiedTableName) source()         {}
func (*QualifiedTableFunctionName) source() {}
func (*SelectStatement) source()            {}

// CloneSource returns a deep copy src.
func CloneSource(src Source) Source {
	if src == nil {
		return nil
	}

	switch src := src.(type) {
	case *JoinClause:
		return src.Clone()
	case *ParenSource:
		return src.Clone()
	case *QualifiedTableName:
		return src.Clone()
	case *QualifiedTableFunctionName:
		return src.Clone()
	case *SelectStatement:
		return src.Clone()
	default:
		panic(fmt.Sprintf("invalid source type: %T", src))
	}
}

// SourceName returns the name of the source.
// Only returns for QualifiedTableName & ParenSource.
func SourceName(src Source) string {
	switch src := src.(type) {
	case *JoinClause, *SelectStatement:
		return ""
	case *ParenSource:
		return IdentName(src.Alias)
	case *QualifiedTableName:
		return src.TableName()
	default:
		return ""
	}
}

// SourceList returns a list of scopes in the current scope.
func SourceList(src Source) []Source {
	var a []Source
	ForEachSource(src, func(s Source) bool {
		a = append(a, s)
		return true
	})
	return a
}

// ForEachSource calls fn for every source within the current scope.
// Stops iteration if fn returns false.
func ForEachSource(src Source, fn func(Source) bool) {
	forEachSource(src, fn)
}

func forEachSource(src Source, fn func(Source) bool) bool {
	if !fn(src) {
		return false
	}

	switch src := src.(type) {
	case *JoinClause:
		if !forEachSource(src.X, fn) {
			return false
		} else if !forEachSource(src.Y, fn) {
			return false
		}
	case *SelectStatement:
		if !forEachSource(src.Source, fn) {
			return false
		}
	}
	return true
}

// ResolveSource returns a source with the given name.
// This can either be the table name or the alias for a source.
func ResolveSource(root Source, name string) Source {
	var ret Source
	ForEachSource(root, func(src Source) bool {
		switch src := src.(type) {
		case *ParenSource:
			if IdentName(src.Alias) == name {
				ret = src
			}
		case *QualifiedTableName:
			if src.TableName() == name {
				ret = src
			}
		}
		return ret == nil // continue until we find the matching source
	})
	return ret
}

type QualifiedTableName struct {
	Project    *Ident // project name
	Dot1       Pos    // dot after project
	Schema     *Ident // schema name
	Dot        Pos    // position of dot
	Name       *Ident // table name
	As         Pos    // position of AS keyword
	Alias      *Ident // optional table alias
	Indexed    Pos    // position of INDEXED keyword
	IndexedBy  Pos    // position of BY keyword after INDEXED
	Not        Pos    // position of NOT keyword before INDEXED
	NotIndexed Pos    // position of NOT keyword before INDEXED
	Index      *Ident // name of index
}

// TableName returns the name used to identify n.
// Returns the alias, if one is specified. Otherwise returns the name.
func (n *QualifiedTableName) TableName() string {
	if s := IdentName(n.Alias); s != "" {
		return s
	}
	return IdentName(n.Name)
}

// Clone returns a deep copy of n.
func (n *QualifiedTableName) Clone() *QualifiedTableName {
	if n == nil {
		return nil
	}
	other := *n
	other.Project = n.Project.Clone()
	other.Schema = n.Schema.Clone()
	other.Name = n.Name.Clone()
	other.Alias = n.Alias.Clone()
	other.Index = n.Index.Clone()
	return &other
}

// String returns the string representation of the table name.
func (n *QualifiedTableName) String() string {
	var buf bytes.Buffer
	if n.Project != nil {
		buf.WriteString(n.Project.String())
		buf.WriteString(".")
	}
	if n.Schema != nil {
		buf.WriteString(n.Schema.String())
		buf.WriteString(".")
	}
	buf.WriteString(n.Name.String())
	if n.Alias != nil {
		fmt.Fprintf(&buf, " AS %s", n.Alias.String())
	}

	if n.Index != nil {
		fmt.Fprintf(&buf, " INDEXED BY %s", n.Index.String())
	} else if n.NotIndexed.IsValid() {
		buf.WriteString(" NOT INDEXED")
	}
	return buf.String()
}

type ParenSource struct {
	Lparen Pos    // position of left paren
	X      Source // nested source
	Rparen Pos    // position of right paren
	As     Pos    // position of AS keyword (select source only)
	Alias  *Ident // optional table alias (select source only)
}

// Clone returns a deep copy of s.
func (s *ParenSource) Clone() *ParenSource {
	if s == nil {
		return nil
	}
	other := *s
	other.X = CloneSource(s.X)
	other.Alias = s.Alias.Clone()
	return &other
}

// String returns the string representation of the source.
func (s *ParenSource) String() string {
	if s.Alias != nil {
		return fmt.Sprintf("(%s) AS %s", s.X.String(), s.Alias.String())
	}
	return fmt.Sprintf("(%s)", s.X.String())
}

type JoinClause struct {
	X          Source         // lhs source
	Operator   *JoinOperator  // join operator
	Y          Source         // rhs source
	Constraint JoinConstraint // join constraint
}

// Clone returns a deep copy of c.
func (c *JoinClause) Clone() *JoinClause {
	if c == nil {
		return nil
	}
	other := *c
	other.X = CloneSource(c.X)
	other.Y = CloneSource(c.Y)
	other.Constraint = CloneJoinConstraint(c.Constraint)
	return &other
}

// String returns the string representation of the clause.
func (c *JoinClause) String() string {
	var buf bytes.Buffer

	// Print the left side
	buf.WriteString(c.X.String())

	// Print the operator
	buf.WriteString(c.Operator.String())

	// Handle the right side
	if y, ok := c.Y.(*JoinClause); ok {
		// Special case: right side is a JoinClause

		// Check if the X of the nested JoinClause is also a JoinClause
		if yx, ok := y.X.(*JoinClause); ok {
			// Handle the double-nested case

			// Print the first table of the inner JoinClause
			buf.WriteString(yx.X.String())

			// Add the constraint for the first join
			if c.Constraint != nil {
				fmt.Fprintf(&buf, " %s", c.Constraint.String())
			}

			// Print the operator of the inner JoinClause
			buf.WriteString(yx.Operator.String())

			// Print the second table of the inner JoinClause
			buf.WriteString(yx.Y.String())

			// Add the constraint for the inner JoinClause
			if yx.Constraint != nil {
				fmt.Fprintf(&buf, " %s", yx.Constraint.String())
			}

			// Print the operator of the outer JoinClause
			buf.WriteString(y.Operator.String())

			// Print the right side of the outer JoinClause
			buf.WriteString(y.Y.String())

			// Add the constraint for the outer JoinClause
			if y.Constraint != nil {
				fmt.Fprintf(&buf, " %s", y.Constraint.String())
			}
		} else {
			// Handle the singly-nested case

			// Print the left side of the nested JoinClause
			buf.WriteString(y.X.String())

			// Add the constraint for the first join
			if c.Constraint != nil {
				fmt.Fprintf(&buf, " %s", c.Constraint.String())
			}

			// Print the operator of the nested JoinClause
			buf.WriteString(y.Operator.String())

			// Print the right side of the nested JoinClause
			buf.WriteString(y.Y.String())

			// Add the constraint for the nested JoinClause
			if y.Constraint != nil {
				fmt.Fprintf(&buf, " %s", y.Constraint.String())
			}
		}
	} else {
		// Normal case: right side is not a JoinClause
		buf.WriteString(c.Y.String())

		// Add the constraint
		if c.Constraint != nil {
			fmt.Fprintf(&buf, " %s", c.Constraint.String())
		}
	}

	return buf.String()
}

// JoinConstraint represents either an ON or USING join constraint.
type JoinConstraint interface {
	Node
	joinConstraint()
}

func (*OnConstraint) joinConstraint()    {}
func (*UsingConstraint) joinConstraint() {}

// CloneJoinConstraint returns a deep copy cons.
func CloneJoinConstraint(cons JoinConstraint) JoinConstraint {
	if cons == nil {
		return nil
	}

	switch cons := cons.(type) {
	case *OnConstraint:
		return cons.Clone()
	case *UsingConstraint:
		return cons.Clone()
	default:
		panic(fmt.Sprintf("invalid join constraint type: %T", cons))
	}
}

type OnConstraint struct {
	On Pos  // position of ON keyword
	X  Expr // constraint expression
}

// Clone returns a deep copy of c.
func (c *OnConstraint) Clone() *OnConstraint {
	if c == nil {
		return nil
	}
	other := *c
	other.X = CloneExpr(c.X)
	return &other
}

// String returns the string representation of the constraint.
func (c *OnConstraint) String() string {
	return "ON " + c.X.String()
}

type UsingConstraint struct {
	Using   Pos      // position of USING keyword
	Lparen  Pos      // position of left paren
	Columns []*Ident // column list
	Rparen  Pos      // position of right paren
}

// Clone returns a deep copy of c.
func (c *UsingConstraint) Clone() *UsingConstraint {
	if c == nil {
		return nil
	}
	other := *c
	other.Columns = cloneIdents(c.Columns)
	return &other
}

// String returns the string representation of the constraint.
func (c *UsingConstraint) String() string {
	var buf bytes.Buffer
	buf.WriteString("USING (")
	for i, col := range c.Columns {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
	}
	buf.WriteString(")")
	return buf.String()
}

type JoinOperator struct {
	Comma   Pos // position of comma
	Natural Pos // position of NATURAL keyword
	Left    Pos // position of LEFT keyword
	Outer   Pos // position of OUTER keyword
	Full    Pos // positon of FULL keyword
	Inner   Pos // position of INNER keyword
	Cross   Pos // position of CROSS keyword
	Join    Pos // position of JOIN keyword
}

// Clone returns a deep copy of op.
func (op *JoinOperator) Clone() *JoinOperator {
	if op == nil {
		return nil
	}
	other := *op
	return &other
}

// String returns the string representation of the operator.
func (op *JoinOperator) String() string {
	if op.Comma.IsValid() {
		return ", "
	}

	var buf bytes.Buffer
	if op.Natural.IsValid() {
		buf.WriteString(" NATURAL")
	}
	if op.Left.IsValid() {
		buf.WriteString(" LEFT")
		if op.Outer.IsValid() {
			buf.WriteString(" OUTER")
		}
	} else if op.Inner.IsValid() {
		buf.WriteString(" INNER")
	} else if op.Cross.IsValid() {
		buf.WriteString(" CROSS")
	} else if op.Full.IsValid() {
		buf.WriteString(" FULL")
		if op.Outer.IsValid() {
			buf.WriteString(" OUTER")
		}
	}
	buf.WriteString(" JOIN ")

	return buf.String()
}

type QualifiedTableFunctionName struct {
	Name   *Ident // table function name
	Lparen Pos    // position of left paren
	Args   []Expr // argument list
	Rparen Pos    // position of right paren
	As     Pos    // position of AS keyword
	Alias  *Ident // optional table alias
}

// TableName returns the name used to identify n.
// Returns the alias, if one is specified. Otherwise returns the name.
func (n *QualifiedTableFunctionName) TableName() string {
	if s := IdentName(n.Alias); s != "" {
		return s
	}
	return IdentName(n.Name)
}

// Clone returns a deep copy of n.
func (n *QualifiedTableFunctionName) Clone() *QualifiedTableFunctionName {
	if n == nil {
		return nil
	}
	other := *n
	other.Name = n.Name.Clone()
	other.Args = cloneExprs(n.Args)
	other.Alias = n.Alias.Clone()
	return &other
}

// String returns the string representation of the table name.
func (n *QualifiedTableFunctionName) String() string {
	var buf bytes.Buffer
	buf.WriteString(n.Name.String())
	buf.WriteString("(")
	for i, arg := range n.Args {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(arg.String())
	}
	buf.WriteString(")")
	if n.Alias != nil {
		fmt.Fprintf(&buf, " AS %s", n.Alias.String())
	}

	return buf.String()
}

type OverClause struct {
	Over       Pos               // position of OVER keyword
	Name       *Ident            // window name
	Definition *WindowDefinition // window definition
}

// Clone returns a deep copy of c.
func (c *OverClause) Clone() *OverClause {
	if c == nil {
		return nil
	}
	other := *c
	other.Name = c.Name.Clone()
	other.Definition = c.Definition.Clone()
	return &other
}

// String returns the string representation of the clause.
func (c *OverClause) String() string {
	if c.Name != nil {
		return fmt.Sprintf("OVER %s", c.Name.String())
	}
	return fmt.Sprintf("OVER %s", c.Definition.String())
}

type OrderingTerm struct {
	X Expr // ordering expression

	Asc  Pos // position of ASC keyword
	Desc Pos // position of DESC keyword

	Nulls      Pos // position of NULLS keyword
	NullsFirst Pos // position of FIRST keyword
	NullsLast  Pos // position of LAST keyword
}

// Clone returns a deep copy of t.
func (t *OrderingTerm) Clone() *OrderingTerm {
	if t == nil {
		return nil
	}
	other := *t
	other.X = CloneExpr(t.X)
	return &other
}

func cloneOrderingTerms(a []*OrderingTerm) []*OrderingTerm {
	if a == nil {
		return nil
	}
	other := make([]*OrderingTerm, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the term.
func (t *OrderingTerm) String() string {
	var buf bytes.Buffer
	buf.WriteString(t.X.String())

	if t.Asc.IsValid() {
		buf.WriteString(" ASC")
	} else if t.Desc.IsValid() {
		buf.WriteString(" DESC")
	}

	if t.NullsFirst.IsValid() {
		buf.WriteString(" NULLS FIRST")
	} else if t.NullsLast.IsValid() {
		buf.WriteString(" NULLS LAST")
	}

	return buf.String()
}

type Window struct {
	Name       *Ident            // name of window
	As         Pos               // position of AS keyword
	Definition *WindowDefinition // window definition
}

// Clone returns a deep copy of w.
func (w *Window) Clone() *Window {
	if w == nil {
		return nil
	}
	other := *w
	other.Name = w.Name.Clone()
	other.Definition = w.Definition.Clone()
	return &other
}

func cloneWindows(a []*Window) []*Window {
	if a == nil {
		return nil
	}
	other := make([]*Window, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the window.
func (w *Window) String() string {
	return fmt.Sprintf("%s AS %s", w.Name.String(), w.Definition.String())
}

type WindowDefinition struct {
	Lparen        Pos             // position of left paren
	Base          *Ident          // base window name
	Partition     Pos             // position of PARTITION keyword
	PartitionBy   Pos             // position of BY keyword (after PARTITION)
	Partitions    []Expr          // partition expressions
	Order         Pos             // position of ORDER keyword
	OrderBy       Pos             // position of BY keyword (after ORDER)
	OrderingTerms []*OrderingTerm // ordering terms
	Rparen        Pos             // position of right paren
}

// Clone returns a deep copy of d.
func (d *WindowDefinition) Clone() *WindowDefinition {
	if d == nil {
		return nil
	}
	other := *d
	other.Base = d.Base.Clone()
	other.Partitions = cloneExprs(d.Partitions)
	other.OrderingTerms = cloneOrderingTerms(d.OrderingTerms)
	return &other
}

// String returns the string representation of the window definition.
func (d *WindowDefinition) String() string {
	var buf bytes.Buffer
	buf.WriteString("(")
	if d.Base != nil {
		buf.WriteString(d.Base.String())
	}

	if len(d.Partitions) != 0 {
		if buf.Len() > 1 {
			buf.WriteString(" ")
		}
		buf.WriteString("PARTITION BY ")

		for i, p := range d.Partitions {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(p.String())
		}
	}

	if len(d.OrderingTerms) != 0 {
		if buf.Len() > 1 {
			buf.WriteString(" ")
		}
		buf.WriteString("ORDER BY ")

		for i, term := range d.OrderingTerms {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(term.String())
		}
	}

	buf.WriteString(")")

	return buf.String()
}

type SetStatement struct {
	Set   Pos
	Key   string
	Equal Pos
	Value string
}

func (s *SetStatement) Clone() *SetStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Key = s.Key
	other.Value = s.Value
	return &other
}

func (s *SetStatement) String() string {
	return fmt.Sprintf("SET %s=%s", s.Key, s.Value)
}

type DeclarationStatement struct {
	Name  *Ident
	Value Expr
	Type  Expr
}

func (s *DeclarationStatement) Clone() *DeclarationStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.Name = s.Name.Clone()
	other.Value = CloneExpr(s.Value)
	other.Type = CloneExpr(s.Type)
	return &other
}

func (s *DeclarationStatement) String() string {
	if s.Value != nil {
		if s.Type != nil {
			return fmt.Sprintf("%s := %s %s", s.Name, s.Type.String(), s.Value.String())
		}
		return fmt.Sprintf("%s := %s", s.Name, s.Value.String())
	}
	return fmt.Sprintf("%s %s", s.Name, s.Type.String())
}
