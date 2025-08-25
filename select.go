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

func (*SelectStatement) stmt() {}

type SelectStatement struct {
	WithClause *WithClause `json:"with_clause"`

	Values     Pos         `json:"values"`
	ValueLists []*ExprList `json:"value_lists"`

	Select   Pos             `json:"select"`
	Distinct Pos             `json:"distinct"`
	All      Pos             `json:"all"`
	Columns  []*ResultColumn `json:"columns"`

	From   Pos    `json:"from"`
	Source Source `json:"source"`

	Where     Pos  `json:"where"`
	WhereExpr Expr `json:"where_expr"`

	Group        Pos    `json:"group"`
	GroupBy      Pos    `json:"group_by"`
	GroupByAll   Pos    `json:"group_by_all"`
	GroupByExprs []Expr `json:"group_by_exprs"`
	Grouping     Pos    `json:"grouping"`
	GroupingSet  Pos    `json:"grouping_set"`
	GroupingExpr Expr   `json:"grouping_expr"`
	Having       Pos    `json:"having"`
	HavingExpr   Expr   `json:"having_expr"`
	Qualify      Pos    `json:"qualify"`
	QualifyExpr  Expr   `json:"qualify_expr"`

	Window  Pos       `json:"window"`
	Windows []*Window `json:"windows"`

	Union     Pos              `json:"union"`
	UnionDist Pos              `json:"union_dist"`
	UnionAll  Pos              `json:"union_all"`
	Intersect Pos              `json:"intersect"`
	Compound  *SelectStatement `json:"compound"`

	Order         Pos             `json:"order"`
	OrderBy       Pos             `json:"order_by"`
	OrderingTerms []*OrderingTerm `json:"ordering_terms"`

	Limit       Pos  `json:"limit"`
	LimitExpr   Expr `json:"limit_expr"`
	Offset      Pos  `json:"offset"`
	OffsetComma Pos  `json:"offset_comma"`
	OffsetExpr  Expr `json:"offset_expr"`
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
			} else if s.Grouping.IsValid() {
				buf.WriteString(" GROUPING SETS ")
				buf.WriteString(s.GroupingSet.String())
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
		if s.QualifyExpr != nil {
			fmt.Fprintf(&buf, " QUALIFY %s", s.HavingExpr.String())
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
			if s.UnionDist.IsValid() {
				buf.WriteString(" DISTINCT ")
			}
		case s.Intersect.IsValid():
			buf.WriteString(" INTERSECT")
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
	With      Pos    `json:"with"`
	Recursive Pos    `json:"recursive"`
	CTEs      []*CTE `json:"ctes"`
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
	TableName     *Ident           `json:"tableName"`
	ColumnsLparen Pos              `json:"columns_lparen"`
	Columns       []*Ident         `json:"columns"`
	ColumnsRparen Pos              `json:"columns_rparen"`
	As            Pos              `json:"as"`
	SelectLparen  Pos              `json:"select_lparen"`
	Select        *SelectStatement `json:"select"`
	SelectRparen  Pos              `json:"select_rparen"`
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

type Within struct {
	Within         Pos           `json:"within"`
	Group          Pos           `json:"group"`
	GroupLparen    Pos           `json:"group_lparen"`
	GroupOrder     Pos           `json:"group_order"`
	GroupOrderBy   Pos           `json:"group_order_by"`
	OrderingTerm   *OrderingTerm `json:"ordering_term"`
	GroupLimit     Pos           `json:"group_limit"`
	GroupLimitExpr Expr          `json:"group_limit_expr"`
	GroupRparen    Pos           `json:"group_rparen"`
	Index          *NumberLit    `json:"index"`
}

func (wi *Within) String() string {
	var buf bytes.Buffer
	buf.WriteString("WITHIN GROUP ")
	buf.WriteRune('(')
	buf.WriteString("ORDER BY ")
	buf.WriteString(wi.OrderingTerm.String())
	if wi.GroupLimit.IsValid() {
		buf.WriteString(" LIMIT ")
		buf.WriteString(wi.GroupLimit.String())
	}
	buf.WriteString(")")
	if wi.Index != nil {
		buf.WriteString(" [" + wi.Index.String() + "]")
	}
	return buf.String()
}

type ResultColumn struct {
	Star  Pos    `json:"star"`
	Expr  Expr   `json:"expr"`
	As    Pos    `json:"as"`
	Alias *Ident `json:"alias"`

	Except    Pos  `json:"except"`
	ExceptCol Expr `json:"except_col"`

	Within *Within `json:"within"`
}

// String returns the string representation of the column.
func (c *ResultColumn) String() string {
	if c.Star.IsValid() {
		if c.Except.IsValid() {
			return "* EXCEPT " + c.ExceptCol.String()
		}

		return "*"
	} else if c.Alias != nil {
		return fmt.Sprintf("%s AS %s", c.Expr.String(), c.Alias.String())
	}
	exp := c.Expr.String()
	if c.Except.IsValid() {
		return exp + " EXCEPT " + c.ExceptCol.String()
	}

	if c.Within != nil {
		return exp + " " + c.Within.String()
	}

	return exp
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

// LateralView can be used to create a view from array or map column
// Defined in maxcompute like
//
//	LATERALVIEW: LATERAL VIEW [OUTER] <udtf_name>(<expression>) <table_alias> AS <columnAlias> (',' <columnAlias>)
//	fromClause: FROM <baseTable> (LATERALVIEW) [(LATERALVIEW) ...]
type LateralView struct {
	Lateral    Pos      `json:"lateral"`
	View       Pos      `json:"view"`
	Outer      Pos      `json:"outer"`
	Udtf       *Call    `json:"udtf"`
	TableAlias *Ident   `json:"table_alias"`
	As         Pos      `json:"as"`
	ColAlias   []*Ident `json:"col_alias"`
}

func (l *LateralView) String() string {
	var buf bytes.Buffer

	buf.WriteString("Lateral View ")
	if l.Outer.IsValid() {
		buf.WriteString("Outer ")
	}

	if l.Udtf != nil {
		buf.WriteString(l.Udtf.String())
		buf.WriteString(" ")
	}
	if l.TableAlias != nil {
		buf.WriteString(l.TableAlias.String())
		buf.WriteString(" ")
	}
	if l.As.IsValid() {
		buf.WriteString("As ")
	}
	for i, col := range l.ColAlias {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
	}

	return buf.String()
}

type QualifiedTableName struct {
	Name  *MultiPartIdent `json:"name"`
	As    Pos             `json:"as"`
	Alias *Ident          `json:"alias"`

	LateralViews []*LateralView `json:"lateral_views"`
}

// TableName returns the name used to identify n.
// Returns the alias, if one is specified. Otherwise returns the name.
func (n *QualifiedTableName) TableName() string {
	if s := IdentName(n.Alias); s != "" {
		return s
	}
	return MIdentName(n.Name)
}

// String returns the string representation of the table name.
func (n *QualifiedTableName) String() string {
	var buf bytes.Buffer
	buf.WriteString(n.Name.String())
	if n.Alias != nil {
		fmt.Fprintf(&buf, " AS %s", n.Alias.String())
	}

	for _, lv := range n.LateralViews {
		fmt.Fprintf(&buf, " %s", lv.String())
	}

	return buf.String()
}

type ParenSource struct {
	Lparen Pos    `json:"lparen"`
	X      Source `json:"x"`
	Rparen Pos    `json:"rparen"`
	As     Pos    `json:"as"`
	Alias  *Ident `json:"alias"`
}

// String returns the string representation of the source.
func (s *ParenSource) String() string {
	if s.Alias != nil {
		return fmt.Sprintf("(%s) AS %s", s.X.String(), s.Alias.String())
	}
	return fmt.Sprintf("(%s)", s.X.String())
}

type JoinClause struct {
	X          Source         `json:"x"`
	Operator   *JoinOperator  `json:"operator"`
	Y          Source         `json:"y"`
	Constraint JoinConstraint `json:"constraint"`
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

type OnConstraint struct {
	On Pos  `json:"on"`
	X  Expr `json:"x"`
}

// String returns the string representation of the constraint.
func (c *OnConstraint) String() string {
	return "ON " + c.X.String()
}

type UsingConstraint struct {
	Using   Pos      `json:"using"`
	Lparen  Pos      `json:"lparen"`
	Columns []*Ident `json:"columns"`
	Rparen  Pos      `json:"rparen"`
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
	Comma   Pos `json:"comma"`
	Natural Pos `json:"natural"`
	Left    Pos `json:"left"`
	Outer   Pos `json:"outer"`
	Full    Pos `json:"full"`
	Inner   Pos `json:"inner"`
	Cross   Pos `json:"cross"`
	Join    Pos `json:"join"`
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
	Name   *Ident `json:"name"`
	Lparen Pos    `json:"lparen"`
	Args   []Expr `json:"args"`
	Rparen Pos    `json:"rparen"`
	As     Pos    `json:"as"`
	Alias  *Ident `json:"alias"`
}

// TableName returns the name used to identify n.
// Returns the alias, if one is specified. Otherwise returns the name.
func (n *QualifiedTableFunctionName) TableName() string {
	if s := IdentName(n.Alias); s != "" {
		return s
	}
	return IdentName(n.Name)
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
	Over       Pos               `json:"over"`
	Name       *Ident            `json:"name"`
	Definition *WindowDefinition `json:"definition"`
}

// String returns the string representation of the clause.
func (c *OverClause) String() string {
	if c.Name != nil {
		return fmt.Sprintf("OVER %s", c.Name.String())
	}
	return fmt.Sprintf("OVER %s", c.Definition.String())
}

type OrderingTerm struct {
	X Expr `json:"x"`

	Asc  Pos `json:"asc"`
	Desc Pos `json:"desc"`

	Nulls      Pos `json:"nulls"`
	NullsFirst Pos `json:"nulls_first"`
	NullsLast  Pos `json:"nulls_last"`
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
	Name       *Ident
	As         Pos
	Definition *WindowDefinition
}

// String returns the string representation of the window.
func (w *Window) String() string {
	return fmt.Sprintf("%s AS %s", w.Name.String(), w.Definition.String())
}

type WindowDefinition struct {
	Lparen        Pos             `json:"lparen"`
	Base          *Ident          `json:"base"`
	Partition     Pos             `json:"partition"`
	PartitionBy   Pos             `json:"partition_by"`
	Partitions    []Expr          `json:"partitions"`
	Order         Pos             `json:"order"`
	OrderBy       Pos             `json:"order_by"`
	OrderingTerms []*OrderingTerm `json:"ordering_terms"`
	Rparen        Pos             `json:"rparen"`
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
