package query

import (
	"bytes"
	"fmt"
)

func (*DeclarationStatement) node() {}
func (*DeleteStatement) node()      {}
func (*InsertStatement) node()      {}
func (*SetStatement) node()         {}

type Statement interface {
	Node
	stmt()
}

func (*DeclarationStatement) stmt() {}
func (*DeleteStatement) stmt()      {}
func (*InsertStatement) stmt()      {}
func (*SetStatement) stmt()         {}

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

type InsertStatement struct {
	WithClause *WithClause // clause containing CTEs

	Insert           Pos // position of INSERT keyword
	Replace          Pos // position of REPLACE keyword
	InsertOr         Pos // position of OR keyword after INSERT
	InsertOrReplace  Pos // position of REPLACE keyword after INSERT OR
	InsertOrRollback Pos // position of ROLLBACK keyword after INSERT OR
	InsertOrAbort    Pos // position of ABORT keyword after INSERT OR
	InsertOrFail     Pos // position of FAIL keyword after INSERT OR
	InsertOrIgnore   Pos // position of IGNORE keyword after INSERT OR
	Into             Pos // position of INTO keyword

	Table *Ident // table name
	As    Pos    // position of AS keyword
	Alias *Ident // optional alias

	ColumnsLparen Pos      // position of column list left paren
	Columns       []*Ident // optional column list
	ColumnsRparen Pos      // position of column list right paren

	Values     Pos         // position of VALUES keyword
	ValueLists []*ExprList // lists of lists of values

	Select *SelectStatement // SELECT statement

	Default       Pos // position of DEFAULT keyword
	DefaultValues Pos // position of VALUES keyword after DEFAULT

	UpsertClause    *UpsertClause    // optional upsert clause
	ReturningClause *ReturningClause // optional RETURNING clause
}

// Clone returns a deep copy of s.
func (s *InsertStatement) Clone() *InsertStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.WithClause = s.WithClause.Clone()
	other.Table = s.Table.Clone()
	other.Alias = s.Alias.Clone()
	other.Columns = cloneIdents(s.Columns)
	other.ValueLists = cloneExprLists(s.ValueLists)
	other.Select = s.Select.Clone()
	other.UpsertClause = s.UpsertClause.Clone()
	other.ReturningClause = s.ReturningClause.Clone()
	return &other
}

// String returns the string representation of the statement.
func (s *InsertStatement) String() string {
	var buf bytes.Buffer
	if s.WithClause != nil {
		buf.WriteString(s.WithClause.String())
		buf.WriteString(" ")
	}

	if s.Replace.IsValid() {
		buf.WriteString("REPLACE")
	} else {
		buf.WriteString("INSERT")
		if s.InsertOrReplace.IsValid() {
			buf.WriteString(" OR REPLACE")
		} else if s.InsertOrRollback.IsValid() {
			buf.WriteString(" OR ROLLBACK")
		} else if s.InsertOrAbort.IsValid() {
			buf.WriteString(" OR ABORT")
		} else if s.InsertOrFail.IsValid() {
			buf.WriteString(" OR FAIL")
		} else if s.InsertOrIgnore.IsValid() {
			buf.WriteString(" OR IGNORE")
		}
	}

	fmt.Fprintf(&buf, " INTO %s", s.Table.String())
	if s.Alias != nil {
		fmt.Fprintf(&buf, " AS %s", s.Alias.String())
	}

	if len(s.Columns) != 0 {
		buf.WriteString(" (")
		for i, col := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	if s.DefaultValues.IsValid() {
		buf.WriteString(" DEFAULT VALUES")
	} else if s.Select != nil {
		fmt.Fprintf(&buf, " %s", s.Select.String())
	} else {
		buf.WriteString(" VALUES")
		for i := range s.ValueLists {
			if i != 0 {
				buf.WriteString(",")
			}
			buf.WriteString(" (")
			for j, expr := range s.ValueLists[i].Exprs {
				if j != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(expr.String())
			}
			buf.WriteString(")")
		}
	}

	if s.UpsertClause != nil {
		fmt.Fprintf(&buf, " %s", s.UpsertClause.String())
	}
	if s.ReturningClause != nil {
		fmt.Fprintf(&buf, " %s", s.ReturningClause.String())
	}

	return buf.String()
}

type UpsertClause struct {
	On         Pos // position of ON keyword
	OnConflict Pos // position of CONFLICT keyword after ON

	Lparen    Pos              // position of column list left paren
	Columns   []*IndexedColumn // optional indexed column list
	Rparen    Pos              // position of column list right paren
	Where     Pos              // position of WHERE keyword
	WhereExpr Expr             // optional conditional expression

	Do              Pos           // position of DO keyword
	DoNothing       Pos           // position of NOTHING keyword after DO
	DoUpdate        Pos           // position of UPDATE keyword after DO
	DoUpdateSet     Pos           // position of SET keyword after DO UPDATE
	Assignments     []*Assignment // list of column assignments
	UpdateWhere     Pos           // position of WHERE keyword for DO UPDATE SET
	UpdateWhereExpr Expr          // optional conditional expression for DO UPDATE SET
}

// Clone returns a deep copy of c.
func (c *UpsertClause) Clone() *UpsertClause {
	if c == nil {
		return nil
	}
	other := *c
	other.Columns = cloneIndexedColumns(c.Columns)
	other.WhereExpr = CloneExpr(c.WhereExpr)
	other.Assignments = cloneAssignments(c.Assignments)
	other.UpdateWhereExpr = CloneExpr(c.UpdateWhereExpr)
	return &other
}

// String returns the string representation of the clause.
func (c *UpsertClause) String() string {
	var buf bytes.Buffer
	buf.WriteString("ON CONFLICT")

	if len(c.Columns) != 0 {
		buf.WriteString(" (")
		for i, col := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")

		if c.WhereExpr != nil {
			fmt.Fprintf(&buf, " WHERE %s", c.WhereExpr.String())
		}
	}

	buf.WriteString(" DO")
	if c.DoNothing.IsValid() {
		buf.WriteString(" NOTHING")
	} else {
		buf.WriteString(" UPDATE SET ")
		for i := range c.Assignments {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Assignments[i].String())
		}

		if c.UpdateWhereExpr != nil {
			fmt.Fprintf(&buf, " WHERE %s", c.UpdateWhereExpr.String())
		}
	}

	return buf.String()
}

type ReturningClause struct {
	Returning Pos             // position of RETURNING keyword
	Columns   []*ResultColumn // list of result columns in the SELECT clause
}

// Clone returns a deep copy of c.
func (c *ReturningClause) Clone() *ReturningClause {
	if c == nil {
		return nil
	}
	other := *c
	return &other
}

// String returns the string representation of the clause.
func (c *ReturningClause) String() string {
	var buf bytes.Buffer
	buf.WriteString("RETURNING ")
	for i, col := range c.Columns {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
	}
	return buf.String()
}

type DeleteStatement struct {
	WithClause *WithClause         // clause containing CTEs
	Delete     Pos                 // position of UPDATE keyword
	From       Pos                 // position of FROM keyword
	Table      *QualifiedTableName // table name

	Where     Pos  // position of WHERE keyword
	WhereExpr Expr // conditional expression

	Order         Pos             // position of ORDER keyword
	OrderBy       Pos             // position of BY keyword after ORDER
	OrderingTerms []*OrderingTerm // terms of ORDER BY clause

	Limit       Pos  // position of LIMIT keyword
	LimitExpr   Expr // limit expression
	Offset      Pos  // position of OFFSET keyword
	OffsetComma Pos  // position of COMMA (instead of OFFSET)
	OffsetExpr  Expr // offset expression

	ReturningClause *ReturningClause // optional RETURNING clause
}

// Clone returns a deep copy of s.
func (s *DeleteStatement) Clone() *DeleteStatement {
	if s == nil {
		return nil
	}
	other := *s
	other.WithClause = s.WithClause.Clone()
	other.Table = s.Table.Clone()
	other.WhereExpr = CloneExpr(s.WhereExpr)
	other.OrderingTerms = cloneOrderingTerms(s.OrderingTerms)
	other.LimitExpr = CloneExpr(s.LimitExpr)
	other.OffsetExpr = CloneExpr(s.OffsetExpr)
	return &other
}

// String returns the string representation of the clause.
func (s *DeleteStatement) String() string {
	var buf bytes.Buffer
	if s.WithClause != nil {
		buf.WriteString(s.WithClause.String())
		buf.WriteString(" ")
	}

	fmt.Fprintf(&buf, "DELETE FROM %s", s.Table.String())
	if s.WhereExpr != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.WhereExpr.String())
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

// Assignment is used within the UPDATE statement & upsert clause.
// It is similiar to an expression except that it must be an equality.
type Assignment struct {
	Lparen  Pos      // position of column list left paren
	Columns []*Ident // column list
	Rparen  Pos      // position of column list right paren
	Eq      Pos      // position of =
	Expr    Expr     // assigned expression
}

// Clone returns a deep copy of a.
func (a *Assignment) Clone() *Assignment {
	if a == nil {
		return nil
	}
	other := *a
	other.Columns = cloneIdents(a.Columns)
	other.Expr = CloneExpr(a.Expr)
	return &other
}

func cloneAssignments(a []*Assignment) []*Assignment {
	if a == nil {
		return nil
	}
	other := make([]*Assignment, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the clause.
func (a *Assignment) String() string {
	var buf bytes.Buffer
	if len(a.Columns) == 1 {
		buf.WriteString(a.Columns[0].String())
	} else if len(a.Columns) > 1 {
		buf.WriteString("(")
		for i, col := range a.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	fmt.Fprintf(&buf, " = %s", a.Expr.String())
	return buf.String()
}

type IndexedColumn struct {
	X         Expr   // column expression
	Collate   Pos    // position of COLLATE keyword
	Collation *Ident // collation name
	Asc       Pos    // position of optional ASC keyword
	Desc      Pos    // position of optional DESC keyword
}

// Clone returns a deep copy of c.
func (c *IndexedColumn) Clone() *IndexedColumn {
	if c == nil {
		return nil
	}
	other := *c
	other.X = CloneExpr(c.X)
	other.Collation = c.Collation.Clone()
	return &other
}

func cloneIndexedColumns(a []*IndexedColumn) []*IndexedColumn {
	if a == nil {
		return nil
	}
	other := make([]*IndexedColumn, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// String returns the string representation of the column.
func (c *IndexedColumn) String() string {
	var buf bytes.Buffer
	buf.WriteString(c.X.String())

	if c.Collate.IsValid() {
		buf.WriteString(" COLLATE ")
		buf.WriteString(c.Collation.String())
	}

	if c.Asc.IsValid() {
		buf.WriteString(" ASC")
	} else if c.Desc.IsValid() {
		buf.WriteString(" DESC")
	}

	return buf.String()
}
