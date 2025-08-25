package query

import (
	"bytes"
	"fmt"
)

func (*DeclarationStatement) node() {}
func (*DeleteStatement) node()      {}
func (*InsertStatement) node()      {}
func (*SetStatement) node()         {}
func (*CreateTableStatement) node() {}
func (*DropTableStatement) node()   {}
func (*MergeStatement) node()       {}
func (*FunctionStatement) node()    {}
func (*TruncateStatement) node()    {}

type Statement interface {
	Node
	stmt()
}

func (*DeclarationStatement) stmt() {}
func (*DeleteStatement) stmt()      {}
func (*InsertStatement) stmt()      {}
func (*SetStatement) stmt()         {}
func (*CreateTableStatement) stmt() {}
func (*DropTableStatement) stmt()   {}
func (*MergeStatement) stmt()       {}
func (*FunctionStatement) stmt()    {}
func (*TruncateStatement) stmt()    {}

type SetStatement struct {
	Set   Pos    `json:"set"`
	Key   string `json:"key"`
	Equal Pos    `json:"equal"`
	Value string `json:"value"`
}

func (s *SetStatement) String() string {
	return fmt.Sprintf("SET %s=%s", s.Key, s.Value)
}

type DeclarationStatement struct {
	Name  *Ident `json:"name"`
	Value Expr   `json:"value,omitempty"`
	Type  Expr   `json:"type,omitempty"`
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
	WithClause *WithClause `json:"with_clause,omitempty"`

	Insert    Pos `json:"insert"`
	Replace   Pos `json:"replace,omitempty"`
	Into      Pos `json:"into,omitempty"`
	Overwrite Pos `json:"overwrite,omitempty"`
	TablePos  Pos `json:"table_pos"`

	Table *MultiPartIdent `json:"table"`
	As    Pos             `json:"as"`
	Alias *Ident          `json:"alias"`

	ColumnsLparen Pos      `json:"columns_lparen"`
	Columns       []*Ident `json:"columns"`
	ColumnsRparen Pos      `json:"columns_rparen"`

	Values     Pos         `json:"values"`
	ValueLists []*ExprList `json:"value_lists"`

	SelLparen Pos              `json:"sel_lparen"`
	Select    *SelectStatement `json:"select"`
	SelRparen Pos              `json:"sel_rparen"`

	Default       Pos `json:"default"`
	DefaultValues Pos `json:"default_values"`

	UpsertClause    *UpsertClause    `json:"upsert_clause"`
	ReturningClause *ReturningClause `json:"returning_clause"`
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
	}
	buf.WriteString(" INTO")
	if s.TablePos.IsValid() {
		buf.WriteString(" TABLE ")
	}

	buf.WriteString(s.Table.String())
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
	On         Pos `json:"on"`
	OnConflict Pos `json:"on_conflict"`

	Lparen    Pos              `json:"lparen"`
	Columns   []*IndexedColumn `json:"columns"`
	Rparen    Pos              `json:"rparen"`
	Where     Pos              `json:"where"`
	WhereExpr Expr             `json:"where_expr"`

	Do              Pos           `json:"do"`
	DoNothing       Pos           `json:"do_nothing"`
	DoUpdate        Pos           `json:"do_update"`
	DoUpdateSet     Pos           `json:"do_updateSet"`
	Assignments     []*Assignment `json:"assignments"`
	UpdateWhere     Pos           `json:"update_where"`
	UpdateWhereExpr Expr          `json:"update_where_expr"`
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
	Returning Pos             `json:"returning"`
	Columns   []*ResultColumn `json:"columns"`
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
	WithClause *WithClause         `json:"with_clause"`
	Delete     Pos                 `json:"delete"`
	From       Pos                 `json:"from"`
	Table      *QualifiedTableName `json:"table"`

	Where     Pos  `json:"where"`
	WhereExpr Expr `json:"where_expr"`

	Order         Pos             `json:"order"`
	OrderBy       Pos             `json:"order_by"`
	OrderingTerms []*OrderingTerm `json:"ordering_terms"`

	Limit       Pos  `json:"limit"`
	LimitExpr   Expr `json:"limit_expr"`
	Offset      Pos  `json:"offset"`
	OffsetComma Pos  `json:"offset_comma"`
	OffsetExpr  Expr `json:"offset_expr"`

	ReturningClause *ReturningClause `json:"returning_clause"`
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
	Lparen  Pos               `json:"lparen"`
	Columns []*MultiPartIdent `json:"columns"`
	Rparen  Pos               `json:"rparen"`
	Eq      Pos               `json:"eq"`
	Expr    Expr              `json:"expr"`
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
	X         Expr   `json:"x"`
	Collate   Pos    `json:"collate"`
	Collation *Ident `json:"collation"`
	Asc       Pos    `json:"asc"`
	Desc      Pos    `json:"desc"`
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

type CreateTableStatement struct {
	Create      Pos             `json:"create"`
	Or          Pos             `json:"or"`
	Replace     Pos             `json:"replace"`
	Table       Pos             `json:"table"`
	If          Pos             `json:"if"`
	IfNot       Pos             `json:"if_not"`
	IfNotExists Pos             `json:"if_not_exists"`
	Name        *MultiPartIdent `json:"name"`

	Lparen  Pos                 `json:"lparen"`
	Columns []*ColumnDefinition `json:"columns"`
	Rparen  Pos                 `json:"rparen"`

	As     Pos              `json:"as"`
	Select *SelectStatement `json:"select"`
}

// String returns the string representation of the statement.
func (s *CreateTableStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("CREATE TABLE")
	if s.IfNotExists.IsValid() {
		buf.WriteString(" IF NOT EXISTS")
	}
	buf.WriteString(" ")
	buf.WriteString(s.Name.String())

	if s.Select != nil {
		buf.WriteString(" AS ")
		buf.WriteString(s.Select.String())
	} else {
		buf.WriteString(" (")
		for i := range s.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(s.Columns[i].String())
		}
		buf.WriteString(")")
	}

	return buf.String()
}

type ColumnDefinition struct {
	Name *Ident `json:"name"`
	Type *Type  `json:"type"`
}

// String returns the string representation of the statement.
func (c *ColumnDefinition) String() string {
	var buf bytes.Buffer
	buf.WriteString(c.Name.String())
	if c.Type != nil {
		buf.WriteString(" ")
		buf.WriteString(c.Type.String())
	}
	return buf.String()
}

type DropTableStatement struct {
	Drop     Pos             `json:"drop"`
	Table    Pos             `json:"table"`
	If       Pos             `json:"if"`
	IfExists Pos             `json:"if_exists"`
	Name     *MultiPartIdent `json:"name"`
}

// String returns the string representation of the statement.
func (s *DropTableStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP TABLE")
	if s.IfExists.IsValid() {
		buf.WriteString(" IF EXISTS")
	}
	fmt.Fprintf(&buf, " %s", s.Name.String())
	return buf.String()
}

type MatchedCondition struct {
	When    Pos `json:"when"`
	Not     Pos `json:"not"`
	Matched Pos `json:"matched"`

	And     Pos  `json:"and"`
	AndExpr Expr `json:"and_expr"`
	Then    Pos  `json:"then"`

	Update      Pos           `json:"update"`
	UpdateSet   Pos           `json:"update_set"`
	Assignments []*Assignment `json:"assignments"`

	Delete Pos `json:"delete"`

	Insert     Pos       `json:"insert"`
	Star       Pos       `json:"star"`
	ColList    *ExprList `json:"col_list"`
	Values     Pos       `json:"values"`
	ValueLists *ExprList `json:"value_lists"`
}

type MergeStatement struct {
	Merge Pos `json:"merge"`
	Into  Pos `json:"into"`

	Target Source `json:"target"`
	Using  Pos    `json:"using"`
	Source Source `json:"source"`

	On     Pos  `json:"on"`
	OnExpr Expr `json:"on_expr"`

	Matched []*MatchedCondition `json:"matched"`
}

func (s *MergeStatement) String() string {
	return "MergeStatement"
}

type FunctionStatement struct {
	Function Pos             `json:"function"`
	Name     *MultiPartIdent `json:"name"`

	Lparen Pos                 `json:"lparen"`
	Params []*ColumnDefinition `json:"params"`
	Rparen Pos                 `json:"rparen"`

	Returns     Pos               `json:"returns"`
	ReturnParam *ColumnDefinition `json:"return_param"`

	As     Pos  `json:"as"`
	Begin  Pos  `json:"begin"`
	FnExpr Expr `json:"fn_expr"`
	End    Pos  `json:"end"`
}

func (s *FunctionStatement) String() string {
	return "FunctionStatement"
}

type TruncateStatement struct {
	Truncate Pos             `json:"truncate"`
	Table    Pos             `json:"table"`
	Name     *MultiPartIdent `json:"name"`
}

func (s *TruncateStatement) String() string {
	return "TRUNCATE TABLE " + s.Name.String()
}
