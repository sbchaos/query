package lineage

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/sbchaos/query"
)

type Table struct {
	Project string `yaml:"project"`
	Schema  string `yaml:"schema"`
	Name    string `yaml:"name"`

	SubTable []*Table `yaml:"subTable"`
	CTE      []*Table `yaml:"cte"`
	Join     []*Table `yaml:"join"`
	IsCte    bool     `yaml:"isCte"`
	Alias    string   `yaml:"alias"`

	Columns []Column `yaml:"columns"`
}

func ParseQuery(name string, str string) (*Table, error) {
	stmts, err := query.NewParser(strings.NewReader(str)).ParseStatements()
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %v", name, err)
	}

	t := &Table{}
	for _, stmt := range stmts {
		switch stmt := stmt.(type) {
		case *query.SelectStatement:
			t.fromSelect(stmt)
		}
	}
	return t, nil
}

func (t *Table) DisplayName() string {
	var buf bytes.Buffer
	if t.Project != "" {
		buf.WriteString(t.Project)
		buf.WriteString(".")
	}
	if t.Schema != "" {
		buf.WriteString(t.Schema)
		buf.WriteString(".")
	}
	buf.WriteString(t.Name)
	return buf.String()
}

func (t *Table) fromSelect(sel *query.SelectStatement) {
	if sel.WithClause != nil {
		for _, cte := range sel.WithClause.CTEs {
			t.fromCTE(cte)
		}
	}

	if sel.Source != nil {
		t.processSource(sel.Source)
	}

	if sel.Compound != nil {
		t2 := &Table{}
		t.SubTable = append(t.SubTable, t2)
		t2.fromSelect(sel.Compound)
	}

	for _, col := range sel.Columns {
		cs := processColumn(col)
		for _, c1 := range cs {
			if c1.Name != "" {
				t.addColumn(c1)
			}
		}
	}
}

func (t *Table) fromCTE(clause *query.CTE) {
	t2 := &Table{
		IsCte: true,
	}

	if clause.TableName != nil {
		t2.Name = clause.TableName.String()
	}

	for _, col := range clause.Columns {
		t2.Columns = append(t2.Columns, Column{
			Name: col.Name,
		})
	}

	if clause.Select != nil {
		t3 := &Table{}
		t2.SubTable = append(t2.SubTable, t3)

		t3.fromSelect(clause.Select)
	}
	t.CTE = append(t.CTE, t2)
}

func (t *Table) processSource(src query.Source) {
	switch src := src.(type) {
	case *query.SelectStatement:
		t2 := &Table{}
		t2.SubTable = append(t2.SubTable, t2)
		t2.fromSelect(src)

	case *query.JoinClause:
		if src.X != nil {
			t.processSource(src.X)
		}
		if src.Y != nil {
			t2 := &Table{}
			t.Join = append(t.Join, t2)
			t2.processSource(src.Y)
		}

	case *query.ParenSource:
		t2 := &Table{}
		t2.SubTable = append(t2.SubTable, t2)
		if src.Alias != nil {
			t2.Alias = src.Alias.String()
		}

		if src.X != nil {
			t2.processSource(src.X)
		}

	case *query.QualifiedTableName:
		if src.Alias != nil {
			t.Alias = src.Alias.Name
		}

		if src.Name != nil {
			mIdent := src.Name
			if mIdent.First != nil {
				if src.Name.Second != nil {
					t.Project = mIdent.First.Name
					t.Schema = mIdent.Second.Name
				} else {
					t.Schema = mIdent.First.Name
				}
			}
			t.Name = mIdent.Name.Name
		}
	}
}

func (t *Table) addColumn(c1 Column) {
	if c1.Ref == "" {
		t.Columns = append(t.Columns, c1)
		return
	}

	for _, t1 := range t.SubTable {
		if strings.EqualFold(t1.Alias, c1.Ref) {
			t1.addColumn(c1)
		}
	}

	for _, t2 := range t.Join {
		if strings.EqualFold(t2.Alias, c1.Ref) {
			t2.addColumn(c1)
		}
	}
}
