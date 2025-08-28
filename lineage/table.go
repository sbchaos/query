package lineage

import "github.com/sbchaos/query"

type Table struct {
	Project string
	Schema  string
	Name    string

	SubTable []*Table
	CTE      []*Table
	Join     []*Table
	IsCte    bool
	Alias    string

	Columns []Column
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
}

func (t *Table) fromCTE(clause *query.CTE) {
	t2 := &Table{}

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
