package lineage

import "github.com/sbchaos/query"

type Column struct {
	Ref  string `json:"ref"`
	Name string `json:"name"`

	Transform string `json:"transform"`
}

func processColumn(col *query.ResultColumn) []Column {
	if col.Star.IsValid() {
		return []Column{{Name: "*"}}
	}

	if col.Expr == nil {
		return nil
	}

	return processExpr(col.Expr)
}

func processExpr(expr query.Expr) []Column {
	switch ex := expr.(type) {
	case *query.Ident:
		return []Column{{Name: ex.Name}}
	case *query.MultiPartIdent:
		if ex.First == nil {
			return []Column{{Name: ex.Name.Name}}
		}
		return []Column{{
			Ref:  ex.First.Name,
			Name: ex.String(),
		}}

	case *query.CastExpr:
		cols := processExpr(ex.X)
		for _, c := range cols {
			c.Transform = ex.String()
		}
		return cols

	case *query.ParenExpr:
		return processExpr(ex.X)

	case *query.QualifiedRef:
		c1 := Column{}
		if ex.Star.IsValid() {
			c1.Name = "*"
		}
		if ex.Name != nil {
			c1.Name = ex.Name.String()
		}
		return []Column{c1}

	case *query.UnaryExpr:
		cols := processExpr(ex.X)
		for _, c := range cols {
			c.Transform = ex.String()
		}
		return cols

	case *query.IndexExpr:
		cols := processExpr(ex.X)
		transform := ex.String()
		for _, c := range cols {
			c.Transform = transform
		}
		return cols
	// Multiple return expr
	case *query.Range:
		cols := processExpr(ex.X)
		cols = append(cols, processExpr(ex.Y)...)
		transform := ex.String()
		for _, col := range cols {
			col.Transform = transform
		}
		return cols

	//case *query.Exists:
	case *query.ExprList:
		cols := []Column{}
		for _, cs := range ex.Exprs {
			cols = append(cols, processExpr(cs)...)
		}
		transform := ex.String()
		for _, col := range cols {
			col.Transform = transform
		}
		return cols

	case *query.CaseExpr:
		cols := []Column{}
		cols = append(cols, processExpr(ex.Operand)...)
		cols = append(cols, processExpr(ex.ElseExpr)...)
		for _, c1 := range ex.Blocks {
			cols = append(cols, processExpr(c1.Condition)...)
			cols = append(cols, processExpr(c1.Body)...)
		}
		transform := ex.String()
		for _, col := range cols {
			col.Transform = transform
		}

	case *query.Call:
		cols := []Column{}
		for _, a := range ex.Args {
			cols = append(cols, processExpr(a.X)...)
		}
		transform := ex.String()
		for _, col := range cols {
			col.Transform = transform
		}
		return cols

	case *query.BinaryExpr:
		cols := []Column{}
		cols = append(cols, processExpr(ex.X)...)
		cols = append(cols, processExpr(ex.Y)...)
		transform := ex.String()
		for _, col := range cols {
			col.Transform = transform
		}
		return cols

	case *query.Null:
		return processExpr(ex.X)
	}

	return nil
}
