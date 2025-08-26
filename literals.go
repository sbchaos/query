package query

func (*BoolLit) node()      {}
func (*IntervalLit) node()  {}
func (*NullLit) node()      {}
func (*NumberLit) node()    {}
func (*RawLit) node()       {}
func (*StringLit) node()    {}
func (*TimestampLit) node() {}
func (*TemplateStr) node()  {}

// Literal expression
func (*BoolLit) expr()      {}
func (*IntervalLit) expr()  {}
func (*NullLit) expr()      {}
func (*NumberLit) expr()    {}
func (*RawLit) expr()       {}
func (*StringLit) expr()    {}
func (*TimestampLit) expr() {}
func (*TemplateStr) expr()  {}

type RawLit struct {
	ValuePos Pos    `json:"value_pos"`
	Value    string `json:"value"`
}

func (lit *RawLit) String() string {
	return `r'` + lit.Value + `'`
}

type BoolLit struct {
	ValuePos Pos  `json:"value_pos"`
	Value    bool `json:"value"`
}

// String returns the string representation of the expression.
func (lit *BoolLit) String() string {
	if lit.Value {
		return "TRUE"
	}
	return "FALSE"
}

type NullLit struct {
	Pos Pos `json:"pos"`
}

// String returns the string representation of the expression.
func (lit *NullLit) String() string {
	return "NULL"
}

type NumberLit struct {
	ValuePos Pos    `json:"value_pos"`
	Value    string `json:"value"`
}

// String returns the string representation of the expression.
func (lit *NumberLit) String() string {
	return lit.Value
}

type StringLit struct {
	ValuePos Pos    `json:"value_pos"`
	Value    string `json:"value"`
	Quote    rune   `json:"quote"`
}

// String returns the string representation of the expression.
func (lit *StringLit) String() string {
	if lit.Quote == 0 {
		return lit.Value
	}
	return string(lit.Quote) + lit.Value + string(endQuote(lit.Quote))
}

type TimestampLit struct {
	ValuePos Pos    `json:"value_pos"`
	Value    string `json:"value"`
}

// String returns the string representation of the expression.
func (lit *TimestampLit) String() string {
	return lit.Value
}

type TemplateStr struct {
	TmplPos  Pos    `json:"template_pos"`
	Template string `json:"template"`
}

func (lit *TemplateStr) String() string {
	return "{{" + lit.Template + "}}"
}

type IntervalLit struct {
	Interval Pos    `json:"interval_pos"`
	Value    string `json:"value"`
	Unit     string `json:"unit"`
}

func (lit *IntervalLit) String() string {
	return "INTERVAL " + lit.Value + " " + lit.Unit
}
