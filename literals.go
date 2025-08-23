package query

func (*BlobLit) node()      {}
func (*BoolLit) node()      {}
func (*NullLit) node()      {}
func (*NumberLit) node()    {}
func (*StringLit) node()    {}
func (*TimestampLit) node() {}
func (*TemplateStr) node()  {}

// Literal expression
func (*BlobLit) expr()      {}
func (*BoolLit) expr()      {}
func (*NullLit) expr()      {}
func (*NumberLit) expr()    {}
func (*StringLit) expr()    {}
func (*TimestampLit) expr() {}
func (*TemplateStr) expr()  {}

type BlobLit struct {
	ValuePos Pos    `json:"value_pos"`
	Value    string `json:"value"`
}

// Clone returns a deep copy of lit.
func (lit *BlobLit) Clone() *BlobLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *BlobLit) String() string {
	return `x'` + lit.Value + `'`
}

type BoolLit struct {
	ValuePos Pos  `json:"value_pos"`
	Value    bool `json:"value"`
}

// Clone returns a deep copy of lit.
func (lit *BoolLit) Clone() *BoolLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
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

// Clone returns a deep copy of lit.
func (lit *NullLit) Clone() *NullLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *NullLit) String() string {
	return "NULL"
}

type NumberLit struct {
	ValuePos Pos    `json:"value_pos"`
	Value    string `json:"value"`
}

// Clone returns a deep copy of lit.
func (lit *NumberLit) Clone() *NumberLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
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

// Clone returns a deep copy of lit.
func (lit *StringLit) Clone() *StringLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
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

// Clone returns a deep copy of lit.
func (lit *TimestampLit) Clone() *TimestampLit {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

// String returns the string representation of the expression.
func (lit *TimestampLit) String() string {
	return lit.Value
}

type TemplateStr struct {
	TmplPos  Pos    `json:"template_pos"`
	Template string `json:"template"`
}

func (lit *TemplateStr) Clone() *TemplateStr {
	if lit == nil {
		return nil
	}
	other := *lit
	return &other
}

func (lit *TemplateStr) String() string {
	return "{{" + lit.Template + "}}"
}
