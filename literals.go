package query

import "strings"

func (*BlobLit) node()      {}
func (*BoolLit) node()      {}
func (*NullLit) node()      {}
func (*NumberLit) node()    {}
func (*StringLit) node()    {}
func (*TimestampLit) node() {}

// Literal expression
func (*BlobLit) expr()      {}
func (*BoolLit) expr()      {}
func (*NullLit) expr()      {}
func (*NumberLit) expr()    {}
func (*StringLit) expr()    {}
func (*TimestampLit) expr() {}

type BlobLit struct {
	ValuePos Pos    // literal position
	Value    string // literal value
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
	ValuePos Pos  // literal position
	Value    bool // literal value
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
	Pos Pos
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
	ValuePos Pos    // literal position
	Value    string // literal value
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
	ValuePos Pos    // literal position
	Value    string // literal value (without quotes)
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
	return `'` + strings.Replace(lit.Value, `'`, `''`, -1) + `'`
}

type TimestampLit struct {
	ValuePos Pos    // literal position
	Value    string // literal value
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
