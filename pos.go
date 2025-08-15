package query

import "fmt"

type Pos struct {
	Offset int // offset, starting at 0
	Line   int // line number, starting at 1
	Column int // column number, starting at 1 (byte count)
}

// String returns a string representation of the position.
func (p Pos) String() string {
	if !p.IsValid() {
		return "-"
	}
	s := fmt.Sprintf("%d", p.Line)
	if p.Column != 0 {
		s += fmt.Sprintf(":%d", p.Column)
	}
	return s
}

// IsValid returns true if p is non-zero.
func (p Pos) IsValid() bool {
	return p != Pos{}
}

func assert(condition bool) {
	if !condition {
		panic("assert failed")
	}
}
