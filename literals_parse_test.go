package query_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sbchaos/query"
)

func TestStringLit_String(t *testing.T) {
	AssertExprStringer(t, &query.StringLit{Value: "foo"}, `foo`)
}

func TestNumberLit_String(t *testing.T) {
	AssertExprStringer(t, &query.NumberLit{Value: "123.45"}, `123.45`)
}

func TestBlobLit_String(t *testing.T) {
	AssertExprStringer(t, &query.BlobLit{Value: "0123abcd"}, `x'0123abcd'`)
}

func TestBoolLit_String(t *testing.T) {
	AssertExprStringer(t, &query.BoolLit{Value: true}, `TRUE`)
	AssertExprStringer(t, &query.BoolLit{Value: false}, `FALSE`)
}

func TestNullLit_String(t *testing.T) {
	AssertExprStringer(t, &query.NullLit{}, `NULL`)
}

func AssertExprStringer(tb testing.TB, expr query.Expr, s string) {
	tb.Helper()

	exp, err := query.ParseExprString(s)
	assert.NoError(tb, err)

	assert.Equal(tb, exp.String(), expr.String())
}
