package query_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sbchaos/query"
)

func TestScanner_Scan(t *testing.T) {
	t.Run("IDENT", func(t *testing.T) {
		t.Run("Unquoted", func(t *testing.T) {
			AssertScan(t, `foo_BAR123`, query.IDENT, `foo_BAR123`)
		})
		t.Run("Quoted", func(t *testing.T) {
			AssertScan(t, `"crazy ~!#*&# column name"" foo"`, query.QIDENT, `crazy ~!#*&# column name" foo`)
		})
		t.Run("NoEndQuote", func(t *testing.T) {
			AssertScan(t, `"unfinished`, query.ILLEGAL, `"unfinished`)
		})
		t.Run("x", func(t *testing.T) {
			AssertScan(t, `x`, query.IDENT, `x`)
		})
		t.Run("StartingX", func(t *testing.T) {
			AssertScan(t, `xyz`, query.IDENT, `xyz`)
		})
	})

	t.Run("COMMENT", func(t *testing.T) {
		t.Run("SingleLine", func(t *testing.T) {
			t.Run("Newline", func(t *testing.T) {
				AssertScan(t, "-- foo bar\n--baz", query.COMMENT, `-- foo bar`)
			})
			t.Run("EOF", func(t *testing.T) {
				AssertScan(t, "-- foo bar", query.COMMENT, `-- foo bar`)
			})
			t.Run("NoContent", func(t *testing.T) {
				AssertScan(t, "--", query.COMMENT, `--`)
			})
		})
		t.Run("MultiLine", func(t *testing.T) {
			t.Run("Newline", func(t *testing.T) {
				AssertScan(t, "/* foo bar */", query.COMMENT, `/* foo bar */`)
			})
			t.Run("EOF", func(t *testing.T) {
				AssertScan(t, "/* foo bar", query.COMMENT, `/* foo bar`)
			})
			t.Run("NoContent", func(t *testing.T) {
				AssertScan(t, "/**/", query.COMMENT, `/**/`)
			})
		})
	})

	t.Run("KEYWORD", func(t *testing.T) {
		AssertScan(t, `BEGIN`, query.BEGIN, `BEGIN`)
	})

	t.Run("STRING", func(t *testing.T) {
		t.Run("OK", func(t *testing.T) {
			AssertScan(t, `'this is ''a'' string'`, query.STRING, `this is 'a' string`)
		})
		t.Run("Allow ticks", func(t *testing.T) {
			AssertScan(t, "`table`", query.TSTRING, `table`)
		})
		t.Run("NoEndQuote", func(t *testing.T) {
			AssertScan(t, `'unfinished`, query.ILLEGAL, `'unfinished`)
		})
	})
	t.Run("BLOB", func(t *testing.T) {
		t.Run("LowerX", func(t *testing.T) {
			AssertScan(t, `x'0123456789abcdef'`, query.BLOB, `0123456789abcdef`)
		})
		t.Run("UpperX", func(t *testing.T) {
			AssertScan(t, `X'0123456789ABCDEF'`, query.BLOB, `0123456789ABCDEF`)
		})
		t.Run("NoEndQuote", func(t *testing.T) {
			AssertScan(t, `x'0123`, query.ILLEGAL, `x'0123`)
		})
		t.Run("BadHex", func(t *testing.T) {
			AssertScan(t, `x'hello`, query.ILLEGAL, `x'h`)
		})
	})

	t.Run("INTEGER", func(t *testing.T) {
		AssertScan(t, `012`, query.INTEGER, `012`)
		AssertScan(t, `123`, query.INTEGER, `123`)
		AssertScan(t, `0xe3`, query.INTEGER, `0xe3`)
		// BUG: see comment in scanner
		// AssertScanError(t, `0x`, query.ILLEGAL)
		// AssertScanError(t, `4xe3`, query.ILLEGAL)
		// AssertScanError(t, `0x12345678912345678`, query.ILLEGAL, ``)
	})

	t.Run("FLOAT", func(t *testing.T) {
		AssertScan(t, `123.456`, query.FLOAT, `123.456`)
		AssertScan(t, `0.01`, query.FLOAT, `0.01`)
		AssertScan(t, `.1`, query.FLOAT, `.1`)
		AssertScan(t, `123e456`, query.FLOAT, `123e456`)
		AssertScan(t, `123E456`, query.FLOAT, `123E456`)
		AssertScan(t, `123.456E78`, query.FLOAT, `123.456E78`)
		AssertScan(t, `123.E45`, query.FLOAT, `123.E45`)
		AssertScan(t, `123E+4`, query.FLOAT, `123E+4`)
		AssertScan(t, `123E-4`, query.FLOAT, `123E-4`)
		AssertScan(t, `.0E-2`, query.FLOAT, `.0E-2`)
		AssertScan(t, `123E`, query.ILLEGAL, `123E`)
		AssertScan(t, `123E+`, query.ILLEGAL, `123E+`)
		AssertScan(t, `123E-`, query.ILLEGAL, `123E-`)
	})
	t.Run("BIND", func(t *testing.T) {
		AssertScan(t, `?'`, query.BIND, `?`)
		AssertScan(t, `?123'`, query.BIND, `?123`)
		AssertScan(t, `:foo_bar123'`, query.BIND, `:foo_bar123`)
		AssertScan(t, `@bar'`, query.BIND, `@bar`)
		AssertScan(t, `$baz'`, query.BIND, `$baz`)
	})

	t.Run("EOF", func(t *testing.T) {
		AssertScan(t, " \n\t\r", query.EOF, ``)
	})

	t.Run("SEMI", func(t *testing.T) {
		AssertScan(t, ";", query.SEMI, ";")
	})
	t.Run("LP", func(t *testing.T) {
		AssertScan(t, "(", query.LP, "(")
	})
	t.Run("RP", func(t *testing.T) {
		AssertScan(t, ")", query.RP, ")")
	})
	t.Run("COMMA", func(t *testing.T) {
		AssertScan(t, ",", query.COMMA, ",")
	})
	t.Run("NE", func(t *testing.T) {
		AssertScan(t, "!=", query.NE, "!=")
		AssertScan(t, "<>", query.NE, "<>")
	})
	t.Run("BITNOT", func(t *testing.T) {
		AssertScan(t, "~", query.BITNOT, "~")
	})
	t.Run("EQ", func(t *testing.T) {
		AssertScan(t, "=", query.EQ, "=")
		AssertScan(t, "==", query.EQ, "==")
	})
	t.Run("LE", func(t *testing.T) {
		AssertScan(t, "<=", query.LE, "<=")
	})
	t.Run("LSHIFT", func(t *testing.T) {
		AssertScan(t, "<<", query.LSHIFT, "<<")
	})
	t.Run("LT", func(t *testing.T) {
		AssertScan(t, "<", query.LT, "<")
	})
	t.Run("GE", func(t *testing.T) {
		AssertScan(t, ">=", query.GE, ">=")
	})
	t.Run("RSHIFT", func(t *testing.T) {
		AssertScan(t, ">>", query.RSHIFT, ">>")
	})
	t.Run("GT", func(t *testing.T) {
		AssertScan(t, ">", query.GT, ">")
	})
	t.Run("BITAND", func(t *testing.T) {
		AssertScan(t, "&", query.BITAND, "&")
	})
	t.Run("CONCAT", func(t *testing.T) {
		AssertScan(t, "||", query.CONCAT, "||")
	})
	t.Run("BITOR", func(t *testing.T) {
		AssertScan(t, "|", query.BITOR, "|")
	})
	t.Run("PLUS", func(t *testing.T) {
		AssertScan(t, "+", query.PLUS, "+")
	})
	t.Run("MINUS", func(t *testing.T) {
		AssertScan(t, "-", query.MINUS, "-")
	})
	t.Run("STAR", func(t *testing.T) {
		AssertScan(t, "*", query.STAR, "*")
	})
	t.Run("SLASH", func(t *testing.T) {
		AssertScan(t, "/", query.SLASH, "/")
	})
	t.Run("REM", func(t *testing.T) {
		AssertScan(t, "%", query.REM, "%")
	})
	t.Run("DOT", func(t *testing.T) {
		AssertScan(t, ".", query.DOT, ".")
		AssertScan(t, `.E2`, query.DOT, `.`)
	})
	t.Run("JSON_EXTRACT_JSON", func(t *testing.T) {
		AssertScan(t, "->", query.JSON_EXTRACT_JSON, "->")
	})
	t.Run("JSON_EXTRACT_SQL", func(t *testing.T) {
		AssertScan(t, "->>", query.JSON_EXTRACT_SQL, "->>")
	})
	t.Run("ILLEGAL", func(t *testing.T) {
		AssertScan(t, "^", query.ILLEGAL, "^")
	})
}

// AssertScan asserts the value of the first scan to s.
func AssertScan(tb testing.TB, s string, expectedTok query.Token, expectedLit string) {
	tb.Helper()
	_, tok, lit := query.NewScanner(strings.NewReader(s)).Scan()
	assert.Equal(tb, expectedLit, lit)
	assert.Equal(tb, expectedTok, tok)
}

func Benchmark_NewScanner(b *testing.B) {
	s := `SELECT * FROM foo WHERE bar = 1`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		scanner := query.NewScanner(strings.NewReader(s))
		for {
			_, tok, lit := scanner.Scan()
			if tok == query.EOF {
				break
			}
			if tok == query.ILLEGAL {
				b.Fatalf("Unexpected ILLEGAL token: %s", lit)
			}
		}
	}
}
