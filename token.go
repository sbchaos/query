package query

import (
	"strconv"
	"strings"
)

var (
	keywords      = make(map[string]Token)
	bareTokensMap = make(map[Token]struct{})
)

func init() {
	for i := keyword_beg + 1; i < keyword_end; i++ {
		keywords[tokens[i]] = i
	}
	keywords[tokens[NULL]] = NULL
	keywords[tokens[TRUE]] = TRUE
	keywords[tokens[FALSE]] = FALSE

	for _, tok := range bareTokens {
		bareTokensMap[tok] = struct{}{}
	}
}

// Token is the set of lexical tokens of the Go programming language.
type Token int

// The list of tokens.
const (
	// Special tokens
	ILLEGAL Token = iota
	EOF
	COMMENT
	SPACE

	literal_beg
	IDENT   // IDENT
	QIDENT  // "IDENT"
	STRING  // 'string'
	TSTRING // `string`
	RAWSTR  // r'string'
	FLOAT   // 123.45
	INTEGER // 123
	NULL    // NULL
	TRUE    // true
	FALSE   // false
	BIND    // @VVV
	TMPL    // {{ Content }}
	literal_end

	operator_beg
	SEMI   // ;
	LP     // (
	RP     // )
	LSB    // [
	RSB    // ]
	COMMA  // ,
	NE     // !=
	EQ     // =
	LE     // <=
	LT     // <
	GT     // >
	GE     // >=
	EQN    // <=>
	BITAND // &
	BITOR  // |
	BITNOT // ~
	LSHIFT // <<
	RSHIFT // >>
	PLUS   // +
	MINUS  // -
	STAR   // *
	SLASH  // /
	REM    // %
	CONCAT // ||
	DOT    // .
	ASSIGN // :=

	JSON_EXTRACT_JSON // ->
	JSON_EXTRACT_SQL  // ->>
	operator_end

	keyword_beg
	ALL
	AND
	AS
	ASC
	BEGIN
	BETWEEN
	BY
	CASE
	CAST
	COLLATE
	CONFLICT
	CREATE
	CROSS
	CURRENT_TIME
	CURRENT_DATE
	CURRENT_TIMESTAMP
	DATE
	DELETE
	DESC
	DISTINCT
	DO
	DROP
	ELSE
	END
	EXCEPT
	EXISTS
	FIRST
	FROM
	FULL
	FUNCTION
	GLOB
	GROUP
	GROUPING
	HAVING
	IF
	IN
	INNER
	INSERT
	INTERSECT
	INTERVAL
	INTO
	IS
	ISNOT
	ISNULL // TODO: REMOVE?
	JOIN
	LATERAL
	LAST
	LEFT
	LIKE
	LIMIT
	MATCH
	MATCHED
	MERGE
	NATURAL
	NOT
	NOTBETWEEN
	NOTGLOB
	NOTHING
	NOTIN
	NOTLIKE
	NOTMATCH
	NOTNULL
	NOTREGEXP
	NULLS
	OFFSET
	ON
	OR
	ORDER
	OUTER
	OVER
	OVERWRITE
	PARTITION
	QUALIFY
	RECURSIVE
	REGEXP
	REPLACE
	RETURNS
	RETURNING
	RIGHT
	RLIKE
	ROWID
	SELECT
	SET
	SETS
	TABLE
	THEN
	TIMESTAMP
	TRUNCATE
	UNION
	UPDATE
	USING
	VALUES
	VIEW
	WHEN
	WHERE
	WINDOW
	WITH
	WITHIN
	keyword_end

	token_end
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	COMMENT: "COMMENT",
	SPACE:   "SPACE",

	IDENT:   "IDENT",
	QIDENT:  "QIDENT",
	STRING:  "STRING",
	RAWSTR:  "RAWSTR",
	FLOAT:   "FLOAT",
	INTEGER: "INTEGER",
	NULL:    "NULL",
	TRUE:    "TRUE",
	FALSE:   "FALSE",
	BIND:    "BIND",

	SEMI:   ";",
	LP:     "(",
	RP:     ")",
	LSB:    "[",
	RSB:    "]",
	COMMA:  ",",
	NE:     "!=",
	EQ:     "=",
	LE:     "<=",
	LT:     "<",
	GT:     ">",
	GE:     ">=",
	EQN:    "<=>",
	BITAND: "&",
	BITOR:  "|",
	BITNOT: "~",
	LSHIFT: "<<",
	RSHIFT: ">>",
	PLUS:   "+",
	MINUS:  "-",
	STAR:   "*",
	SLASH:  "/",
	REM:    "%",
	CONCAT: "||",
	DOT:    ".",
	ASSIGN: ":=",

	ALL:               "ALL",
	AND:               "AND",
	AS:                "AS",
	ASC:               "ASC",
	BEGIN:             "BEGIN",
	BETWEEN:           "BETWEEN",
	BY:                "BY",
	CASE:              "CASE",
	CAST:              "CAST",
	COLLATE:           "COLLATE",
	CONFLICT:          "CONFLICT",
	CREATE:            "CREATE",
	CROSS:             "CROSS",
	CURRENT_TIME:      "CURRENT_TIME",
	CURRENT_DATE:      "CURRENT_DATE",
	CURRENT_TIMESTAMP: "CURRENT_TIMESTAMP",
	DATE:              "DATE",
	DELETE:            "DELETE",
	DESC:              "DESC",
	DISTINCT:          "DISTINCT",
	DO:                "DO",
	DROP:              "DROP",
	ELSE:              "ELSE",
	END:               "END",
	EXCEPT:            "EXCEPT",
	EXISTS:            "EXISTS",
	FIRST:             "FIRST",
	FROM:              "FROM",
	FULL:              "FULL",
	FUNCTION:          "FUNCTION",
	GLOB:              "GLOB",
	GROUP:             "GROUP",
	GROUPING:          "GROUPING",
	HAVING:            "HAVING",
	IF:                "IF",
	IN:                "IN",
	INNER:             "INNER",
	INSERT:            "INSERT",
	INTERSECT:         "INTERSECT",
	INTERVAL:          "INTERVAL",
	INTO:              "INTO",
	IS:                "IS",
	ISNOT:             "ISNOT",
	ISNULL:            "ISNULL",
	JOIN:              "JOIN",
	LATERAL:           "LATERAL",
	LAST:              "LAST",
	LEFT:              "LEFT",
	LIKE:              "LIKE",
	LIMIT:             "LIMIT",
	MATCH:             "MATCH",
	MATCHED:           "MATCHED",
	MERGE:             "MERGE",
	NATURAL:           "NATURAL",
	NOT:               "NOT",
	NOTBETWEEN:        "NOTBETWEEN",
	NOTGLOB:           "NOTGLOB",
	NOTHING:           "NOTHING",
	NOTIN:             "NOTIN",
	NOTLIKE:           "NOTLIKE",
	NOTMATCH:          "NOTMATCH",
	NOTNULL:           "NOTNULL",
	NOTREGEXP:         "NOTREGEXP",
	NULLS:             "NULLS",
	OFFSET:            "OFFSET",
	ON:                "ON",
	OR:                "OR",
	ORDER:             "ORDER",
	OUTER:             "OUTER",
	OVER:              "OVER",
	OVERWRITE:         "OVERWRITE",
	PARTITION:         "PARTITION",
	QUALIFY:           "QUALIFY",
	RECURSIVE:         "RECURSIVE",
	REGEXP:            "REGEXP",
	REPLACE:           "REPLACE",
	RETURNS:           "RETURNS",
	RETURNING:         "RETURNING",
	RIGHT:             "RIGHT",
	RLIKE:             "RLIKE",
	ROWID:             "ROWID",
	SELECT:            "SELECT",
	SET:               "SET",
	SETS:              "SETS",
	TABLE:             "TABLE",
	THEN:              "THEN",
	TIMESTAMP:         "TIMESTAMP",
	TRUNCATE:          "TRUNCATE",
	UNION:             "UNION",
	UPDATE:            "UPDATE",
	USING:             "USING",
	VALUES:            "VALUES",
	VIEW:              "VIEW",
	WHEN:              "WHEN",
	WHERE:             "WHERE",
	WINDOW:            "WINDOW",
	WITH:              "WITH",
	WITHIN:            "WITHIN",
}

// A list of keywords that can be used as unquoted identifiers.
var bareTokens = [...]Token{
	ASC, BY, CAST, CONFLICT, CROSS, CURRENT_DATE, CURRENT_TIME,
	CURRENT_TIMESTAMP, DATE, DESC, DO, END, FIRST, FULL, GLOB, IF, INNER, INTEGER,
	LAST, LEFT, LIKE, MATCH, NATURAL, NULLS, OFFSET, OUTER, OVER,
	PARTITION, RECURSIVE, REGEXP, REPLACE, TIMESTAMP, VIEW, WINDOW, WITH,
}

func (t Token) String() string {
	s := ""
	if 0 <= t && t < Token(len(tokens)) {
		s = tokens[t]
	}
	if s == "" {
		s = "token(" + strconv.Itoa(int(t)) + ")"
	}
	return s
}

func Lookup(ident string) Token {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return IDENT
}

// isBareToken returns true if keyword token can be used as an identifier.
func isBareToken(tok Token) bool {
	_, ok := bareTokensMap[tok]
	return ok
}

func (t Token) IsLiteral() bool {
	return t > literal_beg && t < literal_end
}

func (t Token) IsBinaryOp() bool {
	switch t {
	case PLUS, MINUS, STAR, SLASH, REM, CONCAT, NOT, BETWEEN,
		LSHIFT, RSHIFT, BITAND, BITOR, LT, LE, GT, GE, EQ, NE, EQN,
		IS, IN, LIKE, GLOB, MATCH, REGEXP, AND, OR,
		JSON_EXTRACT_JSON, JSON_EXTRACT_SQL:
		return true
	default:
		return false
	}
}

func isAllowedIdent(tok Token) bool {
	return isIdentToken(tok) || isBareToken(tok)
}

func isIdentToken(tok Token) bool {
	return tok == IDENT || tok == QIDENT || tok == TSTRING || tok == BIND || tok == TMPL
}

// isExprIdentToken returns true if tok can be used as an identifier in an expression.
// It includes IDENT, QIDENT, and certain keywords.
func isExprIdentToken(tok Token) bool {
	switch tok {
	case IDENT, QIDENT, TSTRING, BIND:
		return true
	// List keywords that can be used as identifiers in expressions
	case ROWID, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP:
		return true
	// Special Cases
	case GROUPING, DATE, TIMESTAMP, LEFT, RIGHT:
		return true
	// Core functions
	case REPLACE, LIKE, GLOB, IF:
		return true
	// Add any other non-reserved keywords here
	default:
		return false
	}
}

func isTypeToken(lit string) bool {
	l1 := strings.ToUpper(lit)
	switch l1 {
	case "BIGINT", "BINARY", "BOOLEAN", "CHARACTER", "CLOB", "DATE", "DATETIME",
		"DECIMAL", "DOUBLE", "FLOAT", "INT", "INT64", "INTEGER", "NCHAR", "NULL", "UUID",
		"NUMERIC", "NVARCHAR", "REAL", "SMALLINT", "STRING", "TEXT", "TINYINT", "TIMESTAMP", "TIMESTAMP_NTZ", "VARCHAR":
		return true
	default:
		return false
	}
}

const (
	LowestPrec  = 0 // non-operators
	UnaryPrec   = 13
	HighestPrec = 14
)

func (t Token) Precedence() int {
	switch t {
	case OR:
		return 1
	case AND:
		return 2
	case NOT:
		return 3
	case IS, MATCH, LIKE, GLOB, REGEXP, BETWEEN, IN, ISNULL, NOTNULL, NE, EQ, RLIKE:
		return 4
	case GT, LE, LT, GE, EQN:
		return 5
	//case ESCAPE:
	//	return 6
	case BITAND, BITOR, LSHIFT, RSHIFT:
		return 7
	case PLUS, MINUS:
		return 8
	case STAR, SLASH, REM:
		return 9
	case CONCAT, JSON_EXTRACT_JSON, JSON_EXTRACT_SQL, LSB:
		return 10
	case BITNOT:
		return 11
	default:
		return LowestPrec
	}
}
