package dsqllint

import (
	"regexp"
	"strings"
)

// The patterns here say what text starts a SQL statement. They cover every
// command of PostgreSQL's SQL command reference, not only the ones Forma
// sends today, because a generator of any command DSQL refuses (DO, CALL,
// a role GRANT, VACUUM, LISTEN, ...) would otherwise reach a classified
// executor unclassified itself (#740 review R8-F1). TestEveryPostgresCommand
// holds one example of each command to them, so a family cannot drop out
// unnoticed.
//
// A command whose keyword is also an English word (DO, CALL, TABLE, CLOSE,
// FETCH, END, ...) only counts in a shape prose does not take: followed by
// the punctuation or keyword its syntax requires, or as the whole text.

// cteHead is the head of a WITH query up to its AS: a bare or double-quoted
// name, optionally followed by a column list (WITH ids(id) AS ...), which
// the head may hold as a whole or, read from fragments, as a dynamic piece
// (#740 review R4-F1). A bare name with no column list must be followed by
// space, so prose such as "with alias" is not a head. What follows AS
// ([NOT] MATERIALIZED, the query) is not part of the head.
const cteHead = `(?:\w+\s+|"(?:[^"]|"")+"\s*|(?:\w+|"(?:[^"]|"")+")\s*\([^()]*\)\s*)AS\b`

// ddlObject is the kind of object a CREATE, ALTER, DROP or COMMENT ON
// statement names, every kind PostgreSQL has, so a statement about one DSQL
// does not support (CREATE MATERIALIZED VIEW, DROP EXTENSION) is found as
// surely as one about a table (#740 review R7-F1).
const ddlObject = `(?:TABLE|INDEX|SEQUENCE|(?:MATERIALIZED\s+)?VIEW|SCHEMA|EXTENSION|TYPE|DOMAIN|` +
	`FUNCTION|PROCEDURE|ROUTINE|AGGREGATE|(?:EVENT\s+)?TRIGGER|RULE|POLICY|ROLE|USER(?:\s+MAPPING)?|GROUP|` +
	`DATABASE|TABLESPACE|LANGUAGE|CAST|COLLATION|CONVERSION|OPERATOR|STATISTICS|PUBLICATION|SUBSCRIPTION|` +
	`SERVER|FOREIGN\s+(?:TABLE|DATA\s+WRAPPER)|TEXT\s+SEARCH|ACCESS\s+METHOD|TRANSFORM|LARGE\s+OBJECT)\b`

// createModifier is a word a CREATE statement may put before the kind of
// object it creates, in any number and order: CREATE UNLOGGED TABLE, CREATE
// GLOBAL TEMPORARY TABLE, CREATE OR REPLACE TRUSTED PROCEDURAL LANGUAGE,
// CREATE CONSTRAINT TRIGGER, CREATE DEFAULT CONVERSION (#740 review
// R7-F1). DSQL refuses several of them, so a generator of one must reach
// the inventory, not slip past it on its modifier.
const createModifier = `(?:OR\s+REPLACE|GLOBAL|LOCAL|TEMP(?:ORARY)?|UNLOGGED|UNIQUE|RECURSIVE|CONSTRAINT|DEFAULT|TRUSTED|PROCEDURAL)`

// sqlName is a name in a statement head: a bare, dotted or double-quoted
// name, a fmt verb, or dynamicText.
const sqlName = `[\w$.%"]+`

// sqlEnd is the end of a statement that must be the whole text.
const sqlEnd = `\s*;?\s*$`

// optParens is an optional parenthesised option or column list.
const optParens = `(?:\s*\([^()]*\))?`

// statementHeads are the alternatives of sqlStatement, by family.
var statementHeads = []string{
	// Definitions: CREATE, ALTER, DROP and the other DDL commands.
	`CREATE\s+(?:` + createModifier + `\s+)*` + ddlObject,
	`ALTER\s+(?:` + ddlObject + `|SYSTEM\b|DEFAULT\s+PRIVILEGES\b)`,
	`DROP\s+(?:` + ddlObject + `|OWNED\b)`,
	`REFRESH\s+MATERIALIZED\s+VIEW\b`,
	`COMMENT\s+ON\s+(?:COLUMN\b|CONSTRAINT\b|` + ddlObject + `)`,
	`SECURITY\s+LABEL\b`, `IMPORT\s+FOREIGN\s+SCHEMA\b`, `REASSIGN\s+OWNED\b`,
	// Privileges, on objects (GRANT SELECT, INSERT ON t TO r) and of roles
	// (GRANT app_role TO app_user, REVOKE ADMIN OPTION FOR r FROM u).
	`GRANT\s+[\w\s,()"%]+?\s(?:ON|TO)\b`,
	`REVOKE\s+[\w\s,()"%]+?\s(?:ON|FROM)\b`,
	// Queries and data changes.
	`MERGE\s+INTO\s+\S+\s+(?:(?:AS\s+)?\w+\s+)?USING\b`,
	`INSERT\s+INTO\b`,
	`UPDATE\s+\S+\s+(?:(?:AS\s+)?\w+\s+)?SET\b`,
	`DELETE\s+FROM\b`,
	`SELECT\s`,
	`WITH\s+(?:RECURSIVE\s+)?` + cteHead,
	`VALUES\s*\(`,
	`TABLE\s+(?:ONLY\s+)?` + sqlName + `\s*\*?\s*(?:;|\)|$|(?:UNION|INTERSECT|EXCEPT|ORDER|LIMIT|OFFSET|FETCH|FOR|WINDOW)\b)`,
	`TRUNCATE\b`,
	`LOCK\s+(?:TABLE\b|(?:ONLY\s+)?` + sqlName + `\s*\*?\s*(?:,\s*` + sqlName + `\s*)*(?:IN\b|NOWAIT\b|;|$))`,
	`COPY\s*[\w(]`,
	// Routines and anonymous blocks.
	`CALL\s+` + sqlName + `\s*\(`,
	`DO\s+(?:LANGUAGE\s+\S+\s+)?(?:\$|'|E')`,
	// EXPLAIN of a statement, a dynamic one included.
	`EXPLAIN\s*(?:\(|(?:(?:ANALY[SZ]E|VERBOSE)\s+)*(?:(?:SELECT|INSERT|UPDATE|DELETE|MERGE|VALUES|WITH|TABLE|EXECUTE|DECLARE|CREATE)\b|(?:_|%[sv])` + sqlEnd + `))`,
	// Prepared statements and cursors.
	`PREPARE\s+` + sqlName + optParens + `\s*AS\b`,
	`PREPARE\s+TRANSACTION\b`,
	`EXECUTE\s+` + sqlName + `\s*(?:\(|;?\s*$)`,
	`DEALLOCATE\s+(?:PREPARE\s+)?` + sqlName + sqlEnd,
	`DECLARE\s+` + sqlName + `\s+(?:(?:BINARY|ASENSITIVE|INSENSITIVE|NO\s+SCROLL|SCROLL)\s+)*CURSOR\b`,
	`(?:FETCH|MOVE)\s+(?:NEXT|PRIOR|FIRST|LAST|ABSOLUTE|RELATIVE|ALL|FORWARD|BACKWARD|[-+]?\s*\d+)\b`,
	`(?:FETCH|MOVE)\s+(?:(?:FROM|IN)\s+)?` + sqlName + sqlEnd,
	`CLOSE\s+` + sqlName + sqlEnd,
	// Maintenance.
	`(?:VACUUM|ANALY[SZ]E)\b` + optParens + `(?:\s+(?:FULL|FREEZE|VERBOSE|ANALY[SZ]E|SKIP_LOCKED)\b)*` +
		`(?:\s+` + sqlName + optParens + `(?:\s*,\s*` + sqlName + optParens + `)*)?` + sqlEnd,
	`CLUSTER\b` + optParens + `(?:\s+VERBOSE)?(?:\s+` + sqlName + `(?:\s+USING\s+` + sqlName + `)?)?` + sqlEnd,
	`REINDEX\b` + optParens + `\s*(?:INDEX|TABLE|SCHEMA|DATABASE|SYSTEM)\b`,
	`CHECKPOINT` + sqlEnd,
	`LOAD\s+(?:'|E')`,
	// Notifications.
	`(?:LISTEN|UNLISTEN)\s+(?:\*|` + sqlName + `)` + sqlEnd,
	`NOTIFY\s+` + sqlName + `\s*(?:,|;|$)`,
	// Configuration, in every form, not only SET TRANSACTION, SET SESSION
	// and SET LOCAL: a bare SET name = value or SET name TO value, SET TIME
	// ZONE, SET ROLE, and a whole RESET, SHOW or DISCARD statement (#740
	// review R6-F1), since DSQL rejects or restricts them.
	`SET\s+(?:TRANSACTION|SESSION|LOCAL|TIME\s+ZONE|ROLE|SCHEMA|NAMES|CONSTRAINTS)\b`, `SET\s+\S+\s*(?:=|TO\b)`,
	`RESET\s+(?:ALL|ROLE|SESSION\s+AUTHORIZATION|[\w$.]+)` + sqlEnd,
	`SHOW\s+(?:ALL|TRANSACTION\s+ISOLATION\s+LEVEL|TIME\s+ZONE|[\w$.]+)` + sqlEnd,
	`DISCARD\s+(?:ALL|PLANS|SEQUENCES|TEMP(?:ORARY)?)` + sqlEnd,
	// Transaction control other than BEGIN and START TRANSACTION
	// (beginStatement).
	`(?:COMMIT|ROLLBACK|ABORT|END)(?:\s+(?:WORK|TRANSACTION))?(?:\s+AND\s+(?:NO\s+)?CHAIN)?` + sqlEnd,
	`(?:COMMIT|ROLLBACK)\s+PREPARED\b`,
	`ROLLBACK(?:\s+(?:WORK|TRANSACTION))?\s+TO\s+(?:SAVEPOINT\s+)?` + sqlName + sqlEnd,
	`RELEASE\s+(?:SAVEPOINT\s+)?` + sqlName + sqlEnd,
	`SAVEPOINT\s+` + sqlName + sqlEnd,
}

// sqlStatement matches text that starts a SQL statement, in any letter
// case. LooksLikeSQL also reads it in the token form sqlTokens writes, so
// a keyword may run into punctuation (SELECT(1), SELECT*FROM t,
// UPDATE"t"SET a = 1) or a comment the way it runs into a space (#740
// review R5-F1). The statement may sit inside parentheses: (SELECT 1)
// UNION (SELECT 2). A leading BEGIN or START TRANSACTION only counts as
// the whole text (beginStatement), so error text such as "begin tx: ..."
// is not SQL.
var sqlStatement = regexp.MustCompile(`(?i)^(?:\(\s*)*(?:` + strings.Join(statementHeads, "|") + `)`)

var beginStatement = regexp.MustCompile(`(?i)^(?:BEGIN|START\s+TRANSACTION)(?:\s+(?:TRANSACTION|WORK))?` +
	`(?:[\s,]+(?:ISOLATION\s+LEVEL\s+(?:SERIALIZABLE|REPEATABLE\s+READ|READ\s+COMMITTED|READ\s+UNCOMMITTED)|READ\s+(?:ONLY|WRITE)|(?:NOT\s+)?DEFERRABLE))*\s*;?\s*$`)
