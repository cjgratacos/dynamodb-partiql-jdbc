package org.cjgratacos.jdbc;

/**
 * Constants class containing all supported PartiQL keywords, functions, and syntax elements for
 * DynamoDB. This class provides a comprehensive reference for PartiQL syntax that can be used for
 * type hinting, autocomplete, and syntax highlighting in IDEs and tools.
 *
 * <p>DynamoDB's PartiQL implementation supports a subset of the full PartiQL specification. This
 * class documents what is specifically supported by DynamoDB.
 *
 * <h2>Usage Examples:</h2>
 *
 * <pre>{@code
 * // Using keywords for query building
 * String query = PartiQLKeywords.SELECT + " * " +
 *                PartiQLKeywords.FROM + " \"users\" " +
 *                PartiQLKeywords.WHERE + " id = ?";
 *
 * // Checking if a word is a reserved keyword
 * boolean isReserved = PartiQLKeywords.isReservedKeyword("year");
 * }</pre>
 *
 * @author CJ Gratacos
 * @version 1.0
 * @since 1.0
 */
public final class PartiQLKeywords {

  /** Private constructor to prevent instantiation. */
  private PartiQLKeywords() {
    throw new UnsupportedOperationException("Constants class should not be instantiated");
  }

  // ==================== DML STATEMENTS ====================

  /** SELECT statement for querying data from tables. */
  public static final String SELECT = "SELECT";

  /** INSERT statement for adding new items to tables. */
  public static final String INSERT = "INSERT";

  /** UPDATE statement for modifying existing items. */
  public static final String UPDATE = "UPDATE";

  /** DELETE statement for removing items from tables. */
  public static final String DELETE = "DELETE";

  // ==================== QUERY CLAUSES ====================

  /** FROM clause to specify the table or index to query. */
  public static final String FROM = "FROM";

  /** WHERE clause for filtering results. */
  public static final String WHERE = "WHERE";

  /** INTO clause used with INSERT statements. */
  public static final String INTO = "INTO";

  /** VALUES clause used with INSERT statements. */
  public static final String VALUES = "VALUES";

  /** SET clause used with UPDATE statements. */
  public static final String SET = "SET";

  /** RETURNING clause to return values after DML operations. */
  public static final String RETURNING = "RETURNING";

  // ==================== QUERY MODIFIERS ====================

  /** LIMIT clause to restrict the number of results (via ExecuteStatement API). */
  public static final String LIMIT = "LIMIT";

  /** ORDER BY clause for sorting results (requires partition key in WHERE). */
  public static final String ORDER_BY = "ORDER BY";

  /** ASC keyword for ascending order. */
  public static final String ASC = "ASC";

  /** DESC keyword for descending order. */
  public static final String DESC = "DESC";

  // ==================== LOGICAL OPERATORS ====================

  /** AND logical operator. */
  public static final String AND = "AND";

  /** OR logical operator. */
  public static final String OR = "OR";

  /** NOT logical operator. */
  public static final String NOT = "NOT";

  /** IN operator for matching against a list of values. */
  public static final String IN = "IN";

  /** BETWEEN operator for range queries. */
  public static final String BETWEEN = "BETWEEN";

  /** LIKE operator for pattern matching. */
  public static final String LIKE = "LIKE";

  /** EXISTS operator for checking existence. */
  public static final String EXISTS = "EXISTS";

  // ==================== COMPARISON OPERATORS ====================

  /** Equals operator. */
  public static final String EQ = "=";

  /** Not equals operator. */
  public static final String NE = "<>";

  /** Less than operator. */
  public static final String LT = "<";

  /** Less than or equal operator. */
  public static final String LE = "<=";

  /** Greater than operator. */
  public static final String GT = ">";

  /** Greater than or equal operator. */
  public static final String GE = ">=";

  // ==================== SPECIAL VALUES ====================

  /** NULL value keyword. */
  public static final String NULL = "NULL";

  /** TRUE boolean value. */
  public static final String TRUE = "TRUE";

  /** FALSE boolean value. */
  public static final String FALSE = "FALSE";

  /** MISSING value for absent attributes. */
  public static final String MISSING = "MISSING";

  // ==================== DATA TYPES ====================

  /** String data type. */
  public static final String STRING = "STRING";

  /** Number data type. */
  public static final String NUMBER = "NUMBER";

  /** Binary data type. */
  public static final String BINARY = "BINARY";

  /** Boolean data type. */
  public static final String BOOL = "BOOL";

  /** List data type. */
  public static final String LIST = "LIST";

  /** Map data type. */
  public static final String MAP = "MAP";

  /** Set data type. */
  public static final String SET_TYPE = "SET";

  // ==================== FUNCTIONS ====================

  /** SIZE function to get the size of a collection. */
  public static final String SIZE = "size";

  /** ATTRIBUTE_EXISTS function to check if an attribute exists. */
  public static final String ATTRIBUTE_EXISTS = "attribute_exists";

  /** ATTRIBUTE_NOT_EXISTS function to check if an attribute doesn't exist. */
  public static final String ATTRIBUTE_NOT_EXISTS = "attribute_not_exists";

  /** ATTRIBUTE_TYPE function to get the type of an attribute. */
  public static final String ATTRIBUTE_TYPE = "attribute_type";

  /** BEGINS_WITH function for string prefix matching. */
  public static final String BEGINS_WITH = "begins_with";

  /** CONTAINS function to check if a value is contained. */
  public static final String CONTAINS = "contains";

  // ==================== RESERVED KEYWORDS ====================

  /**
   * Array of reserved keywords in DynamoDB PartiQL. These words cannot be used as unquoted
   * identifiers and must be enclosed in double quotes when used as table or attribute names.
   */
  public static final String[] RESERVED_KEYWORDS = {
    "ABORT",
    "ABSOLUTE",
    "ACTION",
    "ADD",
    "AFTER",
    "AGENT",
    "AGGREGATE",
    "ALL",
    "ALLOCATE",
    "ALTER",
    "ANALYZE",
    "AND",
    "ANY",
    "ARCHIVE",
    "ARE",
    "ARRAY",
    "AS",
    "ASC",
    "ASCII",
    "ASENSITIVE",
    "ASSERTION",
    "ASYMMETRIC",
    "AT",
    "ATOMIC",
    "ATTACH",
    "ATTRIBUTE",
    "AUTH",
    "AUTHORIZATION",
    "AUTHORIZE",
    "AUTO",
    "AVG",
    "BACK",
    "BACKUP",
    "BASE",
    "BATCH",
    "BEFORE",
    "BEGIN",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BIT",
    "BLOB",
    "BLOCK",
    "BOOLEAN",
    "BOTH",
    "BREADTH",
    "BUCKET",
    "BULK",
    "BY",
    "BYTE",
    "CALL",
    "CALLED",
    "CALLING",
    "CAPACITY",
    "CASCADE",
    "CASCADED",
    "CASE",
    "CAST",
    "CATALOG",
    "CHAR",
    "CHARACTER",
    "CHECK",
    "CLASS",
    "CLOB",
    "CLOSE",
    "CLUSTER",
    "CLUSTERED",
    "CLUSTERING",
    "CLUSTERS",
    "COALESCE",
    "COBOL",
    "COLLATE",
    "COLLATION",
    "COLLECTION",
    "COLUMN",
    "COLUMNS",
    "COMBINE",
    "COMMENT",
    "COMMIT",
    "COMPACT",
    "COMPILE",
    "COMPRESS",
    "CONDITION",
    "CONFLICT",
    "CONNECT",
    "CONNECTION",
    "CONSISTENCY",
    "CONSISTENT",
    "CONSTRAINT",
    "CONSTRAINTS",
    "CONSTRUCTOR",
    "CONSUMED",
    "CONTINUE",
    "CONVERT",
    "COPY",
    "CORRESPONDING",
    "COUNT",
    "COUNTER",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "CURSOR",
    "CYCLE",
    "DATA",
    "DATABASE",
    "DATE",
    "DATETIME",
    "DAY",
    "DEALLOCATE",
    "DEC",
    "DECIMAL",
    "DECLARE",
    "DEFAULT",
    "DEFERRABLE",
    "DEFERRED",
    "DEFINE",
    "DEFINED",
    "DEFINITION",
    "DELETE",
    "DELIMITED",
    "DEPTH",
    "DEREF",
    "DESC",
    "DESCRIBE",
    "DESCRIPTOR",
    "DETACH",
    "DETERMINISTIC",
    "DIAGNOSTICS",
    "DIRECTORIES",
    "DISABLE",
    "DISCONNECT",
    "DISTINCT",
    "DISTRIBUTE",
    "DO",
    "DOMAIN",
    "DOUBLE",
    "DROP",
    "DUMP",
    "DURATION",
    "DYNAMIC",
    "EACH",
    "ELEMENT",
    "ELSE",
    "ELSEIF",
    "EMPTY",
    "ENABLE",
    "END",
    "EQUAL",
    "EQUALS",
    "ERROR",
    "ESCAPE",
    "ESCAPED",
    "EVAL",
    "EVALUATE",
    "EXCEEDED",
    "EXCEPT",
    "EXCEPTION",
    "EXCEPTIONS",
    "EXCLUSIVE",
    "EXEC",
    "EXECUTE",
    "EXISTS",
    "EXIT",
    "EXPLAIN",
    "EXPLODE",
    "EXPORT",
    "EXPRESSION",
    "EXTENDED",
    "EXTERNAL",
    "EXTRACT",
    "FAIL",
    "FALSE",
    "FAMILY",
    "FETCH",
    "FILE",
    "FILTER",
    "FILTERING",
    "FINAL",
    "FINISH",
    "FIRST",
    "FIXED",
    "FLATTERN",
    "FLOAT",
    "FOR",
    "FORCE",
    "FOREIGN",
    "FORMAT",
    "FORWARD",
    "FOUND",
    "FREE",
    "FROM",
    "FULL",
    "FUNCTION",
    "FUNCTIONS",
    "GENERAL",
    "GENERATE",
    "GET",
    "GLOB",
    "GLOBAL",
    "GO",
    "GOTO",
    "GRANT",
    "GREATER",
    "GROUP",
    "GROUPING",
    "HANDLER",
    "HASH",
    "HAVE",
    "HAVING",
    "HEAP",
    "HIDDEN",
    "HOLD",
    "HOUR",
    "IDENTIFIED",
    "IDENTITY",
    "IF",
    "IGNORE",
    "IMMEDIATE",
    "IMPORT",
    "IN",
    "INCLUDING",
    "INCLUSIVE",
    "INCREMENT",
    "INCREMENTAL",
    "INDEX",
    "INDEXED",
    "INDEXES",
    "INDICATOR",
    "INFINITE",
    "INITIALLY",
    "INLINE",
    "INNER",
    "INNTER",
    "INOUT",
    "INPUT",
    "INSENSITIVE",
    "INSERT",
    "INSTEAD",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "INVALIDATE",
    "IS",
    "ISOLATION",
    "ITEM",
    "ITEMS",
    "ITERATE",
    "JOIN",
    "KEY",
    "KEYS",
    "LAG",
    "LANGUAGE",
    "LARGE",
    "LAST",
    "LATERAL",
    "LEAD",
    "LEADING",
    "LEAVE",
    "LEFT",
    "LENGTH",
    "LESS",
    "LEVEL",
    "LIKE",
    "LIMIT",
    "LIMITED",
    "LINES",
    "LIST",
    "LOAD",
    "LOCAL",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOCATION",
    "LOCATOR",
    "LOCK",
    "LOCKS",
    "LOG",
    "LOGED",
    "LONG",
    "LOOP",
    "LOWER",
    "MAP",
    "MATCH",
    "MATERIALIZED",
    "MAX",
    "MAXLEN",
    "MEMBER",
    "MERGE",
    "METHOD",
    "METRICS",
    "MIN",
    "MINUS",
    "MINUTE",
    "MISSING",
    "MOD",
    "MODE",
    "MODIFIES",
    "MODIFY",
    "MODULE",
    "MONTH",
    "MULTI",
    "MULTISET",
    "NAME",
    "NAMES",
    "NATIONAL",
    "NATURAL",
    "NCHAR",
    "NCLOB",
    "NEW",
    "NEXT",
    "NO",
    "NONE",
    "NOT",
    "NULL",
    "NULLIF",
    "NUMBER",
    "NUMERIC",
    "OBJECT",
    "OF",
    "OFFLINE",
    "OFFSET",
    "OLD",
    "ON",
    "ONLINE",
    "ONLY",
    "OPAQUE",
    "OPEN",
    "OPERATOR",
    "OPTION",
    "OR",
    "ORDER",
    "ORDINALITY",
    "OTHER",
    "OTHERS",
    "OUT",
    "OUTER",
    "OUTPUT",
    "OVER",
    "OVERLAPS",
    "OVERRIDE",
    "OWNER",
    "PAD",
    "PARALLEL",
    "PARAMETER",
    "PARAMETERS",
    "PARTIAL",
    "PARTITION",
    "PARTITIONED",
    "PARTITIONS",
    "PATH",
    "PERCENT",
    "PERCENTILE",
    "PERMISSION",
    "PERMISSIONS",
    "PIPE",
    "PIPELINED",
    "PLAN",
    "POOL",
    "POSITION",
    "PRECISION",
    "PREPARE",
    "PRESERVE",
    "PRIMARY",
    "PRIOR",
    "PRIVATE",
    "PRIVILEGES",
    "PROCEDURE",
    "PROCESSED",
    "PROJECT",
    "PROJECTION",
    "PROPERTY",
    "PROVISIONING",
    "PUBLIC",
    "PUT",
    "QUERY",
    "QUIT",
    "QUORUM",
    "RAISE",
    "RANDOM",
    "RANGE",
    "RANK",
    "RAW",
    "READ",
    "READS",
    "REAL",
    "REBUILD",
    "RECORD",
    "RECURSIVE",
    "REDUCE",
    "REF",
    "REFERENCE",
    "REFERENCES",
    "REFERENCING",
    "REGEXP",
    "REGION",
    "REINDEX",
    "RELATIVE",
    "RELEASE",
    "REMAINDER",
    "RENAME",
    "REPEAT",
    "REPLACE",
    "REQUEST",
    "RESET",
    "RESIGNAL",
    "RESOURCE",
    "RESPONSE",
    "RESTORE",
    "RESTRICT",
    "RESULT",
    "RETURN",
    "RETURNING",
    "RETURNS",
    "REVERSE",
    "REVOKE",
    "RIGHT",
    "ROLE",
    "ROLES",
    "ROLLBACK",
    "ROLLUP",
    "ROUTINE",
    "ROW",
    "ROWS",
    "RULE",
    "RULES",
    "SAMPLE",
    "SATISFIES",
    "SAVE",
    "SAVEPOINT",
    "SCAN",
    "SCHEMA",
    "SCOPE",
    "SCROLL",
    "SEARCH",
    "SECOND",
    "SECTION",
    "SEGMENT",
    "SEGMENTS",
    "SELECT",
    "SELF",
    "SEMI",
    "SENSITIVE",
    "SEPARATE",
    "SEQUENCE",
    "SERIALIZABLE",
    "SESSION",
    "SET",
    "SETS",
    "SHARD",
    "SHARE",
    "SHARED",
    "SHORT",
    "SHOW",
    "SIGNAL",
    "SIMILAR",
    "SIZE",
    "SKEWED",
    "SMALLINT",
    "SNAPSHOT",
    "SOME",
    "SOURCE",
    "SPACE",
    "SPACES",
    "SPARSE",
    "SPECIFIC",
    "SPECIFICTYPE",
    "SPLIT",
    "SQL",
    "SQLCODE",
    "SQLERROR",
    "SQLEXCEPTION",
    "SQLSTATE",
    "SQLWARNING",
    "START",
    "STATE",
    "STATIC",
    "STATUS",
    "STORAGE",
    "STORE",
    "STORED",
    "STREAM",
    "STRING",
    "STRUCT",
    "STYLE",
    "SUB",
    "SUBMULTISET",
    "SUBPARTITION",
    "SUBSTRING",
    "SUBTYPE",
    "SUM",
    "SUPER",
    "SYMMETRIC",
    "SYNONYM",
    "SYSTEM",
    "TABLE",
    "TABLESAMPLE",
    "TEMP",
    "TEMPORARY",
    "TERMINATED",
    "TEXT",
    "THAN",
    "THEN",
    "THROUGHPUT",
    "TIME",
    "TIMESTAMP",
    "TIMEZONE",
    "TINYINT",
    "TO",
    "TOKEN",
    "TOTAL",
    "TOUCH",
    "TRAILING",
    "TRANSACTION",
    "TRANSFORM",
    "TRANSLATE",
    "TRANSLATION",
    "TREAT",
    "TRIGGER",
    "TRIM",
    "TRUE",
    "TRUNCATE",
    "TTL",
    "TUPLE",
    "TYPE",
    "UNDER",
    "UNDO",
    "UNION",
    "UNIQUE",
    "UNIT",
    "UNKNOWN",
    "UNLOGGED",
    "UNNEST",
    "UNPROCESSED",
    "UNSIGNED",
    "UNTIL",
    "UPDATE",
    "UPPER",
    "URL",
    "USAGE",
    "USE",
    "USER",
    "USERS",
    "USING",
    "UUID",
    "VACUUM",
    "VALUE",
    "VALUED",
    "VALUES",
    "VARCHAR",
    "VARIABLE",
    "VARIANCE",
    "VARINT",
    "VARYING",
    "VIEW",
    "VIEWS",
    "VIRTUAL",
    "VOID",
    "WAIT",
    "WHEN",
    "WHENEVER",
    "WHERE",
    "WHILE",
    "WINDOW",
    "WITH",
    "WITHIN",
    "WITHOUT",
    "WORK",
    "WRAPPED",
    "WRITE",
    "YEAR",
    "ZONE"
  };

  /**
   * Checks if a given word is a reserved keyword in DynamoDB PartiQL.
   *
   * @param word the word to check
   * @return true if the word is reserved, false otherwise
   */
  public static boolean isReservedKeyword(String word) {
    if (word == null || word.isEmpty()) {
      return false;
    }
    String upperWord = word.toUpperCase();
    for (String keyword : RESERVED_KEYWORDS) {
      if (keyword.equals(upperWord)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Quotes an identifier if it's a reserved keyword or contains special characters. This is useful
   * for safely using table and attribute names in queries.
   *
   * @param identifier the identifier to potentially quote
   * @return the quoted identifier if necessary, otherwise the original identifier
   */
  public static String quoteIfNeeded(String identifier) {
    if (identifier == null || identifier.isEmpty()) {
      return identifier;
    }

    // Check if already quoted
    if (identifier.startsWith("\"") && identifier.endsWith("\"")) {
      return identifier;
    }

    // Check if it's a reserved keyword or contains special characters
    if (isReservedKeyword(identifier)
        || identifier.contains("-")
        || identifier.contains(".")
        || identifier.contains(" ")) {
      return "\"" + identifier + "\"";
    }

    return identifier;
  }

  // ==================== SYNTAX PATTERNS ====================

  /**
   * Pattern for a basic SELECT query.
   *
   * <pre>
   * SELECT * FROM "table" WHERE condition
   * </pre>
   */
  public static final String SELECT_PATTERN = "SELECT %s FROM %s WHERE %s";

  /**
   * Pattern for SELECT with index.
   *
   * <pre>
   * SELECT * FROM "table"."index" WHERE condition
   * </pre>
   */
  public static final String SELECT_INDEX_PATTERN = "SELECT %s FROM \"%s\".\"%s\" WHERE %s";

  /**
   * Pattern for INSERT statement.
   *
   * <pre>
   * INSERT INTO "table" VALUE {'attr1': 'value1', 'attr2': 'value2'}
   * </pre>
   */
  public static final String INSERT_PATTERN = "INSERT INTO %s VALUE %s";

  /**
   * Pattern for UPDATE statement.
   *
   * <pre>
   * UPDATE "table" SET attr1 = 'value1' WHERE condition
   * </pre>
   */
  public static final String UPDATE_PATTERN = "UPDATE %s SET %s WHERE %s";

  /**
   * Pattern for DELETE statement.
   *
   * <pre>
   * DELETE FROM "table" WHERE condition
   * </pre>
   */
  public static final String DELETE_PATTERN = "DELETE FROM %s WHERE %s";
}
