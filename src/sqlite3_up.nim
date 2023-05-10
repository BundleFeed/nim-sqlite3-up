# Copyright 2023 Geoffrey Picron.
# SPDX-License-Identifier: (MIT or Apache-2.0)


## A high level SQLite API with support for multi-threading, prepared statements,
## proper typing, zero-copy data paths, debugging, JSON, optimizing, backups, and more.
## 
## Example (using JSON extensions)
## ===============================
##
## .. code-block:: Nim
##  
##  # nim r --threads:on example
##  
##  import sqlite_up, threadpool
##  from strutils import find
##  from os import sleep
##  
##  const Schema = "CREATE TABLE IF NOT EXISTS Example(name TEXT NOT NULL, jsondata TEXT NOT NULL)"
##  
##  type SqlStatements = enum
##    Insert = """INSERT INTO Example (name, jsondata)
##    VALUES (json_extract(?, '$.name'), json_extract(?, '$.data'))"""
##    Count = "SELECT count(*) FROM Example"
##    Select = "SELECT json_extract(jsondata, '$.array') FROM Example"
##  
##  let httprequest = """header BODY:{"name":"Alice", "data":{"info":"xxx", "array":["a","b","c"]}}"""
##  
##  var
##    db: SQLiteDb
##    prepared {.threadvar.}: bool
##    ready: int
##  
##  when not defined(release): db.setLogger(proc(db: SQLiteDb, msg: string, code: int) = echo msg)
##  
##  proc select() =
##    {.gcsafe.}:
##      if not prepared:
##        db.prepareStatements(SqlStatements)
##        prepared = true
##      for row in db.rows(Select):
##        stdout.write(row.getCString(0))
##        stdout.write('\n')
##      discard ready.atomicInc
##  
##  proc run() =
##    db.openDatabase("ex.db", Schema)
##    defer: db.close()
##    db.prepareStatements(SqlStatements)
##    let body = httprequest.toDb(httprequest.find("BODY:") + 5, httprequest.len - 1)
##    if not db.json_valid(body): quit(0)
##    
##    echo "inserting 10000 rows..."
##    db.transaction:
##      for i in 1 .. 10000: discard db.insert(Insert, body, body)
##    
##    echo "10000 rows inserted. Press <Enter> to select all in 4 threads..."
##    discard stdin.readChar()
##    for i in 1 .. 4: spawn(select())
##    while (ready < 4): sleep(20)
##    stdout.flushFile()
##    echo "Selected 4 * ", db.getTheInt(Count), " = " & $(4 * db.getTheInt(Count)) & " rows."
##  
##  run()
##  
## Compiling with sqlite3.c
## ========================
## 
## | First, sqlite3.c amalgamation must be on compiler search path.
## | You can extract it from a zip available at https://www.sqlite.org/download.html.
## | Then, `-d:staticSqlite`compiler option must be used.
## 
## For your convenience, `-d:staticSqlite` triggers some useful SQLite compiler options,
## consult sqliteral source code or `about()` proc for details.
## These can be turned off with `-d:disableSqliteoptions` option.
##

when compileOption("threads"): {.passL: "-lpthread".}
else: {.passC: "-DSQLITE_THREADSAFE=0".}

{.passC: "-DSQLITE_USE_URI=1 -DSQLITE_ENABLE_DBSTAT_VTAB=1".}

when not defined(disableSqliteoptions):
  {.passC: "-DSQLITE_DQS=0 -DSQLITE_OMIT_DEPRECATED -DSQLITE_OMIT_SHARED_CACHE -DSQLITE_LIKE_DOESNT_MATCH_BLOBS".}
  {.passC: "-DSQLITE_ENABLE_JSON1 -DSQLITE_DEFAULT_MEMSTATUS=0 -DSQLITE_OMIT_PROGRESS_CALLBACK".}
  when defined(danger):
    {.passC: "-DSQLITE_USE_ALLOCA -DSQLITE_MAX_EXPR_DEPTH=0".}

import sqlite3_abi

type
  PSqlite3* = ptr sqlite3
  PStmt* = ptr sqlite3_stmt
  PSqlite3_Backup = ptr sqlite3_backup
  

export PStmt, sqlite3_errmsg, sqlite3_reset, sqlite3_step, sqlite3_finalize, SQLITE_ROW
import locks
import std/options
import std/times

from os import getFileSize
from strutils import strip, split, contains, replace

const
  MaxThreadSize* {.intdefine.} = 32
  MaxStatements* {.intdefine.} = 100
    ## Compile time define pragma that limits amount of prepared statements
  
type
  SQLError* = ref object of CatchableError
    ## https://www.sqlite.org/rescode.html
    rescode*: int
  
  InternalStatements = enum
    Jsonextract = "SELECT json_extract(?,?)"
    Jsonpatch = "SELECT json_patch(?,?)"
    Jsonvalid = "SELECT json_valid(?)"
    Jsontree = "SELECT type, fullkey, value FROM json_tree(?)"

  SQLiteDb* = ref object
    sqlite*: PSqlite3
    dbname*: string
    inreadonlymode*: bool
    backupsinprogress*: int
    intransaction: bool
    walmode: bool 
    maxsize: int
    threadindices: array[MaxThreadSize, int]
    threadlen: int
    preparedstatements: array[MaxThreadSize, array[MaxStatements, PStmt]]
    laststatementindex: int
    internalstatements: array[MaxThreadSize, array[ord(Jsontree)+1, PStmt]]
    transactionlock: Lock
    loggerproc: proc(sqliteral: SQLiteDb, statement: string, errorcode: int) {.gcsafe, raises: [].}
    oncommitproc: proc(sqliteral: SQLiteDb) {.gcsafe, raises: [].}
    maxparamloggedlen: int
    Transaction: PStmt
    Commit: PStmt
    Rollback: PStmt

  DbValueKind = enum
    sqliteInteger,
    sqliteReal,
    sqliteText,
    sqliteBlob,
    sqliteNull
  
  DbValue* = object
    ## | Represents a value in a SQLite database.
    ## | https://www.sqlite.org/datatype3.html
    ## | NULL values are not possible to avoid the billion-dollar mistake.
    case kind*: DbValueKind
    of sqliteInteger:
      intVal*: int64
    of sqliteReal:
      floatVal*: float64
    of sqliteText:
      textVal*: tuple[chararray: cstring, len: int32]
    of sqliteBlob:
      blobVal*: seq[byte] # TODO: openArray[byte]
    of sqliteNull:
      discard

#----------------------------------------------------------

when compileOption("threads"):
  proc threadi*(db: SQLiteDb): int = # public only for technical reasons
   let id = getThreadId()
   for i in 0 ..< db.threadlen: (if db.threadindices[i] == id: return i)
   doAssert(false, "uninitialized thread - has prepareStatements been called on this thread?")
else:
  template threadi*(db: SQLiteDb): int = 0


template checkRc*(db: SQLiteDb | PSqlite3, resultcode: int) =
  ## | Raises SQLError if resultcode notin [SQLITE_OK, SQLITE_ROW, SQLITE_DONE]
  ## | https://www.sqlite.org/rescode.html
  let callResult = resultcode
  when db is SQLiteDb:
    if callResult notin [SQLITE_OK, SQLITE_ROW, SQLITE_DONE]:
      let errormsg = $sqlite3_errmsg(db.sqlite)
      if(unlikely) db.loggerproc != nil: db.loggerproc(db, errormsg, callResult)
      raise SQLError(msg: db.dbname & " (code: " & $callResult & ", msg:"  & " " & errormsg & ")", rescode: callResult)
  else:
    if callResult notin [SQLITE_OK, SQLITE_ROW, SQLITE_DONE]:
      let errormsg = $sqlite3_errmsg(db)
      raise SQLError(msg: "sqlite3 " & errormsg, rescode: callResult)

const nullDbValue* = DbValue(kind: sqliteNull)

proc toDb*(val: cstring, len = -1): DbValue {.inline.} =
  if len == -1: DbValue(kind: sqliteText, textVal: (val, int32(val.len())))
  else: DbValue(kind: sqliteText, textVal: (val, int32(len)))

proc toDb*(val: cstring, first, last: int): DbValue {.inline.} =
  DbValue(kind: sqliteText, textVal: (cast[cstring](unsafeAddr(val[first])), int32(1 + last - first)))

proc toDb*(val: string, len = -1): DbValue {.inline.} =
  if len == -1: DbValue(kind: sqliteText, textVal: (cstring(val), int32(val.len())))
  else: DbValue(kind: sqliteText, textVal: (cstring(val), int32(len)))

proc toDb*(val: string, first, last: int): DbValue {.inline.} =
  DbValue(kind: sqliteText, textVal: (cast[cstring](unsafeAddr(val[first])), int32(1 + last - first)))

proc toDb*(val: openArray[char], len = -1): DbValue {.inline.} =
  if len == -1: DbValue(kind: sqliteText, textVal: (cast[cstring](unsafeAddr val[0]), int32(val.len())))
  else: DbValue(kind: sqliteText, textVal: (cast[cstring](unsafeAddr val[0]), int32(len)))

proc toDb*[N; T: char | byte | uint8](val: array[N, T]): DbValue {.inline.} =
  var s = newSeqUninitialized[byte](sizeof(val))
  copyMem(addr s[0], unsafeAddr val[0], sizeof(val))
  DbValue(kind: sqliteBlob, blobVal: s)

proc toDb*(val: bool): DbValue {.inline.} = 
  if val: 
    DbValue(kind: sqliteInteger, intVal: 1) 
  else: 
    nullDbValue

proc toDb*[T: Ordinal](val: T): DbValue {.inline.} = DbValue(kind: sqliteInteger, intVal: val.int64)

proc toDb*[T: SomeFloat](val: T): DbValue {.inline.} = DbValue(kind: sqliteReal, floatVal: val.float64)

proc toDb*(val: seq[byte]): DbValue {.inline.} = DbValue(kind: sqliteBlob, blobVal: val)
  
proc toDb*[T: DbValue](val: T): DbValue {.inline.} = val

proc toDb*(val: Time): DbValue {.inline.} = DbValue(kind: sqliteReal, floatVal: val.toUnixFloat)

proc toDB*[T](n: Option[T]): DbValue =
  if n.isSome:
    toDb(n.get)
  else:
    nullDbValue


proc `$`*[T: DbValue](val: T): string {.inline.} = 
  case val.kind
  of sqliteInteger: $val.intval
  of sqliteReal: $val.floatVal
  of sqliteText: ($val.textVal.chararray)[0 .. val.textVal.len - 1]
  of sqliteBlob: cast[string](val.blobVal)
  of sqliteNull: "NULL"


proc bindParams*(sql: PStmt, params: varargs[DbValue]): int {.inline.} =
  var idx = 1.int32
  for value in params:
    result =
      case value.kind
      of sqliteInteger: sqlite3_bind_int64(sql, idx, value.intval)
      of sqliteReal: sqlite3_bind_double(sql, idx, value.floatVal)
      of sqliteText: sqlite3_bind_text(sql, idx, value.textVal.chararray, value.textVal.len, SQLITE_STATIC)
      of sqliteBlob: sqlite3_bind_blob(sql, idx.int32, cast[string](value.blobVal).cstring, value.blobVal.len.int32, SQLITE_STATIC)
      of sqliteNull: sqlite3_bind_null(sql, idx)
    if result != SQLITE_OK: return
    idx.inc


template log() =
  var logstring = $statement
  var replacement = 0
  while replacement < params.len:
    let position = logstring.find('?')
    if (position == -1):
      logstring = $params.len & "is too many params for: " & $statement
      replacement = params.len
      continue
    let param =
      if db.maxparamloggedlen < 1: $params[replacement]
      else: ($params[replacement]).substr(0, db.maxparamloggedlen - 1)
    logstring = logstring[0 .. position-1] & param & logstring.substr(position+1)
    replacement += 1
  if (logstring.find('?') != -1): logstring &= " (some params missing)"
  db.loggerproc(db, logstring, 0)


proc doLog*(db: SQLiteDb, statement: string, params: varargs[DbValue, toDb]) {.inline.} =
  if statement == "Pstmt rows" or statement == "exec Pstmt": db.loggerproc(db, statement & " " & $params, 0)
  else: log()

#-----------------------------------------------------------------------------------------------------------

proc getInt*(prepared: PStmt, col: int32 = 0): int64 {.inline.} =
  ## Returns value of INTEGER -type column at given column index
  return sqlite3_column_int64(prepared, col)

proc getBool*(prepared: PStmt, col: int32 = 0): bool {.inline.} =
  ## Returns value of INTEGER -type column at given column index
  return sqlite3_column_int64(prepared, col) != 0

proc getString*(prepared: PStmt, col: int32 = 0): string {.inline.} =
  ## Returns value of TEXT -type column at given column index as string
  return $sqlite3_column_text(prepared, col)


proc getCString*(prepared: PStmt, col: int32 = 0): cstring {.inline.} =
  ## | Returns value of TEXT -type column at given column index as cstring.
  ## | Zero-copy, but result is not available after cursor movement or statement reset.
  return sqlite3_column_text(prepared, col)


proc getFloat*(prepared: PStmt, col: int32 = 0): float64 {.inline.} =
  ## Returns value of REAL -type column at given column index
  return sqlite3_column_double(prepared, col)


proc getSeq*(prepared: PStmt, col: int32 = 0): seq[byte] {.inline.} =
  ## Returns value of BLOB -type column at given column index
  let blob = sqlite3_column_blob(prepared, col)
  let bytes = sqlite3_column_bytes(prepared, col)
  result = newSeq[byte](bytes)
  if bytes != 0: copyMem(addr(result[0]), blob, bytes)

proc getBytes*[N : int](prepared: PStmt, col: int32 = 0, output: var array[N, byte]) {.inline.} =
  ## Returns value of BLOB -type column at given column index
  let blob = sqlite3_column_blob(prepared, col)
  let bytes = sqlite3_column_bytes(prepared, col)
  if bytes != sizeof(output):
    raise newException(ValueError, "Array size does not match BLOB size")
  if bytes != 0: copyMem(addr(output[0]), blob, bytes)
  
proc isNull*(prepared: PStmt, col: int32 = 0): bool {.inline.} =
  ## Returns true if value of column at given column index is NULL
  return sqlite3_column_type(prepared, col) == SQLITE_NULL


proc getSeqOf*[T](prepared: PStmt, typ: typedesc[T], col: int32 = 0): seq[T] {.inline.} =
  ## Returns value of BLOB -type column at given column index
  let blob = sqlite3_column_blob(prepared, col)
  let bytes = sqlite3_column_bytes(prepared, col)
  let len = bytes div sizeof(T)
  result = newSeq[T](len)
  if bytes != 0: copyMem(addr(result[0]), blob, bytes)

proc getTime*(prepared: PStmt, col: int32 = 0): Time {.inline.} =
  ## if column is of type TEXT, it is parsed as Time
  ## if column is of type INTEGER, it is interpreted as unix timestamp
  ## if column is of type FLOAT, it is interpreted as unix timestamp with fractional part

  let coltype = sqlite3_column_type(prepared, col)
  if coltype == SQLITE_INTEGER:
    return fromUnix(sqlite3_column_int64(prepared, col))
  elif coltype == SQLITE_FLOAT:
    return fromUnixFloat(sqlite3_column_double(prepared, col))
  elif coltype == SQLITE_TEXT:
    let text = $sqlite3_column_text(prepared, col)
    if text.len == 10 and text[4] == '-' and text[7] == '-':
      return parseTime(text, "yyyy-MM-dd", utc())
    elif text.len == 19 and text[4] == '-' and text[7] == '-' and text[10] == ' ' and text[13] == ':' and text[16] == ':':
      return parseTime(text, "yyyy-MM-dd HH:mm:ss", utc())
    else:
      raise newException(ValueError, "Invalid time format")
  else:
    raise newException(ValueError, "Invalid column type")
  
proc getOptionalTime*(prepared: PStmt, col: int32 = 0): Option[Time] {.inline.} =
  let coltype = sqlite3_column_type(prepared, col)
  if coltype == SQLITE_NULL: 
    result = none(Time)
  else:
    result = some getTime(prepared, col)

proc getAsStrings*(prepared: PStmt): seq[string] =
  ## Returns values of all result columns as a sequence of strings.
  ## This proc is mainly useful for debugging purposes.
  let columncount = sqlite3_column_count(prepared)
  for col in 0 ..< columncount: result.add($sqlite3_column_text(prepared, col))


iterator rows*(db: SQLiteDb, statement: enum, params: varargs[DbValue, toDb]): PStmt =
  ## Iterates over the query results
  if(unlikely) db.loggerproc != nil: log()
  let s = db.preparedstatements[threadi(db)][ord(statement)]
  checkRc(db, bindParams(s, params))
  defer: discard sqlite3_reset(s)
  while sqlite3_step(s) == SQLITE_ROW: yield s


iterator rows*(db: SQLiteDb, pstatement: Pstmt, params: varargs[DbValue, toDb]): PStmt =
  ## Iterates over the query results
  if(unlikely) db.loggerproc != nil: db.doLog("Pstmt rows", params)
  checkRc(db, bindParams(pstatement, params))
  defer: discard sqlite3_reset(pstatement)
  while sqlite3_step(pstatement) == SQLITE_ROW: yield pstatement

proc prepareSql*(db: SQLiteDb, sql: cstring): PStmt {.inline.} =
  ## Prepares a cstring into an executable statement
  # nim 1.2 regression workaround, see https://github.com/nim-lang/Nim/issues/13859
  let len = sql.len.float32
  checkRc(db, sqlite3_prepare_v2(db.sqlite, sql, len.cint, addr result, nil))
  return result

iterator rows*(db: SQLiteDb, statement: string, params: varargs[DbValue, toDb]): PStmt =
  ## Iterates over the query results
  
  let pstatement = db.prepareSql(statement)
  defer: discard sqlite3_finalize(pstatement)

  if(unlikely) db.loggerproc != nil: db.doLog("Pstmt rows", params)
  checkRc(db, bindParams(pstatement, params))
  defer: discard sqlite3_reset(pstatement)
  while sqlite3_step(pstatement) == SQLITE_ROW: yield pstatement

proc freeSql*(db: SQLiteDb, pstatement: PStmt) {.inline.} =
  ## Frees a prepared statement
  discard sqlite3_finalize(pstatement)


proc getTheInt*(db: SQLiteDb, statement: enum, params: varargs[DbValue, toDb]): int64 {.inline.} =
  ## | Executes query and returns value of INTEGER -type column at column index 0 of first result row.
  ## | If query does not return any rows, returns -2147483647 (low(int32) + 1).
  ## | Automatically resets the statement.
  if(unlikely) db.loggerproc != nil: log()
  let s = db.preparedstatements[threadi(db)][ord(statement)]
  checkRc(db, bindParams(s, params))
  defer: discard sqlite3_reset(s)
  if sqlite3_step(s) == SQLITE_ROW: s.getInt(0) else: -2147483647

proc getTheFloat*(db: SQLiteDb, statement: enum, params: varargs[DbValue, toDb]): float64 {.inline.} =
  ## | Executes query and returns value of FLOAT -type column at column index 0 of first result row.
  ## | If query does not return any rows, returns NaN
  ## | Automatically resets the statement.
  if(unlikely) db.loggerproc != nil: log()
  let s = db.preparedstatements[threadi(db)][ord(statement)]
  checkRc(db, bindParams(s, params))
  defer: discard sqlite3_reset(s)
  if sqlite3_step(s) == SQLITE_ROW: s.getFloat(0) else: NaN


proc getTheInt*(db: SQLiteDb, s: string): int64 {.inline.} =
  ## | Dynamically prepares, executes and finalizes given query and returns value of INTEGER -type
  ## column at column index 0 of first result row.
  ## | If query does not return any rows, returns -2147483647 (low(int32) + 1).
  ## | For security and performance reasons, this proc should be used with caution.
  if(unlikely) db.loggerproc != nil: db.loggerproc(db, s, 0)
  let query = db.prepareSql(s)
  try:
    let rc = sqlite3_step(query)
    checkRc(db, rc)
    result = if rc == SQLITE_ROW: query.getInt(0) else: -2147483647
  finally:
    discard sqlite3_finalize(query)


proc getTheString*(db: SQLiteDb, statement: enum, params: varargs[DbValue, toDb]): string {.inline.} =
  ## | Executes query and returns value of TEXT -type column at column index 0 of first result row.
  ## | If query does not return any rows, returns empty string.
  ## | Automatically resets the statement.
  if(unlikely) db.loggerproc != nil: log()
  let s = db.preparedstatements[threadi(db)][ord(statement)]
  checkRc(db, bindParams(s, params))
  defer: discard sqlite3_reset(s)
  if sqlite3_step(s) == SQLITE_ROW: return s.getString(0)


proc getTheString*(db: SQLiteDb, s: string): string {.inline.} =
  ## | Dynamically prepares, executes and finalizes given query and returns value of TEXT -type
  ## column at column index 0 of first result row.
  ## | If query does not return any rows, returns empty string.
  ## | For security and performance reasons, this proc should be used with caution.
  if(unlikely) db.loggerproc != nil: db.loggerproc(db, s, 0)
  let query = db.prepareSql(s)
  try:
    let rc = sqlite3_step(query)
    checkRc(db, rc)
    result = if rc == SQLITE_ROW: query.getString(0) else: ""
  finally:
    discard sqlite3_finalize(query)


proc getLastInsertRowid*(db: SQLiteDb): int64 {.inline.} =
  ## https://www.sqlite.org/c3ref/last_insert_rowid.html
  return db.sqlite.sqlite3_last_insert_rowid()


proc rowExists*(db: SQLiteDb, statement: enum, params: varargs[DbValue, toDb]): bool {.inline.} =
  ## Returns true if query returns any rows
  if(unlikely) db.loggerproc != nil: log()
  let s = db.preparedstatements[threadi(db)][ord(statement)]
  checkRc(db, bindParams(s, params))
  defer: discard sqlite3_reset(s)
  return sqlite3_step(s) == SQLITE_ROW


proc rowExists*(db: SQLiteDb, sql: string): bool {.inline.} =
  ## | Returns true if query returns any rows.
  ## | For security reasons, this proc should be used with caution.
  if(unlikely) db.loggerproc != nil: db.loggerproc(db, sql, 0)
  let preparedstatement = db.prepareSql(sql.cstring)
  defer: discard sqlite3_finalize(preparedstatement)
  return sqlite3_step(preparedstatement) == SQLITE_ROW


template withRow*(db: SQLiteDb, sql: string, row, body: untyped) =
  ## | Dynamically prepares and finalizes an sql query.
  ## | Name for the resulting prepared statement is given with row parameter.  
  ## | The code block will be executed only if query returns a row.
  ## | For security and performance reasons, this proc should be used with caution.
  if(unlikely) db.loggerproc != nil: db.loggerproc(db, sql, 0)
  let preparedstatement = prepareSql(db, sql.cstring)
  try:
    if sqlite3_step(preparedstatement) == SQLITE_ROW:
      var row {.inject.} = preparedstatement
      body
  finally:
    discard sqlite3_finalize(preparedstatement)


template withRowOr*(db: SQLiteDb, sql: string, row, body1, body2: untyped) =
  ## | Dynamically prepares and finalizes an sql query.
  ## | Name for the resulting prepared statement is given with row parameter.  
  ## | First block will be executed if query returns a row, otherwise the second block.
  ## | For security and performance reasons, this proc should be used with caution.
  ## **Example:**
  ## 
  ## .. code-block:: Nim
  ## 
  ##  db.withRowOr("SELECT (1) FROM sqlite_master", rowname):
  ##    echo "database has some tables because first column = ", rowname.getInt(0)
  ##  do:
  ##    echo "we have a fresh database"
  ## 
  if(unlikely) db.loggerproc != nil: db.loggerproc(db, sql, 0)
  let preparedstatement = prepareSql(db, sql.cstring)
  try:
    if sqlite3_step(preparedstatement) == SQLITE_ROW:
      var row {.inject.} = preparedstatement
      body1
    else: body2
  finally:
    discard sqlite3_finalize(preparedstatement)


template withRow*(db: SQLiteDb, statement: enum, params: varargs[DbValue, toDb], row, body: untyped) {.dirty.} =
  ## | Executes given statement.
  ## | Name for the prepared statement is given with row parameter.
  ## | The code block will be executed only if query returns a row.
  if(unlikely) db.loggerproc != nil: doLog(db, $statement, params)
  let s = db.preparedstatements[threadi(db)][ord(statement)]
  defer: discard sqlite3_reset(s)
  checkRc(db, bindParams(s, params))
  if sqlite3_step(s) == SQLITE_ROW:
    var row {.inject.} = s
    body
    

template withRowOr*(db: SQLiteDb, statement: enum, params: varargs[DbValue, toDb], row, body1, body2: untyped) =
  ## | Executes given statement.
  ## | Name for the prepared statement is given with row parameter.  
  ## | First block will be executed if query returns a row, otherwise the second block.
  if(unlikely) db.loggerproc != nil: doLog(db, $statement, params)
  let s = db.preparedstatements[threadi(db)][ord(statement)]
  checkRc(db, bindParams(s, params))
  try:
    if sqlite3_step(s) == SQLITE_ROW:
      var row {.inject.} = s
      body1
    else: body2
  finally:
    discard sqlite3_reset(s)

#-----------------------------------------------------------------------------------------------------------

proc exec*(db: SQLiteDb, pstatement: Pstmt, params: varargs[DbValue, toDb]) {.inline.} =
  ## Executes given prepared statement
  if(unlikely) db.loggerproc != nil: db.doLog("exec Pstmt", params)
  defer: discard sqlite3_reset(pstatement)
  checkRc(db, bindParams(pstatement, params))
  checkRc(db, sqlite3_step(pstatement))


proc exec*(db: SQLiteDb, statement: enum, params: varargs[DbValue, toDb]) {.inline.} =
  ## Executes given statement
  if(unlikely) db.loggerproc != nil: log()
  let s = db.preparedstatements[threadi(db)][ord(statement)]
  defer: discard sqlite3_reset(s)
  checkRc(db, bindParams(s, params))
  checkRc(db, sqlite3_step(s))


proc exes*(db: SQLiteDb, sql: string) =
  ## | Prepares, executes and finalizes given semicolon-separated sql statements.
  ## | For security and performance reasons, this proc should be used with caution.
  if(unlikely) db.loggerproc != nil: db.loggerproc(db, sql, -1)
  var errormsg: cstring
  let rescode = sqlite3_exec(db.sqlite, sql.cstring, nil, nil, addr errormsg)
  if rescode != 0:
    var error: string
    if errormsg != nil:
      error = $errormsg
      sqlite3_free(errormsg)
    if(unlikely) db.loggerproc != nil: db.loggerproc(db, error, rescode)
    raise SQLError(msg: db.dbname & " " & $rescode & " " & error, rescode: rescode)


proc insert*(db: SQLiteDb, statement: enum, params: varargs[DbValue, toDb]): int64 {.inline.} =
  ## | Executes given statement and, if succesful, returns db.getLastinsertRowid().
  ## | If not succesful, returns -2147483647 (low(int32) + 1).
  if(unlikely) db.loggerproc != nil: log()
  let s = db.preparedstatements[threadi(db)][ord(statement)]
  defer: discard sqlite3_reset(s)
  checkRc(db, bindParams(s, params))
  result =
    if sqlite3_step(s) == SQLITE_DONE: db.getLastinsertRowid()
    else: -2147483647


proc update*(db: SQLiteDb, sql: string, column: string, newvalue: DbValue, where: DbValue) =
  ## | Dynamically constructs, prepares, executes and finalizes given update query.
  ## | Update must target one column and WHERE -clause must contain one value.
  ## | For security and performance reasons, this proc should be used with caution.
  if column.find(' ') != -1: raise (ref Exception)(msg: "Column must not contain spaces: " & column)
  let update = sql.replace("Column", column).cstring
  var pstmt: PStmt
  if(unlikely) db.loggerproc != nil: db.loggerproc(db, $update & " (" & $newvalue & ", " & $where & ")", 0)
  checkRc(db, sqlite3_prepare_v2(db.sqlite, update, update.len.cint, addr pstmt, nil))
  try:
    db.exec(pstmt, newvalue, where)
  finally:
    discard pstmt.sqlite3_finalize()


proc columnExists*(db: SQLiteDb, table: string, column: string): bool =
  ## Returns true if given column exists in given table
  result = false
  if table.find(' ') != -1: raise (ref Exception)(msg: "Table must not contain spaces: " & table)
  if column.find(' ') != -1: raise (ref Exception)(msg: "Column must not contain spaces: " & column)
  let sql = ("SELECT count(*) FROM pragma_table_info('" & table & "') WHERE name = '" & column & "'").cstring
  if(unlikely) db.loggerproc != nil: db.loggerproc(db, $sql, 0)
  let pstmt = db.prepareSql(sql)
  try:
    if sqlite3_step(pstmt) == SQLITE_ROW: result = pstmt.getInt(0) == 1
  finally:
    discard sqlite3_reset(pstmt)
    discard pstmt.sqlite3_finalize()
  

template transaction*(db: SQLiteDb, body: untyped) =
  ## | Every write to database must happen inside some transaction.
  ## | Groups of reads must be wrapped in same transaction if mutual consistency required.
  ## | In WAL mode (the default), independent reads must NOT be wrapped in transaction to allow parallel processing.
  if not db.inreadonlymode:
    acquire(db.transactionlock)
    exec(db, db.Transaction)
    db.intransaction = true
    if(unlikely) db.loggerproc != nil: db.loggerproc(db, "--- BEGIN TRANSACTION", 0)
    try: body
    except CatchableError as ex:
      exec(db, db.Rollback)
      if(unlikely) db.loggerproc != nil: db.loggerproc(db, "--- ROLLBACK", 0)
      db.intransaction = false
      raise ex
    finally:
      if db.intransaction:
        exec(db, db.Commit)
        if db.oncommitproc != nil: db.oncommitproc(db)
        db.intransaction = false
        if(unlikely) db.loggerproc != nil: db.loggerproc(db, "--- COMMIT", 0)
      release(db.transactionlock)


template transactionsDisabled*(db: SQLiteDb, body: untyped) =
  ## Executes `body` in between transactions (ie. does not start transaction, but transactions are blocked during this operation).
  acquire(db.transactionlock)
  if(unlikely) db.loggerproc != nil: db.loggerproc(db, "--- TRANSACTIONS DISABLED", 0)
  try:
    body
  finally:
    if(unlikely) db.loggerproc != nil: db.loggerproc(db, "--- TRANSACTIONS ENABLED", 0)
    release(db.transactionlock)


proc isIntransaction*(db: SQLiteDb): bool {.inline.} =
  return db.intransaction


proc setLogger*(db: SQLiteDb, logger: proc(db: SQLiteDb, statement: string, code: int)
 {.gcsafe, raises: [].}, paramtruncat = 50) =
  ## Set callback procedure to gather all executed statements with their parameters.
  ## 
  ## If code > 0, log concerns sqlite error with error code in question.
  ## 
  ## If code == -1, log may be of minor interest (originating from `exes` or statement preparation).
  ##
  ## Paramtruncat parameter limits the maximum log length of parameters so that long inputs won't
  ## clutter logs. Value < 1 disables truncation.
  ##
  ## You can use the same logger for multiple sqliterals, the caller is also given as parameter.
  db.maxparamloggedlen = paramtruncat
  db.loggerproc = logger


proc setOnCommitCallback*(db: SQLiteDb, oncommit: proc(sqliteral: SQLiteDb) {.gcsafe, raises: [].}) =
  ## Set callback procedure that is triggered inside transaction proc, when commit to database has been executed.
  db.oncommitproc = oncommit

#-----------------------------------------------------------------------------------------------------------

template withInternal(db: SQLiteDb, statement: enum, params: varargs[DbValue, toDb], body: untyped) {.dirty.} =
  if(unlikely) db.loggerproc != nil: db.loggerproc(db, $statement & " " & $params, -1)
  let row {.inject.} = db.internalstatements[threadi(db)][ord(statement)]
  defer: discard sqlite3_reset(row)
  checkRc(db, bindParams(row, params))
  if sqlite3_step(row) == SQLITE_ROW:
    body
  else: raise (ref Exception)(msg: "Internal sql failed: " & $statement & " " & $params)


proc json_extract*(db: SQLiteDb, path: string, jsonstring: varargs[DbValue, toDb]): string =
  assert(jsonstring.len == 1)
  db.withInternal(Jsonextract, jsonstring[0], path): row.getString(0)

proc json_patch*(db: SQLiteDb, patch: string, jsonstring: varargs[DbValue, toDb]): string =
  assert(jsonstring.len == 1)
  db.withInternal(Jsonpatch, jsonstring[0], patch): row.getString(0)

proc json_valid*(db: SQLiteDb, jsonstring: varargs[DbValue, toDb]): bool =
  assert(jsonstring.len == 1)
  db.withInternal(Jsonvalid, jsonstring[0]):
    row.getInt(0) == 1

iterator json_tree*(db: SQLiteDb, jsonstring: varargs[DbValue, toDb]): PStmt =
  assert(jsonstring.len == 1)
  if(unlikely) db.loggerproc != nil: db.loggerproc(db, ($Jsontree).replace("?", $jsonstring[0]), -1)
  let s = db.internalstatements[threadi(db)][ord(Jsontree)]
  checkRc(db, bindParams(s, jsonstring[0]))
  defer: discard sqlite3_reset(s)
  while sqlite3_step(s) == SQLITE_ROW: yield s

# -------------------------------------------------------------------------------------------------------------
proc initDatabase(db: SQLiteDb, dbname: string, schemas: openArray[string], maxKbSize: int, wal: bool, ignorableschemaerrors: openArray[string], applicationId: uint32)

proc openDatabase*(dbname: string, schemas: openArray[string],
  maxKbSize = 0, wal = true, ignorableschemaerrors: openArray[string] = @["duplicate column name", "no such column"], applicationId : uint32 = 0): SQLiteDb =
  ## Opens an exclusive connection, boots up the database, executes given schemas and prepares given statements.
  
  result = new SQLiteDb
  initDatabase(result, dbname, schemas, maxKbSize, wal, ignorableschemaerrors, applicationId)

proc initDatabase(db: SQLiteDb, dbname: string, schemas: openArray[string], maxKbSize: int, wal: bool, ignorableschemaerrors: openArray[string], applicationId: uint32) =
  ## Opens an exclusive connection, boots up the database, executes given schemas and prepares given statements.
  ## 
  ## If dbname is not a path, current working directory will be used.
  ## 
  ## | If wal = true, database is opened in WAL mode with NORMAL synchronous setting.
  ## | If wal = false, database is opened in PERSIST mode with FULL synchronous setting.
  ## | https://www.sqlite.org/wal.html
  ## | https://www.sqlite.org/pragma.html#pragma_synchronous
  ## 
  ## If maxKbSize == 0, database size is limited only by OS or hardware with possibly severe consequences.
  ##
  ## `ignorableschemaerrors` is a list of error message snippets for sql errors that are to be ignored.
  ## If a clause may error, it must be given in a separate schema as its unique clause.
  ## If * is given as ignorable error, it means that all errors will be ignored.
  ## 
  ## Note that by default, "duplicate column name" (ADD COLUMN) and "no such column" (DROP COLUMN) -errors will be ignored.
  ## Example below.
  ##
  ## .. code-block:: Nim
  ## 
  ##  const
  ##    Schema1 = "CREATE TABLE IF NOT EXISTS Example(data TEXT NOT NULL)"
  ##    Schema2 = "this is to be ignored"
  ##    Schema3 = """ALTER TABLE Example ADD COLUMN newcolumn TEXT NOT NULL DEFAULT """""
  ## 
  ##  var db1, db2, db3: SQLiteDb
  ## 
  ##  proc logger(db: SQLiteDb, msg: string, code: int) = echo msg
  ##  db1.setLogger(logger); db2.setLogger(logger); db3.setLogger(logger)
  ## 
  ##  db1.openDatabase("example1.db", [Schema1, Schema3]); db1.close()
  ##  db2.openDatabase("example2.db", [Schema1, Schema2],
  ##   ignorableschemaerrors = ["""this": syntax error"""]); db2.close()
  ##  db3.openDatabase("example3.db", [Schema1, Schema2, Schema3],
  ##   ignorableschemaerrors = ["*"]); db3.close()
  ## 
  doAssert dbname != ""
  initLock(db.transactionlock)
  db.dbname = dbname

  if applicationId > 0:
    # try open the database in read-write mode to check if it exists and application id matches
    if sqlite3_open_v2(dbname, addr db.sqlite, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK:
      if dbname != ":memory:":
        let actualAppId : uint32 = db.getTheInt("PRAGMA application_id").uint32
          
        if actualAppId != applicationId:
          discard sqlite3_close(db.sqlite)
          db.sqlite = nil
          raise SQLError(msg:"Database application id does not match. Expected: " & $applicationId & ", actual: " & $actualAppId)
    else:
      db.checkRc(sqlite3_open_v2(dbname, addr db.sqlite, SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE, nil))
      db.exes("PRAGMA application_id = " & $applicationId)
  else:
    db.checkRc(sqlite3_open(dbname, addr db.sqlite))
  
  db.exes("PRAGMA encoding = 'UTF-8'")
  db.exes("PRAGMA foreign_keys = ON")
  db.exes("PRAGMA locking_mode = EXCLUSIVE")
  db.exes("PRAGMA mmap_size = 30000000000")
  db.exes("PRAGMA temp_store = memory")
  db.exes("PRAGMA page_size = 8192")
  db.walmode = wal
  if wal: 
    db.exes("PRAGMA journal_mode = WAL")
    db.exes("PRAGMA synchronous = NORMAL")
  else:
    db.exes("PRAGMA journal_mode = PERSIST")
    db.exes("PRAGMA synchronous = FULL")
  if maxKbSize > 0:
    db.maxsize = maxKbSize
    let pagesize = db.getTheInt("PRAGMA page_size")
    db.exes("PRAGMA max_page_count = " & $(maxKbSize * 1024 div pagesize))
  for schema in schemas:
    try: db.exes(schema)
    except CatchableError:
      var ignorable = false
      for ignorableerror in ignorableschemaerrors:
        if ignorableerror == "*" or getCurrentExceptionMsg().contains(ignorableerror): (ignorable = true; break)
      if not ignorable: raise
  db.Transaction = db.prepareSql("BEGIN IMMEDIATE".cstring)
  db.Commit = db.prepareSql("COMMIT".cstring)
  db.Rollback = db.prepareSql("ROLLBACK".cstring)
  if db.loggerproc != nil: db.loggerproc(db, db.dbname & " opened", -1)
  elif defined(fulldebug): echo "notice: fulldebug defined but logger not set for ", db.dbname
  

proc createStatement(db: SQLiteDb, statement: enum) =
  let index = ord(statement)
  if(unlikely) db.loggerproc != nil:
    when compileOption("threads"):
      if db.threadindices[0] == getThreadId(): db.loggerproc(db, $statement, -1)
    else: db.loggerproc(db, $statement, -1)
  db.preparedstatements[db.threadlen][index] = prepareSql(db, ($statement).cstring)


var preparelock: Lock
initLock(preparelock)

proc prepareStatements*(db: SQLiteDb, Statements: typedesc[enum]) =
  ## Prepares the statements given as enum parameter.
  ## Call this exactly once from every thread that is going to access the database.
  ## Main example shows how this "exactly once"-requirement can be achieved with a boolean threadvar.
  withLock(preparelock):
    when compileOption("threads"): db.threadindices[db.threadlen] = getThreadId()
    else: db.threadindices[db.threadlen] = 0
    for v in low(Statements) .. high(Statements): db.createStatement(v)
    for v in low(InternalStatements) .. high(InternalStatements):
      db.internalstatements[db.threadlen][ord(v)] =  prepareSql(db, ($v).cstring)    
    db.threadlen.inc
    db.laststatementindex = ord(high(Statements))
    

proc setReadonly*(db: SQLiteDb, readonly: bool) =
  ## When in readonly mode:
  ## 
  ## 1) All transactions will be silently discarded
  ## 
  ## 2) Journal mode is changed to PERSIST in order to be able to change locking mode
  ## 
  ## 3) Locking mode is changed from EXCLUSIVE to NORMAL, allowing other connections access the database
  ## 
  ## Setting readonly fails with exception "cannot change into wal mode from within a transaction"
  ## when a statement is being executed, for example a result of a select is being iterated.
  ## 
  ## ``inreadonlymode`` property tells current mode.
  if readonly == db.inreadonlymode: return
  db.transactionsDisabled:
    if readonly:
      db.inreadonlymode = readonly
      db.exes("PRAGMA journal_mode = PERSIST")
      db.exes("PRAGMA locking_mode = NORMAL")
      db.exes("SELECT (1) FROM sqlite_master") #  dummy access to release file lock
    else:
      if db.walmode: db.exes("PRAGMA journal_mode = WAL")
      db.exes("PRAGMA locking_mode = EXCLUSIVE") # next write will keep the file lock
      db.inreadonlymode = readonly
    if(unlikely) db.loggerproc != nil: db.loggerproc(db, "--- READONLY MODE: " & $readonly , 0)
    

proc optimize*(db: SQLiteDb, pagesize = -1, walautocheckpoint = -1) =
  ## Vacuums and optimizes the database.
  ## 
  ## This proc should be run just before closing, when no other thread accesses the database.
  ## 
  ## | In addition, database read/write performance ratio may be adjusted with parameters:
  ## | https://sqlite.org/pragma.html#pragma_page_size
  ## | https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
  acquire(db.transactionlock)
  try:   
    db.exes("PRAGMA optimize")
    if walautocheckpoint > -1: db.exes("PRAGMA wal_autocheckpoint = " & $walautocheckpoint)
    if pagesize > -1:
      db.exes("PRAGMA journal_mode = PERSIST")
      db.exes("PRAGMA page_size = " & $pagesize)  
    db.exes("VACUUM")      
    if pagesize > -1 and db.walmode: db.exes("PRAGMA journal_mode = WAL")
  except CatchableError:
    if db.loggerproc != nil: db.loggerproc(db, getCurrentExceptionMsg(), -1)
    else: echo "Could not optimize ", db.dbname, ": ", getCurrentExceptionMsg()
  finally:
    release(db.transactionlock)


proc initBackup*(db: SQLiteDb, backupfilename: string):
 tuple[backupdb: Psqlite3, backuphandle: PSqlite3_Backup] =
  ## Initializes backup processing, returning variables to use with `stepBackup` proc.
  ## 
  ## Note that `close` will fail with SQLITE_BUSY if there's an unfinished backup process going on.
  db.checkRc(sqlite3_open(backupfilename, addr result.backupdb))
  db.transactionsDisabled:
    result.backuphandle = sqlite3_backup_init(result.backupdb, "main".cstring, db.sqlite, "main".cstring)
    if result.backuphandle == nil: db.checkRc(SQLITE_NULL)
    elif db.loggerproc != nil: db.loggerproc(db, "backup to " & backupfilename, 0)
    discard db.backupsinprogress.atomicInc


proc stepBackup*(db: SQLiteDb, backupdb: Psqlite3, backuphandle: PSqlite3_Backup, pagesperportion = 5.int32): int =
  ## Backs up a portion of the database pages (default: 5) to a destination initialized with `initBackup`.
  ##
  ## Returns percentage of progress; 100% means that backup has been finished.
  ## 
  ## The idea `(check example 2)<https://sqlite.org/backup.html>`_ is to put the thread to sleep
  ## between portions so that other operations can proceed concurrently.
  ##
  ## **Example:**
  ## 
  ## .. code-block:: Nim
  ##  
  ##  from os import sleep 
  ##  let (backupdb , backuphandle) = db.initBackup("./backup.db")
  ##  var progress: int
  ##  while progress < 100:
  ##    sleep(250)
  ##    progress = db.stepBackup(backupdb, backuphandle)
  ## 
  if unlikely(backupdb == nil or backuphandle == nil): db.checkRc(SQLITE_NULL)
  var rc = sqlite3_backup_step(backuphandle, pagesperportion)
  if rc == SQLITE_DONE:
    db.checkRc(sqlite3_backup_finish(backuphandle))
    db.checkRc(backupdb.sqlite3_close())
    discard db.backupsinprogress.atomicDec
    if db.loggerproc != nil: db.loggerproc(db, "backup ok", 0)
    return 100
  if rc notin [SQLITE_OK, SQLITE_BUSY, SQLITE_LOCKED]:
    discard db.backupsinprogress.atomicDec
    if db.loggerproc != nil: db.loggerproc(db, "backup failed", rc)
    discard sqlite3_backup_finish(backuphandle)
    discard backupdb.sqlite3_close()
    db.checkRc(rc)
  return 100 * (backuphandle.sqlite3_backup_pagecount - backuphandle.sqlite3_backup_remaining) div backuphandle.sqlite3_backup_pagecount


proc cancelBackup*(db: SQLiteDb, backupdb: Psqlite3, backuphandle: PSqlite3_Backup) =
  ## Cancels an ongoing backup process.
  if unlikely(backupdb == nil or backuphandle == nil): db.checkRc(SQLITE_NULL)
  discard sqlite3_backup_finish(backuphandle)
  discard backupdb.sqlite3_close()
  discard db.backupsinprogress.atomicDec  
  if db.loggerproc != nil: db.loggerproc(db, "backup canceled", 0)
  

proc about*(db: SQLiteDb) =
  ## Echoes some info about the database.
  echo ""
  echo db.dbname & ": "
  echo "SQLite=", sqlite3_libversion()
  echo "Userversion=", db.getTheString("PRAGMA user_version")
  let Get_options = db.prepareSql("PRAGMA compile_options")
  for row in db.rows(Get_options): echo row.getString()
  discard sqlite3_finalize(Get_options)
  echo "Pagesize=", $db.getTheInt("PRAGMA page_size")
  echo "WALautocheckpoint=", $db.getTheInt("PRAGMA wal_autocheckpoint")
  if db.preparedstatements.len > 0: echo "Preparedstatements=", $(db.preparedstatements[0].len)
  let filesize = getFileSize(db.dbname)
  echo "Filesize=", filesize
  if db.maxsize > 0:
    echo "Maxsize=", db.maxsize * 1024
    echo "Sizeused=", (filesize div ((db.maxsize.float * 10.24).int)), "%"
  echo ""


proc getStatus*(db: SQLiteDb, status: int, resethighest = false): (int, int) =
  ## Retrieves queried status info.
  ## See https://www.sqlite.org/c3ref/c_dbstatus_options.html
  ## 
  ## **Example:**
  ## 
  ## .. code-block:: Nim
  ## 
  ##    const SQLITE_DBSTATUS_CACHE_USED = 1
  ##    echo "current cache usage: ", db.getStatus(SQLITE_DBSTATUS_CACHE_USED)[0]
  var c, h: int32
  db.checkRc(sqlite3_db_status(db.sqlite, status.int32, addr c, addr h, resethighest.int32))
  return (c.int, h.int)


proc close*(db: SQLiteDb) =
  ## Closes the database.
  if db.Transaction == nil:
    if db.loggerproc != nil: db.loggerproc(db, "already closed: " & db.dbname, -1)
    return
  if db.backupsinprogress > 0:
    raise SQLError(msg: "Cannot close, backups still in progress: " & $db.backupsinprogress, rescode: SQLITE_BUSY)
  var rc = 0
  acquire(db.transactionlock)
  try:
    for thread in 0 ..< db.threadlen:
      for i in 0 .. db.laststatementindex:
        discard db.preparedstatements[thread][i].sqlite3_finalize()
      for s in db.internalstatements[thread]: discard s.sqlite3_finalize()
    discard db.Transaction.sqlite3_finalize()
    discard db.Commit.sqlite3_finalize()
    discard db.Rollback.sqlite3_finalize()
    rc = sqlite3_close_v2(db.sqlite)
    if rc == SQLITE_OK:
      db.Transaction = nil
      if db.loggerproc != nil: db.loggerproc(db, db.dbname & " closed", 0)
    else: db.checkRc(rc)
  except CatchableError:
    if db.loggerproc == nil: echo "Could not close ", db.dbname, ": ", getCurrentExceptionMsg()
    elif rc == 0: db.loggerproc(db, getCurrentExceptionMsg(), 1)
  finally:
    release(db.transactionlock)
    if db.Transaction == nil: deinitLock(db.transactionlock)


proc attachDatabase*(db: SQLiteDb, dbname: string, alias: string, schemas: openArray[string]) = 
  var attachedDb = openDatabase(dbname, schemas)
  attachedDb.close()
  
  db.exes("ATTACH DATABASE '" & dbname & "' AS " & alias)
