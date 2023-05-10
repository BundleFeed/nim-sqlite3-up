# Copyright 2023 Geoffrey Picron.
# SPDX-License-Identifier: (MIT or Apache-2.0)


import ../sqlite3_up
import sqlite3_abi

when (NimMajor, NimMinor) < (1, 4):
  {.pragma: sqlitedecl, cdecl, gcsafe, raises: [Defect].}
else:
  {.pragma: sqlitedecl, gcsafe, cdecl, raises: [].}


type
    SqlExtensionError* = object of SqlError

proc enableInternalLoadExtension*(db: var SQLiteDb) =
  ## Enable the loading of SQLite extensions using the c api.
  var enabled : cint
  db.checkRc(sqlite3_db_config(db.sqlite, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1.cint, enabled.addr))

  if enabled != 1:
    raise newException(SqlExtensionError, "Failed to enable loading of SQLite extensions")


proc disableInternalLoadExtension*(db: var SQLiteDb) =
  ## Disable the loading of SQLite extensions using the c api.
  var enabled : cint
  db.checkRc sqlite3_db_config(db.sqlite, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 0.cint, enabled.addr)

  if enabled != 0:
    raise newException(SqlExtensionError, "Failed to disable loading of SQLite extensions")



proc helloFunc*(a1: ptr sqlite3_context; nargs: cint; args: ptr ptr sqlite3_value) {.sqlitedecl.} =
  ## A simple hello world function.
  debugEcho "Hello world function called"
  sqlite3_result_text(a1, "Hello World!", -1, SQLITE_TRANSIENT)



proc sqlite3_hello_world_init(db: ptr sqlite3, ppError: ptr cstring, pApi: ptr sqlite3_api_routines): cint {.sqlitedecl.} =
  ## Initialize the hello world extension.
  debugEcho "Initializing hello world extension"
  result = sqlite3_create_function(db, "hello", 0, SQLITE_UTF8 or SQLITE_DETERMINISTIC, nil, helloFunc, nil, nil)
  debugEcho "Result of sqlite3_create_function: ", result

type 
  OpaqueEntryPointProc = proc () {.sqlitedecl.}

proc testHelloWorld*() =
  ## Test the loading of the hello world extension.
  echo "Testing hello world extension"

  #discard sqlite3_auto_extension(sqlite3_hello_world_init)