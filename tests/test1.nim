# This is just an example to get you started. You may wish to put all of your
# tests into a single file, or separate them into multiple `test1`, `test2`
# etc. files (better names are recommended, just make sure the name starts with
# the letter 't').
#
# To run these tests, simply execute `nimble test`.

import unittest

import ../src/sqlite3_up
import ../src/sqlite3_up/extension
import std/[tempfiles, os]

suite "Simple integrated tests":

  setup:
    let tempDir = createTempDir("testsqlite_up_", "_end")

  teardown:
    removeDir(tempDir)

  test "Minimal example":
    const Schema = @["CREATE TABLE IF NOT EXISTS Example(string TEXT NOT NULL)"]
    type SqlStatements = enum
      Upsert = """INSERT INTO Example(rowid, string) VALUES(1, ?) ON CONFLICT(rowid) DO UPDATE SET string = ?"""
      SelectAll = "SELECT string FROM Example"
    var db = openDatabase(tempDir & "data.sqlite3", Schema)
    
    db.prepareStatements(SqlStatements)
    var input = "012INPUT89"

    let view: DbValue = input.toDb(3, 7) # zero-copy view into string
    db.transaction: 
      db.exec(Upsert, view, view)
    
    for row in db.rows(SelectAll): check row.getCString(0) == "INPUT"
    
    db.close()  

  test "extension":
    const Schema = @["CREATE TABLE IF NOT EXISTS Example(string TEXT NOT NULL)"]
    var db = openDatabase(tempDir & "data.sqlite3", Schema)
    
    #testHelloWorld()

    for r in  db.rows("select * from pragma_function_list()"):
      echo r.getAsStrings()
    
    #check db.getTheString("SELECT hello()") == "Hello World!"
    
    db.close()

