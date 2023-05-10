# sqlite3_up
A high level SQLite API for Nim.  Initially based on SQLiteral.

- Focused on static compilation, (no dll needed with applications using this library)
- Supports multi-threading, prepared statements, proper typing, zero-copy data paths, debugging, JSON, optimizing, backups.
- Supports creation of extensions

## Fork

This is a fork of [SQLiteral](https://github.com/olliNiinivaara/SQLiteral). 
The original library was based on custom low-level bindings to SQLite. I wanted to base myself on the low-level bindings provided by the [sqlite3_abi](https://github.com/arnetheduck/nim-sqlite3-abi) which is completely generated from the SQLite C headers. This allows for a more complete and up-to-date binding.

## Versioning

The versioning follows the SQLite versioning. The first three digits are the SQLite version, the fourth digit is the version of this library.

## Documentation

[API Documentation](https://bundlefeed.github.io/nim-sqlite3-up/sqlite3_up.html)

## Installation

`nimble install sqlite3_up`

## Example

```nim
import sqlite3_up
const Schema = "CREATE TABLE IF NOT EXISTS Example(string TEXT NOT NULL)"
type SqlStatements = enum
  Upsert = """INSERT INTO Example(rowid, string) VALUES(1, ?)
   ON CONFLICT(rowid) DO UPDATE SET string = ?"""
  SelectAll = "SELECT string FROM Example"
var db: SQLiteral

proc operate(i: string) =
  let view: DbValue = i.toDb(3, 7) # zero-copy view into string
  db.transaction: db.exec(Upsert, view, view)
  for row in db.rows(SelectAll): echo row.getCString(0)

db.openDatabase("example.db", Schema)
db.prepareStatements(SqlStatements)
var input = "012INPUT89"
operate(input)
db.close()
```

## License

This repository is licensed and distributed under either of

* MIT license: [LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT

or

* Apache License, Version 2.0, ([LICENSE-APACHEv2](LICENSE-APACHEv2) or http://www.apache.org/licenses/LICENSE-2.0)

at your option. This file may not be copied, modified, or distributed except according to those terms.

