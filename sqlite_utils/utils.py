try:
    import pysqlite3 as sqlite3
    import pysqlite3.dbapi2

    OperationalError = pysqlite3.dbapi2.OperationalError
except ImportError:
    import sqlite3

    OperationalError = sqlite3.OperationalError
