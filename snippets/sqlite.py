import sqlite3

# Simpler is better, in this case. The main script works with dict types.
def dict_factory(cur, row):
    return {col[0]: row[ix] for ix, col in enumerate(cur.description)}
# End dict_factory


# This is a subclass of sqlite3.Cursor that includes rudimentary __enter__, __exit__ 
# methods so it can be used in with context manager statements
class CMCursor(sqlite3.Cursor):
    def __enter__(self):
        return self
    
    def __exit__(self, e_type, e_value, e_tb):
        self.close()
# End class CMCursor


# This is a subclass of the sqlite3.Connection class. It does nothing but force
# dict_factory as the row factory and the CMCursor as the cursor factory
class CMConnection(sqlite3.Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.row_factory = dict_factory
    
    def cursor(self, *args, **kwargs):
        return super().cursor(factory=CMCursor)
# End class CMConnection


