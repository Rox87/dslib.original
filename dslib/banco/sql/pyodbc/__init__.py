from dslib.banco.sql.pyodbc.pyodbc_class import pyodbc


## lake factory object
def factory(server,database,usuario,senha):
    return pyodbc(server,database,usuario,senha)