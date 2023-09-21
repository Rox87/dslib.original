from dslib.stor.table.stor_table_class import storTable as st


## lake factory object
def factory(tableName:str,storageStrCon:str):
    return st(storageStrCon,tableName)