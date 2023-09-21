from dslib.banco.cosmos.cosmos_class import cosmos


## lake factory object
def factory(cosmos_uri:str,cosmos_key:str):
    return cosmos(cosmos_uri=cosmos_uri,cosmos_key=cosmos_key)