from dslib.kvault.keyvault_class import kvault


## lake factory object
def factory(keyvaultname:str=None,login:str=None,senha:str=None):
    return kvault(keyvaultname=keyvaultname,login=login,senha=senha)