from dslib.stor.blob.blob_class import blob as b

## lake factory object
def factory(storageStrCon:str,container=None,dir=None):
    return b(storageStrCon=storageStrCon,container=container,dir=dir)