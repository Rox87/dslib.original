from dslib.stor.lake.lake_class import lake as lk


## lake factory object
def factory(storageStrCon:str,fs=None,dir=None):
    return lk(storageStrCon=storageStrCon,fs=fs,dir=dir)