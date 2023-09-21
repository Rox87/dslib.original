from dslib.queue.stor.storQueue_class import storQueue


## lake factory object
def factory(nome_fila, connect_str):
    return storQueue(nome_fila, connect_str)