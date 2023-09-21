from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy)

class storQueue:

    __queue_client=None
    
    def __init__(self, fila, connect_str):
        self.fila(fila,connect_str) 

    def fila(self,fila=None,connect_str=None):

        self.__connect_str = connect_str if (connect_str!=None) else self.__connect_str
        self.__fila = fila if (fila!=None) else self.fila

        self.__queue_client = QueueClient.from_connection_string(self.__connect_str, self.__fila,
                        message_encode_policy = BinaryBase64EncodePolicy(),
                        message_decode_policy = BinaryBase64DecodePolicy())

        try:
            self.__queue_client.create_queue()
            print('fila criada')
        except:
            ''   
        
    def send(self,mensagem,encoding='UTF-8'):
        self.__queue_client.send_message(bytes(mensagem,encoding)) #bytes(mensagem,'UTF-8','ignore')

