import logging
import azure.functions as func
from azure.cosmos import exceptions, CosmosClient, PartitionKey
import json
import time
import os
class cosmos:
    '''Classe que contêm os métodos de escrita e leitura do cosmos'''

    __client = None

    def __init__(self,cosmos_uri,cosmos_key):
        self.__client = CosmosClient(cosmos_uri, cosmos_key)

    def create_Database(self,database):
        '''Cria base de dados no cosmos'''
        return self.__client.create_database(database)

    def delete_Database(self,database):
        '''Apaga base de dados no cosmos'''
        return self.__client.delete_database(database)


    def create_Container(self,database,container,partition_key):
        '''Cria um container/coleção na database especificada'''
        
        database = self.read_Database(database)
        return database.create_container(id=container, partition_key=PartitionKey(path=partition_key))


    def delete_Container(self,database, container):

        print("\n6. Delete Container")
        
        try:
            client_db = self.read_Database(database)
        except:
            pass

        try:
            container1 = client_db.get_container_client(container)
            client_db.delete_container(container1)
            
            print('Container with id \'{0}\' was deleted'.format(container))
        except Exception as ex:
            print('A container with id \'{0}\' does not exist'.format(container))
    
    def read_Database(self,database):
        return self.__client.get_database_client(database)
        
    def read_Container(self,database, container):
        print("\n4. Get a Container by id")
        db = self.read_Database(database)
        try:
            container1 = db.get_container_client(container)
            print('Container with id \'{0}\' was found, it\'s link is {1}'.format(container1.id, container1.container_link))

        except exceptions.CosmosResourceNotFoundError:
            print('A container with id \'{0}\' does not exist'.format(id))

        return container1

    def delete_Docs(self,database,container,partition_key,query="SELECT * FROM c "):

        container1=self.read_Container(database,container)
        items = list(container1.query_items(query=query, enable_cross_partition_query=True))
        erase_count = 0
        fail_count=0
        for item in items:

            try:
                item[partition_key]
                try:
                    container1.delete_item(item,item[partition_key])
                    erase_count+=1
                except:
                    fail_count+=1

            except:
                partition_key=None
                try:
                    container1.delete_item(item,{})
                    erase_count+=1
                except Exception as ex:
                    fail_count+=1

                
                    
        print(f'{fail_count} >> falha(s)')
        print(f'{erase_count} >> item(s) apagado(s)')
        
    def list_Containers(self,database):
        print("\n5. List all Container in a Database")

        print('Containers:')

        containers = list(self.read_Database(database).list_containers())

        if not containers:
            return

        for container in containers:
            print(container['id'])

    def merge(self,database,container,id_key,content,partition_key=None,retry=1,numeric_id=False) -> None:
        """ 
        efetua o merge dos documentos gravado com o conteudo atual
        
        **** caso o id seja inteiro necessario remover as aspas no codigo

        ### Parametros:

        #### content
            dicionario com os valores que serão inseridos ou atualizados. Deve conter o item id_key

        #### merge
            se true >> efetua o merge odo item novo com o antigo (prevalecendo o novo)\n
            se false >> substitui o item antigo pelo novo
        
        #### container
            sinônimo de coleção

        #### partition_key
            chave única >> usada na crianção do container/coleção
        """

        while retry>0:
            try:

                try:
                    db = self.__client.get_database_client(database)
                except Exception as e:
                    db = self.__client.create_database(database)


                try:
                    c = db.get_container_client(container)
                except Exception as e:
                    c = db.create_container(id=container, partition_key=PartitionKey(path=partition_key))



                #id = content[id_key]
                #if(numeric_id):
                #    query=f'SELECT * FROM {container} r WHERE r.{id_key}={id}'
                #else:
                #    query=f'SELECT * FROM {container} r WHERE r.{id_key}="{id}"'
                    
                #o={}
                #for item in c.query_items(
                #        query=query,
                #        enable_cross_partition_query=True):
                #    o.update(item)
                #o.update(content)
                
                try:
                    logging.info(content['id'])
                except KeyError as k:
                    content['id'] = f"{id}"

                c.upsert_item(content)
                   

                retry=-1
                print(f"Done! >> {content[id_key]}")
            except Exception as e:
                time.sleep(1)
                retry=retry-1
                ex = str(e)
        
        if retry==0:
            print(f"Fail! >> {ex}")



    def overwrite(self,database,container,id_key,content,partition_key=None,retry=1,numeric_id=False) -> None: 
        """ 
        Método que sobrescreve O
        
        **** caso o id seja inteiro necessario remover as aspas no codigo

        ### Parametros:

        #### content
            dicionario com os valores que serão inseridos ou atualizados. Deve conter o item id_key

        #### merge
            se true >> efetua o merge odo item novo com o antigo (prevalecendo o novo)\n
            se false >> substitui o item antigo pelo novo
        
        #### container
            sinônimo de coleção

        #### partition_key
            chave única >> usada na crianção do container/coleção
        """

        while retry>0:
            try:

                try:
                    db = self.__client.get_database_client(database)
                except Exception as e:
                    db = self.__client.create_database(database)


                try:
                    c = db.get_container_client(container)
                except Exception as e:
                    c = db.create_container(id=container, partition_key=PartitionKey(path=partition_key))


                id = content[id_key]

                if(numeric_id):
                    query=f'SELECT * FROM {container} r WHERE r.{id_key}={id}'
                else:
                    query=f'SELECT * FROM {container} r WHERE r.{id_key}="{id}"'
                
                o={}
                for item in c.query_items(
                    query=query,
                    enable_cross_partition_query=True):
                    item_id = item['id']
                    content.update({f'id':f'{item_id}'})
                o.update(content)
                try:
                    logging.info(o['id'])
                except KeyError as k:
                    o['id'] = f"{id}"

                c.upsert_item(o) 
                retry=-1
                print(f"Done! >> {content[id_key]}")
            except Exception as e:
                time.sleep(1)
                retry=retry-1
                ex = str(e)
        
        if retry==0:
            print(f"Fail! >> {ex}")
          
          
    def read(self,database,container,query):
        '''
        Efetua uma query sql no cosmos (nosql)

        ### query
            Consulta SQL, pesquisa, que sera executada no container/coleção - sample >> "SELECT v.id from vivo_bko as v where v.ID_MAIL='5'"'''
        db = self.__client.get_database_client(database)
        c = db.get_container_client(container)
        return list(c.query_items(
                            query=query,
                            enable_cross_partition_query=True))

        