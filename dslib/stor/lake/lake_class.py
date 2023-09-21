
import os
import tempfile
try:
    from azure.storage.filedatalake import DataLakeServiceClient
except:
    os.system('pip install azure-storage-file-datalake')
    from azure.storage.filedatalake import DataLakeServiceClient

class lake():
    
    __fs = None
    __dir = ''
    __openfile = None

    def __init__(self,storageStrCon:str,fs=None,dir=None) -> None:
        '''
        Classe que referência um objeto do tipo Azure Datalake  
        e possui funções de manipulação desse objeto

        Parameters:
        *storageStrCon* é a string de conexão da conta de armazenamentoe, como opcional 
        há a possibilidade de definir o *fs* (filesystem) e o *dir* (diretório) de inicio
        '''
        self.__service_client = DataLakeServiceClient.from_connection_string(conn_str=storageStrCon)
        self.dir(dir,fs)

    def dir(self,dir=None,fs=None):
        '''redefine o *dir* (diretório) e o *fs* (filesystem) dos próximos comandos'''
        self.__fs = fs if(fs!=None) else self.__fs
        self.__dir = dir if(dir!=None) else self.__dir

    def close(self):
        '''fecha o último arquivo aberto'''
        try:
            self.__openfile.close()
        except:
            pass

    def down(self,lake_filename,dir='',fs=None,renew=False):
        '''efetua o download de um arquivo no datalake
         e retorna o arquivo local aberto p/ uso.
        ***********************************************
        *fs* é filesystem, *dir* o caminho do arquivo e 
        *renew* determina se o método baixará novamente 
        mesmo se o arquivo local já existir'''
        self.dir(dir,fs)

        try:
            tmp_dir=tempfile.gettempdir()
            local_file=os.path.join(tmp_dir,f"{lake_filename}")

            if(os.path.isfile(local_file) and not renew):  
                return open(local_file,'rb')
            else:
                    
                file_system_client = self.__service_client.get_file_system_client(file_system=self.__fs)
                
                try:
                    directory_client = file_system_client.get_directory_client(self.__dir)
                    file_client = directory_client.get_file_client(lake_filename)
                except:
                    file_client = file_system_client.get_file_client(lake_filename)

                download = file_client.download_file()
                downloaded_bytes = download.readall()

                self.close()

                open_file = open(local_file,'wb')
                open_file.write(downloaded_bytes)
                open_file.close()



                self.__openfile = open(local_file,'rb')

                return self.__openfile

        except Exception as e:
            print(e)
            
    def up(self,content,lake_filename,dir=None,fs=None,create=True,overwrite=True):
        '''efetua o upload de uma string (content) para o arquivo (lake_filename)
        no caminho dir e no filesystem fs. A opção overwrite determina se o arquivo 
        será sobreescrito caso já exista'''
        self.dir(dir,fs)

        file_system_client = self.__service_client.get_file_system_client(file_system=self.__fs)
        
        if(create):
            self.createIfDir(dir,fs)
        
        try:
            directory_client = file_system_client.get_directory_client(self.__dir)
            file_client = directory_client.get_file_client(lake_filename)
        except:
            file_client = file_system_client.get_file_client(lake_filename)

        file_client.upload_data(content, overwrite=overwrite)

    def delete(self,lake_filename,fs=None,dir=''):

        self.dir(dir,fs)

        try:
                 
            file_system_client = self.__service_client.get_file_system_client(file_system=self.__fs)
            
            try:
                directory_client = file_system_client.get_directory_client(self.__dir)
                file_client = directory_client.get_file_client(lake_filename)
            except:
                file_client = file_system_client.get_file_client(lake_filename)

            file_client.delete_file()
                
        except Exception as e:
                print(e)

    def list(self,fs=None,dir='',endwith=''):

            self.dir(dir,fs)
            endwith = "" if(endwith==None) else endwith

            try:
                l = []
                file_system_client = self.__service_client.get_file_system_client(file_system=self.__fs)

                paths = file_system_client.get_paths(path=self.__dir)

                for path in paths:
                    if str(path.name).count('/') == dir.count('/'):
                        if(str(path.name).endswith(endwith)):
                            if(str(path.name).startswith(dir)):
                                l.append(path)
                
                return l

            except Exception as e:
                print(e)
    def list_recursive(self,fs=None,dir='',endwith=''):

            self.dir(dir,fs)
            endwith = "" if(endwith==None) else endwith

            try:
                l = []
                file_system_client = self.__service_client.get_file_system_client(file_system=self.__fs)

                paths = file_system_client.get_paths(path=self.__dir)

                for path in paths:
                        if(str(path.name).endswith(endwith)):
                            if(str(path.name).startswith(dir)):
                                l.append(path)
                
                return l

            except Exception as e:
                print(e)

    def newerInDir(self,fs=None,dir='',endwith=''):

            i = 0

            self.dir(dir,fs)
            endwith = "" if(endwith==None) else endwith

            for file in self.list(fs,dir,endwith):
                if(i==0):
                    last_file = file
                    i = 1
                    continue
                if(file.last_modified>last_file.last_modified):
                    last_file = file

            try:
                return self.down(last_file.name.replace(f'{dir}/',''),dir,fs)
            except:
                pass

    def deleteDir(self,dir=None,fs=None):

        self.dir(dir,fs)

        try:
                    
            file_system_client = self.__service_client.get_file_system_client(file_system=self.__fs)

            directory_client = file_system_client.get_directory_client(self.__dir)
            directory_client.delete_directory()
                
        except Exception as e:
                print(e)


    def moveDir(self,new_dir=None,old_dir=None,fs=None):

        self.dir(old_dir,fs)

        try:
            
            file_system_client = self.__service_client.get_file_system_client(file_system=self.__fs)
            directory_client = file_system_client.get_directory_client(old_dir)
            
            directory_client.rename_directory(new_name=directory_client.file_system_name + '/' + new_dir)

        except Exception as e:
            print(e)


    def createIfDir(self,dir=None,fs=None):

        self.dir(dir,fs)

        try:
            if(self.__fs!=None):
                file_system_client = self.__service_client.create_file_system(file_system=self.__fs)
                print('fs created')
        except:
            pass

        try:
            if(self.__dir!=None):
                file_system_client=self.__service_client.get_file_system_client(file_system=self.__fs)
                file_system_client.create_directory(self.__dir)
                print('dir created')
        except:
            pass