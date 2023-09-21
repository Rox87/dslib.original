import os
import tempfile
try:
    os.system('pip install azure-storage-blob -U')
    from azure.storage.blob import BlobServiceClient, BlobClient
except:	
    pass

class blob:

    __storageStrCon=None
    __container=None
    __dir=''

    def __init__(self,storageStrCon:str,container=None,dir=None) -> None:
        self.__storageStrCon=storageStrCon
        self.__blob_service_client = BlobServiceClient.from_connection_string(self.__storageStrCon)

        self.dir(dir,container)

    def dir(self,fake_dir='',container=None):
        self.__container = container if(container!=None) else self.__container
        self.__dir = fake_dir if(fake_dir!=None) else self.__dir

    def close(self):
        try:
            self.__openfile.close()
        except Exception as e:
            ''

    def down(self,blob_filename,fake_dir='',container=None,renew=False):

        self.dir(fake_dir,container)
        
        tmp_dir=tempfile.gettempdir()
        try:
            os.mkdir(os.path.join(tmp_dir,self.__dir))
        except:
            pass
        blob_path = os.path.join(self.__dir,f"{blob_filename}")
        local_file=os.path.join(tmp_dir,blob_path)
        
        if(os.path.isfile(local_file) and not renew):  
                return open(local_file,'rb')
        else:

            blob = BlobClient.from_connection_string(conn_str=self.__storageStrCon, container_name=self.__container, blob_name=blob_path)
            
            self.close()
            
            with open(local_file, "wb") as my_blob:
                blob_data = blob.download_blob()
                blob_data.readinto(my_blob)
                
            self.__openfile = open(local_file,'rb')

            return self.__openfile
   
    def up(self,content,blob_filename,fake_dir=None,container=None,overwrite=True):
        
        self.dir(fake_dir,container)
        blob_path=os.path.join(self.__dir,f"{blob_filename}")

        blob = BlobClient.from_connection_string(conn_str=self.__storageStrCon, container_name=self.__container, blob_name=blob_path)
        
        blob.upload_blob(content,overwrite=overwrite)
    
    def delete(self,blob_filename,container=None,fake_dir=''):
        
        self.dir(fake_dir,container)
        blob_path=os.path.join(self.__dir,f"{blob_filename}")

        blob = BlobClient.from_connection_string(conn_str=self.__storageStrCon, container_name=self.__container, blob_name=blob_path) 
        
        blob.delete_blob()      
    def list(self,container=None,fake_dir='',endWith=''):

            self.dir(fake_dir,container)

            try:     
                container_client = self.__blob_service_client.get_container_client(self.__container)
                blob_list = container_client.list_blobs()
                l = []
                for blob in blob_list:
                    
                    if str(blob.name).count('/') == fake_dir.count('/'):
                        if(str(blob.name).endswith(endWith)):
                            if(str(blob.name).startswith(fake_dir)):
                                l.append(blob)
                return l

            except Exception as e:
                print(e)
    def list_recursive(self,container=None,fake_dir='',endWith=''):

            self.dir(fake_dir,container)

            try:
                  
                container_client = self.__blob_service_client.get_container_client(self.__container)
                blob_list = container_client.list_blobs()
                l = []

                for blob in blob_list:

                    if(str(blob.name).endswith(endWith)):
                        if(str(blob.name).startswith(fake_dir)):
                            l.append(blob)
                return l

            except Exception as e:
                print(e)

    def deleteDir(self,container=None,fake_dir=None):
        self.dir(fake_dir,container)
        p = self.__dir + '/' if fake_dir == '' else fake_dir
        try:     
            container_client = self.__blob_service_client.get_container_client(self.__container)
            blob_list = container_client.list_blobs()

            for blob in blob_list:
                if(str(blob.name).startswith(p)):
                    try:
                        self.delete(blob.name,self.__container,'')
                    except:
                        pass
                    
            return 'done'

        except Exception as e:
            print(e)
    def deleteContainer(self,container=None):

        self.dir('',container)

        try:
            container_client = self.__blob_service_client.get_container_client(container)   
            container_client.delete_container()
            print('container deleted')
        except Exception as e:
                print(e)


    def createContainer(self,container=None):

        self.dir('',container)

        try:
                self.__blob_service_client.create_container(container)
                print('container created')
        except:
            pass