import os

try:
    from azure.keyvault.secrets import SecretClient
except:
    os.system('pip install azure-keyvault-secrets')
    from azure.keyvault.secrets import SecretClient
try:
  from azure.identity import DefaultAzureCredential
except:
  pass

try:
    from azureml.core import Workspace
except:
    pass

from IPython.display import clear_output

class kvault:

    __client=None
    __Warning=True
    isdatabricks=None
    
    def __init__(self, keyvaultname=None,login=None,senha=None):
        self.__Warning=True

        isdatabricks = self.isRunningInDatabricks()
        
        if(isdatabricks):
          return
        try:
            #AZURE ML
            ws = Workspace.from_config()
            self.__client = ws.get_default_keyvault()
        except:
            try:
                #LOCAL
                self.__localLogin(keyvaultname)
            except:
                try:
                 os.system(f'az login -u {login} -p {senha}')
                 self.__localLogin(keyvaultname)
                except Exception as e:
                    print(str(e))
                
    def __localLogin(self,keyvaultname):
            credential = DefaultAzureCredential(logging_enable=False)
            self.__client = SecretClient(vault_url=f"https://{keyvaultname}.vault.azure.net/", credential=credential)

    def isRunningInDatabricks(self): 
        return "DATABRICKS_RUNTIME_VERSION" in os.environ
    
    def get(self,key,scope="@ds"):
      '''recebe o valor da chave solicitada'''
      try:
        val = self.__client.get_secret(key)._value
        if(self.__Warning):
            clear_output()
            self.__Warning=False
        return val

      except:
      
        try:
            val = self.__client.get_secret(key)
            if(self.__Warning):
                clear_output()
                self.__Warning=False
            return val
      
        except:
      
            #DATABRICKS (READ ONLY)
            try:
                val = os.eval(f"dbutils.secrets.get(scope = {scope}, key = {key})")
                if(self.__Warning):
                    clear_output()
                    self.__Warning=False
                return val
            except:
                pass

    def CurrentKeyVaultURL(self):
        a = self.__client.get_secret('teste').properties.vault_url
        print(f"Current_KeyVault_URL >> {a}")
        return

    # DATABRICKS (NO SET)
    if(not isdatabricks):
      def set(self,secretName,secretValue):
          '''seta a chave para o valor desejado'''

          self.__client.set_secret(secretName,secretValue)
          if(self.__Warning):
                clear_output()
                self.__Warning=False
          print('variável definada')
