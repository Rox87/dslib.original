import pyodbc as odbc

class pyodbc:

    __cnxn = ''

    def __init__(self,server,database,usuario,senha):
        driver_name = ''
        driver_names = [x for x in odbc.drivers() if x.endswith(' for SQL Server')]
        if driver_names:
            driver_name = driver_names[0]
            self.__cnxn=odbc.connect(f'Driver={driver_name};Server=tcp:{server}.database.windows.net,1433;Database={database};Uid={usuario};Pwd={senha};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')                     
            print('done')
        else:
            print('fail')

    def query_sem_retorno(self,query):
        __cursor = self.__cnxn.cursor()
        __cursor.execute(query)
        self.__cnxn.commit()
        rows = __cursor.rowcount
        print({'rows affected':rows})
        __cursor.close()
    
    def query_com_retorno(self,query):
        __cursor = self.__cnxn.cursor()
        __cursor.execute(query)
        return __cursor.fetchall()

