import pandas as pd
from azure.data.tables import TableServiceClient

class storTable:
    table_service = None
    def __init__(self,con_str,tableName):
        self.set_client(con_str)
        self.set_table(tableName)

    def set_client(self,conn_str):
        self.con = TableServiceClient.from_connection_string(conn_str=conn_str)
 
    def set_table(self,tableName):
        """ After client  """
        try:
            self.table_service = self.con.get_table_client(tableName)
        except Exception as e:
            print(str(e))
        

    def get_dataframe(self,query_filter):
        """ Create a dataframe from table storage data """
        return pd.DataFrame(self.get_data(query_filter=query_filter))

    def get_data(self,query_filter):
        """ Retrieve data from Table Storage """
        for record in self.table_service.query_entities(query_filter=query_filter):
            yield record
