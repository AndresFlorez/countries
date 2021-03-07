import sqlite3



class Database:

    
    def __init__(self, name=None):
        
        self.conn = None
        self.cursor = None

        if name:
            self.open(name)

    
    def open(self, name):
        
        try:
            self.conn = sqlite3.connect(name)
            self.cursor = self.conn.cursor()

        except sqlite3.Error as e:
            print("Error connecting to database!")

    
    def close(self):
        
        if self.conn:
            self.conn.commit()
            self.cursor.close()
            self.conn.close()


    def __enter__(self):
        
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        
        self.close()
    
    def create_table(self, table_name, fields=None):
        """create a database table if it does not exist already"""
        str_fields = ', \n    '.join(fields)
        query = """
CREATE TABLE IF NOT EXISTS {table_name}(
    {str_fields}    
)
        """.format(table_name=table_name, str_fields=str_fields)
        self.cursor.execute(query)

    def datraframe_to_db(self, database, table, dataframe):
        if dataframe is None or dataframe.empty or not table or not database: return False
        dataframe.to_sql(table, con=self.conn,
                             index=False, if_exists='append')
        return True
