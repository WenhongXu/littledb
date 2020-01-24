import records as rd

class connection:
    def __init__(self, location: str):
        self.db = rd.Database('sqlite:///'+location)
        self.connection = self.db.get_connection()

    def get_table_names(self):
        return self.db.get_table_names()
        #return list(self.connection.query('select name from sqlite_master where type=\'table\''))

    def get_rd_conn(self):
        return self.connection

    def __enter__(self):
        return self.connection

    def __exit__(self,exc_type, exc_val, exc_tb):
        self.connection.close()
        self.db.close()

    def __del__(self):
        self.connection.close()
        self.db.close()

    def desc(self,table_name):
        rows=self.connection.query('pragma table_info({})'.format(table_name))
        print(rows.dataset)
        return rows.dataset

    def sample(self,table_name,limit=10):
        rows=self.connection.query('select * from '+table_name+' limit '+str(limit))
        print(rows.dataset)
        return rows.dataset

    def get_content(self,table_name,objlist,mode='=',**kargs):
        a='select '+','.join(objlist)
        b='where'
        split=' and '
        mode = ' '+mode+' ' if mode=='like' else mode
        listx=[str(key)+mode+str(value) for key,value in kargs]
        rows=self.connection.query(a+' '+b+' '+split.join(listx))
        return rows

    def create_table(self,fields,table_name):
        a='create table '+ str(table_name)
        b='('+',\n'.join([str(x)+' '+str(y) for x,y in fields])+')'
        self.connection.query(a+b)

    def gene_from_excel(self,path,name,header=0,mode='fail',sheet=None):
        '''
        mode in (fail,replace,append)
        '''
        import pandas as pd
        pd.read_excel(path,sheet_name=(sheet if sheet else 0),header=header).to_sql(name,self.connection,if_exists=mode)
        return 0

    def gene_from_csv(self,path,name,header=0,delimiter=',',mode='fail'):
        '''
        mode in (fail,replace,append)
        '''
        import pandas as pd
        pd.read_csv(path,header=header,delimiter=delimiter).to_sql(name,self.connection,if_exists=mode)
        return 0
