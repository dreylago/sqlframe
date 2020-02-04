from sqlalchemy import create_engine
import pandas as pd


#------------------------------------------------------------------------------#

sql_engines = dict()

class DataFrame():

    #--------------------------------------------------------------------------#

    def __init__(self, data, alias='t', if_exists='replace', namespace='global',
                 **kwargs):
        """
            Initializes a frame

            data: list of records for pandas DataFrame constructor
            **kwargs: arguments to pandas DataFrame constructor

            alias: alias of the table in sql domain
            if_exist: ['replace','append']
            namespace: sql connector name

        """
        if namespace not in sql_engines:
            sql_engines[namespace] = create_engine('sqlite://', echo=False)
        self.engine = sql_engines[namespace]
        df = pd.DataFrame(data, **kwargs)
        self.columns = list(df.columns)
        self.index_name = df.index.name
        self.alias = alias
        df.to_sql(alias, con=self.engine, index=False, if_exists=if_exists)

    #--------------------------------------------------------------------------#

    def get_all(self, query):
        """
            Returns list of rows from a sql query result

        """
        res = self.engine.execute(query)
        return list(dict(x) for x in res)

    #--------------------------------------------------------------------------#

    def __len__(self):
        """
            Length of data frame
        """
        return self.get_all(f'select count(1) as cnt from {self.alias}')[0]['cnt']

    #--------------------------------------------------------------------------#

    def get_one(self, query):
        """
            Returns one row from a sql query result

        """
        res = self.engine.execute(query)
        for row in res:
            return dict(row)

        return None

    #--------------------------------------------------------------------------#

    def execute(self, query):
        """
            Executes a query w/o returning result

        """
        self.engine.execute(query)

    #--------------------------------------------------------------------------#

    def get_iter(self, query:str):
        """
            Gets iterator from query

        """
        return self.engine.execute(query)

    #--------------------------------------------------------------------------#

    def get_dict(self, query:str, levels:list=None):
        """
            Returns a multilevels dictionary from query

            levels: column names corresponding to levels of the dictionary
                    (default: ordered list of columns of query minus last one)

            This is intented for 'group by' queries

        """
        data = dict()
        keys = None
        aggs = None
        for _row in self.get_iter(query):

            row = dict(_row)

            if keys is None:
                keys = list(row.keys())
            if levels is None:
                levels = keys[:-1]
            if aggs is None:
                aggs = keys[len(levels):]

            p = data
            for key in levels[:-1]:
                if row[key] not in p:
                    p[row[key]] = dict()
                p = p[row[key]]

            key = levels[-1]

            if len(aggs) == 1:
                p[row[key]] = row[aggs[0]]
            else:
                if row[key] not in p:
                    p[row[key]] = dict()
                p = p[row[key]]
                for key in aggs:
                    p[key] = row[key]

        return data

    #--------------------------------------------------------------------------#

    def to_parquet(self, fname, **kwargs):

        """
            Write to parquet file

        """
        data = self.get_all(f'select * from {self.alias}')
        df = pd.DataFrame(data)
        df.to_parquet(fname, **kwargs)

    #--------------------------------------------------------------------------#

    def to_csv(self, fname, index=False, **kwargs):

        """
            Write to csv file

        """
        data = self.get_all(f'select * from {self.alias}')
        df = pd.DataFrame(data)
        df.to_csv(fname, index=index, **kwargs)


#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#
#------------------------------------------------------------------------------#

def read_parquet(fname, alias='t', **kwargs):

    df = pd.read_parquet(fname, **kwargs)
    return DataFrame(df, alias=alias)

#------------------------------------------------------------------------------#

def read_csv(fname, alias='t', **kwargs):

    df = pd.read_csv(fname, **kwargs)
    return DataFrame(df, alias=alias)
