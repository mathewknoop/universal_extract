import os,sys,uuid,sorcery,glob
from sorcery.core import node_names
import pandas as pd
import numpy as np
from collections import defaultdict
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.engine.base import Engine

def db_connect(string):
    #originaly had this wired up to accept a configparser input
    connection = string
    if(os.path.isfile(connection)):
        with open(connection, 'r') as myfile:
            conn_string = myfile.read()
    else:
        conn_string = string
    connection_name = conn_string.split(":")
    engine = create_engine(conn_string)
    print('connecting to: '+str(connection_name[0]))
    return engine

def odbc_execute_query(connection,sql,friendly_name='Executed Query'):
    error = None
    if isinstance(connection, Engine):
        engine = connection
    else:
        engine = db_connect(connection)
    try:
        print('Executing Query: '+friendly_name)
        engine.execute(text(sql).execution_options(autocommit=True))
        print('Executed Query: '+friendly_name)
    except Exception as e:
        error = e
    if error is not None:
        try:
            engine.execute(str(sql))
            print('Executed Query: '+friendly_name)
            error = None
        except Exception as e:
            error = e
    if error is not None:
        print('Failed to Execute Query: '+friendly_name)
        print(error)
    engine.dispose()
    try:
        del engine
    except:
        pass
    print('-----')
    return error

def odbc_get_query(query,connection,normalize=True):
    if isinstance(connection, Engine):
        engine = connection
    else:
        engine = db_connect(connection)
    import pandas as pd
    
    if normalize ==True:
        rs_df = normalize_column_names(pd.read_sql(str(query),con=engine))
    else:
        rs_df = pd.read_sql(str(query),con=engine)
    engine.dispose()
    try:
        print(rs_df.dtypes)
        del engine
    except:
        pass
    return rs_df

def pretty_print(data):
    import json
    import pprint
    if hasattr(data, '__dict__'):
        pp = pprint.PrettyPrinter(indent=3)
        pp.pprint(vars(data))
    else:
        try:
            print(json.dumps(data, indent=3, sort_keys=False))
        except:
            try:
                pp = pprint.PrettyPrinter(indent=3)
                pp.pprint(data)
            except:
                pass
    return

def pprint(data=''):
    pretty_print(data)
    return

def threaded(threaded_func,input_objects,threads=None):
    import os
    from multiprocessing.pool import ThreadPool as Pool
    if threads == None:
        threads = os.sched_getaffinity(0)
    with Pool(len(threads)) as p:
        p.map(threaded_func,input_objects)

def dedupe_cols(frame):
    renamer = defaultdict()
    for column_name in frame.columns[frame.columns.duplicated(keep=False)].tolist():
        if str(column_name) not in renamer:
            renamer[column_name] = list()
            renamer[column_name].append(str(str(column_name)+'_0'))
        else:
            renamer[column_name].append(str(column_name) +'_'+str(len(renamer[str(column_name)])))
    cols = list()
    for column in frame:
        if column in renamer:
            cols.append(renamer[column].pop(0))
        else:
            cols.append(str(column))
    frame.columns = cols
    return frame

def normalize_column_names(df):
    if isinstance(df, pd.DataFrame):
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_', regex=False).str.replace('(', '', regex=False).str.replace(')', '', regex=False).str.replace('%', 'percent',regex=False).str.replace('.', '',regex=False).str.replace(':', '',regex=False).str.replace('-', '', regex=False).str.replace('__', '_', regex=False)
        df = dedupe_cols(df)
        try:
            df.reset_index(level=0, inplace=True)
        except:
            pass
        cols = list(df.columns.values)
        try:
            cols.remove('index')
        except:
            pass
        try:
            cols.remove('level_0')
        except:
            pass
        return df[cols]
    else:
        print('was passed a object of type '+str(type(df))+' instead of dataframe.')

class pickle:
    def __init__(self,file,quiet=True):
        import os
        print(file)
        self.file = str(file)
        self.object = None
        self.quiet = quiet
        if os.path.isfile(file):
            self.load()
       
    def save(self,obj=None):
        import pickle
        with open(self.file,'wb') as f:
            if self.quiet == False:
                pprint(obj)
            pickle.dump(obj,f)
        
    def load(self):
        import pickle
        with open(self.file,'rb') as f:
            obj = pickle.load(f)
            if self.quiet == False:
                pprint(obj)
            self.object = obj
            return obj
        
class ue:
    def __init__(self,log=False,verbose=False,preserve_memory=True):
        import uuid
        self.preserve_memory = preserve_memory
        self.log = log
        self.verbose = verbose
        self.script = os.path.basename(sys.argv[0])
        self.guid = str(str(uuid.uuid4()).replace("-", ""))  
        self.odbc_source = None
        self.odbc_destination = None
        self.dataframes = {}
        self.df = None
        self.table = None
        self.schema = 'public'
        self.step_number = 0
        self.context = {}
        self.history = {}
        
    def count(self,variable_name=None,table_name=None):
        self.step_number = self.step_number + 1
        print('executing step number: '+str(self.step_number))
        if variable_name:
            print('result dataframe is named: '+str(variable_name))
        if table_name:
            print('result database table is named: '+str(table_name))

        
    def set_connections(self):
        if hasattr(self, 'odbc_source_string'):

            if self.odbc_source_string is not None:
                self.odbc_source_connection = db_connect(self.odbc_source_string)
                self.odbc_source_string = None
                
        if hasattr(self, 'odbc_destination_string'):
            if self.odbc_destination_string is not None:
                self.odbc_destination_connection = db_connect(self.odbc_destination_string)
                self.odbc_destination_string = None
            
    def history(self,sql):
        pass
   
    @sorcery.spell
    def duckdb_query(self,frame_info,sql,tables):
        if sql:
            variable_name = node_names(frame_info.call.parent.targets[0])[0]
            self.count(variable_name=variable_name)
            self.context_dataframes = {}
            if tables:
                if not isinstance(tables, dict):
                    
                    arg_node = frame_info.call.args[1]
                    tables_dict = dict(zip(node_names(arg_node), tables))

                #opportunity for multithreading here!
                dataframes = {}
                for table in tables_dict.keys():
                    ndf = normalize_column_names(tables_dict[table])
                    if not self.preserve_memory:
                        self.dataframes[table]=ndf
                    self.context_dataframes[table]=ndf
            #execute duckdb query!
            #need to improve error handling and sql escaping

            

            print(str(sql))
            df = normalize_column_names(eval('exec("import duckdb") or duckdb.sql("'+str(sql)+'").fetchdf()',{},self.context_dataframes))
            print('using the following source dataframes in query context: '+str(self.context_dataframes.keys()))
            print()
            self.context_dataframes.pop('duckdb', None)
            inputs = self.context_dataframes
            output = df
            self.df = df
            
            self.context[self.step_number]={"inputs":inputs,
                                            "sql":sql,
                                            "output":output}
            
            self.dataframes[variable_name] = df
            self.dataframes.pop('duckdb', None)
            
            
            if self.verbose:
                self.preview()
            if self.log:
                self.log_context()
            return df
        
    def log_context(self):
        #pprint(self.context)
        self.history[self.step_number] = self.context[self.step_number]
        print('logging context for step number: '+str(self.step_number))
        #context = pickle(str(self.guid)+'-'+str(self.step_number))
        #context.save(obj=self.context[self.step_number])
        print()
        
        
        
        pass
    
    @sorcery.spell
    def odbc_query(self,frame_info,sql=None,odbc_source=None,chunksize=150000):
        if odbc_source:
            self.odbc_source_string = odbc_source
        self.set_connections()
        
        variable_name = node_names(frame_info.call.parent.targets[0])[0]
        self.count(variable_name=variable_name)
        if self.odbc_source_connection:
            file_exists = os.path.exists(sql)
            if file_exists:
                with open(sql, 'r') as myfile:
                    query = myfile.read()
                    myfile.close()
            elif isinstance(sql, str):
                query = str(sql)
            else:
                print('query is not a filepath or string')
                
            files = []
            file_no = []
            chunk_no = 0
            for chunk in pd.read_sql_query(query,self.odbc_source_connection, chunksize=chunksize):
                file = "/tmp/part_"+str(self.guid)+"_"+str(chunk_no)+".feather"
                chunk.to_feather(file)
                files.append(file)
                file_no.append(chunk_no)
                chunk_no = chunk_no + 1
            #self.table = str(table)
            self.files = files
            #self.file_no = file_no
            print('extracted '+str(chunk_no)+' chunks')
            self.transform()
            self.combine()
            self.cleanup()
            print('extracted dataframe to '+str(variable_name))
            print()
            self.dataframes[variable_name] = self.df
            
            self.context[self.step_number]={"sql":sql,
                                            "output":self.df}
            if self.log:
                self.log_context()
            self.dataframes[variable_name] = self.df
            if self.verbose:
                self.preview()
            return self.df

            
    def combine(self):
        df = pd.DataFrame()
        for f in self.files:
            dfa = pd.read_feather(f)
            df = pd.concat([df, dfa], verify_integrity=False)
        print('dataframe shape '+str(df.shape))
        #print(df.dtypes)
        df = df.reset_index(drop=True)
        self.dataframes[self.table] = df
        self.df = df
        del df
        
    def transform(self):
        import pandas as pd
        from multiprocessing.pool import ThreadPool as Pool
        from multiprocessing import cpu_count
        def process_chunk(f):
            df = pd.read_feather(f)
            df = normalize_column_names(dedupe_cols(df))
            df.to_feather(f)
            
        with Pool(cpu_count()) as p:
            p.map(process_chunk,self.files)
        #self.files = None
        
    def load(self,dataframe=None,odbc_destination=None,table=None,dtypes=None,chunksize=10000):
        if odbc_destination:
            self.odbc_destination_string = odbc_destination
        self.set_connections()
  
        if table is not None:
            #print('loading to table: '+str(table.lower()))
            dest = table.split(".", 1)
            if len(dest)>1:
                schema = dest[0].lower()
                table = dest[1].lower()
            else:
                schema = 'public'
                
            self.table = table
            self.count(table_name=table)
            self.schema = schema.lower()
            if dtypes:
                self.df.to_sql('temp_'+str(self.guid),schema=self.schema, con=self.odbc_destination_connection, if_exists='replace',index=False,chunksize=chunksize,method='multi',dtype=dtypes)
            else:
                self.df.to_sql('temp_'+str(self.guid)+'_'+str(self.step_number),schema=self.schema, con=self.odbc_destination_connection, if_exists='replace',index=False,chunksize=chunksize,method='multi')
            replace_sql = '''BEGIN;
DROP TABLE IF EXISTS "'''+self.schema+'"."'+self.table+'''";
ALTER TABLE '''+str(self.schema)+'''."temp_'''+str(self.guid)+'_'+str(self.step_number)+'''" RENAME TO "'''+self.table+'''";
COMMIT;'''
            print(replace_sql)
            odbc_execute_query(self.odbc_destination_connection, replace_sql,friendly_name='replacing destination table')
            print('loaded '+str(len(self.df))+' rows to table: '+str(table) +' and schema '+str(schema))
            
            self.context[self.step_number]={
                                            "input":self.df}
            self.dataframes[str(table)] = self.df
            if self.verbose:
                self.preview()
            if self.log:
                self.log_context()

            
    def transact(self,query=None,destination=None):
        if self.destination is None:
            self.destination = db_connect(destination)
        error = odbc_execute_query(self.destination,query)
        print('Ran transact query against Engine')
        if error:
            raise Exception('Failed to run query')
            
    def preview(self):
        pprint(self.context[self.step_number])
        print()
        
    def reset(self):
        pass
        
    def cleanup(self):
        for f in glob.glob("/tmp/*"+str(self.guid)+"*"):
            os.remove(f)
        self.table = None
        self.files = None
        
            
    def __del__(self):
        self.cleanup()
