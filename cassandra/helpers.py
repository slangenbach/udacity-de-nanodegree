import subprocess
from pprint import pprint
from itertools import zip_longest
import pandas as pd
import numpy as np
import cassandra
from cassandra.query import BatchStatement
from cassandra.cluster import Cluster


def load_all_records(file_list: list, columns: list) -> pd.DataFrame:
    """
    Given a list of files, load each of them as pandas dataframe, optionally
    specifying the columns that should be used. Then combine all previously 
    loaded dataframes into a single dataframe and return it.
    
    :param file_list: List of (csv) files
    :param columns: Columns of files to use when loading files in file_list
    :return: Combined dataframe consisting of all data loaded from files in file_list
    """    
    dfs = [pd.read_csv(f, usecols=columns) for f in file_list]
    combined_df = pd.concat(dfs)
    
    return combined_df
        

def convert_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """
    If the input dataframe has missing values, convert them to None
    and return a copy of the original dataframe in order to insert
    data into Cassandra.
    """
    if df.isnull().any().any():
        converted_df = df.replace(np.nan, None)
        return converted_df
    else:
        return df
    
    
def make_split(data: pd.DataFrame, size: int = 500) -> list:
    """
    Split dataframe into n subframes, where n is determined by number of
    rows of dataframe divided by size
    
    :param data: Data to be split
    :param size: Length of split
    :return: List containing n splitted dataframes
    """
    
    # determine number of batches by dividing rows of dataframe by size
    n = np.ceil(data.shape[0] / size) 
    
    # split data into n sections
    splitted_data = np.array_split(data, n)
    
    return splitted_data
        

def batch_insert(cql: str, cols: tuple, data: pd.DataFrame, size: int, 
                 session: cassandra.cluster.Session):
    """
    Given an Apache Cassandra session, use the provided CQL statement to insert via batches.
    http://datastax.github.io/python-driver/api/cassandra/query.html#module-cassandra.query
    
    :param cql: CQL insert statement
    :param cols: Columns in which data is inserted
    :param data: Data to be inserted
    :param size: Size of batch
    :param session: Apache Cassandra session
    """
    query = session.prepare(cql)
    # convert NaNs to None and subset input data
    converted_data = convert_to_none(data[cols])
    splits = make_split(converted_data, size)
    
    # generate batches
    print(f"Starting batch insert for {converted_data.shape[0]} rows in {len(splits)} batches of size {size}")
    
    for ix, split in enumerate(splits):
        try:
            batch = BatchStatement()
            
            # add rows of splits to batch
            for _, row in split.iterrows():
                batch.add(query, list(row))
            
            session.execute(batch)
            print(f"Inserted {len(split)} rows of data")
        
        except Exception as e:
            print(f"Insert for batch {ix} failed: {e}")
    
    print("Batch insert finished")
        
        
def query(cql: str, session: cassandra.cluster.Session, print_result: bool = True):
    """
    Given CQL query and Cassandra session, send query to
    database, retrieve and return result and optionally (pretty)print it.
    
    :param cql: CQL statement
    :param print_result: Boolean indicating whether to print query result
    :param session: Apache Cassandra session
    """
    res = [e for e in session.execute(cql)]
    
    if print_result:
        pprint(res)
        
    return res


def batch_copy(keyspace: str, table: str, file_path: str, fname: str):
    """
    Given an existing keyspace and table, insert data into table from CSV file via
    Cassandra's shell - it ain't pretty, but it works ;-) 
    https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlshCopy.html
    
    :param keyspace: Cassandra keyspace
    :param table: Cassandra table to copy data to
    :param file_path: Path to directory were file(s) to copy are stored
    :param fname: Name of the file to copy
    """
    cqlsc = f"COPY {keyspace}.{table} FROM '{file_path}/{fname}' WITH HEADER = TRUE"
    try:
        subprocess.check_call(["cqlsh", cqlsc], shell=False)
        print("Batch copy succeeded")
    except Exception as e:
        print(f"Batch copy failed: {e}")