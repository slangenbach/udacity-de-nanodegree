import os
import glob
import psycopg2
import pandas as pd


def get_files(filepath: str) -> list:
    """
    Load all *.json files found in filepath and its subdirectories.
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files


def head_table(table: str, cur: psycopg2.extensions.cursor, n: int = 5):
    """
    Return the n-first rows of a given table
    """
    try:
        cur.execute(f"SELECT * FROM {table} LIMIT {n}")
        return [e for e in cur.fetchall()]
    except psycopg2.Error as e:
        print(f"Error: {e}")
        

def load_all_records(file_list: list, columns: list, to_disk: bool = False, file_path: str = None,
                     fname: str = None) -> pd.DataFrame:
    """
    Return a dataframe containing data (specified by columns)
    from all files in filelist and optionally save it to disk.
    """
    dfs = []
    
    for f in file_list:
        df = pd.read_json(f, orient="records", typ="frame", lines=True)
        dfs.append(df[columns])
        
    combined_df = pd.concat(dfs)
    
    if to_disk:
        combined_df.to_csv(f"{file_path}/{fname}.csv", index=False)
    
    return combined_df


def drop_and_load(query_list: list, data: pd.DataFrame, cur: psycopg2.extensions.cursor):
    """
    Execute all queries in query list.
    """
    for query in query_list:
        try:
            # mind the newline character o_O
            if query.startswith("\nINSERT"):
                    for _, row in data.iterrows():
                        cur.execute(query, list(row))
            else:
                cur.execute(query)

        except psycopg2.Error as e:
            print(e)
            

def bulk_insert(table: str, file_path: str, fname: str, cur: psycopg2.extensions.cursor):
    """
    Insert data from a CSV file on disk into a table.
    """
    try:
        cur.execute(f"COPY {table} FROM '{file_path}/{fname}.csv' WITH CSV HEADER")
        print("Bulk insert succeeded")
    except psycopg2.Error as e:
        print(f"Bulk insert failed: {e}")           