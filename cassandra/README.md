# Data modeling data with Apache Cassandra

## About
The scripts (c.f. ```helpers.py``` ) and notebooks within this repository perform ETL (extract, transform, load) for
event data found in the context of a music-streaming service (Sparkify). After individual files stored on the local 
filesystem in CSV-format have been processed into a suitable format, three Cassandra tables are created to enable to 
answer specific queries (c.f. ```queries.py```).  

In particular, the following tables will be created:

| table | columns |
--- | ---
| song_playlist_session | sessionId, itemInSession, artist, song , length |
| song_playlist_user | userId, name, sessionId, itemInSession, artist, song, firstName, lastName |
| song_user_name | song, userId, firstName, lastName |


## Prerequisites
* Access to a Apache Cassandra database with create/drop/insert/update rights
* Python 3.6+
* Python packages jupyter, [cassandra](https://datastax.github.io/python-driver/) and pandas
* Unix-like environment (Linux, macOS, WSL on Windows)

## Usage
Start jupyter notebook
```
jupyter notebook
```
Open ```cassandra.ipynb``` and run all cells


## Limitations
* For now, all code in embedded in Jupyter notebooks and can not be run directly from the command line.

## Ressources
* [Python Casssandra Driver](https://datastax.github.io/python-driver/)
* [Cassandra CQL documentation](https://docs.datastax.com/en/dse/6.7/cql/)
* [Cassandra data types](https://docs.datastax.com/en/dse/6.7/cql/cql/cql_reference/refDataTypes.html)