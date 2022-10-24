import pyodbc
import pandas

def connect(path):
    try:
        odbc_connection_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;' % (db_path)
        connection = pyodbc.connect(odbc_connection_str)
        print('connected successfully')
        return connection
    except pyodbc.Error as e:
        print('error in connection', e)

db_path = "C:/Users/michalsh/Downloads/TurtlesDB_be.mdb"
connection = connect(db_path)

cursor = connection.cursor()
query = "SELECT * FROM Clutches"
cursor.execute(query)
cursor.columns()
for row in cursor.fetchall():
    print(row)
    
# df= pandas.read_sql('select * from Clutches', connection)