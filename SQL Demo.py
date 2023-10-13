import pandas as pd
import sqlite3

### SQL Database connection info

### Create a database
conn = sqlite3.connect('X:\DB\my_database.sqlite')

# Below is a temporary DB that can be used
#conn = sqlite3.connect(':memory:') 

cur = conn.cursor()

# define input and output files

output = "G:\My Drive\My Projects"
input0 = "G:\My Drive\My Projects\Apple Store Dataset\Copy of AppleStore.csv"
input1 = "G:\My Drive\My Projects\Apple Store Dataset\Copy of appleStore_description1.csv"
input2 = "G:\My Drive\My Projects\Apple Store Dataset\Copy of appleStore_description2.csv"
input3 = "G:\My Drive\My Projects\Apple Store Dataset\Copy of appleStore_description3.csv"
input4 = "G:\My Drive\My Projects\Apple Store Dataset\Copy of appleStore_description4.csv"

#create an input list 

inputlist = ["input0","input1","input2","input3","input4"]

# test the input list

for x in inputlist:
  print(x)

#place csv in df, then df in SQL DB

df0 = pd.read_csv(input0)
df1 = pd.read_csv(input1)
df2 = pd.read_csv(input2)
df3 = pd.read_csv(input3)
df4 = pd.read_csv(input4)

df0.to_sql(name='Apps', con=conn, if_exists='replace')
df1.to_sql(name='descriptionfile1', con=conn, if_exists='replace')
df2.to_sql(name='descriptionfile2', con=conn, if_exists='replace')
df3.to_sql(name='descriptionfile3', con=conn, if_exists='replace')
df4.to_sql(name='descriptionfile4', con=conn, if_exists='replace')

# Union all description tables into 1 table 

### NEED TO CREATE CONTROL TABLE WITH COUNTS OF 4 DESC TABLES

cur.executescript("""
DROP TABLE IF EXISTS Descriptionfile;                 
CREATE TABLE Descriptionfile AS 
                  SELECT * FROM descriptionfile1 
                  UNION 
                  SELECT * FROM descriptionfile2
                  UNION 
                  SELECT * FROM descriptionfile3
                  UNION 
                  SELECT * FROM descriptionfile4
                  ;

DROP TABLE IF EXISTS Control;                 

CREATE TABLE Control AS 
                  Select count(*),"file1" as file FROM descriptionfile1; 
""")

# print column names for apps table

# Execute the SELECT * FROM table_name; query
cur.execute('SELECT * FROM Apps')

# Get the column names from the cursor object's description attribute
column_names = cur.description


# Print the column names
for column_name in column_names:
    print(column_name[0])

# print column names for Descriptionfile table


cur.execute('SELECT * FROM Descriptionfile')
column_names = cur.description

# Print the column names
for column_name in column_names:
    print(column_name[0])


# SQL query to list all tables
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cur.fetchall())

# SQL query for row counts 
print("Table data :")
cur.execute("SELECT count(*) FROM Descriptionfile") 
print(cur.fetchall())

# SQL query for ID count
cur.execute("SELECT count(distinct id) as AppIds FROM Descriptionfile") 
print(cur.fetchall())

# SQL query for ID count
cur.execute("SELECT count(distinct id) as AppIds FROM Apps") 
print(cur.fetchall())

# Execute the SELECT * FROM table_name; query
cur.execute('SELECT * FROM Descriptionfile')

# Get the column names from the cursor object's description attribute
column_names = cur.description

# Print the column names
for column_name in column_names:
    print(column_name[0])


# SQL query for any missing values in key fields
cur.execute("""SELECT count(*) as MissingValues 
            FROM Descriptionfile
            WHERE track_name is null or app_desc IS null
            """) 
print(cur.fetchall())

# SQL query for any missing values in key fields
cur.execute("""SELECT count(*) as MissingValues 
            FROM Descriptionfile
            WHERE track_name is null 
            """) 
print(cur.fetchall())


# SQL query for genre
cur.execute("""SELECT prime_genre, count(*) as NumApps 
            FROM Apps
            GROUP BY prime_genre
            ORDER BY NumApps desc
            """) 
print(cur.fetchall())

# SQL query for user ratings
cur.execute("""
            SELECT 
            min(user_rating) as MinRating, 
            max(user_rating) as MaxRating,
            avg(user_rating) as AvgRating
            FROM Apps
            """) 
print(cur.fetchall())

# SQL query for user ratings
cur.execute("""
            SELECT 
            min(user_rating) as MinRating, 
            max(user_rating) as MaxRating,
            avg(user_rating) as AvgRating
            FROM Apps
            """) 
print(cur.fetchall())

#

cur.execute("""
            SELECT 
            (price / 2) * 2 as test 
            FROM Apps
            """) 
print(cur.fetchall())





# Close the connection to the SQLite database
conn.close()
