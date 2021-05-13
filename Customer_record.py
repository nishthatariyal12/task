#!/usr/bin/env python
# coding: utf-8

# In[1]:


with open('filename.txt','r') as firstfile, open('second.txt','w') as secondfile:
  line1 = firstfile.readline().strip()
  line2 = firstfile.readline().strip()

  line=line1+line2
  newl=line.replace('|',"\n",1)
  secondfile.write(newl.rstrip())
  for line in firstfile:
    newline=line.replace('|',"\n",1)
    secondfile.write(newline.rstrip())


    


# In[2]:


import pandas as pd


# In[3]:


usersDf =  pd.read_csv('second.txt', sep="|"  , engine='python')


# In[4]:


print('Contents of Dataframe : ')
pd.set_option('max_columns', None)
usersDf


# In[5]:


df = usersDf.iloc[: , 1:]
df


# In[6]:


df[('DOB')] = pd.to_datetime(df[('DOB')]).dt.strftime('%Y%m%d')
df


# In[7]:


pip install mysql-connector-python


# In[8]:


import mysql.connector as msql
from mysql.connector import Error
try:
    conn = msql.connect(host='localhost', user='root',  
                        password='root')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE testdb")
        print("testdb database is created")
except Error as e:
    print("Error while connecting to MySQL", e)


# In[9]:


try:
    conn = msql.connect(host='localhost', 
                           database='testdb', user='root', 
                           password='root')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        cursor.execute('DROP TABLE IF EXISTS CUSTOMERS;')
        print('Creating table....')
        cursor.execute("CREATE TABLE CUSTOMERS(Customer_Name VARCHAR(255) PRIMARY KEY ,Customer_Id int(18) NOT NULL,Open_Date Date NOT NULL,Last_Consulted_Date DATE,Vaccination_Id CHAR(5),Dr_Name CHAR(255),State char(255),Country char(5),DOB date,Is_Active char(1))")
        print("Customer table is created....")
        for i,row in df.iterrows():
            sql = "INSERT INTO testdb.CUSTOMERS VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            conn.commit()
except Error as e:
    print("Error while connecting to MySQL", e)


# In[10]:


sql = "SELECT * FROM testdb.CUSTOMERS"
cursor.execute(sql)
# Fetch all the records
result = cursor.fetchall()
for i in result:
    print(i)


# In[12]:


if (conn.is_connected()):
    cursor.close()
    conn.close()
    print('MySQL connection is closed')


# In[ ]:




