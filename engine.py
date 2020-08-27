from collections import defaultdict
import mysql.connector
import re
import math
import json
import os

DOC_NUM = 23503

mydb = mysql.connector.connect(
    host = 'localhost',
    user = 'root',
    passwd = '123456',
    database = 'CS121')

mycursor = mydb.cursor()

def calculate(tf:int,df:int) -> float:
    global DOC_NUM
    return (1+math.log10(tf))*math.log10(DOC_NUM/df)

def search(query:str) -> "list":
    query = re.sub('[\'\"!@#$%^&*()_+,=]',' ',query)
    query = query.split()
    d = defaultdict(float)
    for t in query:
        #print(t)
        sql = f"""SELECT documentID, termfrequency
FROM TermFrequency WHERE term = '{t}'"""
        mycursor.execute(sql)
        #mydb.commit()
        myresult = mycursor.fetchall()
        #print(len(myresult))
        for i in myresult:
            tfidf = calculate(i[1],len(myresult))
            d[i[0]] += tfidf;
##    for k,v in d.items():
##        print(k,v)
    top_ten = sorted(d.items(),key=lambda x:x[1],reverse = True)[:20]
    l = []
    
    for i in top_ten:
        i = i[0].split('\\')
        i = '/'.join([i[-2],i[-1]])
        l.append(i)

    return l, len(myresult)

        
##def main():
##    search('artificial intelligence')
##
##main()






