from django.shortcuts import redirect
import pymysql
import json
from datetime import datetime, timedelta


def conn_mysql(user, db, password, host, port):
    conn = pymysql.connect(user=user, db=db, password=password, host=host, port=port, charset='utf8')
    return conn


def use_db(connect, sql):
    conn = connect
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data
