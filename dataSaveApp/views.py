from datetime import datetime
import json
import uuid
from functools import reduce

import redis

from django.http import JsonResponse
from django.shortcuts import render

# Create your views here.
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from past.builtins import cmp
from dataSaveApp.util import *
from dataSaveApp.dbutil import *

r = redis.Redis(host="localhost", port=6379, decode_responses=True)


def requestTime():
    return datetime.strftime(datetime.now() + timedelta(days=0), "%Y-%m-%d %H:%M:%S")


@csrf_exempt
@require_http_methods(["POST"])
def saveData(request):
    """
    保存传入数据到数据库
    :param request: 请求体，其中传入json数据{}
    :return: 返回请求状态，1001为传入参数不合法，1002为接口id不存在，0为正确传输并存入数据
    """
    params_columns_list = []
    table_columns_list = []
    body = request.body.decode('utf-8')
    try:
        body = json.loads(body)
    except Exception as e:
        return JsonResponse({'error': "1001", "message": "未传入合法JSON格式数据。", "timeStamp": requestTime()})
    try:
        params_struct = json.loads(r.hget("ParamsStruct", body.get("interfaceId")))
    except Exception as e:
        return JsonResponse({'error': "1002", "message": "接口id不存在。", "timeStamp": requestTime()})
    table_name = params_struct.get("tableName")
    data_source_id = params_struct.get("dataSourceId")

    # 传入参数columns
    for i in body.get("datas").keys():
        params_columns_list.append(i)
    # 数据库应传入参数columns
    params_columns = params_struct.get("columns")
    for i in params_columns:
        table_columns_list.append(i.get("column"))
    # print(table_columns_list)
    # 校验传入参数与参数是否一致
    if cmp(table_columns_list, params_columns_list) != 0:
        not_input_params = [x for x in table_columns_list if x not in params_columns_list]
        error_input_params = [x for x in params_columns_list if x not in table_columns_list]
        if len(not_input_params) == 0 and len(error_input_params) != 0:
            return JsonResponse({
                "error": "1031",
                "message": "错误传入以下参数：" + reduce(lambda x, y: "{},{}".format(x, y), error_input_params),
                "timeStamp": requestTime()})
        elif len(not_input_params) != 0 and len(error_input_params) == 0:
            return JsonResponse({
                "error": "1032",
                "message": "未传入以下参数：" + reduce(lambda x, y: "{},{}".format(x, y), not_input_params),
                "timeStamp": requestTime()})
        elif len(not_input_params) != 0 and len(error_input_params) != 0:
            return JsonResponse({
                "error": "1033",
                "message": "未传入以下参数：" + reduce(lambda x, y: "{},{}".format(x, y), not_input_params) +
                           "。错误传入以下参数：" + reduce(lambda x, y: "{},{}".format(x, y), error_input_params),
                "timeStamp": requestTime()})
    # body传入值并写入sql value
    insert_data = ""
    for i in body.get("datas").keys():
        columns = params_struct.get("columns")
        for j in columns:
            if j.get("column") == i:
                if j.get("type") not in ["int", "bigint", "decimal"]:
                    insert_data += "'{}',".format(body.get("datas").get(i))
                else:
                    insert_data += "{},".format(body.get("datas").get(i))
    insert_data = insert_data[0:-1]
    # 生成插入sql
    sql = "insert into " + table_name + \
          " (" + reduce(lambda x, y: "{},{}".format(x, y), params_columns_list) + ",biz_load_time) values" + \
          " (" + insert_data + ",'" + requestTime() + "');"
    print(sql)
    # 连接数据源并插入数据
    data_source = json.loads(r.hget("DBStruct", data_source_id))
    username = data_source.get("conf").get("username")
    password = decrypt(data_source.get("conf").get("password").encode("utf-8"))
    host = data_source.get("conf").get("host")
    db = data_source.get("conf").get("db")
    port = data_source.get("conf").get("port")
    # if data_source.get("dbtype") == "mysql":
    #     conn = conn_mysql(username, db, password, host, port)
    use_db(conn_mysql(username, db, password, host, port), sql)
    return JsonResponse({'code': "200", "timeStamp": requestTime()})


@csrf_exempt
@require_http_methods(["POST"])
def initTable(request):
    interface_id = str(uuid.uuid4())
    table_struct = request.body.decode('utf-8')
    try:
        table_struct = json.loads(table_struct)
    except Exception as e:
        return JsonResponse({'error': "1001", "message": "未传入合法JSON格式数据。", "timeStamp": requestTime()})

    # 校验数据源是否存在表
    table_struct_all = r.hgetall("TableStruct")
    for key in table_struct_all.keys():
        if json.loads(table_struct_all.get(key)).get("dataSourceId") == table_struct.get("dataSourceId") and \
                json.loads(table_struct_all.get(key)).get("tableName") == table_struct.get("tableName"):
            return JsonResponse({'error': "1002", "message": "表已存在", "timeStamp": requestTime()})

    # 增加插入时间
    table_struct.get("columns").append({
        "column": "insertTime",
        "type": "timestamp",
        "length": 0,
        "autoIncrease": "False",
        "comment": "插入时间"
    })
    # 输入参数sql
    columns = table_struct.get("columns")
    init_sql = "create table {} (\n".format(table_struct.get("tableName"))
    for column in columns:
        init_sql += "    {} {}".format(column.get("column"), column.get("type"))
        if column.get("length") != 0:
            init_sql += "({})".format(column.get("length"))
        if column.get("autoIncrease") == "True":
            init_sql += "    AUTO_INCREMENT"
        init_sql += "   COMMENT '{}'".format(column.get("comment"))
        init_sql += ",\n"

    params_struct = table_struct
    primary_keys = []
    primary_keys_index = []
    auto_increase_len = 0
    for index, i in enumerate(params_struct.get("columns")):
        if i.get("autoIncrease") == "True":
            auto_increase_len += 1
            primary_keys.append(i.get("column"))
            primary_keys_index.append(index)
    if auto_increase_len > 1:
        return JsonResponse({'error': "1003", "message": "传入多个自增字段。", "timeStamp": requestTime()})
    primary_keys_index.reverse()
    for index in primary_keys_index:
        params_struct.get("columns").pop(index)

    # print(params_struct)
    # 写入参数结构
    r.hset("ParamsStruct", interface_id, json.dumps(params_struct))

    # 增加落库时间
    table_struct.get("columns").append({
        "column": "biz_load_time",
        "type": "varchar",
        "length": 255,
        "autoIncrease": "False",
        "comment": "落库时间"
    })
    # 写入表结构
    r.hset("TableStruct", interface_id, json.dumps(table_struct))

    # 获取数据库信息，获取连接池
    data_source = json.loads(r.hget("DBStruct", table_struct.get("dataSourceId")))
    username = data_source.get("conf").get("username")
    password = decrypt(data_source.get("conf").get("password").encode("utf-8"))
    host = data_source.get("conf").get("host")
    db = data_source.get("conf").get("db")
    port = data_source.get("conf").get("port")
    # if data_source.get("dbtype") == "mysql":
    #     conn = conn_mysql(username, db, password, host, port)
    conn = conn_mysql(username, db, password, host, port)
    # 在数据库中新建表
    init_sql += "    biz_load_time varchar(255) COMMENT '落库时间'\n"
    if len(primary_keys) != 0:
        init_sql += "   ,PRIMARY KEY ("
        if len(primary_keys) == 1:
            init_sql += "{}".format(primary_keys[0])
        else:
            temp_sql = ""
            for key in primary_keys:
                temp_sql += "{},".format(key)
            temp_sql = temp_sql[0:-1]
            init_sql += temp_sql
        init_sql += ")"
    else:
        pass
    init_sql += ") COMMENT='{}';".format(table_struct.get("tableComment"))
    print(init_sql)
    use_db(conn, init_sql)
    return JsonResponse({'code': "200", 'interfaceId': interface_id, "timeStamp": requestTime()})


@csrf_exempt
@require_http_methods(["POST"])
def addDataSource(request):
    data_source_body = request.body.decode('utf-8')
    try:
        data_source = json.loads(data_source_body)
    except Exception as e:
        return JsonResponse({'error': "1001", "message": "未传入合法JSON格式数据。", "timeStamp": requestTime()})
    conf = data_source.get("conf")
    # 校验数据源是否存在
    db_struct = r.hgetall("DBStruct")
    for key in db_struct.keys():
        struct = json.loads(db_struct.get(key))
        if conf.get("username") == struct.get("conf").get("username") and \
                conf.get("host") == struct.get("conf").get("host") and \
                conf.get("db") == struct.get("conf").get("db") and \
                conf.get("port") == struct.get("conf").get("port") and \
                data_source.get("dbtype") == struct.get("dbtype"):
            return JsonResponse({'error': "1002", "message": "数据源已存在", "timeStamp": requestTime()})

    print(data_source)
    try:
        conn_mysql(conf.get("username"), conf.get("db"), conf.get("password"), conf.get("host"),
                   conf.get("port"))
    except Exception as e:
        return JsonResponse({'error': "1001", "message": str(e), "timeStamp": requestTime()})
    conf["password"] = encrypt(conf.get("password")).decode("utf-8")
    data_source_id = str(uuid.uuid4())
    r.hset("DBStruct", data_source_id, data_source_body)
    return JsonResponse({'code': "200", "data_source_id": data_source_id, "timeStamp": requestTime()})


@csrf_exempt
def token(request):
    pass
