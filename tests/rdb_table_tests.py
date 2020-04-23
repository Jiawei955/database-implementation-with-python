from src.RDBDataTable import RDBDataTable
import pymysql
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def t1():
    c_info = dict(host="localhost", user="dbuser", password="dbuserdbuser",
                  db="lahman2019raw", cursorclass = "pymysql.cursors.DictCursor")

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    print("RDB table = ", r_dbt)




def test_find_by_template():
    tmp = {"birthYear": "1985"}
    c_info = {
        "host": "localhost",
        "user": "root",
        "password": "ZJWcph0a",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    a = r_dbt.find_by_template(tmp,["birthYear","birthMonth"])
    print(a)

def test_find_by_primary_key():
    c_info = {
        "host": "localhost",
        "user": "root",
        "password": "ZJWcph0a",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    primary_key = r_dbt.find_by_primary_key(["abernte01"])
    print(primary_key)

def test_update_by_template():
    c_info = {
        "host": "localhost",
        "user": "root",
        "password": "ZJWcph0a",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }
    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    tmp1 = {"playerID": "aaronha01"}
    new_value = {"birthYear": "2000"}
    print(r_dbt.find_by_template(tmp1))
    a = r_dbt.update_by_template(tmp1,new_value)
    print(r_dbt.find_by_template(tmp1))

def test_insert():
    c_info = {
        "host": "localhost",
        "user": "root",
        "password": "ZJWcph0a",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }
    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    new_value = {"playerID": "Jiawei", "birthYear": "2000"}
    print(r_dbt.find_by_template(new_value))
    r_dbt.insert(new_value)
    print(r_dbt.find_by_template(new_value))

def test_delete_by_template():
    c_info = {
        "host": "localhost",
        "user": "root",
        "password": "ZJWcph0a",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }
    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    tmp = {"playerID": "abbeybe01", "birthYear": "1869"}
    print(r_dbt.find_by_template(tmp))
    r_dbt.delete_by_template(tmp)
    print(r_dbt.find_by_template(tmp))




def test_delete_by_key():
    c_info = {
        "host": "localhost",
        "user": "root",
        "password": "ZJWcph0a",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }
    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    print(r_dbt.find_by_primary_key(["abbotfr01"]))
    r_dbt.delete_by_key(["abbotfr01"])
    print(r_dbt.find_by_primary_key(["abbotfr01"]))

def test_update_by_key():
    c_info = {
        "host": "localhost",
        "user": "root",
        "password": "ZJWcph0a",
        "db": "lahman2019raw",
        "cursorclass": pymysql.cursors.DictCursor
    }
    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    print(r_dbt.find_by_primary_key(["abbotfr01"]))
    new_values = {"birthYear": "2000"}
    r_dbt.update_by_key(["abbotfr01"], new_values)
    print(r_dbt.find_by_primary_key(["abbotfr01"]))










# test_find_by_template()
# test_find_by_primary_key()
# test_update_by_template()
# test_delete_by_template()
# test_insert()
# test_update_by_key()
# test_delete_by_key()
# test_find_by_primary_key()