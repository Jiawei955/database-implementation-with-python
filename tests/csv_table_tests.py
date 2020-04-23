from src.CSVDataTable import CSVDataTable
import logging
import os
import json

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def t_load():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    return CSVDataTable("people", connect_info, ["playerID"] )


    # print("Created table = " + str(csv_tbl))



def test_find_by_template():
    tmp = {"birthYear": "1985", "birthMonth": "12"}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])
    result = csv_tbl.find_by_template(tmp,["playerID"])
    print(json.dumps(result, indent=2))

def test_key_to_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])
    k = csv_tbl.key_to_template(["afsdf"])
    print(k)



def test_delete_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])
    tmp = {"playerID": "aaronha01"}
    print(csv_tbl.find_by_template(tmp))
    a = csv_tbl.delete_by_template(tmp)
    print(a)
    print(csv_tbl.find_by_template(tmp))



def test_update_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])
    tmp1 = {"birthYear": "1886", "birthDay":"12","deathMonth":"12"}
    print(csv_tbl.find_by_template(tmp1))
    new = {"playerID": "aberal01"}
    a = csv_tbl.update_by_template(tmp1,new)
    print(a)
    print(csv_tbl.find_by_template(tmp1))


def test_insert():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])
    tmp1 = {"playerID": "abernte02"}
    new_record = {"playerID": "abernte02","birthYear": "2019"}
    print(csv_tbl.find_by_template(tmp1))
    csv_tbl.insert(new_record)
    print(csv_tbl.find_by_template(tmp1))


def test_find_by_primary_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])
    print(csv_tbl.find_by_primary_key(["abernte01"]))

def test_delete_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])
    print(csv_tbl.find_by_primary_key(["abernte01"]))
    csv_tbl.delete_by_key(["abernte01"])
    print(csv_tbl.find_by_primary_key(["abernte01"]))


def test_update_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("people", connect_info, ["playerID"])
    print(csv_tbl.find_by_primary_key(["abernte01"]))
    new_values = {"birthYear": "2000"}
    csv_tbl.update_by_key(["abernte01"],new_values)
    print(csv_tbl.find_by_primary_key(["abernte01"]))







# test_key_to_template()
# test_find_by_template()
# test_delete_by_template()
test_update_by_template()
# test_insert()
# test_find_by_primary_key()
# test_update_by_key()
# test_delete_by_key()
