
from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug,
            "columns": ["playerID","birthYear","birthMonth","birthDay","birthCountry","birthState","birthCity","deathYear","deathMonth","deathDay","deathCountry","deathState","deathCity","nameFirst","nameLast","nameGiven","weight","height","bats","throws","debut","finalGame","retroID","bbrefID"]
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0,CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result


    def key_to_template(self,key):
        tmp = {}
        if len(key) != len(self._data["key_columns"]):
            raise ValueError("the number of keys must match the number of keys in key columns")
        for i in range(len(key)):
            tmp[self._data["key_columns"][i]] = key[i]
        return tmp

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """


        return self.find_by_template(self.key_to_template(key_fields),field_list)

        # key_cols = self._data.get("key_columns",None)
        # for record in self._rows:
        #     flag = True
        #     for i in range(len(key_fields)):
        #         if key_fields[i] != record.get(key_cols[i],None):
        #             flag = False
        #             break
        #     if flag == True:
        #         record_copy = record.copy()
        #         break
        # if flag == False:
        #     return None
        # if field_list == None:
        #     return record_copy
        # for keys in list(record_copy.keys()):
        #     if keys in field_list:
        #         del record_copy[keys]
        # return record_copy




    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        # if fields is None:
        #     field_list = " * "
        # else:
        #     field_list = " " + ",".join(fields) + " "
        res = []
        for record in self._rows:
            if self.matches_template(record,template):
                res.append(record)
        if field_list is None:
            return res
        else:
            res2 = []
            for record in res:
                dic={}
                for key in record:
                    if key in field_list:
                        dic[key] = record[key]
                        res2.append(dic)
        return res2






    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        return self.delete_by_template(self.key_to_template(key_fields))

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        count = 0
        for record in self._rows:
            if self.matches_template(record,template):
                self._rows.remove(record)
                count += 1
        return count


    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        return self.update_by_template(self.key_to_template(key_fields),new_values)


    @staticmethod
    def update_row(row,new_values):
        new_row = {}
        for key in row:
            if key in new_values:
                new_row[key] = new_values[key]
            else:
                new_row[key] = row[key]
        return new_row


    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        count = 0
        if new_values is None:
            raise ValueError("")
        new_cols = set(new_values.keys())
        tbl_cols = set(self._data["columns"])
        key_cols = self._data.get("key_columns", None)
        key_cols = set(key_cols)
        if not new_cols.issubset(tbl_cols):
            raise ValueError("")


        if key_cols is not None:
            for k in key_cols:
                if new_values.get(k,'a')==None:
                    raise ValueError("")
            for record in self._rows:
                if self.matches_template(record, template):
                    self._rows.remove(record)
                    new_row = self.update_row(record,new_values)
                    key_tmp = self.get_key_tmp(new_row)
                    if len(self.find_by_template(key_tmp)) > 0:
                        self._add_row(record)
                        raise ValueError("duplicate primary key")
                    else:
                        self._add_row(new_row)
                        count += 1
        else:
            for record in self._rows:
                if self.matches_template(record, template):
                    update_row(record, new_values)
                    count += 1
        return count


    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        if new_record is None:
            raise ValueError()
        new_cols = set(new_record.keys())
        tbl_cols = set(self._data["columns"])

        if not new_cols.issubset(tbl_cols):
            raise ValueError("new_cols is not a subset of tbl_cols")

        key_cols = self._data.get("key_columns",None)

        if key_cols is not None:
            key_cols = set(key_cols)
            if not key_cols.issubset(new_cols):
                raise ValueError("key_cols is not a subset of new_cols")
            for k in key_cols:
                if new_record.get(k,None) is None:
                    raise ValueError("")

            key_tmp = self.get_key_tmp(new_record)
            # print(key_tmp)
            # print(self.find_by_template(key_tmp))
            if len(self.find_by_template(key_tmp))>0 :
                raise ValueError("duplicate primary key")
        self._rows.append(new_record)



    def get_key_tmp(self,dic):
        key_tmp = {}
        for i in dic:
            if i in self._data["key_columns"]:
                key_tmp[i] = dic[i]
        return key_tmp

    def get_rows(self):
        return self._rows

