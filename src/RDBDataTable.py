

from src.BaseDataTable import BaseDataTable

import pymysql

import logging
logger = logging.getLogger()
from src.BaseDataTable import BaseDataTable
import json
import pandas as pd

pd.set_option("display.width", 196)
pd.set_option('display.max_columns', 16)


class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        if table_name is None or connect_info is None:
            raise ValueError("Invalid input.")

        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns
        }

        cnx = self.get_connection(connect_info)
        if cnx is not None:
            self._cnx = cnx
        else:
            raise Exception("Could not get a connection.")

    def __str__(self):

        result = "RDBDataTable:\n"
        result += json.dumps(self._data, indent=2)

        row_count = self.get_row_count()
        result += "\nNumber of rows = " + str(row_count)

        some_rows = pd.read_sql(
            "select * from " + self._data["table_name"] + " limit 10",
            con=self._cnx
        )
        result += "First 10 rows = \n"
        result += str(some_rows)

        return result

    def get_row_count(self):

        row_count = self._data.get("row_count", None)
        if row_count is None:
            sql = "select count(*) as count from " + self._data["table_name"]
            res, d = self.run_q(sql, args=None, fetch=True, conn=self._cnx, commit=True)
            row_count = d[0][0]
            self._data['"row_count'] = row_count

        return row_count


    def get_connection(self, connect_info):
        """

        :param connect_info: A dictionary containing the information necessary to make a PyMySQL connection.
        :return: The connection. May raise an Exception/Error.
        """

        cnx = pymysql.connect(**connect_info)
        return cnx

    def run_q(self, sql, args=None, fetch=True, cur=None, conn=None, commit=True):
        '''
        Helper function to run an SQL statement.

        This is a modification that better supports HW1. An RDBDataTable MUST have a connection specified by
        the connection information. This means that this implementation of run_q MUST NOT try to obtain
        a defailt connection.

        :param sql: SQL template with placeholders for parameters. Canno be NULL.
        :param args: Values to pass with statement. May be null.
        :param fetch: Execute a fetch and return data if TRUE.
        :param conn: The database connection to use. This cannot be NULL, unless a cursor is passed.
            DO NOT PASS CURSORS for HW1.
        :param cur: The cursor to use. This is wizard stuff. Do not worry about it for now.
            DO NOT PASS CURSORS for HW1.
        :param commit: This is wizard stuff. Do not worry about it.

        :return: A pair of the form (execute response, fetched data). There will only be fetched data if
            the fetch parameter is True. 'execute response' is the return from the connection.execute, which
            is typically the number of rows effected.
        '''

        cursor_created = False
        connection_created = False

        try:

            if conn is None:
                raise ValueError("In this implementation, conn cannot be None.")

            if cur is None:
                cursor_created = True
                cur = conn.cursor()

            if args is not None:
                log_message = cur.mogrify(sql, args)
            else:
                log_message = sql

            logger.debug("Executing SQL = " + log_message)

            res = cur.execute(sql, args)

            if fetch:
                data = cur.fetchall()
            else:
                data = None

            # Do not ask.
            if commit == True:
                conn.commit()

        except Exception as e:
            raise (e)

        return (res, data)

    def template_to_where_clause(self,template):
        """
        :param template: One of those weird templates
        :return: WHERE clause corresponding to the template.
        """

        if template is None or template == {}:
            result = (None, None)
        else:
            args = []
            terms = []

            for k, v in template.items():
                terms.append(" " + k + "=%s ")
                args.append(v)

            w_clause = "AND".join(terms)
            w_clause = " WHERE " + w_clause

            result = (w_clause, args)

        return result



    def create_select(self,table_name, template, fields, order_by=None, limit=None, offset=None):
        """
        Produce a select statement: sql string and args.
        :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
        :param fields: Columns to select (an array of column name)
        :param template: One of Don Ferguson's weird JSON/python dictionary templates.
        :param order_by: Ignore for now.
        :param limit: Ignore for now.
        :param offset: Ignore for now.
        :return: A tuple of the form (sql string, args), where the sql string is a template.
        """

        if fields is None:
            field_list = " * "
        else:
            field_list = " " + ",".join(fields) + " "

        w_clause, args = self.template_to_where_clause(template)

        sql = "select " + field_list + " from " + table_name + " " + w_clause

        return (sql, args)


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
        tmp = self.key_to_template(key_fields)
        return self.find_by_template(tmp,field_list)


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
        sql, args = self.create_select(self._data["table_name"],template,field_list)
        return self.run_q(sql, args, conn=self._cnx)



    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        tmp = self.key_to_template(key_fields)
        return self.delete_by_template(tmp)

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        w_clause, w_args = self.template_to_where_clause(template)

        sql = "delete from " + self._data["table_name"] + " " + w_clause
        args = w_args
        return self.run_q(sql, args,conn=self._cnx)


    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        tmp = self.key_to_template(key_fields)
        return self.update_by_template(tmp,new_values)

    def create_update(self, table_name, new_values, template):
        """
        :param new_values: A dictionary containing cols and the new values.
        :param template: A template to form the where clause.
        :return: An update statement template and args.
        """
        set_terms = []
        args = []

        for k, v in new_values.items():
            set_terms.append(k + "=%s")
            args.append(v)

        s_clause = ",".join(set_terms)
        w_clause, w_args = self.template_to_where_clause(template)

        # There are %s in the SET clause and the WHERE clause. We need to form
        # the combined args list.
        args.extend(w_args)

        sql = "update " + table_name + " set " + s_clause + " " + w_clause

        return sql, args

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        sql, args = self.create_update(self._data["table_name"], new_values, template)
        return self.run_q(sql, args, conn=self._cnx)

    def create_insert(self, table_name, row):
        """
        :param table_name: A table name, which may be fully qualified.
        :param row: A Python dictionary of the form: { ..., "column_name" : value, ...}
        :return: SQL template string, args for insertion into the template
        """

        result = "Insert into " + table_name + " "
        cols = []
        vals = []

        # This is paranoia. I know that calling keys() and values() should return in matching order,
        # but in the long term only the paranoid survive.
        for k, v in row.items():
            cols.append(k)
            vals.append(v)

        col_clause = "(" + ",".join(cols) + ") "

        no_cols = len(cols)
        terms = ["%s"] * no_cols
        terms = ",".join(terms)
        value_clause = " values (" + terms + ")"

        result += col_clause + value_clause

        return (result, vals)

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        sql, args = self.create_insert(self._data["table_name"], new_record)
        self.run_q(sql,args,conn=self._cnx)

    def get_rows(self):
        return self._rows




