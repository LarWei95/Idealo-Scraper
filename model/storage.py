'''
Created on 05.01.2022

@author: larsw
'''
from abc import ABC, abstractmethod
import sqlite3

import mysql.connector
import mysql.connector.errors as mysqlerrors
import time
import numpy as np
import pandas as pd
import datetime as dt
from io import BytesIO, StringIO
import gzip


def _escape_string (string):
    string = string.replace("\"", "\\\"")
    string = string.replace("'", "\\'")
    return string

class StorageInsertError(Exception):
    MESSAGE_BASE = "Storing data failed with the following info:\n{:s}"

    def __init__(self, msg):
        super().__init__(StorageInsertError.MESSAGE_BASE.format(msg))

class Storage(ABC):

    def __init__(self):
        pass
    
    @abstractmethod    
    def store_product (self, product_id, name, category_id):
        pass
    
    @abstractmethod
    def store_category (self, category_id, category_name):
        pass
    
    @abstractmethod
    def store_prices (self, product_id, df):
        pass
        
class SQLiteStorage (Storage):
    CREATE_CATEGORY_SQL = """
    CREATE TABLE IF NOT EXISTS category (
        cid INTEGER PRIMARY KEY,
        name TEXT NOT NULL    
    );"""
    CREATE_PRODUCT_SQL = """
    CREATE TABLE IF NOT EXISTS product (
        pid INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        cid INTEGER,
        
        FOREIGN KEY (cid)
            REFERENCES category (cit)
                ON DELETE CASCADE
                ON UPDATE NO ACTION
    );"""
    CREATE_PRICE_SQL = """
    CREATE TABLE IF NOT EXISTS price (
        pid INTEGER,
        date TEXT,
        price REAL,
        PRIMARY KEY (pid, date),
        FOREIGN KEY (pid)
            REFERENCES product (pid)
                ON DELETE CASCADE
                ON UPDATE NO ACTION
    );"""
        
    INSERT_CATEGORY_SQL = """
    INSERT OR IGNORE INTO category (cid, name) VALUES ({:s}, \"{:s}\");
    """
    INSERT_PRODUCT_SQL = """
    INSERT OR IGNORE INTO product (pid, name, cid) VALUES ({:s}, \"{:s}\", {:s}); 
    """
    
    def __init__ (self, path):
        self._con = sqlite3.connect(path)
        self._cur = self._con.cursor()
        self._initialize()
        
    def _initialize (self):
        self._cur.execute(SQLiteStorage.CREATE_CATEGORY_SQL)
        self._cur.execute(SQLiteStorage.CREATE_PRODUCT_SQL)
        self._cur.execute(SQLiteStorage.CREATE_PRICE_SQL)
        self._con.commit()
        
    def __del__ (self):
        self._cur.close()
        self._con.close()    
        
    def store_product (self, product_id, name, category_id):
        sql = SQLiteStorage.INSERT_PRODUCT_SQL.format(
                product_id, name, category_id
            )
        self._cur.execute(sql)
        self._con.commit()
    
    def store_category (self, category_id, category_name):
        sql = SQLiteStorage.INSERT_CATEGORY_SQL.format(
                category_id, category_name
            )
        self._cur.execute(sql)
        self._con.commit()
        
    def store_prices (self, product_id, df):
        sql = "INSERT OR IGNORE INTO price (pid, date, price) VALUES {:s};"
        
        fmt = "("+product_id+",\"{:s}\", {:f})"
        
        values = []
        
        for ind in df.index:
            value = df[ind]
            
            ind = ind.strftime("%Y-%m-%d")
            value = fmt.format(ind, value)
            values.append(value)

        values = ",".join(values)
        sql = sql.format(values)
        
        self._cur.execute(sql)
        self._con.commit()
        
    @classmethod
    def _get_product_selector (cls, product_id, category_id):
        selectors = []
        
        if isinstance(product_id, (list, tuple)):
            sel = "pid IN ({:s})".format(
                    ", ".join(
                            "{:d}".format(int(x))
                            for x in product_id
                        )
                )
            selectors.append(sel)
        elif product_id is not None:
            sel = "pid = \"{:s}\"".format(
                    product_id
                )
            selectors.append(sel)
            
        if isinstance(category_id, (list, tuple)):
            sel = "cid IN ({:s})".format(
                    ", ".join(
                            "{:d}".format(int(x))
                            for x in category_id
                        )
                )
            selectors.append(sel)
        elif category_id is not None:
            sel = "cid = {:d}".format(int(category_id))
            selectors.append(sel)
            
        if len(selectors) != 0:
            sql = "WHERE {:s}".format(
                    " AND ".join(selectors)
                )
        else:
            sql = ""
            
        return sql
            
        
    def get_products (self, product_id=None, category_id=None):
        sql = "SELECT * FROM product {:s};".format(
                SQLiteStorage._get_product_selector(product_id, category_id)
            )
        print(sql)
        self._cur.execute(sql)
        
        rows = self._cur.fetchall()
        
        df = pd.DataFrame(rows, columns=["ProdId", "Name", "CatId"])
        df = df.set_index("ProdId")
        return df
        
class _DBCon ():
    def __init__ (self, host, user, passwd, db_name):
        self._host = host
        self._user = user
        self._passwd = passwd
        self._db_name = db_name
        
        self._con = None
        
    def __enter__ (self):
        attempts = 1000
        last_attempt = attempts - 1
        
        for attempt in range(attempts):
            try:
                self._con = mysql.connector.connect(
                        host=self._host,
                        user=self._user,
                        password=self._passwd,
                        database=self._db_name
                    )
                return self._con.cursor()
            except Exception as e:
                if attempt != last_attempt:
                    if attempt % 100 == 0:
                        print("Failed to connect - {:d}: {:s}".format(
                                attempt, str(e)
                            ))
                        
                    time.sleep(1)
                else:
                    raise e
    
    def __exit__ (self, exc_type, exc_val, exc_tb):
        self._con.commit()
        self._con.close()
        
class MySQLStorage (Storage):
    V_CATEGORY_ID = "CategoryId"
    V_CATEGORY_NAME = "CategoryName"
    V_PRODUCT_ID = "ProductId"
    V_PRODUCT_NAME = "ProductName"
    V_DATE = "Date"
    V_TIMESTAMP = "Timestamp"
    V_PRICE = "Price"
    V_DATASHEET = "Datasheet"
    V_AGE = "Age"
    V_PERIOD = "Period"

    CATEGORY_COLUMNS = [V_CATEGORY_ID, V_CATEGORY_NAME]
    CATEGORY_INDEX = V_CATEGORY_ID

    CATEGORY_UPDATE_COLUMNS = [V_CATEGORY_ID, V_TIMESTAMP]
    CATEGORY_UPDATE_INDEX = V_CATEGORY_ID
    
    PRODUCT_COLUMNS = [V_PRODUCT_ID, V_PRODUCT_NAME, V_CATEGORY_ID, V_DATASHEET]
    PRODUCT_INDEX = V_PRODUCT_ID
    
    PRODUCT_PRICE_COLUMNS = [V_PRODUCT_ID, V_DATE, V_PRICE]
    PRODUCT_PRICE_INDEX = [V_PRODUCT_ID, V_DATE]
    
    CATEGORY_PRODUCT_PRICE_COLUMNS = [V_CATEGORY_ID, V_PRODUCT_ID, V_DATE, V_PRICE]
    CATEGORY_PRODUCT_PRICE_INDEX = [V_CATEGORY_ID, V_PRODUCT_ID, V_DATE]
    
    LAST_PRICE_DATE_COLUMNS = [V_PRODUCT_ID, V_DATE]
    LAST_PRICE_DATE_INDEX = V_PRODUCT_ID
    
    LAST_PRICE_AGE_COLUMNS = [V_PRODUCT_ID, V_AGE]
    LAST_PRICE_AGE_INDEX = V_PRODUCT_ID
    
    UPDATE_RUN_COLUMNS = [V_DATE, V_PRODUCT_ID, V_PERIOD]
    UPDATE_RUN_INDEX = [V_DATE, V_PRODUCT_ID]
    
    CATEGORY_UPDATE_RUN_COLUMNS = [V_TIMESTAMP, V_CATEGORY_ID]
    CATEGORY_UPDATE_RUN_INDEX = V_CATEGORY_ID
    
    def __init__(self, host, user, passwd, db_name="idealo_data"):
        self._host = host
        self._user = user
        self._passwd = passwd
        self._db_name = db_name
        
        self._initialize()
    
    
    def _create_database(self):
        self._con = _DBCon(self._host, self._user, self._passwd, None)
        
        with self._con as cur:
            sql = "CREATE DATABASE IF NOT EXISTS {:s};".format(
                    self._db_name
                )
            cur.execute(sql)
            
        self._con = _DBCon(self._host, self._user, self._passwd, self._db_name)
        
    def _create_category_table(self, cur):
        sql = """CREATE TABLE IF NOT EXISTS category (
            cid INTEGER UNSIGNED PRIMARY KEY,
            name TEXT NOT NULL
        );"""
        cur.execute(sql)

    def _create_last_category_update_table(self, cur):
        sql = """CREATE TABLE IF NOT EXISTS last_category_update (
            cid INTEGER UNSIGNED PRIMARY KEY,
            ts DATETIME NOT NULL,

            FOREIGN KEY(cid)
                REFERENCES category (cid)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION
        );"""
        cur.execute(sql)
        
    def _create_category_update_run_table(self, cur):
        sql = """
        CREATE TABLE IF NOT EXISTS category_update_run (
            ts DATETIME,
            cid INTEGER UNSIGNED PRIMARY KEY,

            FOREIGN KEY (cid)
                REFERENCES category (cid)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION
        );"""
        cur.execute(sql)

    def _create_product_table(self, cur):
        sql = """CREATE TABLE IF NOT EXISTS product (
            pid INTEGER UNSIGNED PRIMARY KEY,
            name TEXT NOT NULL,
            cid INTEGER UNSIGNED NOT NULL,
            datasheet BLOB NULL,
            
            FOREIGN KEY (cid)
                REFERENCES category(cid)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
        );"""
        cur.execute(sql)
        
    def _create_price_table(self, cur):
        sql = """
        CREATE TABLE IF NOT EXISTS price (
            pid INTEGER UNSIGNED,
            date DATE NOT NULL,
            price DOUBLE UNSIGNED NOT NULL,
            PRIMARY KEY (pid, date),
            FOREIGN KEY (pid)
                REFERENCES product (pid)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION
        );"""
        cur.execute(sql)
        
    def _create_last_price_date_table(self, cur):
        sql = """
        CREATE TABLE IF NOT EXISTS last_price_date (
            pid INTEGER UNSIGNED PRIMARY KEY,
            date DATE NOT NULL,
            
            FOREIGN KEY (pid)
                REFERENCES product (pid)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION
        );"""
        cur.execute(sql)
        
    def _create_update_run_table(self, cur):
        sql = """
        CREATE TABLE IF NOT EXISTS update_run (
            date DATETIME,
            pid INTEGER UNSIGNED,
            period VARCHAR(5),
            
            PRIMARY KEY (date, pid),
            FOREIGN KEY (pid)
                REFERENCES product (pid)
                    ON DELETE CASCADE
                    ON UPDATE NO ACTION
        );"""
        cur.execute(sql)

    def _create_category_update_run_delete_trigger(self, cur):
        sql = "DROP TRIGGER IF EXISTS delete_category_update_run_trigger;"
        cur.execute(sql)

        sql = """
        CREATE TRIGGER delete_category_update_run_trigger
        AFTER DELETE
        ON category_update_run
        FOR EACH ROW
        INSERT INTO last_category_update (cid, ts)
        VALUES (OLD.cid, UTC_TIMESTAMP())
        ON DUPLICATE KEY UPDATE
        ts = UTC_TIMESTAMP()
        """
        cur.execute(sql)
    
    def _create_category_insert_trigger(self, cur):
        sql = "DROP TRIGGER IF EXISTS insert_category_trigger;"
        cur.execute(sql)

        sql = """
        CREATE TRIGGER insert_category_trigger
        AFTER INSERT
        ON category
        FOR EACH ROW
        INSERT IGNORE INTO last_category_update (cid, ts)
        VALUES (NEW.cid, UTC_TIMESTAMP());
        """
        cur.execute(sql)

    def _create_price_insert_trigger(self, cur):
        sql = "DROP TRIGGER IF EXISTS insert_price_trigger;"
        cur.execute(sql)

        sql = """
        CREATE TRIGGER insert_price_trigger
        AFTER INSERT
        ON price
        FOR EACH ROW
        INSERT INTO last_price_date (pid, date)
        VALUES (NEW.pid, NEW.date)
        ON DUPLICATE KEY UPDATE date = GREATEST(date, VALUES(date));
        """
        cur.execute(sql)
        
    def _initialize(self):
        self._create_database()
        
        with self._con as cur:
            self._create_category_table(cur)
            self._create_last_category_update_table(cur) 
            self._create_category_update_run_table(cur)
            self._create_product_table(cur)
            self._create_price_table(cur)
            self._create_last_price_date_table(cur)
            self._create_update_run_table(cur)
            
            self._create_category_update_run_delete_trigger(cur)
            self._create_category_insert_trigger(cur)
            self._create_price_insert_trigger(cur)
            
    def get_category(self, category_id=None):
        if category_id is not None:
            sql = "SELECT cid, name FROM category WHERE cid {:s};"
            
            if isinstance(category_id, (list, tuple)):
                subsql = ",".join(str(x) for x in category_id)
                sql = sql.format("IN ({:s})".format(subsql))
            else:
                subsql = str(category_id)
                sql = sql.format("= {:s}".format(subsql))
        else:
            sql = "SELECT cid, name FROM category;"
            
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
                
        s = pd.DataFrame(rows, columns=MySQLStorage.CATEGORY_COLUMNS)
        s = s.set_index(MySQLStorage.CATEGORY_INDEX)
        return s
                
    def get_last_category_update(self, category_id=None):
        if category_id is not None:
            sql = "SELECT cid, ts FROM last_category_update WHERE cid {:s};"

            if isinstance(category_id, (list, tuple)):
                subsql = ",".join(str(x) for x in category_id)
                sql = sql.format(f"IN ({subsql})")
            else:
                sql = sql.format(f"= {category_id}")
        else:
            sql = "SELECT cid, ts FROM last_category_update;"

        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        s = pd.DataFrame(rows, columns=MySQLStorage.CATEGORY_UPDATE_COLUMNS)
        s = s.set_index(MySQLStorage.CATEGORY_UPDATE_INDEX)
        return s
    
    def get_category_update_run(self, category_id=None):
        if category_id is not None:
            sql = "SELECT ts, cid FROM category_update_run WHERE cid {:s};"

            if isinstance(category_id, (list, tuple)):
                subsql = ",".join(str(x) for x in category_id)
                sql = sql.format(f"IN ({subsql})")
            else:
                sql = sql.format(f"= {category_id}")
        else:
            sql = "SELECT ts, cid FROM category_update_run;"

        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        s = pd.DataFrame(rows, columns=MySQLStorage.CATEGORY_UPDATE_RUN_COLUMNS)
        s = s.set_index(MySQLStorage.CATEGORY_UPDATE_RUN_INDEX)
        return s

    def store_last_category_update(self, category_id, timestamp):
        timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""INSERT INTO last_category_update (cid, ts)
        VALUES ({category_id}, \"{timestamp}\")
        ON DUPLICATE KEY UPDATE
            ts = VALUES(last);"""

        with self._con as cur:
            cur.execute(sql)

    def store_category_update_run(self, category_id, timestamp):
        timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""INSERT INTO category_update_run (ts, cid)
        VALUES (\"{timestamp}\", {category_id})
        ON DUPLICATE KEY UPDATE
            ts = VALUES(ts);"""

        with self._con as cur:
            cur.execute(sql)
            
    def store_category (self, category_id, category_name):
        sql = """INSERT INTO category (cid, name) 
        VALUES ({:d}, \"{:s}\")
        ON DUPLICATE KEY UPDATE
            name = VALUES(name)
        ;"""
        sql = sql.format(category_id, _escape_string(category_name))
        
        with self._con as cur:
            cur.execute(sql)
            
    def store_product (self, product_id, name, category_id, datasheet):
        if datasheet is not None:
            bytesio = BytesIO()
            
            with gzip.open(bytesio, "wb") as f:
                stringio = StringIO()
                datasheet.to_csv(stringio, sep=";", line_terminator="\n")
                
                stringio = stringio.getvalue().encode("utf-8")
                
                f.write(stringio)
                
            datasheet = bytesio.getvalue().hex()
            datasheet = "X'{:s}'".format(datasheet)
        else:
            datasheet = "NULL"
        
        
        sql = """INSERT INTO product (pid, name, cid, datasheet)
        VALUES ({:d}, \"{:s}\", {:d}, {:s})
        ON DUPLICATE KEY UPDATE
            name = VALUES(NAME),
            cid = VALUES(cid),
            datasheet = VALUES(datasheet);""".format(
                product_id, _escape_string(name),
                category_id,
                datasheet
            )
        try: 
            with self._con as cur:
                cur.execute(sql)
        except mysqlerrors.DatabaseError as e:
            msg = f"Product {product_id} {name} {category_id}"
            raise StorageInsertError(msg)


    def store_prices (self, product_id, df):
        sql = """INSERT INTO price (pid, date, price)
        VALUES {:s}
        ON DUPLICATE KEY UPDATE
            price = VALUES(price);"""
        
        fmt = "({:d},\"{:s}\", {:f})"
        
        values = []
        
        for ind in df.index:
            value = df[ind]
            
            ind = ind.strftime("%Y-%m-%d")
            value = fmt.format(product_id, ind, value)
            values.append(value)

        values = ",".join(values)
        sql = sql.format(values)
        
        try:
            with self._con as cur:
                cur.execute(sql)
        except mysqlerrors.DatabaseError as e:
            msg = f"Prices for {product_id}"
            raise StorageInsertError(msg)


    def store_update_runs (self, update_df):
        # update_df:
        # Index: ProductId
        # Columns: Period (e.g. P3M), Date (datetime)
        
        sql = """INSERT INTO update_run (date, pid, period)
        VALUES {:s}
        ON DUPLICATE KEY UPDATE 
            date = VALUES(date), pid = VALUES(pid), period = VALUES(period);"""
        
        fmt = "(\"{:s}\",{:d},\"{:s}\")"
        lines = []
        
        for product_id in update_df.index:
            v = update_df.loc[product_id]
            period = v[MySQLStorage.V_PERIOD]
            date = v[MySQLStorage.V_DATE].to_pydatetime()
            date = date.strftime("%Y-%m-%d %H:%M:%S")
            
            lines.append(fmt.format(date, product_id, period))
            
            if len(lines) >= 1000:
                subsql = sql.format(",".join(lines))
                lines.clear()
                
                with self._con as cur:
                    cur.execute(subsql)
            
        if len(lines) != 0:
            sql = sql.format(",".join(lines))
            
            with self._con as cur:
                cur.execute(sql)
    
    def delete_category_update_run(self, category_id):
        sql = f"DELETE FROM category_update_run WHERE cid = {category_id};"

        with self._con as cur:
            cur.execute(sql)

    @classmethod
    def _get_product_info_selector (cls, product_id, category_id):
        selectors = []
        
        if product_id is not None:
            if isinstance(product_id, (list, tuple, np.ndarray)):
                sel = "p.pid IN({:s})".format(
                        ",".join(str(x) for x in product_id)
                    )
            else:
                sel = "p.pid = {:d}".format(product_id)
                
            selectors.append(sel)
        
        if category_id is not None:
            if isinstance(category_id, (list, tuple, np.ndarray)):
                sel = "p.cid IN({:s})".format(
                        ",".join(str(x) for x in category_id)
                    )
            else:
                sel = "p.cid = {:d}".format(category_id)
                
            selectors.append(sel)
            
        if len(selectors) != 0:
            selectors = " AND ".join(selectors)
            selectors = " WHERE {:s}".format(selectors)
        else:
            selectors = ""
            
        return selectors
        
            
    @classmethod
    def _process_byte_string (cls, string):
        string = string.replace(b"\xa0", b" ")
        string = string.replace(b"\xc2", b"")
        string = string.replace(b"\xb0", b"deg")
        
        string = string.decode("iso-8859-1")
        return string
            
    def get_product_info (self, product_id=None, category_id=None):
        sql = """SELECT p.pid, p.name, p.cid, p.datasheet
            FROM product AS p
            {:s};""".format(
                    MySQLStorage._get_product_info_selector(product_id,
                                                            category_id)
                )
                
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            
                
        new_rows = []
        
        for pid, name, cid, datasheet in rows:
            bytesio = BytesIO(datasheet)
            
            with gzip.open(bytesio, "rb") as f:
                datasheet = f.read()
                
            datasheet = MySQLStorage._process_byte_string(datasheet)
                
            datasheet = StringIO(datasheet)

            try:
                datasheet = pd.read_csv(datasheet, sep=";", lineterminator="\n",
                                        index_col=[0, 1])["0"]
                new_rows.append((pid, name, cid, datasheet))
            except pd.errors.EmptyDataError:
                continue
            
        s = pd.DataFrame(new_rows, columns=MySQLStorage.PRODUCT_COLUMNS)
        s = s.set_index(MySQLStorage.PRODUCT_INDEX)
        return s
    
    
            
    def get_prices_of_product (self, product_id):
        sql = """SELECT p.pid, pr.date, pr.price
        FROM product AS p
        INNER JOIN price AS pr
            ON p.pid = pr.pid
        WHERE p.pid {:s};"""
        
        multi = isinstance(product_id, (list, tuple))
        
        if multi:
            subsql = ",".join(str(x) for x in product_id)
            sql = sql.format("IN ({:s})".format(subsql))
            
            with self._con as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            
            df = pd.DataFrame(rows, columns=MySQLStorage.PRODUCT_PRICE_COLUMNS)
            df = df.set_index(MySQLStorage.PRODUCT_PRICE_INDEX)
        else:
            subsql = str(product_id)
            sql = sql.format("= {:s}".format(subsql))
            
            with self._con as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                
            df = pd.DataFrame(rows, columns=MySQLStorage.PRODUCT_PRICE_COLUMNS)
            df = df.drop(MySQLStorage.PRODUCT_PRICE_INDEX[0], axis=1)
            df = df.set_index(MySQLStorage.PRODUCT_PRICE_INDEX[1])
            
        df = df.sort_index()
        return df
            
    def get_prices_of_category (self, category_id):
        sql = """SELECT c.cid, p.pid, pr.date, pr.price
        FROM category AS c
        INNER JOIN product AS p
            ON c.cid = p.cid
        INNER JOIN price AS pr
            ON p.pid = pr.pid
        WHERE c.cid {:s};"""
            
        multi = isinstance(category_id, (list, tuple))
        
        if multi:
            subsql = ",".join(str(x) for x in category_id)
            sql = sql.format("IN ({:s})".format(subsql))
            
            with self._con as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            
            df = pd.DataFrame(rows, columns=MySQLStorage.CATEGORY_PRODUCT_PRICE_COLUMNS)
            df = df.set_index(MySQLStorage.CATEGORY_PRODUCT_PRICE_INDEX)
        else:
            subsql = str(category_id)
            sql = sql.format("= {:s}".format(subsql))
            
            with self._con as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                
            df = pd.DataFrame(rows, columns=MySQLStorage.CATEGORY_PRODUCT_PRICE_COLUMNS)
            df = df.drop(MySQLStorage.CATEGORY_PRODUCT_PRICE_INDEX[0], axis=1)
            df = df.set_index(MySQLStorage.CATEGORY_PRODUCT_PRICE_INDEX[1:])
            
        df = df.sort_index()
        return df
    
    def get_last_price_dates (self):
        sql = """
        SELECT pid, date
        FROM last_price_date
        ORDER BY date ASC;"""
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            
        df = pd.DataFrame(rows, columns=MySQLStorage.LAST_PRICE_DATE_COLUMNS)
        df = df.set_index(MySQLStorage.LAST_PRICE_DATE_INDEX)
        return df
    
    def get_last_price_ages (self, reference_datetime):
        sql = """
        SELECT pid, TIMESTAMPDIFF(SECOND, date, \"{:s}\") "age"
        FROM last_price_date
        ORDER BY date ASC;
        """.format(str(reference_datetime))
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            
        df = pd.DataFrame(rows, columns=MySQLStorage.LAST_PRICE_AGE_COLUMNS)
        df = df.set_index(MySQLStorage.LAST_PRICE_AGE_INDEX)
        df[MySQLStorage.V_AGE] = pd.to_timedelta(df[MySQLStorage.V_AGE], unit="S")
        return df
    
    def get_update_runs (self):
        sql = """
        SELECT date, pid, period
        FROM update_run;"""
        
        with self._con as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            
        df = pd.DataFrame(rows, columns=MySQLStorage.UPDATE_RUN_COLUMNS)
        df = df.set_index(MySQLStorage.UPDATE_RUN_INDEX)
        return df
        
    def delete_update_run (self, date, product_id):
        sql = """DELETE FROM update_run
        WHERE date = \"{:s}\" AND pid = {:d};
        """.format(
                date.strftime("%Y-%m-%d %H:%M:%S"),
                product_id
            )
        
        with self._con as cur:
            cur.execute(sql)
        
