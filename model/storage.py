'''
Created on 05.01.2022

@author: larsw
'''
from abc import ABC, abstractmethod
import sqlite3

import pandas as pd

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
    
    @abstractmethod
    def get_products (self, product_id=None, category_id=None):
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
        