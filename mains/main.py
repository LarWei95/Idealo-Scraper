# coding=iso-8859-1
'''
Created on 05.01.2022

@author: larsw
'''
from pprint import pprint
from control.scraping import IdealoRequester, WebRequestManager
from control.loader import Loader
from model.storage import MySQLStorage
from webrequestmanager.control.api import WebRequestAPIClient
import json
import datetime as dt
import matplotlib.pyplot as plt
import pandas as pd
from collections import defaultdict
from control.datasheet import Datasheet

# 7712

def main ():
    with open("credentials.json", "r") as f:
        credentials = json.load(f)
    
    user = credentials["user"]
    password = credentials["password"]
    

    print("Opened the credentials. Connecting to database.")
    storage = MySQLStorage("192.168.188.23", user, password)
    api = WebRequestAPIClient("http://172.25.0.2", 35353)
    request_manager = WebRequestManager(api, dt.timedelta(weeks=54))

    requester = IdealoRequester(request_manager)
    
    loader = Loader(storage, requester)
    print("Required connections built. Executing updates.")

    loader.update_categories(dt.timedelta(days=31*9))
    print("Done updating categories.")
    loader.update_prices(dt.timedelta(days=7))
    print("Done updating prices.")
    # Externe Festplatten
    # loader.load_full_category(7712)
    # Grafikkarten
    # loader.load_full_category(16073)
    # Festplatten 3011
    # loader.load_full_category(3011)
    # Monitore
    # loader.load_full_category(3832)
    # Tastaturen
    # loader.load_full_category(3047)
    # Maeuse
    # loader.load_full_category(3046)
    # Einplatinencomputer
    # loader.load_full_category(31123)
    # USB-Sticks
    # loader.load_full_category(4312)
    
    # Handys & Smartphones 
    # loader.load_full_category(19116)
    # SSD-Festplatten 
    # loader.load_full_category(14613)
    # Isolierkannen 13593
    # loader.load_full_category(13593)
    
    # Gaming Headsets 5172
    # loader.load_full_category(5172)
    # Prozessoren 3019
    # loader.load_full_category(3019)
    # Mainboards 3018
    # loader.load_full_category(3018)
    # RAM 4552
    # loader.load_full_category(4552)
    
    # Tastaturen 3047
    # loader.load_full_category(3047)
    # Mauspads 10472
    # loader.load_full_category(10472)
    
    # Zelte 9174
    # loader.load_full_category(9174)
    # Schlafsaecke 9175
    # loader.load_full_category(9175)
    # Matten 9292
    # loader.load_full_category(9292)
    
    # VR Headset 29550
    # loader.load_full_category(29550)
    # Laptops 3751
    # loader.load_full_category(3751)
    # PC-Systeme 3020
    # loader.load_full_category(3020)
    # WLAN Sticks 16935
    # loader.load_full_category(16935)
    # Netzwerkkarten
    # loader.load_full_category(3089)
    
    # Muesli
    # loader.load_full_category(25436)
    # Speiseoel
    # loader.load_full_category(25269)
    
    # Hundefutter 11153
    # loader.load_full_category(11153)
    # Katenzfutter 11152
    # loader.load_full_category(11152)
    # Tiertransportbox 18330
    # loader.load_full_category(18330)
    # Hundegeschirr 11372
    # loader.load_full_category(11372)
    # Hundespielzeug 29713
    # loader.load_full_category(29713)
    # Hundeleine 32487
    # loader.load_full_category(32487)
    # Hundesport 32519
    # loader.load_full_category(32519)
    # Outdoor-Hundezubehoer 32520
    # loader.load_full_category(32520)
    # Tierbetten 13654
    # loader.load_full_category(13654)
    # Fellpflege 13652
    # loader.load_full_category(13652)
    # Futternaepfe 32413
    # loader.load_full_category(32413)
    # Hundebekleidung 18721
    # loader.load_full_category(18721)
    
    # SD-Karten 4734
    # loader.load_full_category(4734)
    
    # Laufschuhe 22875
    # loader.load_full_category(22875)
    # Sneaker 18817
    # loader.load_full_category(18817)
    
    # Outdoor-Schuhe 18854
    # loader.load_full_category(18854)
        
def graphics_card_performance (storage):
    cid = 16073
    
    product = storage.get_product_info(category_id=cid)
    datasheets = Datasheet.collapse_product_info_to_datasheets(product)
    print(product)
    attributes, values = Datasheet.get_attribute_value_pairs(datasheets)
    
    pprint(attributes)
    pprint(values)
    
    continuous_categories = [('Grafikchip', 'Chiptakt base'),
                             ('Grafikchip', 'Chiptakt boost'),
                             ('Grafikspeicher', 'SpeichergrößŸe'),
                             ('Grafikchip', 'Stream Processing Units'),
                             ('Grafikspeicher', 'Speichertakt'),
                             ('Grafikspeicher', 'Speicherbandbreite')]
    
    index_translation, df = Datasheet.interpret(datasheets, continuous_categories)    
    prices = storage.get_prices_of_product(list(df.index.values)).median(level=0)
    
    performance_df = Datasheet.analyse(df, prices)
    performance_df = performance_df.join(product[["ProductName"]]).join(df).join(prices)
    print(performance_df)
    performance_df.to_csv("Performance.csv", sep=";")
    
def singleboard_computer_performance (storage):
    cid = 31123
    
    product = storage.get_product_info(category_id=cid)
    datasheets = Datasheet.collapse_product_info_to_datasheets(product)
    print(product)
    attributes, values = Datasheet.get_attribute_value_pairs(datasheets)
    
    pprint(attributes)
    pprint(values)
    
    continuous_categories = [('Spezifikationen', 'RAM'),
                             ('Spezifikationen', 'Taktfrequenz'),
                             ('Spezifikationen', 'Anzahl Prozessorkerne')]
    
    index_translation, df = Datasheet.interpret(datasheets, continuous_categories)    
    prices = storage.get_prices_of_product(list(df.index.values)).max(level=0)
    
    performance_df = Datasheet.analyse(df, prices)
    performance_df = performance_df.join(product[["ProductName"]]).join(df).join(prices)
    print(performance_df)
    performance_df.to_csv("Performance.csv", sep=";")
    
def flashdrive_performance (storage):
    cid = 4312
    
    product = storage.get_product_info(category_id=cid)
    datasheets = Datasheet.collapse_product_info_to_datasheets(product)
    print(product)
    attributes, values = Datasheet.get_attribute_value_pairs(datasheets)
    
    pprint(attributes)
    pprint(values)
    
    continuous_categories = [('Eigenschaften', 'SpeichergrößŸe'),
                             ('Leistungsmerkmale', 'Übertragungsrate')]
    
    index_translation, df = Datasheet.interpret(datasheets, continuous_categories)    
    prices = storage.get_prices_of_product(list(df.index.values)).max(level=0)
    
    performance_df = Datasheet.analyse(df, prices)
    performance_df = performance_df.join(product[["ProductName"]]).join(df).join(prices)
    print(performance_df)
    performance_df.to_csv("Performance.csv", sep=";")
    
def mouse_performance (storage):
    cid = 3046
    
    product = storage.get_product_info(category_id=cid)
    datasheets = Datasheet.collapse_product_info_to_datasheets(product)
    print(product)
    attributes, values = Datasheet.get_attribute_value_pairs(datasheets)
    
    pprint(attributes)
    pprint(values)
    
    continuous_categories = [('Tasten', 'Tastenanzahl'),
                             ('Sensor', 'Max. AuflÃ¶sung'),
                             ('Leistungsmerkmale', 'max. Signalrate')]
    
    index_translation, df = Datasheet.interpret(datasheets, continuous_categories)    
    prices = storage.get_prices_of_product(list(df.index.values)).max(level=0)
    
    performance_df = Datasheet.analyse(df, prices)
    performance_df = performance_df.join(product[["ProductName"]]).join(df).join(prices)
    print(performance_df)
    performance_df.to_csv("Performance.csv", sep=";")
    
def external_hdd_performance (storage):
    cid = 7712
    
    product = storage.get_product_info(category_id=cid)
    datasheets = Datasheet.collapse_product_info_to_datasheets(product)
    print(product)
    attributes, values = Datasheet.get_attribute_value_pairs(datasheets)
    
    pprint(attributes)
    pprint(values)
    
    continuous_categories = [('General', 'SpeicherkapazitÃ¤t'),
                             ('KonnektivitÃ¤t', 'Ã\x9cbertragungsrate')]
    
    index_translation, df = Datasheet.interpret(datasheets, continuous_categories)    
    prices = storage.get_prices_of_product(list(df.index.values)).max(level=0)
    
    performance_df = Datasheet.analyse(df, prices)
    performance_df = performance_df.join(product[["ProductName"]]).join(df).join(prices)
    print(performance_df)
    performance_df.to_csv("Performance.csv", sep=";")
    
def read_data ():
    with open("../credentials.json", "r") as f:
        credentials = json.load(f)
    
    user = credentials["user"]
    password = credentials["password"]
    
    storage = MySQLStorage("localhost", user, password)
    
    external_hdd_performance(storage)
    
    
if __name__ == '__main__':
    main()
