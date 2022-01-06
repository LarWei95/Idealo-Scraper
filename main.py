'''
Created on 05.01.2022

@author: larsw
'''
from pprint import pprint
from control.scraping import IdealoRequester
from model.storage import SQLiteStorage

def load ():
    # Aus dynamischem Laden entnommen
    requester = IdealoRequester()
    storage = SQLiteStorage("data.db")
    
    storage.store_category("3099", "Elektronische Zahnbuerste")
        
    products = requester.get_products_of_category("3099")
    
    for name in products:
        product_id = products[name]
        
        storage.store_product(product_id, name, "3099")

def main ():
    requester = IdealoRequester()
    storage = SQLiteStorage("data.db")
    df = storage.get_products(
        category_id="3099")
    
    for product_id in df.index.values:
        print(product_id)
        product_id = str(product_id)
        
        df = requester.get_api(product_id)
    
        storage.store_prices(product_id, df)
    
if __name__ == '__main__':
    main()