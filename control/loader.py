'''
Created on 14.02.2022

@author: larsw
'''
from pprint import pprint
from model.storage import MySQLStorage, StorageInsertError
from control.scraping import IdealoRequester
import datetime as dt
import numpy as np
import sys
import traceback as tb


class Loader(object):
    '''
    classdocs
    '''


    def __init__(self, storage, scraper):
        self._storage = storage
        self._scraper = scraper
        
    def load_products_of_category (self, category_id, min_date=None):
        category_name = self._scraper.get_name_of_category(category_id,
                                                           min_date=min_date)
        self._storage.store_category(category_id, category_name)
        
        # Product ID -> (Name, Product URL)
        product_ids = self._scraper.get_products_of_category(category_id,
                                                             min_date=min_date)
        
        # {ProductID : Product URL}
        product_detail_pages = {}
        
        for product_id in product_ids:
            # Product Name, Product URL
            _, product_url = product_ids[product_id]
            product_detail_pages[product_id] = IdealoRequester.URL_BASE+product_url
            
        return product_detail_pages
            
    def load_prices (self, product_ids, min_date=None):
        # Product ID -> Series
        prices = self._scraper.get_api(product_ids, min_date=min_date)
        
        for product_id in prices:
            series = prices[product_id]
            self._storage.store_prices(product_id, series)
            
        return prices
    
    def load_product_variants (self, variant_urls, product_categories, min_date=None):
        # variant_urls: {Product ID : Product URL}
        # product_categories: {Product ID : Category ID}
        
        # {Product ID : (Product Name, Datasheet (uncompressed))}
        product_details = self._scraper.get_details_from_variant_pages(variant_urls,
                                                                       min_date=min_date)
        
        stored_pids = []

        for product_id in product_details:
            category_id = product_categories[product_id]
            product_name, datasheet = product_details[product_id]
            
            try:
                self._storage.store_product(product_id, product_name, 
                                            category_id, datasheet)
                stored_pids.append(product_id)
            except StorageInsertError as e:
                errmsg = f"""Product storage failed with
                PID: {product_id}
                CID: {category_id}
                Name: {product_name}
                Datasheet:
                {datasheet}
                """
                print(errmsg, file=sys.stderr)
                tb.print_exc()

        self.load_prices(stored_pids, min_date=min_date)
    
    def load_full_category (self, category_id, min_date=None):        
        # {ProductID : Product Detail Page}
        product_detail_urls = self.load_products_of_category(category_id,
                                                             min_date=min_date)
        print("Product detail URLs: "+str(len(product_detail_urls)))
        pprint(product_detail_urls, indent=3)

        # {Product ID : [URL1, URL2, ...]
        variant_urls = self._scraper.get_product_variants_of_product_detail_page(
                    product_detail_urls,
                    min_date=min_date
        )
        pprint(variant_urls)
        # {Variant Product ID : Variant URL}
        variant_urls = {
                int(IdealoRequester.product_offer_url_to_id(variant_url)) : variant_url
                for x in variant_urls
                for variant_url in variant_urls[x]
            }
        print("Variant URLs: "+str(len(variant_urls)))
        
        # {Product ID : (Product Name, Datasheet (uncompressed))}
        product_details = self._scraper.get_details_from_variant_pages(variant_urls,
                                                                       min_date=min_date)
        product_categories = {x : category_id for x in product_details}
        self.load_product_variants(variant_urls, product_categories,
                                   min_date=min_date)
        
    def _get_executeable_update_runs (self, update_runs, min_update_age):
        if len(update_runs) == 0:
            now = dt.datetime.utcnow()
            last_price_ages = self._storage.get_last_price_ages(now)
            sel = last_price_ages["Age"] >= min_update_age
            last_price_ages = last_price_ages[sel]
            last_price_ages = last_price_ages * 2
            
            periods = [
                    IdealoRequester.get_api_period_for_timedelta(
                            dt.timedelta(seconds=x / np.timedelta64(1, 's'))
                        )
                    for x in last_price_ages["Age"].values
                ]
            
            last_price_ages["Period"] = periods
            
            update_runs = last_price_ages.drop("Age", axis=1)
            update_runs["Date"] = [now for _ in range(len(update_runs))]
            
            self._storage.store_update_runs(update_runs)
        else:
            update_runs = update_runs.reset_index()
            update_runs = update_runs.set_index("ProductId")
        
        return update_runs
        
    def update_prices (self, min_update_age):
        update_runs = self._storage.get_update_runs()
        update_runs = self._get_executeable_update_runs(update_runs, min_update_age)
        
        product_ids = list(update_runs.index.values)
        min_dates = [x.to_pydatetime() for x in update_runs["Date"]]
        periods = list(update_runs["Period"].values)
        
        # Product ID -> Series
        prices = self._scraper.get_api(product_ids, min_date=min_dates, period=periods)
        
        for product_id in prices:
            series = prices[product_id]
            self._storage.store_prices(product_id, series)
        
        for product_id in product_ids:
            v = update_runs.loc[product_id]
            run_date = v["Date"].to_pydatetime()
            
            self._storage.delete_update_run(run_date, product_id)

    def _update_category_indices(self, updateable_df):
        # updateable_df: V_CATEGORY_ID -> [V_TIMESTAMP]
        
        for category_id in updateable_df.index.values:
            timestamp = updateable_df.loc[category_id][MySQLStorage.V_TIMESTAMP]
            print(f"Update of {category_id} at {timestamp}")

            # Loading category ...
            self.load_full_category(category_id, min_date=timestamp)

            self._storage.delete_category_update_run(category_id) 
    
    def _run_current_category_update_runs(self):
        # V_CATEGORY_ID -> V_TIMESTAMP 
        update_runs = self._storage.get_category_update_run()

        if len(update_runs) != 0:
            self._update_category_indices(update_runs)

    def _select_updateable_categories(self, last_updates, min_update_age):
        # V_CATEGORY_ID -> V_TIMESTAMP 

        last_updates[MySQLStorage.V_TIMESTAMP] += min_update_age
        selector = last_updates[MySQLStorage.V_TIMESTAMP] <= dt.datetime.utcnow()
        last_updates = last_updates[selector]
        return last_updates.index.unique()

    def update_categories(self, min_update_age):
        self._run_current_category_update_runs()

        # V_CATEGORY_ID -> V_TIMESTAMP 
        last_updates = self._storage.get_last_category_update()
        updateables = self._select_updateable_categories(last_updates,
                                                         min_update_age)
        utcnow = dt.datetime.utcnow()

        for updateable in updateables:
            self._storage.store_category_update_run(updateable, utcnow)
        
        self._run_current_category_update_runs()
