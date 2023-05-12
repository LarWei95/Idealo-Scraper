'''
Created on 05.01.2022

@author: larsw
'''
from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import time
from abc import ABC, abstractmethod
import datetime as dt
from webrequestmanager.control.api import WebRequestAPIClient
import numpy as np

class StatusError (Exception):
    def __init__ (self, url, status_code, response_header):
        super().__init__("Error: {:d}\n{:s}\n{:s}".format(
                status_code, url, str(response_header)
            ))
        
        self.url = url
        self.status_code = status_code

class RequestManager (ABC):
    def __init__ (self):
        pass
    
    @abstractmethod
    def request (self, url, header, status_codes=200, max_age=None, min_date=None):
        pass
    
    @abstractmethod
    def fetch (self, key):
        pass
    
class StandardRequestManager (RequestManager):
    def __init__ (self):
        self._session = requests.Session()
        
        self._buffer = {}
        
    def request (self, url, header, status_codes=200, max_age=None, min_date=None):
        hashable_header = tuple(list(header.keys()) + list(header.values()))
        
        h = hash(url) + 31 * hash(hashable_header)
        
        html = self._session.get(url, headers=IdealoRequester.HEADERS_DICT,
                            allow_redirects=False)
        
        if html.status_code == status_codes:
            self._buffer[h] = html.content
        else:
            exc = StatusError(url, html.status_code, html.headers)
            raise exc
        
    def fetch (self, key):
        c = self._buffer[key]
        del self._buffer[key]
        
        return c, 200
    
class WebRequestManager (RequestManager):
    def __init__ (self, api, max_age):
        self._api = api
        self._max_age = max_age
        
    def request (self, url, header, status_codes=200, max_age=None, min_date=None):
        if min_date is None:
            if max_age is None:
                max_age = self._max_age
            
            max_date = dt.datetime.utcnow()
            min_date = max_date - max_age
        else:
            max_date = dt.datetime.utcnow()
        
        request_id = self._api.post_page_request(url, header, 
                                                 accepted_status=status_codes,
                                                 min_date=min_date, max_date=max_date)
        return request_id
    
    def fetch (self, key):
        response_df = self._api.get_response(request_id=key, wait=True)
        return response_df["Content"], response_df["StatusCode"]

class IdealoRequester ():
    HEADERS_DICT = {
            "Accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Encoding" : "gzip, deflate, br",
            "Accept-Language" : "de,en-US;q=0.7,en;q=0.3",
            "Cache-Control" : "max-age=0",
            "Connection" : "keep-alive",
            "Host" : "www.idealo.de",
            "Sec-Fetch-Dest" : "document",
            "Sec-Fetch-Mode" : "navigate",
            "Sec-Fetch-Site" : "same-origin",
            "TE" : "trailers",
            "Upgrade-Insecure-Requests" : "1",
            "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0"
        }
    
    URL_BASE = "https://www.idealo.de" 
    PRODOFFERS_SEGMENT_FORMAT = URL_BASE+"/offerpage/offerlist/product/{:s}/start/{:d}/sort/default?includeFilters=0&excludeFilters="
    API_FORMAT = URL_BASE+"/offerpage/pricechart/api/{:d}?period={:s}"
    API_PERIODS = {
            dt.timedelta(days=30) : "P1M",
            dt.timedelta(days=90) : "P3M",
            dt.timedelta(days=180) : "P6M",
            dt.timedelta(days=365) : "P1Y",
            dt.timedelta(days=2) : "P2D",
            dt.timedelta(days=500) : "P500D"
        }
    SORTED_API_PERIODS = sorted(list(API_PERIODS.keys()))
    CAT_START_FORMAT = URL_BASE+"/preisvergleich/ProductCategory/{:d}.html"
    CAT_CONT_FORMAT = URL_BASE+"/preisvergleich/ProductCategory/{:d}I16-{:d}.html"
    
    def __init__ (self, request_manager):
        # Time to wait before each request in order to
        # mitigate HTTP 429 Too Many Requests
        
        self._reqman = request_manager        
    
    @classmethod
    def get_api_period_for_timedelta (cls, td):
        start = dt.timedelta(seconds=0)
        
        for i in range(0, len(cls.SORTED_API_PERIODS)):
            end = cls.SORTED_API_PERIODS[i]
            
            if (td >= start) and (td < end):
                return cls.API_PERIODS[end]
            
            start = end
            
        return cls.API_PERIODS[cls.SORTED_API_PERIODS[-1]]
        
        
    
    def get_api (self, product_id, max_age=None, min_date=None, period=None):
        if period is None:
            period = "P500D"
        
        if isinstance(product_id, int):
            url = IdealoRequester.API_FORMAT.format(product_id, period)
            html, _ = self._reqman.fetch(
                        self._reqman.request(
                                url, 
                                IdealoRequester.HEADERS_DICT, 
                                max_age=max_age, 
                                min_date=min_date
                        )
            )
            
            data = json.loads(html.decode("utf-8"))["data"]
            
            indx = []
            values = []
            
            for v in data:
                x = v["x"]
                y = v["y"]
                
                indx.append(x)
                values.append(y)
            
            indx = pd.Index(indx)
            indx = pd.to_datetime(indx)
            
            return pd.Series(values, indx)
        elif isinstance(product_id, (list, tuple)):
            if not isinstance(max_age, (list, tuple)):
                max_age = [max_age for _ in range(len(product_id))]
                
            if not isinstance(min_date, (list, tuple)):
                min_date = [min_date for _ in range(len(product_id))]
                
            if not isinstance(period, (list, tuple)):
                period = [period for _ in range(len(product_id))]
            
            keys = []
            
            for pid, cmax_age, cmin_date, cperiod in zip(product_id, max_age, min_date, period):
                url = IdealoRequester.API_FORMAT.format(pid, cperiod)
                keys.append(
                    self._reqman.request(
                        url, 
                        IdealoRequester.HEADERS_DICT,
                        max_age=cmax_age,
                        min_date=cmin_date
                    )
                )
                
            datas = {}
                
            for key, pid in zip(keys, product_id):
                html, _ = self._reqman.fetch(key)
                
                data = json.loads(html.decode("utf-8"))["data"]
                
                indx = []
                values = []
                
                for v in data:
                    x = v["x"]
                    y = v["y"]
                    
                    indx.append(x)
                    values.append(y)
                
                indx = pd.Index(indx)
                indx = pd.to_datetime(indx)
                
                datas[pid] = pd.Series(values, index=indx)
                
            return datas
        else:
            errmsg = "Wrong product id type: {:s}".format(str(type(product_id)))
            raise TypeError(errmsg)
    
    @classmethod
    def product_offer_url_to_id (cls, url):
        last_slash = url.rfind("/")
        url = url[last_slash+1:]
        
        if "_-" in url:
            separator = url.rfind("_-")
        else:
            separator = url.rfind(".")
            
        return url[:separator]
        
    @classmethod
    def scrape_items_from_product_category (cls, html):
        all_products = {}
        
        html = BeautifulSoup(html, "html.parser")
        html = html.find("div", {"class" : "resultlist"})
        products = html.find_all("div", {"class" : "offerList-item"})
        
        for product in products:
            a = product.find("a", {"class" : "offerList-itemWrapper"})
            product_url = a["href"].strip()
            product_id = IdealoRequester.product_offer_url_to_id(product_url)
            
            try:
                product_id = int(product_id)
            except:
                continue
            
            name = a.find("div", {"class" : "offerList-item-detailsWrapper"})
            name = name.find("div", {"class" : "offerList-item-description-title"}).get_text().strip()
            
            all_products[product_id] = (name, product_url)
            
        return all_products
        
    def get_name_of_category (self, category_id, max_age=None, min_date=None):
        url = IdealoRequester.CAT_START_FORMAT.format(category_id)
        content, _ = self._reqman.fetch(
            self._reqman.request(
                url,
                IdealoRequester.HEADERS_DICT,
                200,
                max_age=max_age,
                min_date=min_date
            )
        )
        
        content = BeautifulSoup(content, "html.parser")
        header = content.find("div", {"class" : "category-headline"})
        header = header.find("h1", {"class" : "offerList-title"}).get_text().strip()
        
        return header
        
    def get_products_of_category (self, category_id, max_age=None, min_date=None):
        # https://www.idealo.de/preisvergleich/ProductCategory/16073.html
        # https://www.idealo.de/preisvergleich/ProductCategory/16073I16-15.html
        # Index zu hoch -> 301 Moved Permanently, Verweis auf Anfangsseite
        all_products = {}
        
        url = IdealoRequester.CAT_START_FORMAT.format(category_id)
        
        offset_count = 40
        step = 15
        counter_start = 0
        
        while True:
            offsets = np.arange(counter_start, counter_start + step * offset_count, step)
            print("Loading products: "+str(offsets))
            counter_start = counter_start + step * offset_count
            
            request_ids = []
            
            for offset in offsets:
                if offset == 0:
                    url = IdealoRequester.CAT_START_FORMAT.format(category_id)
                else:
                    url = IdealoRequester.CAT_CONT_FORMAT.format(category_id, offset)
                    
                request_id = self._reqman.request(
                    url,
                    IdealoRequester.HEADERS_DICT,
                    [200, 301],
                    max_age=max_age,
                    min_date=min_date
                )
                request_ids.append(request_id)
                
            hit_final_page = False
                
            for request_id in request_ids:
                content, status_code = self._reqman.fetch(request_id)
                
                if status_code != 301:
                    subproducts = IdealoRequester.scrape_items_from_product_category(content)
                    all_products.update(subproducts)
                else:
                    hit_final_page = True
                    break
                
            if hit_final_page:
                break
        
        return all_products
    
    @classmethod
    def scrape_variants_from_product_detail (cls, html):
        html = BeautifulSoup(html, "html.parser")
        html = html.find("div", id="product-variants")
        
        if html is None:
            # No variants exist, given page is the product
            return None
        else:
            html = html.find_all("a", {"class" : "productVariants-listItemWrapper"})
            
            urls = set()
            
            for variant_box in html:
                urls.add(cls.URL_BASE+variant_box["href"].strip())
            
            return urls
    
    def get_product_variants_of_product_detail_page (self, pdp_dict, max_age=None, min_date=None):
        # pdp_dict: {Product ID : Product Detail URL}
        
        request_ids = {
                product_id : self._reqman.request(
                    pdp_dict[product_id], 
                    IdealoRequester.HEADERS_DICT,
                    200,
                    max_age=max_age,
                    min_date=min_date
                )
                for product_id in pdp_dict
            }
        
        variant_urls = {}
        
        for product_id in request_ids:
            request_id = request_ids[product_id]
            html, _ = self._reqman.fetch(request_id)
            
            detail_url = IdealoRequester.scrape_variants_from_product_detail(html)
            
            if detail_url is None:
                detail_url = set([pdp_dict[product_id]])
                
            variant_urls[product_id] = detail_url
        
        return variant_urls
    
    @classmethod
    def _parse_datasheet_rows (cls, rows):
        header = "General"
        
        all_data_headers = []
        all_data_serieses = []
        
        collected_indx = []
        collected_values = []
        
        for row in rows:
            row_cls = row.get("class", None)
            
            if row_cls is None:
                subrows = row.find_all("li")
                
                for subrow in subrows:
                    cols = subrow.find_all("span")
                    
                    col = cols[0].get_text().strip()
                    value = cols[1].get_text().strip()
                    
                    collected_indx.append(col)
                    collected_values.append(value)
            else:
                new_header = row.get_text().replace("\t", "").replace("\n", "")
                
                if len(collected_indx) != 0:
                    s = pd.Series(collected_values, index=collected_indx)
                    all_data_headers.append(header)
                    all_data_serieses.append(s)
                    
                    collected_indx = []
                    collected_values = []
                    
                header = new_header
                
        if len(collected_indx) != 0:
            s = pd.Series(collected_values, index=collected_indx)
            all_data_headers.append(header)
            all_data_serieses.append(s)
            
        if len(all_data_serieses) != 0:
            all_data_serieses = pd.concat(all_data_serieses, axis=0,
                                          keys=all_data_headers)
        else:
            all_data_serieses = None
            
        return all_data_serieses
    
    @classmethod
    def scrape_product_details (cls, html):
        html = BeautifulSoup(html, "html.parser")
        
        name_element = html.find("h1", {"class" : "oopStage-title"})
        name_element = name_element.find_all("span")
        
        if len(name_element) == 2:
            main_name = name_element[0].get_text().strip()
            variant_name = name_element[1].get_text().strip()
            product_name = main_name+" "+variant_name
        else:
            product_name = name_element[0].get_text().strip()
        
        try:
            print(product_name)
        except:
            print("Produktname kann nicht wiedergegeben werden.")
            
        datasheet = html.find("div", id="datasheet")
        datasheet = datasheet.find("ul", {"class" : "datasheet-list"})
        
        datasheet = cls._parse_datasheet_rows(datasheet.find_all("li"))
        
        return product_name, datasheet
    
    def get_details_from_variant_pages (self, variant_page_dict, max_age=None, min_date=None):
        # variant_page_dict: {Product ID : Variant Page URL}
        
        request_ids = {
                product_id : self._reqman.request(
                        variant_page_dict[product_id],
                        IdealoRequester.HEADERS_DICT,
                        200,
                        max_age=max_age,
                        min_date=min_date
                    )
                for product_id in variant_page_dict
            }
        
        details = {}
        
        for product_id in request_ids:
            request_id = request_ids[product_id]
            
            html, _ = self._reqman.fetch(request_id)
            product_name, datasheet = IdealoRequester.scrape_product_details(html)
            
            details[product_id] = (product_name, datasheet)
            
        return details
