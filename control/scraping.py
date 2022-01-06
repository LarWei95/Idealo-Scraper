'''
Created on 05.01.2022

@author: larsw
'''
from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import time

class StatusError (Exception):
    def __init__ (self, url, status_code, response_header):
        super().__init__("Error: {:d}\n{:s}\n{:s}".format(
                status_code, url, str(response_header)
            ))
        
        self.url = url
        self.status_code = status_code
        

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
            "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0"
        }
    
    PRODOFFERS_SEGMENT_FORMAT = "https://www.idealo.de/offerpage/offerlist/product/{:s}/start/{:d}/sort/default?includeFilters=0&excludeFilters="
    API_FORMAT = "https://www.idealo.de/offerpage/pricechart/api/{:s}?period=P500D"
    
    CAT_START_FORMAT = "https://www.idealo.de/preisvergleich/ProductCategory/{:s}.html"
    CAT_CONT_FORMAT = "https://www.idealo.de/preisvergleich/ProductCategory/{:s}I16-{:d}.html"
    
    def __init__ (self, wait_time=10):
        self._session = requests.Session()
        # Time to wait before each request in order to
        # mitigate HTTP 429 Too Many Requests
        self._wait_time = wait_time
    
    def request (self, url):
        time.sleep(self._wait_time)
        html = self._session.get(url, headers=IdealoRequester.HEADERS_DICT,
                            allow_redirects=False)
        
        if html.status_code == 200:
            return html.content
        else:
            exc = StatusError(url, html.status_code, html.headers)
            raise exc
    
    @classmethod
    def _parse_product_offers_segment (cls, html):
        html = BeautifulSoup(html, "html.parser")
        
        elements = html.find_all("li", {"class" : "productOffers-listItem"})
        prices = []
        
        for element in elements:
            element = element.find("div", {"data-offerlist-column" : "price"})
            element = element.find("a", {"class" : "productOffers-listItemOfferPrice"})
            
            price = element.get_text().strip()[:-2].replace(".", "").replace(",", ".")
            price = float(price)
            prices.append(price)
            
        return prices
    
    def get_product_offers_segment (self, product_id, offset):
        # Trash
        url = IdealoRequester.PRODOFFERS_SEGMENT_FORMAT.format(
                product_id,
                offset
            )
        
        html = self.request(url)
        prices = IdealoRequester._parse_product_offers_segment(html)
        return prices
    
    def get_product_offers (self, product_id):
        # Trash
        all_prices = []
        
        offset = 0
        
        new_prices = True
        
        while new_prices:
            prices = self.get_product_offers_segment(product_id, offset)
            print(prices)
            if offset != 0:
                if prices[0] == all_prices[0]:
                    new_prices = False
                    
            all_prices.extend(prices)
            
            offset += 15
            
        return prices
    
    def get_api (self, product_id):
        url = IdealoRequester.API_FORMAT.format(product_id)
        html = self.request(url)
        
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
    
    @classmethod
    def _product_offer_url_to_id (cls, url):
        last_slash = url.rfind("/")
        separator = url.rfind("_-")
        return url[last_slash+1 : separator]
        
    
    def get_products_of_category (self, category_id):
        # https://www.idealo.de/preisvergleich/ProductCategory/16073.html
        # https://www.idealo.de/preisvergleich/ProductCategory/16073I16-15.html
        # Index zu hoch -> 301 Moved Permanently, Verweis auf Anfangsseite
        all_products = {}
        
        url = IdealoRequester.CAT_START_FORMAT.format(category_id)
        
        counter = 0
        
        while True:
            try:
                html = self.request(url)
            except StatusError as e:
                if e.status_code == 301:
                    break
                else:
                    raise e
            
            html = BeautifulSoup(html, "html.parser")
            html = html.find("div", {"class" : "resultlist"})
            products = html.find_all("div", {"class" : "offerList-item"})
            
            for product in products:
                a = product.find("a", {"class" : "offerList-itemWrapper"})
                product_url = a["href"].strip()
                product_id = IdealoRequester._product_offer_url_to_id(product_url)
                
                try:
                    int(product_id)
                except:
                    continue
                
                name = a.find("div", {"class" : "offerList-item-detailsWrapper"})
                name = name.find("div", {"class" : "offerList-item-description-title"}).get_text().strip()
                
                all_products[name] = product_id
                
            counter += 15
            print(counter, len(all_products))
            url = IdealoRequester.CAT_CONT_FORMAT.format(category_id, counter)
        
        return all_products