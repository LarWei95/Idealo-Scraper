import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from model.storage import MySQLStorage

class Analysis ():
    def __init__(self, storage):
        '''
        Constructor of the class analysis. Requires a Storage,
        e.g. a MySQLStorage instance.
        Parameters:
            storage: Storage instance
        '''
        self._storage = storage

    def get_category_time_series_statistics(self, percentiles=[.25, .5, .75]):
        categories = self._storage.get_category()
        category_indices = categories.index.values
        names = categories[MySQLStorage.V_CATEGORY_NAME].values
        
        nkeys = list(zip(category_indices, names))
        names = [
                    MySQLStorage.V_CATEGORY_ID,
                    MySQLStorage.V_CATEGORY_NAME,
                    MySQLStorage.V_DATE
                ]

        descriptions = []

        for cid in category_indices:
            prices = self._storage.get_prices_of_category(cid)[MySQLStorage.V_PRICE]

            grouped_prices = prices.groupby(MySQLStorage.V_DATE)

            described = grouped_prices.describe(percentiles=percentiles)
            descriptions.append(described)
        
        descriptions = pd.concat(descriptions, keys=nkeys, names=names)
        return descriptions
