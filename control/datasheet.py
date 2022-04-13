'''
Created on 19.02.2022

@author: larsw
'''
import pandas as pd
from collections import defaultdict
import re

class Datasheet():
    @classmethod
    def collapse_product_info_to_datasheets (cls, product_info):
        product_info = product_info["Datasheet"]
        product_info = pd.concat(product_info.values, axis=0, keys=product_info.index.values)
        return product_info
    
    @classmethod
    def get_attribute_occurences (cls, datasheets):
        attribute_counter = defaultdict(dict)
        
        unique_product_ids = datasheets.index.get_level_values(0).unique()
        unique_count = len(unique_product_ids)
        
        for product_id in unique_product_ids:
            subproduct = datasheets.xs(product_id, level=0)
            
            main_level = subproduct.index.get_level_values(0).unique()
            
            for main_lev in main_level:
                subsubproduct = subproduct.xs(main_lev, level=0)
                
                sub_level = subsubproduct.index.get_level_values(0).unique()
                
                for sub_lev in sub_level:
                    if sub_lev in attribute_counter[main_lev]:
                        attribute_counter[main_lev][sub_lev] += 1
                    else:
                        attribute_counter[main_lev][sub_lev] = 1
                        
        df_indx = []
        df_values = []
        
        for main_level in attribute_counter:
            main_level_counter = attribute_counter[main_level]
            
            for sub_level in main_level_counter:
                count = main_level_counter[sub_level] / unique_count
                
                df_indx.append((main_level, sub_level))
                df_values.append(count)
                
        df = pd.Series(df_values, index=pd.MultiIndex.from_tuples(df_indx))
        return df
        
    @classmethod
    def get_attribute_value_occurences (cls, attribute_category, attribute, datasheets):
        selected = datasheets.xs(attribute_category, axis=0, level=1)
        selected = selected.xs(attribute, axis=0, level=1)
        unique_values = selected.unique()
        return unique_values.tolist()
    
    @classmethod
    def get_attribute_value_pairs (cls, datasheets):
        attributes = cls.get_attribute_occurences(datasheets)
        
        all_unique_values = {}
        
        for main_level, sub_level in attributes.index.values:
            unique_values = cls.get_attribute_value_occurences(
                    main_level, 
                    sub_level, 
                    datasheets
                )
            all_unique_values[(main_level, sub_level)] = unique_values
            
        return attributes, all_unique_values
        
    @classmethod
    def _unify_units (cls, values, units):
        size_prefixes = ["", "k", "m", "g", "t"]
        max_unit_length = max(len(x) for x in units)
        
        prefix_indices = []
        
        for unit in units:
            unit = unit.lower()
            unit_length = len(unit)
            
            if unit_length == max_unit_length:
                first_symbol = unit[0]
                symbol_index = size_prefixes.index(first_symbol)
                print(unit, first_symbol, symbol_index)
            else:
                errmsg = "???"+str(unit)
                raise ValueError(errmsg)
            
            
            prefix_indices.append(symbol_index)
            
        max_prefix_index = max(prefix_indices)
        new_values = []
        new_units = []
        
        for unit, value, prefix_index in zip(units, values, prefix_indices):
            unit = size_prefixes[max_prefix_index]+unit[1:]
            value = value / (1000**(max_prefix_index - prefix_index))
            
            new_units.append(unit)
            new_values.append(value)
            
        return new_units, new_values
        
    @classmethod
    def _interpret_numerical (cls, datasheets, indices):
        index_translation = {}
        dataframes = []
        
        for category, attribute in indices:
            selected = datasheets.xs(category, axis=0, level=1)
            selected = selected.xs(attribute, axis=0, level=1)
            
            product_ids = selected.index.values
            values = selected.values
            units = []
            
            for i in range(len(values)):
                value = values[i]
                
                if " " in value:
                    value = value.split(" ")
                    
                    unit = value[1]
                    value = value[0].replace(".", "").replace(",", ".")
                    
                    value = float(value)
                    values[i] = value
                    units.append(unit)
                else:
                    value = value.replace(".", "").replace(",", ".")
                    value = float(value)
                    values[i] = value
        
            if len(set(units)) == 1:
                units = units[0]
                new_attribute = "{:s} ({:s})".format(attribute, units)
            elif len(set(units)) == 0:
                units = None
                new_attribute = attribute
            else:
                units, values = cls._unify_units(values, units)
                units = units[0]
                new_attribute = "{:s} ({:s})".format(attribute, units)
            
            s = pd.DataFrame({new_attribute : values}, index=product_ids)
            
            index_translation[(category, attribute)] = new_attribute
            dataframes.append(s)
            
        dataframes = pd.concat(dataframes, axis=1)
        return index_translation, dataframes
            
    @classmethod
    def interpret (cls, datasheets, continuous=[]):
        # continuous, ...: [(Category, Attribute), ...]
        index_translation, dataframe = cls._interpret_numerical(datasheets, continuous)
        
        return index_translation, dataframe
    
    @classmethod
    def analyse (cls, df, prices):
        df = df.copy().dropna()
        prices = prices.copy().reindex(df.index).dropna()
        
        std_data = df.subtract(df.mean(axis=0)).divide(df.std(axis=0))
        std_prices = prices.subtract(prices.mean(axis=0)).divide(prices.std(axis=0))
        
        performance = std_data.mean(axis=1)
        performance_price_diff = performance - std_prices["Price"]
        
        performance = performance.to_frame("Performance")
        performance_price_diff = performance_price_diff.to_frame("Difference")
        std_prices = std_prices.rename({"Price" : "StdPrice"}, axis=1)
        
        performance_df = pd.concat([performance, std_prices, performance_price_diff],
                                    axis=1)
        performance_df = performance_df.sort_values("Difference")
        return performance_df