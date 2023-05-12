'''
Created on 19.02.2022

@author: larsw
'''
import pandas as pd
from collections import defaultdict
import re
from abc import ABC, abstractmethod
import os.path as osp
import os
import json

class InterpretationConfig(ABC):
    CATEGORIES_KEY = "categories"

    def __init__(self, categories):
        '''
        Constructor of InterpretationConfig. Requires
        the list of hierarchical category names that
        are selected in this instance.
        Parameters:
            categories: Categories (list of tuples)
        '''
        self._categories = categories

    def get_categories(self):
        '''
        Returns the categories covered by this
        InterpretationConfig instance.
        Returns:
            Categories (list of tuples)
        '''
        return self._categories

    @abstractmethod
    def interpret(self, datasheets):
        pass
    
    @abstractmethod
    def apply_transformations(self, index_translations, dataframe):
        pass

    def to_dict(self):
        d = {
            InterpretationConfig.CATEGORIES_KEY : self._categories
        }
        return d

    def to_json_dict(self):
        categories = [
            list(x)
            for x in self._categories
        ]
        
        d = {
            InterpretationConfig.CATEGORIES_KEY : categories
        }
        return d

class ContinuousConfig(InterpretationConfig):
    INVERSIONS_KEY = "inversions"

    def __init__(self, categories, inversions):
        '''
        Constructor of ContinuousConfig. Requires the
        categories with continuous values and inversion
        list.
        Parameters:
            categories: Categories (list of tuples, length n)
            inversions: Inversions (list of booleans, length n)
        '''
        super().__init__(categories)

        if len(categories) == len(inversions):
            self._inversions = inversions
        else:
            errmsg = "The lengths of categories and inversions do not match."
            raise ValueError(errmsg)

    def get_inversions(self):
        return self._inversions
    
    def to_dict(self):
        d = super().to_dict()
        d[ContinuousConfig.INVERSIONS_KEY] = self._inversions
        return d
    
    @classmethod
    def of_dict(cls, d):
        return ContinuousConfig(
            d[cls.CATEGORIES_KEY],
            d[cls.INVERSIONS_KEY]
        )

    @classmethod
    def of_json_dict(cls, d):
        categories = [
            tuple(x)
            for x in d[cls.CATEGORIES_KEY]
        ]

        return ContinuousConfig(
            categories,
            d[cls.INVERSIONS_KEY]
        )

    def to_json_dict(self):
        d = super().to_json_dict()
        d[ContinuousConfig.INVERSIONS_KEY] = self._inversions
        return d

    @classmethod
    def _unify_units(cls, values, units):
        size_prefixes = ["", "k", "m", "g", "t"]
        max_unit_length = max(len(x) for x in units)
        
        prefix_indices = []
        
        for unit in units:
            unit = unit.lower()
            unit_length = len(unit)
            
            if unit_length == max_unit_length:
                first_symbol = unit[0]
                symbol_index = size_prefixes.index(first_symbol)
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
    def _try_parse_number(self, value):
        if " " in value:
            value = value.split(" ")
            
            unit = value[1]
            value = value[0].replace(".", "").replace(",", ".")
            
            value = float(value)
            return value, unit
        elif "-" in value:
            value = value.split("-")

            unit = value[1]
            value = value[0].replace(".", "").replace(",", ".")
            
            value = float(value)
            return value, unit
        else:
            value = value.replace(".", "").replace(",", ".")
            value = float(value)
            return value, None

    @classmethod
    def _decide_new_attribute_name(cls, units, values, attribute):
        if len(set(units)) == 1:
            units = units[0]
            new_attribute = "{:s} ({:s})".format(attribute, units)
        elif len(set(units)) == 0:
            units = None
            new_attribute = attribute
        else:
            units, values = ContinuousConfig._unify_units(values, units)
            units = units[0]
            new_attribute = "{:s} ({:s})".format(attribute, units)

        return new_attribute, values

    def interpret(self, datasheets): 
        index_translation = {}
        dataframes = []
        
        for category, attribute in self._categories:
            selected = datasheets.xs(category, axis=0, level=1)
            selected = selected.xs(attribute, axis=0, level=1)
            
            product_ids = selected.index.values
            values = selected.values
            units = []
            
            for i in range(len(values)):
                value = values[i]
                
                value, unit = ContinuousConfig._try_parse_number(value)
                values[i] = value

                if unit:
                    units.append(unit)

            new_attribute, values = ContinuousConfig._decide_new_attribute_name(
                units, values, attribute
            )

            s = pd.DataFrame({new_attribute : values}, index=product_ids)
            
            index_translation[(category, attribute)] = new_attribute
            dataframes.append(s)
            
        dataframes = pd.concat(dataframes, axis=1)
        return index_translation, dataframes

    def apply_transformations(self, index_translations, dataframe):
        for cat_attr_pair, invert in zip(self._categories, self._inversions):
            new_column_name = index_translations[cat_attr_pair]

            if invert:
                dataframe[new_column_name] = -dataframe[new_column_name]

        return dataframe

class Interpretation():
    CONTINUOUS_KEY = "continuous"

    def __init__(self, continuous):
        '''
        Constructor of the Interpretation class. Requires the
        configuration for continuous data.
        Parameters:
            continuous: ContinuousConfig instance
        '''
        self._continuous = continuous

    def interpret(self, datasheets):
        return self._continuous.interpret(datasheets)
    
    def apply_transformations(self, index_translations, dataframe):
        dataframe = self._continuous.apply_transformations(index_translations,
                                                           dataframe)
        return dataframe

    @classmethod
    def of_json_dict(cls, d):
        continuous = ContinuousConfig.of_json_dict(d[cls.CONTINUOUS_KEY])

        return Interpretation(continuous)

class InterpretationManager():
    def __init__(self, configs_folder):
        if osp.isdir(configs_folder):
            self._configs_folder = configs_folder
        else:
            errmsg = f"The given folder does not exist:\n{configs_folder}"
            raise ValueError(errmsg)

    def interpret(self, category_id, datasheets,
                  apply_transformations=True):
        if isinstance(category_id, int):
            category_id = str(category_id)

        configs = os.listdir(self._configs_folder)
        configs = [
            x
            for x in configs
            if x.endswith(".json")
        ]
        
        interpretation = None

        for config in configs:
            if config.replace(".json", "") == category_id:
                config = osp.join(self._configs_folder, config)

                with open(config, "rb") as f:
                    config = f.read()
                
                config = config.decode("iso8859-1")
                config = json.loads(config)

                interpretation = Interpretation.of_json_dict(config)
                break

        if interpretation:
            index_translations, dataframe = interpretation.interpret(datasheets)
            
            if apply_transformations:
                dataframe = interpretation.apply_transformations(index_translations,
                                                                 dataframe)

            print(index_translations)
            print(dataframe)

            return index_translations, dataframe 

        errmsg = f"No interpretation has been found for {category_id}."
        raise ValueError(errmsg)

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
