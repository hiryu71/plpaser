# -*- coding: utf-8 -*-
import pandas as pd
import sys
import numpy as np
import re

import plpaser.consts as cs
import plpaser.df_reader as dr

class PartsList(object):
    
    def __init__(self):
        self.items_dict = cs.ITEMS_DICT
        self.reference = 'Reference'
        self.quantity = 'Quantity'
        self.__check_item(self.reference)
        self.__check_item(self.quantity)
    
    def __check_item(self, item):
        if item not in list(self.items_dict.keys()):
            print('項目名に{0}が含まれていません。'.format(item))
            sys.exit()

    def read_parts_list(self, file_path):
        pl = dr.read_excel_df(file_path, self.items_dict[self.reference])
        pl = dr.format_items(pl, self.items_dict)
        self.pl = pl.assign(memo='')

    # TODO:read_bomメソッドを作成する

    def change_format_A_to_B(self):
        formatter = Formatter(self)
        self.pl = formatter()


class Formatter(object):

    # TODO:initの無駄代入を止める
    def __init__(self, parts_list):
        self.items_dict = parts_list.items_dict 
        self.reference = parts_list.reference
        self.quantity = parts_list.quantity
        self.__init_pl(parts_list)
    
    def __init_pl(self, parts_list):
        self.pl = parts_list.pl.dropna()
        self.pl = self.pl.astype({self.quantity:int})

    def __call__(self):
        self.__split_reference()
        self.pl = self.pl.sort_values(['Ref_mark', 'min_ref_number'], ascending=True)
        self.__prepare_rows()
        self.__reset_reference()
        self.__check_quantity()
        self.__blank()
        self.__reset_items()
        return self.pl
    
    def __split_reference(self):
        self.pl = self.pl.assign(
            Ref_mark = self.pl[self.reference].str.extract(r'(\D+)', expand=False),
            Ref_number='',
            Ref_quantity=np.int32(0),
            min_ref_number=np.int32(0)
        )

        for index, row in self.pl.iterrows():
            number_list = list(map(int, re.findall(r'(\d+)', row[self.reference])))
            number_list.sort()
            self.pl.at[index, 'Ref_number'] = number_list
            self.pl.at[index, 'Ref_quantity'] = len(number_list)
            self.pl.at[index, 'min_ref_number'] = min(number_list)
    
    def __prepare_rows(self):
        # 部品1個ずつに1行用意、部品の種類毎に番号割り振り、indexリセット
        self.pl['Ref_group'] = list(self.pl.index)
        self.pl = self.pl.loc[np.repeat(self.pl.index.values, self.pl.Ref_quantity)]
        self.pl['Ref_count'] = self.pl.groupby('Ref_group').cumcount()
        self.pl = self.pl.reset_index()
    
    def __reset_reference(self):
        for index, row in self.pl.iterrows():
            reference = row['Ref_mark'] + str(row['Ref_number'][row['Ref_count']])
            self.pl.at[index, self.reference] = reference

    def __check_quantity(self):
        WRONG_QUANTITY = (self.pl['Ref_count'] == 0)\
                       & (self.pl[self.quantity] != self.pl['Ref_quantity'])
        self.pl.loc[WRONG_QUANTITY, 'memo'] = '数量が間違っています'
    
    def __blank(self):
        self.pl = self.pl.astype({self.quantity:str})
        self.pl.loc[self.pl['Ref_count'] != 0, self.quantity] = ''
    
    def __reset_items(self):
        items_list = list(self.items_dict.keys())
        items_list.append('memo')
        self.pl = self.pl[items_list]

        


        
