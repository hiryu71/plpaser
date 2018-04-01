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

    def __init__(self, parts_list):
        self.parts_list = parts_list
        self.__init_pl()
    
    def __init_pl(self):
        self.parts_list.pl = self.parts_list.pl.dropna()
        self.parts_list.pl = self.parts_list.pl.astype({self.parts_list.quantity:int})

    def __call__(self):
        self.__split_reference()
        self.parts_list.pl = self.parts_list.pl.sort_values(['Ref_mark', 'min_ref_number'], ascending=True)
        self.__prepare_rows()
        self.__reset_reference()
        self.__check_quantity()
        self.__blank()
        self.__reset_items()
        return self.parts_list.pl
    
    def __split_reference(self):
        self.parts_list.pl = self.parts_list.pl.assign(
            Ref_mark = self.parts_list.pl[self.parts_list.reference].str.extract(r'(\D+)', expand=False),
            Ref_number='',
            Ref_quantity=np.int32(0),
            min_ref_number=np.int32(0)
        )

        for index, row in self.parts_list.pl.iterrows():
            number_list = list(map(int, re.findall(r'(\d+)', row[self.parts_list.reference])))
            number_list.sort()
            self.parts_list.pl.at[index, 'Ref_number'] = number_list
            self.parts_list.pl.at[index, 'Ref_quantity'] = len(number_list)
            self.parts_list.pl.at[index, 'min_ref_number'] = min(number_list)
    
    def __prepare_rows(self):
        # 部品1個ずつに1行用意、部品の種類毎に番号割り振り、indexリセット
        self.parts_list.pl['Ref_group'] = list(self.parts_list.pl.index)
        self.parts_list.pl = self.parts_list.pl.loc[np.repeat(self.parts_list.pl.index.values, self.parts_list.pl.Ref_quantity)]
        self.parts_list.pl['Ref_count'] = self.parts_list.pl.groupby('Ref_group').cumcount()
        self.parts_list.pl = self.parts_list.pl.reset_index()
    
    def __reset_reference(self):
        for index, row in self.parts_list.pl.iterrows():
            reference = row['Ref_mark'] + str(row['Ref_number'][row['Ref_count']])
            self.parts_list.pl.at[index, self.parts_list.reference] = reference

    def __check_quantity(self):
        WRONG_QUANTITY = (self.parts_list.pl['Ref_count'] == 0)\
                       & (self.parts_list.pl[self.parts_list.quantity] != self.parts_list.pl['Ref_quantity'])
        self.parts_list.pl.loc[WRONG_QUANTITY, 'memo'] = '数量が間違っています'
    
    def __blank(self):
        self.parts_list.pl = self.parts_list.pl.astype({self.parts_list.quantity:str})
        self.parts_list.pl.loc[self.parts_list.pl['Ref_count'] != 0, self.parts_list.quantity] = ''
    
    def __reset_items(self):
        items_list = list(self.parts_list.items_dict.keys())
        items_list.append('memo')
        self.parts_list.pl = self.parts_list.pl[items_list]

        


        
