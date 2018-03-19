# -*- coding: utf-8 -*-
import pandas as pd

import plpaser.consts as cs
import plpaser.df_reader as dr

class PartsList:
    
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