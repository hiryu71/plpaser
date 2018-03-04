# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re

import plpaser.filepaser as fp
import plpaser.fileservice as fs
import plpaser.df_reader as dr
import plpaser.consts as cs

# 部品表を比較するための前処理
def plpaser(read_folder, write_folder):
    
    # ファイル読み込み
    print('ファイル読み込み')
    files_list = dr.create_files_list('data', 'xls*')
    index = list(cs.COLS_DICT.values())[0]

    old_df = dr.read_excel_df(files_list[0], index)
#    old_df = dr.format_items(old_df, cs.COLS_DICT)

    new_df = dr.read_excel_df(files_list[1], index)
#    new_df = dr.format_items(new_df, cs.COLS_DICT)    

    # 部品表を前処理
    print('旧フォーマットの部品表を前処理')
    old_df = fp.old_format_paser(old_df)

    print('新フォーマットの部品表を前処理')
    new_df = fp.new_format_paser(new_df)

    print('差分チェック')
    old_df, new_df, dif_str = fp.check_dif(old_df, new_df)

    # ファイル出力
    fs.file_writer(old_df, new_df, dif_str, write_folder)
