# -*- coding: utf-8 -*-
import pandas as pd
import sys
import glob

SEARCH_ROWS = 20
SEARCH_COLS = 5

# ファイルリストを生成
def create_files_list(folder_path, extension='*'):

    files_path = folder_path + '/*.' + extension
    files_list = glob.glob(files_path)
    files_list.sort()

    return files_list

# 項目の行を検索
def search_items_row(df, index):

    items_row = -1
    for i in range(SEARCH_COLS):
        tmp = df[i].str.contains(index)
        for j, value in tmp.iteritems():
            if value == True:
                items_row = j
                break
        else:
            continue
        break
    
    if items_row == -1:
        print('項目のセルが見つかりません。')
        sys.exit()  

    return  items_row

# ExcelファイルからDataFrameを読み込み
def read_excel_df(file_path, index):

    df = pd.read_excel(file_path, header=None, nrows=SEARCH_ROWS, usecols=range(SEARCH_COLS))
    items_row = search_items_row(df, index)    
    df = pd.read_excel(file_path, skiprows=items_row)

    return df

# 項目を整形する
def format_items(df, items_dict):

    values = list(items_dict.values())
    keys = list(items_dict.keys())

    try:
        df = df[values]
    except KeyError:
        print('項目名が不足しています。')
        print('項目名に%sが含まれているか確認してださい。' % values)
        sys.exit()

    df.columns = keys
    df = df.dropna(how='all')

    return df
