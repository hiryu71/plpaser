# -*- coding: utf-8 -*-
import pandas as pd
import glob

# ファイル読み込みとチェック（最初のシートに部品表があることが前提）
def read_and_check(file_path):

    SEARCH_ROWS = 20
    SEARCH_COLS = 5
    KEY = '部品番号'
    CHOISE_COLS = ['部品番号', '型式', 'メーカ', '数量']

    # 項目の行数を検索
    items_row = -1
    df = pd.read_excel(file_path, header=None, nrows=SEARCH_ROWS, usecols=range(SEARCH_COLS))
    for i in range(SEARCH_COLS):
        tmp = df[i].str.contains(KEY)
        for j, value in tmp.iteritems():
            if value == True:
                items_row = j
                
    if items_row == -1:
        print('部品番号の項目のセルが見つかりません。')

    # ファイル読み込み
    df = pd.read_excel(file_path, skiprows=items_row)

    tmp = df.columns
    tmp_set = set(tmp)
    choice_cols_set = set(CHOISE_COLS)
    tmp = list(tmp_set & choice_cols_set)
    if len(CHOISE_COLS) != len(tmp):
        print('項目名が不足しています。')
        print('項目名に%sが含まれているか確認してださい。' % CHOISE_COLS)
    
    return df


# ファイル読み込み
def file_reader(folder_path):

    search_file_path = folder_path + '/*.xls*'
    file_list = glob.glob(search_file_path)
    file_list.sort()
    old_file = file_list[0]
    new_file = file_list[1]

    old_df = read_and_check(old_file)
    new_df = read_and_check(new_file)
    
    return old_df, new_df

# ファイル書き込み
def file_writer(old_df, new_df, folder_path):
    old_path = folder_path + '/01_oldFormat.csv'
    new_path = folder_path + '/02_newFormat.csv'

    old_df.to_csv(old_path, index=False)
    new_df.to_csv(new_path, index=False)

