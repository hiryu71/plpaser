# -*- coding: utf-8 -*-
import pandas as pd
import glob
import numpy as np
import re

# 部品表を比較するための前処理
def plpaser():
    # ファイル読み込み
    SEARCH_ROWS = 20
    SEARCH_COLS = 5
    KEY = '部品番号'

    items_row = -1
    file_name = glob.glob('data/*.xls*')[0]

    df = pd.read_excel(file_name, header=None, nrows=SEARCH_ROWS, usecols=range(SEARCH_COLS))
    for i in range(SEARCH_COLS):
        tmp = df[i].str.contains(KEY)
        for j, value in tmp.iteritems():
            if value == True:
                items_row = j
                
    if items_row == -1:
        print('部品番号の項目のセルが見つかりません。')

    df = pd.read_excel(file_name, skiprows=items_row)

    tmp = df.columns
    choice_cols = ['部品番号', '型式', 'メーカ', '数量']

    tmp_set = set(tmp)
    choice_cols_set = set(choice_cols)
    tmp = list(tmp_set & choice_cols_set)
    if len(choice_cols) != len(tmp):
        print('項目名が不足しています。')
        print('項目名に%sが含まれているか確認してださい。' % choice_cols)

    # 準備
    choice_cols = ['部品番号', '型式', 'メーカ', '数量']
    df0 = df[choice_cols]
    df0.columns = ['index', 'value', 'maker', 'quantity']
    df0['index_mark'] = ''
    df0['min_index_number'] = 0
    df0['index_numbers'] = ''
    df0['index_quantity'] = 0
    df0['index_group'] = 0
    df0['index_count'] = 0
    df0['memo'] = ''
    df0 = df0.dropna()

    # 各部品の先頭行の部品番号を最小にするための下準備
    df1 = df0.assign(index_mark = df0['index'].str.extract('(\D+)', expand=False))
    for i, row in df1.iterrows():
        number_list = []
        indexs = re.split(r",\s|,", row['index'])
        for j in range(len(indexs)):
            number_list.append(int(re.findall(r'(\d+|\D+)', indexs[j])[1]))
        df1.at[i, 'min_index_number'] = min(number_list)
        number_list.sort()
        df1.at[i, 'index_numbers'] = number_list
        df1.at[i, 'index_quantity'] = len(indexs)
        df1.at[i, 'index_group'] = i

    # ソート、部品を1個ずつ1行用意、各部品毎に番号割り振り、indexリセット
    df2 = df1.sort_values(by=['index_mark', 'min_index_number'], ascending=True)
    df2 = df2.loc[np.repeat(df2.index.values, df2.index_quantity)]
    df2['index_count'] = df2.groupby('index_group').cumcount()
    df2 = df2.reset_index()

    # 部品番号を分解、各部品の先頭行以外の数量を0に変更、エラー処理
    df3 = df2.copy()
    for i, row in df3.iterrows():
        strings = row['index_mark'] + str(row['index_numbers'][row['index_count']])
        df3.at[i, 'index'] = strings

        if row['index_count'] == 0:
            if row['quantity'] != len(row['index_numbers']):
                df3.at[i, 'memo'] = '数量が間違っています'
        else:
            df3.at[i, 'quantity'] = 0
            
    # 不要な行を削除
    drop_col = ['level_0', 'index_mark', 'min_index_number', 'index_numbers', 'index_quantity', 'index_group', 'index_count']
    df4 = df3.drop(drop_col, axis=1)

    # 数量を修正
    df4['quantity'] = df4['quantity'].astype('int')
    df4['quantity'] = df4['quantity'].astype('str')
    df4.loc[df4.quantity == '0', 'quantity'] = ''

    # ファイル出力
    df4.to_csv('data/01_oldFormat.csv', index=False)