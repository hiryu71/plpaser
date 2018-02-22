# -*- coding: utf-8 -*-
import pandas as pd
import sys
import numpy as np
import re

import plpaser.consts as cs

# 旧フォーマットの部品表を前処理
def old_format_paser(df):

    # 準備
    try:
        df0 = df[cs.CHOISE_COLS]
    except KeyError:
        print('項目名が不足しています。')
        print('項目名に%sが含まれているか確認してださい。' % cs.CHOISE_COLS)
        sys.exit()
    
    try:
        df0.columns = cs.NEW_COLS
    except ValueError:
        print('項目数が不足しています。')
        sys.exit()

    df0 = df0.dropna(how='all')

    df0['Reference_mark'] = ''
    df0['min_reference_number'] = np.int32(0)
    df0['Reference_number'] = ''
    df0['Reference_quantity'] = np.int32(0)
    df0['Reference_group'] = np.int32(0)
    df0['Reference_count'] = np.int32(0)
    df0['memo'] = ''

    df0 = df0.dropna()
    
    # 各部品の先頭行の部品番号を最小にするための下準備
    df1 = df0.assign(Reference_mark = df0['Reference'].str.extract(r'(\D+)', expand=False))
    for i, row in df1.iterrows():
        number_list = []
        indexs = re.split(r",\s|,", row['Reference'])
        for j in range(len(indexs)):
            number_list.append(int(re.findall(r'(\d+|\D+)', indexs[j])[1]))
        df1.at[i, 'min_reference_number'] = min(number_list)
        number_list.sort()
        df1.at[i, 'Reference_number'] = number_list
        df1.at[i, 'Reference_quantity'] = len(indexs)
        df1.at[i, 'Reference_group'] = i

    # ソート、部品を1個ずつ1行用意、各部品毎に番号割り振り、indexリセット
    df2 = df1.sort_values(['Reference_mark', 'min_reference_number'], ascending=True)
    df2 = df2.loc[np.repeat(df2.index.values, df2.Reference_quantity)]
    df2['Reference_count'] = df2.groupby('Reference_group').cumcount()
    df2 = df2.reset_index()

    # 部品番号を合体、各部品の先頭行以外の数量を0に変更、エラー処理
    df3 = df2.copy()
    for i, row in df3.iterrows():
        strings = row['Reference_mark'] + str(row['Reference_number'][row['Reference_count']])
        df3.at[i, 'Reference'] = strings

        if row['Reference_count'] == 0:
            if row['Quantity'] != len(row['Reference_number']):
                df3.at[i, 'memo'] = '数量が間違っています'
        else:
            df3.at[i, 'Quantity'] = 0

    # 不要な行を削除
    drop_col = ['index', 'Reference_mark', 'min_reference_number', 'Reference_number', 'Reference_quantity', 'Reference_group', 'Reference_count']
    df4 = df3.drop(drop_col, axis=1)

    # 数量を修正
    df4['Quantity'] = df4['Quantity'].astype('int')
    df4['Quantity'] = df4['Quantity'].astype('str')
    df4.loc[df4.Quantity == '0', 'Quantity'] = ''

    return df4

# 新フォーマットの部品表を前処理
def new_format_paser(df):

    # 準備
    try:
        df = df[cs.CHOISE_COLS]
    except KeyError:
        print('項目名が不足しています。')
        print('項目名に%sが含まれているか確認してださい。' % cs.CHOISE_COLS)
        sys.exit()
    df.columns = cs.NEW_COLS
    df = df.dropna(thresh=2)
    df = df.dropna(subset=['Reference'])
    df['Reference_mark'] = ''
    df['Reference_number'] = 0
    df['memo'] = ''

    # 各部品の先頭行の部品番号を最小にする
    cols_list = list(df.columns)
    memo_col = cols_list.index('memo')
    quantity_col = cols_list.index('Quantity')

    df = df.assign(Reference_mark=df['Reference'].str.extract(r'(\D+)', expand=False))
    df = df.assign(Reference_number=df['Reference'].str.extract(r'(\d+)', expand=False).astype(int))
    df = df.sort_values(['Reference_mark', 'Reference_number'], ascending=True)
    df = df.reset_index(drop=True)

    # 誤記の確認
    item_df = df['Value']
    item_df = item_df.drop_duplicates()
    item_lists = item_df.values.tolist()

    df = df.fillna(0)
    df3 = pd.DataFrame()
    for i in item_lists:
        df2 = df[df.Value == i]
        quantity = max(df2['Quantity'])
        if quantity != len(df2):
            df2.iat[0, memo_col] += '数量が間違っています。'
            
        quantity = df2.iloc[0, quantity_col]
        if quantity == 0:
            df2.iat[0, memo_col] += '数量が入力されるべき行です。'

        df3 = pd.concat([df3, df2])

    # 不要な行を削除
    drop_col = ['Reference_mark', 'Reference_number']
    df4 = df3.drop(drop_col, axis=1)

    # 数量を修正
    df4['Quantity'] = df4['Quantity'].astype('int')
    df4['Quantity'] = df4['Quantity'].astype('str')
    df4.loc[df4.Quantity == '0', 'Quantity'] = ''

    return df4