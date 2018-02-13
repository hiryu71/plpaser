# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re

CHOISE_COLS = ['部品番号', '型式', 'メーカ', '数量']
NEW_COLS = ['index', 'value', 'maker', 'quantity']

# 旧フォーマットの部品表を前処理
def old_format_paser(df):

    # 準備
    df0 = df[CHOISE_COLS]
    df0.columns = NEW_COLS
    df0['index_mark'] = ''
    df0['min_index_number'] = 0
    df0['index_numbers'] = ''
    df0['index_quantity'] = 0
    df0['index_group'] = 0
    df0['index_count'] = 0
    df0['memo'] = ''
    df0 = df0.dropna()

    # 各部品の先頭行の部品番号を最小にするための下準備
    df1 = df0.assign(index_mark = df0['index'].str.extract(r'(\D+)', expand=False))
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
    df2 = df1.sort_values(['index_mark', 'min_index_number'], ascending=True)
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

    return df4


# 新フォーマットの部品表を前処理
def new_format_paser(df):

    df = df[CHOISE_COLS]
    df.columns = NEW_COLS
    df = df.dropna(thresh=2)
    df['index_mark'] = ''
    df['index_number'] = 0
    df['memo'] = ''

    cols_list = list(df.columns)
    memo_col = cols_list.index('memo')
    quantity_col = cols_list.index('quantity')

    df = df.assign(index_mark=df['index'].str.extract(r'(\D+)', expand=False))
    df = df.assign(index_number=df['index'].str.extract(r'(\d+)', expand=False).astype(int))
    df = df.sort_values(['index_mark', 'index_number'], ascending=True)
    df = df.reset_index(drop=True)

    item_df = df['value']
    item_df = item_df.drop_duplicates()
    item_lists = item_df.values.tolist()

    df = df.fillna(0)
    df3 = pd.DataFrame()
    for i in item_lists:
        df2 = df[df.value == i]
        quantity = max(df2['quantity'])
        if quantity != len(df2):
            df2.iat[0, memo_col] += '数量が間違っています。'
            
        quantity = df2.iloc[0, quantity_col]
        if quantity == 0:
            df2.iat[0, memo_col] += '数量が入力されるべき行です。'

        df3 = pd.concat([df3, df2])

    # 不要な行を削除
    drop_col = ['index_mark', 'index_number']
    df4 = df3.drop(drop_col, axis=1)

    # 数量を修正
    df4['quantity'] = df4['quantity'].astype('int')
    df4['quantity'] = df4['quantity'].astype('str')
    df4.loc[df4.quantity == '0', 'quantity'] = ''

    return df4