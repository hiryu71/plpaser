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

# 差分チェック
def check_dif(old_df, new_df):

    ref = cs.NEW_COLS[0]
    dif_old_df = old_df[~old_df[ref].isin(new_df[ref])]
    dif_new_df = new_df[~new_df[ref].isin(old_df[ref])]

    # 削除
    dif_old_list = dif_old_df[ref].values.tolist()
    dif_old_str = '\n'.join(dif_old_list)

    # 追加
    dif_new_list = dif_new_df[ref]
    dif_new_str = '\n'.join(dif_new_list)

    # 旧フォーマットの重複
    dup_old = dif_old_df[dif_old_df.duplicated('Reference', keep=False)]
    dup_old_arr = dup_old.values.tolist()
    str_list = []
    for i in range(len(dup_old_arr)):
        tmp = ','.join(dup_old_arr[i])
        str_list.append(tmp)
    dup_old_str = '\n'.join(str_list)

    # 新フォーマットの重複
    dup_new = dif_new_df[dif_new_df.duplicated('Reference', keep=False)]
    dup_new_arr = dup_new.values.tolist()
    str_list = []
    for i in range(len(dup_new_arr)):
        tmp = ','.join(dup_new_arr[i])
        str_list.append(tmp)
    dup_new_str = '\n'.join(str_list)

    # 差分
    dif_str = '# 削除--------------\n' + dif_old_str + '\n'\
            + '# 追加--------------\n' + dif_new_str + '\n'\
            + '# 旧フォーマットの重複--------------\n' + dup_old_str + '\n'\
            + '# 新フォーマットの重複--------------\n' + dup_new_str + '\n'
    
    # 部品番号の行を揃える
    old_df, new_df = align_line(old_df, new_df, dif_old_list, dif_new_list)

    return old_df, new_df, dif_str

# 部品番号の行を揃える
def align_line(old_df, new_df, dif_old_list, dif_new_list):

    old_arr = old_df.values
    del_row = []
    for item in dif_old_list:
        tmp = np.where(old_arr==item)[0]
        del_row.extend(list(tmp))
    del_row = list(set(del_row))
    del_row.sort()
    
    new_arr = new_df.values
    add_row = []
    for item in dif_new_list:
        tmp = np.where(new_arr==item)[0]
        add_row.extend(list(tmp))
    add_row = list(set(add_row))
    add_row.sort()

    print(del_row)
    print(add_row)
    
    return old_df, new_df
