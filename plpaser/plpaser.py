# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re

import plpaser.filepaser as fp
import plpaser.fileservice as fs

# 部品表を比較するための前処理
def plpaser(read_folder, write_folder):
    
    # ファイル読み込み
    print('ファイル読み込み')
    old_df, new_df = fs.file_reader(read_folder)

    # 部品表を前処理
    print('旧フォーマットの部品表を前処理')
    old_df = fp.old_format_paser(old_df)

    print('新フォーマットの部品表を前処理')
    new_df = fp.new_format_paser(new_df)

    print('差分チェック')
    old_df, new_df, dif_str = fp.check_dif(old_df, new_df)

    # ファイル出力
    fs.file_writer(old_df, new_df, dif_str, write_folder)
