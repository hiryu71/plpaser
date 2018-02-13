# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re

import plpaser.filepaser as fp
import plpaser.fileservice as fs

# 部品表を比較するための前処理
def plpaser(read_folder, write_folder):
    
    # ファイル読み込み
    old_df, new_df = fs.file_reader('data')

    # 部品表を前処理
    old_df = fp.old_format_paser(old_df)
    new_df = fp.new_format_paser(new_df)

    # ファイル出力
    fs.file_writer(old_df, new_df, write_folder)


    print('実行成功!!!')