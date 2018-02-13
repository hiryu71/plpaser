# -*- coding: utf-8 -*-
from plpaser import plpaser

READ_FOLDER_PATH = 'data'
WRITE_FOLDER_PATH = 'data'

# main
if __name__ == "__main__":

    # 旧フォーマットと新フォーマットの部品表を比較するための前処理
    plpaser.plpaser(READ_FOLDER_PATH, WRITE_FOLDER_PATH)
