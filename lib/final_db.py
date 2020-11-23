import pandas as pd
import numpy as np
import nltk
import time
import re
import time
import math
import seaborn as sb

class final_db():
    def __init__(self):
        self.df_main = pd.read_csv("dataframe.csv",index_col = 0)
        self.df_sup = pd.read_csv("dataframe_supplement.csv",index_col = 0)
        self.df_joined = pd.merge(self.df_main,self.df_sup, on="steam_appid")
        
    def cleanup(self, df = None):
        if df is None:
            df = self.df_joined
            
        # The lack of reviews will make it impossible to judge the quality of a game. Unfortunately those games will have to be filtered out from further analysis and the model. There is also no viable way of gathering this data.
        self.df_joined = df[df["reviews_total"] > 0]
        self.df_joined["steam_appid"].astype(int, copy=False)
        
        return self.df_joined
        
    def save_db(self, df = None):
        if df is None:
            df = self.df_joined
        
        df.to_csv("gamedata_final.csv", header=True)