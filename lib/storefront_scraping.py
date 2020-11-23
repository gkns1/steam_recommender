import math
import re
import time

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


# data that's needed from the storefront:
# appid to join with the API call data
# total reviews and % positive
# recent reviews and % positive
# popular user-defined tags for the product
# review summary
# Todo: check other regions & languages for potentially missing IDs

class storefront_data():
    def __init__(self):
        self.apps = 0
        self.last_db = pd.read_csv("dataframe_supplement.csv")
        self.df_exists = False
        self.old_apps = len(self.last_db.index)
        self.old_db_used = False

    def get_id_df(self, use_old_db=False):
        # if use_old_db is set to True, ignores the API call and loads the old database
        # returns applist dataframe and a list apps_lst for scraping
        if self.apps == 0 and use_old_db == False:
            with open("apikey.txt", "r") as f:
                key = f.read()
                f.close()
            try:
                games = requests.get(
                    'https://api.steampowered.com/IStoreService/GetAppList/v1/?include_games=1&include_dlc=0&include_software=0&include_videos=0&include_hardware=0&key={0}&max_results=50000'.format(
                        key))
                pd.set_option("display.max_columns", 500)
                games_json = games.json()['response']['apps']
                applist = pd.DataFrame(games_json)
                applist = applist.sort_values('appid')
                applist.reset_index(drop=True)
                apps_lst = list(applist['appid'])
                self.apps = len(apps_lst)
                print("returned a new list of %d ids in the latest applist" % self.apps)
            except:
                print("a problem occured with steam api connection. returning the last api list...")
                applist = self.last_db
                apps_lst = list(applist['steam_appid'])
                self.apps = len(apps_lst)
                self.old_db_used = True
            return applist, self.apps
        elif self.apps == 0 and use_old_db:
            print("Loading the old applist...")
            applist = self.last_db
            apps_lst = list(applist['steam_appid'])
            self.apps = len(apps_lst)
            self.old_db_used = True
            return applist, self.apps
        else:
            print("apps already generated!")

    def load_df(self, applist=None):
        if applist is None:
            applist = applist
        # add a join with old df
        # uses the applist dataframe to set up other fields and returns the dataframe
        if self.df_exists == False and self.apps != 0 and self.old_db_used == False:
            df = pd.DataFrame()
            df['steam_appid'] = applist['appid']
            df['tags'] = np.nan
            df['reviews_total'] = np.nan
            df['reviews_recent'] = np.nan
            df['reviews_total_positive_percent'] = np.nan
            df['reviews_recent_positive_percent'] = np.nan
            df['review_summary'] = np.nan
            df['last_refreshed'] = np.nan
            print("Dataframe set up!")
            self.df_exists = True
            if self.last_db is not None:
                print("updating the last db, there are %d new IDs." % (self.apps - self.old_apps))
                df.update(self.last_db)
            else:
                print("old db not found, starting from scratch")
            return df
        elif self.old_db_used:
            print("Using the old db...")
            return self.last_db
        elif self.df_exists:
            print("Dataframe is already set up!")
        else:
            print("Run setup.get_id_df() first!")

    # by default only scrapes data for appids refreshed over 14 days ago, or are found for the first time
    # by setting rebuild = True, rebuilds the whole dataframe from scratch
    def scrape_storefront(self, df, limit=None, start_time=time.time(), rebuild=False):
        if limit is None:
            limit = len(df.index)
        batch_time = start_time
        updated = 0
        for i, app in enumerate(df[:limit]["steam_appid"]):

            batches_total = math.ceil(limit / 100)
            # the cookie is required to get through the age check for games ranked as "mature"
            cookies = {'birthtime': '283993201', 'mature_content': '1'}
            updated_days_ago = (df.loc[i, 'last_refreshed'] - start_time) / 86400
            if rebuild == True or updated_days_ago > 14 or math.isnan(updated_days_ago):
                updated += 1
                try:
                    text = requests.get('https://store.steampowered.com/app/{0}'.format(app), cookies=cookies).text
                except:
                    print("A timeout error occured, sleeping for 5 minutes and retrying.")
                    time.sleep(300)
                    text = requests.get('https://store.steampowered.com/app/{0}'.format(app), cookies=cookies).text
                soup = BeautifulSoup(text, "html.parser")
                tags_regexp = re.findall(r'(?<=,"name":")([^"]+)', soup.text)
                reviews_total_regexp = re.search(r'(?<=of the )(.)+(?= user reviews for this game are positive.)',
                                                  soup.text)
                reviews_recent_regexp = re.search(r'(?<=of the )(.)+(?= user reviews in the last 30 days are positive)',
                                                 soup.text)
                reviews_total_positive_regexp = re.search(
                    r'(\d\d)(?=% of the (.)+ user reviews in the last 30 days are positive)', soup.text)
                reviews_recent_positive_regexp = re.search(
                    r'(\d\d)(?=% of the (.)+ user reviews for this game are positive.)', soup.text)
                review_summary = soup.find('span', attrs={"class": "game_review_summary"})

                tags_regexp_string = (', '.join(tags_regexp))

                df.loc[i, 'tags'] = tags_regexp_string
                if reviews_recent_regexp:
                    df.loc[i, 'reviews_recent'] = reviews_recent_regexp.group(0)
                if reviews_total_regexp:
                    df.loc[i, 'reviews_total'] = reviews_total_regexp.group(0)
                if reviews_total_positive_regexp:
                    df.loc[i, 'reviews_total_positive_percent'] = reviews_total_positive_regexp.group(0)
                if reviews_recent_positive_regexp:
                    df.loc[i, 'reviews_recent_positive_percent'] = reviews_recent_positive_regexp.group(0)
                if review_summary:
                    df.loc[i, 'review_summary'] = review_summary.text
                df.loc[i, 'last_refreshed'] = start_time

            if i % 100 == 0 and i != 0:
                end_time = time.time()
                elapsed = end_time - batch_time
                print("batch {} of {} finished in {:.2f} seconds.".format(int(i / 100), batches_total, elapsed))
                batch_time = time.time()

            elif i == limit - 1:
                end_time = time.time()
                elapsed_batch = end_time - batch_time
                elapsed_total = end_time - start_time
                print(
                    "Batch {} of {} finished in {:.2f} seconds. All finished in {:.2f} seconds, {} indexes processed, {} updated!".format(
                        math.ceil(i / 100), batches_total, elapsed_batch, elapsed_total, i + 1, updated))

        return df

    def fill_missing(self, df = None):
        
        if df is None:
            df = pd.read_csv("dataframe_supplement.csv",index_col = 0)
        
        df['reviews_total'].fillna(0, inplace=True)
        df['reviews_recent'].fillna(0, inplace=True)
        df['reviews_total_positive_percent'].fillna(0, inplace=True)
        df['reviews_recent_positive_percent'].fillna(0, inplace=True)
        df['review_summary'].fillna("None", inplace=True)
        
        return df
    
    def save_db(self, df):
        df.to_csv("dataframe_supplement.csv", header=True)
