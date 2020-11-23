import json
import math
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup


# The call will only get games. Possible to extend it to DLC for recommendation
# Todo: check other regions & languages for potentially missing IDs
# all we need from this API call is the appids to collect all the other information about the games
# handle restarts & crashes, add more information about runtime etc.
class store_info():

    def __init__(self):
        with open("apikey.txt", "r") as f:
            self.key = f.read()
        f.close()
        self.old_gamedata = pd.read_csv("dataframe.csv", index_col=0)
        self.use_old_db = False
        self.apps = 0

    def get_applist(self, use_old_db=False):
        # if use_old_db is set to True, ignores the API call and loads the old database
        # returns applist dataframe and a list apps_lst for scraping
        if self.apps == 0 and use_old_db == False:
            try:
                games = requests.get(
                    'https://api.steampowered.com/IStoreService/GetAppList/v1/?include_games=1&include_dlc=0&include_software=0&include_videos=0&include_hardware=0&key={0}&max_results=50000'.format(
                        self.key))
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
                applist = self.old_gamedata["steam_appid"]
                apps_lst = list(applist['steam_appid'])
                self.apps = len(apps_lst)
                self.use_old_db = True
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

    # add old db handling

    def get_store_info(self, apps, key=None, last_df=None, start_time=time.time(), rebuild=False, test=False):
        appinfo = []
        if key is None:
            key = self.key
        if last_df is None:
            last_df = self.old_gamedata["steam_appid"]
        if test:
            apps = apps[:220]
            limit = 220
        else:
            apps = apps[apps.appid.isin(last_df) == False]
            limit = len(apps.index)

        batches_total = math.ceil(limit / 198)
        with open('data.json', 'a', encoding='utf-8') as f:
            f.truncate(0)
            for i, app in enumerate(apps["appid"]):
                batch_time = start_time
                keyid = str(app)
                appresp = requests.get(
                    'https://store.steampowered.com/api/appdetails?appids={0}&key={1}'.format(app, self.key)).json()
                if i != 0 and i % 198 == 0:
                    end_time = time.time()
                    elapsed = end_time - batch_time
                    print(
                        "Batch {} of {} finished in {:.2f} seconds. Sleeping for 5 minutes.".format(math.ceil(i / 198),
                                                                                                    batches_total,
                                                                                                    elapsed))
                    batch_time = time.time()
                    time.sleep(
                        300)  # Even though there's no documentation on it, steam API seems to lock after 199 calls, hence the batching. It's supposedly 200 every 5 minutes, but I was consistently 429'd at 199.
                elif i == limit - 1:
                    end_time = time.time()
                    elapsed_batch = end_time - batch_time
                    elapsed_total = end_time - start_time
                    print(
                        "Batch {} of {} finished in {:.2f} seconds. All finished in {:.2f} seconds, {} indexes processed!".format(
                            math.ceil(i / 100), batches_total, elapsed_batch, elapsed_total, i + 1))

                if appresp is not None:
                    # appinfo.append(appresp[keyid]['data'])
                    if str(appresp[keyid]['success']) == 'True':
                        json.dump(appresp[keyid], f, ensure_ascii=False, indent=4)
                        if i != limit - 1:
                            f.write(",")
                elif appresp is None:
                    # json.dump("{success}: ID %s returned an error!" % app, f ,ensure_ascii=False, indent=4)
                    print("ID %s returned an error!" % app)
                # the initial export crashed midway after ~10h. A second part took 10h6min. Due to the crash, there likely will be duplicates to clean later.
        f.close()

    def clean_dataframe(self, df=None):
        if df is None:
            with open('data.json', 'r+', encoding='utf-8') as f:
                gamedata = f.read()
                gamedata = json.loads("[" + gamedata + "]")
            df = pd.json_normalize(gamedata)
            f.close()

        print("Cleaning up the column names...")
        df.columns = df.columns.str.replace('data.', '')
        df.columns = df.columns.str.replace('.', '_')

        print("Removing unnecessary columns...")
        df = df[['steam_appid', 'name', 'about_the_game', 'is_free', 'detailed_description',
                 'short_description', 'supported_languages', 'pc_requirements_minimum', 'pc_requirements',
                 'pc_requirements_recommended', 'mac_requirements', 'linux_requirements',
                 'developers', 'publishers', 'price_overview_initial', 'price_overview_final',
                 'platforms_windows', 'platforms_mac', 'platforms_linux', 'categories',
                 'genres', 'release_date_date', 'release_date_coming_soon', 'mac_requirements_minimum',
                 'mac_requirements_recommended', 'linux_requirements_minimum',
                 'linux_requirements_recommended', 'metacritic_score', 'reviews', 'recommendations_total']]

        # df.set_index('steam_appid',inplace=True)

        print("Filling in empty values...")
        # Requirements are only filled in when there are no minimum or recommended.
        # They will be populated into the minimum requirements table for their respective platforms.
        df['pc_requirements_minimum'].fillna(df['pc_requirements'], inplace=True)
        df['linux_requirements_minimum'].fillna(df['linux_requirements'], inplace=True)
        df['mac_requirements_minimum'].fillna(df['mac_requirements'], inplace=True)
        df.drop(columns=['pc_requirements', 'linux_requirements', 'mac_requirements'], inplace=True)

        # let's deal with requirements first. It appears a large number of games does now have recommended requirements.
        # Those would be small games where recommended specs make no difference over minimum.
        # In those cases we can simply replace empty recommended with minimum requirements for that game.
        # There are likely a few cases that will remain null as not all games are available on all the platforms
        df['pc_requirements_recommended'].fillna(df['pc_requirements_minimum'], inplace=True)
        df['linux_requirements_recommended'].fillna(df['linux_requirements_minimum'], inplace=True)
        df['mac_requirements_recommended'].fillna(df['mac_requirements_minimum'], inplace=True)

        # We will do the same where there is a recommended value, but not minimum
        df['pc_requirements_minimum'].fillna(df['pc_requirements_recommended'], inplace=True)
        df['linux_requirements_minimum'].fillna(df['linux_requirements_recommended'], inplace=True)
        df['mac_requirements_minimum'].fillna(df['mac_requirements_recommended'], inplace=True)

        # if a game is missing the price it could be for multiple reasons. It may not be available yet, not available anymore, or the game is simply free.
        # all those can be filled with 0.00. We will be dealing with each case differently based on other fields. (e.g. release_date_coming_soon, is_free)
        df['price_overview_initial'].fillna(0, inplace=True)
        df['price_overview_final'].fillna(0, inplace=True)

        # All entries with empty "developers" field have a publisher present. The games could be abandoned or published by the developer.
        # In any case, filling empty developers with their publishers content should not hurt the quality of the data
        df['developers'].fillna(df['publishers'], inplace=True)

        # Empty categories and genres could be inferred based on the descriptions, however for the time being they will be replaced with "Not provided"
        df['genres'].fillna('Not provided', inplace=True)
        df['categories'].fillna('Not provided', inplace=True)

        # The vast majority of the games do not have a metacritic score. In this case the column will be dropped and we will need additional data to judge the quality of the game.
        df.drop(columns=['metacritic_score'], inplace=True)

        # reviews - again, the vast majority of the titles do not have that field and it appears to be a sort of advertising more than actual reviews. The column will be dropped.
        df.drop(columns=['reviews'], inplace=True)

        # recommendations_total - here a real problem starts. The vast majority of the games does not have that field in the API. It will have to be dropped.
        # it will be extremely important to get positive and negative reviews (or a total and percentage of positive, as Steam counts it on their storefront).
        # this means another stage of data gathering game by game. Fortunately we can use steam_appid to both select the games and get the additional data.
        df.drop(columns=['recommendations_total'], inplace=True)

        # entries with no supported languages will be dropped.
        df.dropna(subset=['supported_languages'], axis=0, inplace=True)

        print("Removing html code from strings...")
        # this will remove all html from the string columns and leave only the text.
        for col in df.select_dtypes(include=['object']):
            print(col, "cleaned.")
            df[col] = [BeautifulSoup(str(text)).get_text() for text in df[col]]

        print("Cleaning up genres and categories...")
        df['categories'] = df['categories'].str.findall(r"(?<='description': ')([^']+)'")
        df['genres'] = df['genres'].str.findall(r"(?<='description': ')([^']+)'")
        # clean up the list columns, change to string
        df['categories'] = df['categories'].apply(', '.join)
        df['genres'] = df['genres'].apply(', '.join)

        return df

    def join_save(self, df):

        print("Joining the new dataframe to the previous version...")
        df = pd.concat([df, self.old_gamedata], ignore_index=False)

        print("Cleaning possible duplicates and resetting index...")
        df.drop_duplicates(subset=["steam_appid"], keep='last', inplace=True, ignore_index=True)

        print("Saving the database to csv...")
        df.to_csv("dataframe.csv", header=True)

        return df
