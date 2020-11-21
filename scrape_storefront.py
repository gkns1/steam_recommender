import lib.storefront_scraping as scr

storefront = scr.storefront_data()
applist, limit = storefront.get_id_df()
df = storefront.load_df(applist)
df_scraped = storefront.scrape_storefront(df)
storefront.save_db(df_scraped)