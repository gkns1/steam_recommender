import lib.storefront_api

store_info = store_info()
applist, limit = store_info.get_applist()
df = store_info.get_store_info(apps=applist)
#let's see which fields may be useful for content recommendations based on their contents and data availability.
df = store_info.clean_dataframe(df=None)
store_info.join_save(df)