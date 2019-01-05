# coding: utf-8
import pandas as pd
from pytrends.request import TrendReq
from itertools import zip_longest, filterfalse
from InsertElasticsearch import bulk_insert

# import matplotlib as mpl
# import pandas as pd
# import japanize_matplotlib

INDEX_NAME = "smash_bros_trend"
pytrends = TrendReq(hl='ja-JP', tz=360)

df_character = pd.read_csv('data/characterData.csv', index_col=0)

# print(df_character.loc['リンク']['series'])

CHARACTER_LIST = df_character.index
PREFIX_WORD = "スマブラ SP "
timeframe_val = '2018-12-07 2018-12-31'

search_character_list = []
for character in CHARACTER_LIST:
    search_character_list.append(PREFIX_WORD+character)

base_character = "ガノンドロフ"

group_by = 4
chunks = zip_longest(*[iter(search_character_list)]*group_by)


def p(x): return x is None


df_result = None

for elems in list(chunks):
    elems = list(filterfalse(p, elems))
    elems.append(PREFIX_WORD + base_character)
    pytrends.build_payload(elems, timeframe=timeframe_val, geo='JP')
    df = pytrends.interest_over_time()
    del df['isPartial']
    df = df.astype('float64')
    df = df.div(df[PREFIX_WORD + base_character].max(), axis=0)

    if df_result is None:
        df_result = df
    else:
        del df[PREFIX_WORD + base_character]
        df_result = df_result.join(df)

json_list = []
for date_val, series in df_result.iterrows():
    for character, value in series.iteritems():
        character_name = character.replace(PREFIX_WORD, "")
        insert_json = {
                '@timestamp': int(date_val.timestamp()),
                'character_name': character_name,
                'series': df_character.loc[character_name]['series'],
                'smash_bros_ver': df_character.loc[character_name]['first'],
                'value': value}
        json_list.append(insert_json)

bulk_insert(INDEX_NAME, json_list)
