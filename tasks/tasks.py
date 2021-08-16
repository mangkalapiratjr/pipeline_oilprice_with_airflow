import os
import requests
import re
import json
import pandas as pd
from sqlalchemy import create_engine 
from scrapy import Selector


# Web Scraping from sanook.com to get today oilprice
def get_oilprice():
    response = requests.get('https://money.sanook.com/economic/oil-price/')
    sel = Selector(response)
    oilprice = sel.css('article.fuel--today > div:nth-child(2)::text, table.fuel--table-today h3 ::text, table.fuel--table-today td:nth-child(2) ::text').extract()
    oilprice[0] = re.sub('[()]', '', oilprice[0]).strip()

    # Save output as json file
    oilprice_file = 'datalake/oilprice.json'
    if not os.path.exists(oilprice_file):
        with open(oilprice_file, mode='w', encoding='utf8') as file:
            file.write('[]')
    with open(oilprice_file, mode='r+', encoding='utf8') as file:
        data = json.load(file)
        date = oilprice[0]
        if oilprice[0] not in data:
            new_entry = { 'date': date }
            [new_entry.update({ oilprice[i]: oilprice[i+1]}) for i in range(1, 12, 2)]
            data.append(new_entry)
            file.seek(0)
            json.dump(data, file)

# Web Scraping from bot.or.th to get latest exchange rate
def get_exchange_rate():
    response = requests.get('https://www.bot.or.th/Thai')
    sel = Selector(response)
    exchange_rate = sel.css('div.exchangerate .date ::text , div.exchangerate table tr:nth-child(3) > td ::text').extract()
    exchange_rate[0] = re.search('\d+.*\d{4}', exchange_rate[0]).group(0)
    
    # Save output as csv file
    with open('datalake/exchange_rate.csv', mode='a', encoding='utf8') as file:
        file.write(','.join(exchange_rate) + '\n')

# Merge OilPrice and Exchange Rate, then convert OilPrice to USD and save output as csv file
def transform_data():
    df_oilprice = pd.read_json('datalake/oilprice.json', orient='records')
    df_oilprice.rename(columns={  col: rename_oilprice_column(col) for col in df_oilprice.columns }, inplace=True)
    df_oilprice['date'] = df_oilprice['date'].apply(reformat_date)
    print(df_oilprice)

    df_exchange_rate = pd.read_csv('datalake/exchange_rate.csv', header=None, names=['date', 'ex_rate'], usecols=[0,4])
    df_exchange_rate['date'] = df_exchange_rate['date'].apply(reformat_date)
    print(df_exchange_rate)

    df_merged = df_oilprice.merge(df_exchange_rate, on='date', how='outer').sort_values('date')
    df_merged['ex_rate'].ffill(axis=0, inplace=True)
    df_merged = df_merged.dropna()
    print(df_merged)
    for col, value in df_merged.iteritems():
        if col not in ['date', 'ex_rate']:
            df_merged[col] /= df_merged['ex_rate']

    print( df_merged)
    
    df_merged.to_csv('output/oilprice_usd.csv', index=False)

def rename_oilprice_column(col):
    return col.replace('แก๊สโซฮอล', 'Gasohol').replace(' ', '_').replace('ก๊าซ', 'Gas').replace('ดีเซล', 'Diesel')

def reformat_date(date):
    day, month, year = date.split(' ')
    month_names = ['มกราคม', 'กุมภาพันธ์', 'มีนาคม', 'เมษายน', 'พฤษภาคม', 'มิถุนายน', 'กรกฎาคม', 'สิงหาคม', 'กันยายน', 'ตุลาคม', 'พฤศจิกายน', 'ธันวาคม']
    month_shorts = ['ม.ค.', 'ก.พ.', 'มี.ค.', 'เม.ย.', 'พ.ค.', 'มิ.ย.', 'ก.ค.', 'ส.ค.', 'ก.ย.', 'ต.ค.', 'พ.ย.', 'ธ.ค.']
    month_no = (month_names.index(month) if month in month_names else month_shorts.index(month)) +1
    return f'{int(year)-543}-{month_no:02d}-{int(day):02d}'
    
def load_db():
    engine = create_engine('sqlite:///dummy_db.sqlite')
    data = pd.read_csv('output/oilprice_usd.csv')
    data.to_sql('oilprice_usd', con=engine, index=False, if_exists='replace')