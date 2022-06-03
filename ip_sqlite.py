#!/usr/bin/env python3
from teal_data_utils.http_utils.web_requesting_utils import WEB_REQUESTER
from teal_data_utils.logger_util import get_logger_for_file


import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
from hashlib import md5

get_logger_for_file('makan_apartment_locality_wise_price_trend')

"""
Makaan.com is a real estate market website. One of the sections on the website provides a locality-wise price summary.
Requirements: requests, bs4, sqlite, access to util files either via copy paste or install teal_data_utils, postgres/docker.
Task part 1 (Replicating tracker & sqlite flow - Using teal utils):
        The entry point to this section can be found here.
        We need to request the above link via web requester util - here.
        We want city_name & city_trend_link (Clicking on city name)
        We need to make sqlite db file, table & insert all city_name, city_trend_link status, hsh to sqlite table by using utils here. (return_conn_tup_sqlite_table & wrapper_insert_dict_to_sqlite_table)
            Make sqlite hsh via util here. Column to use for hash can just be city_trend_link as it's unique.
        Call the sqlite db as makan_apartment_price_trend.db
        Call table as: locality_wise_price_trend
        Columns will be: city_name, city_trend_link, status, hsh

Task Part 2 (Replicate teal's tracker scraping - along with common used utils): (Make a local postgres - with same schema name & table name as sqlite or use docker for doing same. Anything is cool.)
    Declare a logger with collection_name = 'makan_apartment_locality_wise_price_trend' . Using util here.
    Fetch sqlite rows via utils here. But all this rows to a Multi processing Queue. Call it task_queue.
    Write a function that does the following: (Trigger this by Multiprocessing process) Func name: process_and_scrape_each_sqlite_task_row
        Inputs to this will be: task_queue, pg_queue, logger, process_number.
        Inside a loop, read 1 task row dict - which is 1 sqlite row. (Till there are no rows left - Use timeout rather then queue empty)
        Use web requester util mentioned above to go to city_trend_link & scrape data table & all pages available:
            Column names: locality_name, price_range_per_sq_ft, avg_price_per_sq_ft, price_rise
            Log which city is currently being scraped - if data or not & which page no it's on.
        Make a config dict to send to pg_queue:
            In case of data:
                Add parsed row dict + sqlite row dict + sqlite status = 'COMPLETED' -> Send/Put to pg_queue.
            In case of no data:
                Add empty parsed dict + sqlite row dict + sqlite status = 'NO_DATA' -> Send/Put to pg_queue.
            In case of exception:
                Add empty parsed dict + sqlite row dict + sqlite status -> Status returned from util here. -> Send/Put to pg_queue.
    Write another function that does the following: (Trigger this by Multiprocessing process) Func name: handle_pg_queue_and_db_operations
        Inputs to this are: pg_queue, logger, testing_flag
        Make sqlite connection - via util mentioned on task 1.
        Make pg connection - via util here. (Util will only work if same env variable names & proper values used OR pass in proper values to it's arguments !)
        If scrape dict + proper scrape status:
                Make a merge dict of sqlite dict + parsed row dict. Add a new hsh of all columns using same util. Add last_downloaded filed with constant value: "2999-12-31T00:00:00Z"
                Insert the above merge dict to psotgres table using util here. Can be done via docker or local postgres.
                Update sqlite status - whatever we got from config dict.
                Log this success.
        Else: Just update the sqlite table to status from config dict. Log this error or exception.
        Note: In case testing_flag is passed true -> Do not update sqlite table - print the update statement.
"""
def get_soup(url):
    req = WEB_REQUESTER()
    resp = req.get_http_response_obj(url)
    return BeautifulSoup(resp.content, 'lxml')


def get_city_name_trend_link(soup):
    div_prop_buy = soup.find("div", {'data-parent':'#city-trend-buy'})
    table = div_prop_buy.find("table", {"class": "tbl", "data-trend-type":"apartment"})
    tr_elements = table.find_all("tr")[3:]
    results = []
    for tr in tr_elements:
        td_elements = tr.find_all("td")
        city_name = td_elements[0].text
        city_link = td_elements[0].find('a')['href']
        hsh = md5(city_link.encode()).hexdigest()
        fields = {'city_name':city_name, 'city_link':city_link, 'hsh': hsh, 'status':'PENDING'}
        results.append(fields)
    return results

url = 'https://www.makaan.com/price-trends'
soup_main_page = get_soup(url)
data = get_city_name_trend_link(soup_main_page)
df = pd.DataFrame(data)
db_name = 'makan_apartment_price_trend.db'
table_name = 'locality_wise_price_trend'
connection = sqlite3.connect(db_name)
cursor = connection.cursor()
cursor.execute(f'\
            CREATE TABLE IF NOT EXISTS {table_name} \
            (city_name text, city_trend_link text, status text, hsh text PRIMARY KEY)\
            ')
df.to_sql(name=table_name, con=connection, if_exists='replace', index=False)
print('done')

