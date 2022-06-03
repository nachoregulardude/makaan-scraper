#!/usr/bin/env python3
from teal_data_utils.http_utils.web_requesting_utils import WEB_REQUESTER

import concurrent.futures
from hashlib import md5

import pandas as pd
from bs4 import BeautifulSoup
import sqlite3


def get_soups(url):
    req = WEB_REQUESTER()
    resp = req.get_http_response_obj(url)
    return BeautifulSoup(resp.content, 'lxml')


def process_and_scrape_each_sqlite_task_row(soup):
    table_div = soup.find('div', {'class':'tbl-wrap'})
    if table_div is None:
        return [{
            'locality_names': 'no_data',
            'price_range_per_sq_ft': 'no_data',
            'avg_prices_per_sq_ft': 'no_data',
            'price_rise': 'no_data'
            }]
        
    table = table_div.find('table', {'data-trend-type': 'apartment', 'class':'tbl'})
    tr_elements = table.find_all('tr')
    results = []
    for tr in tr_elements[2:]:
        if tr:
            tds = tr.find_all('td')
            locality_names = tds[0].text
            price_range_per_sq_ft = tds[1].text
            avg_prices_per_sq_ft = tds[2].text
            price_rise = tds[3].text
            fields = {
                    'locality_names':locality_names,
                    'price_range_per_sq_ft': price_range_per_sq_ft,
                    'avg_prices_per_sq_ft': avg_prices_per_sq_ft,
                    'price_rise': price_rise
                    }
            results.append(fields)
        else:
            continue
    return results


def get_next_page_url(soup):
    next_page_url = soup.find('a', {'aria-label':'nextPage'})
    return next_page_url['href'] if next_page_url else False


db_name = 'makan_apartment_price_trend.db'
conn = sqlite3.connect(db_name)
cur = conn.cursor()
cur.execute('''
            SELECT city_link, hsh FROM locality_wise_price_trend WHERE status='PENDING'
            ''')
rows = cur.fetchall()
links = 0
for row in rows:
    url = row[0]
    print(f'going to {url}')
    hsh = row[1]
    soup = get_soups(url)
    data = []
    data.extend(process_and_scrape_each_sqlite_task_row(soup))
    next_page_url = get_next_page_url(soup)
    pages = 1
    while next_page_url:
        print('\t',next_page_url)
        soup_next_page = get_soups(next_page_url)
        data.extend(process_and_scrape_each_sqlite_task_row(soup_next_page))
        next_page_url = get_next_page_url(soup_next_page)
        soup_next_page = get_soups(next_page_url) if next_page_url else None
        pages += 1
    links += 1
    print(f'got {pages} from {url}')
print(data, 'got')
print(pages, 'total pages scraped')
print(links, 'cities scraped')
