#!/bin/python3
#
#         Script to get info from website http://www.notaires.fr
#

import sys
import sqlite3
import os.path
import concurrent.futures
import io
import re
import json
import time
from bs4 import BeautifulSoup
from requests import Session
import requests

NWORKERS = 60
save_to_filename = 'data/just_links.sdat'
ajax_request_filename = 'data/xmlhhttprequest'
website = 'http://www.notaires.fr'
ajax_url = website + '/fr/views/ajax?type=&\
field_office_department_value=&\
field_notary_surname_value=&\
field_jv_adresses_line_6city_value=&\
field_jv_adresses_line_6cp_value=&\
departement1=&\
field_notary_langueparlee_taxo_tid=All'
hdrs = {
    # 'Host': 'www.notaires.fr',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0',
    # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    # 'Accept-Language': 'en-US,en;q=0.5',
    # 'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
}
ajaxfile = open(ajax_request_filename, 'r')
payload_part1 = ajaxfile.readline().rstrip('\n')
payload_part2 = ajaxfile.readline().rstrip('\n')
    
def getPayload(page_number):
    payload = payload_part1+str(page_number)+payload_part2
    return payload

def getProfile(url):
    r  = requests.get(url)
    return r.text

def getPage(payload, s):
    r  = s.post(ajax_url, data=payload, headers=hdrs)
    if r.status_code != 200:
        save_file.write('\n'.join(exp.findall(pages.getvalue()))+'\n')
        print("Status code not 200 number of pages %",page_number)
        return r.status_code
    return r.json()[1]['data']

def makeRequests(pages, executor):
    s = Session()
    future_to_page = {executor.submit(getPage, getPayload(page_number), s):
                      page_number for page_number in range(1,977)}
    for future in concurrent.futures.as_completed(future_to_page):
        page = future_to_page[future]
        try:
            data = future.result()
            pages.write(data)
        except Exception as exc:
            print('Page %i generated an exception: %s' % (page,exc))
            return
        else:
            print('Got page %i' % page)

def getProfileLinks(executor):
    pages = io.StringIO()
    page_number = 1
    save_file = open(save_to_filename,'a')
    exp = re.compile('<a class="btn btn-actions mq-hos" href="?\'?([^"\'>]*)')
    makeRequests(pages, executor)
    links = exp.findall(pages.getvalue())
    pages.close()
    save_file.write('\n'.join(links)+'\n')
    save_file.close()
    return links

def saveData(data, db, link):
    soup = BeautifulSoup(data, 'lxml')
    divs = soup.find_all('div', class_='body-fiche-tab')
    notaire = divs[0].find_all('p')
    name = str(divs[0].h1.string)
    address = ' '.join(notaire[0].stripped_strings)
    g = [n for n in notaire[1].stripped_strings]
    if len(g) > 0:
        tel = g[0]
        if len(g) > 1:
            fax = g[1]
        else:
            fax = 'N/A' 
    else:
        tel = 'N/A'
        fax = 'N/A' 
    mail = ''.join([d for d in notaire[2].stripped_strings if d != 'Courriel :'])
    #buro = divs[1].find_all('p')
    try:
        societe  = next(divs[1].h2.stripped_strings)
        coworkers = str([next(div.stripped_strings) for div in divs[1](itemprop="member")])
        cp = str([next(div.stripped_strings) for div in divs[1](itemprop="postalCode")])
        ex =  re.compile('.*@.*')
        sm =  str(ex.findall(divs[1].get_text())).split(' : ')[-1]
        db.execute("INSERT INTO notaires (Name,Address,Cp,Tel,Fax,Mail,Societe,Coworkers,Link,Html) VALUES (?,?,?,?,?,?,?,?,?,?);", (name, address, cp, tel, fax, mail, societe, coworkers, link,str(divs)))
        db.execute('''INSERT INTO societes(Name,Address,Cp,Mail,Employees,Link) 
    SELECT ?,?,?,?,?,?
    WHERE NOT EXISTS(SELECT 1 FROM societes WHERE Name == ?);''',(societe, address, cp, sm, coworkers, link, societe))
    except StopIteration as exc:
        print(exc)
    return

def getData(links, executor):
    n = 0
    dbconn = sqlite3.connect('data/notaires.db')
    c = dbconn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS notaires(Name TEXT NOT NULL, Address TEXT, Cp TEXT, Tel TEXT, Fax TEXT, Mail TEXT, Societe TEXT, Coworkers TEXT, Link TEXT, Html TEXT);')
    c.execute('CREATE TABLE IF NOT EXISTS societes(Name TEXT NOT NULL, Address TEXT,Cp TEXT, Mail TEXT, Employees TEXT, Link TEXT, Html TEXT);')
    future_to_page = {executor.submit(getProfile, website+uri): uri for uri in links}
    for future in concurrent.futures.as_completed(future_to_page):
        link = future_to_page[future]
        n += 1
        data = future.result()
        saveData(data, dbconn, link)
        print('Got link number %i at %s' % (n,link))
    dbconn.commit()
    dbconn.close()
    
def main():
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=NWORKERS)
    if os.path.isfile(save_to_filename):
        f = open(save_to_filename,'r')
        links = [line.strip('\n') for line in f.readlines()]
    else:
        links = getProfileLinks(executor)
    getData(links, executor)

if __name__ == "__main__":
    main()
