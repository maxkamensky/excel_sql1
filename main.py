#1
import csv
import datetime
import os
from random import choice, randint
from operator import itemgetter
import pandas as pd
import pyodbc
import tqdm
import time

xlsx_file = "C:\\bigdata\\original\\bigdata2-100.xlsx"
work_folder = "C:\\bigdata\\csv_files\\"
months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
years = [2017, 2018, 2019, 2020, 2021]


def connect():
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=ASUSALEX\SQLEXPRESS;'
                          'Database=python_sql;'
                          'Trusted_Connection=yes;')
    conn.commit()
    return conn


def drop_table(table_name, cursor):
    cursor.execute(f"drop table {table_name}")
    print("данные удалены")
    cursor.commit()


def clear_folder(path):
    print("чищу каталог")
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))


def get_csv(xlsx_file, work_folder):
    date = datetime.datetime.now()
    csv_name = date.strftime("%Y%m%d%H%M%S")
    print("открываю xlsx")
    data_xls = pd.read_excel(xlsx_file, sheet_name='GroceryMar Pyat chips energ 10-')
    print("xlsx в csv")
    data_xls.to_csv(f'{work_folder}{csv_name}.csv', encoding='utf-8', index=False, header = False)
    return csv_name


def csv_from_excel(work_folder, csv_name):
    print("шлифую csv")
    products_sums_list = {tuple(['','']):''}
    products_sums_list_help = {tuple(['','']):''}
    with open(f'{work_folder}{csv_name}.csv', encoding='utf-8') as csvf:
        data = str(csvf.read())
    with open(f"{work_folder}{csv_name}v.csv", mode="w", encoding='utf-8') as w_file:
        csv_writer = csv.writer(w_file, delimiter=",", lineterminator="\r")
        for row in tqdm.tqdm(data.splitlines()):
            row = row.replace('"', '').replace('\n', '')
            row = list(row.split(','))

            if row[5] == '':
                row[5] = randint(50, 100)
            if row[6] == '':
                row[6] = randint(100.0, 1000.0)
            if row[7] == '':
                row[7] = float(row[6]) * 1.35
            csv_writer.writerow(row)
        for row in tqdm.tqdm(data.splitlines()):
            row = list(row.split(','))
            row[0] = choice(months)
            row[1] = choice(years)
            for product, price in products_sums_list_help.items():
                if row[3] != product:
                    products_sums_list.update({tuple([row[3], row[1]]):float(round((float(row[7])-float(row[6]))*int(row[5]), 2))})
                if row[3] == product:
                    products_sums_list.update({product: price + float(round((float(row[7]) - float(row[6])) * int(row[5]), 2))})
            products_sums_list_help = products_sums_list.copy()
        products_sums_list_new = {}
        for key, value in products_sums_list.items():
            if not isinstance(value, str):
                products_sums_list_new[key] = value

        products_sums_list = dict(sorted(products_sums_list_new.items(), key=itemgetter(1), reverse=True))
        i = 1
        print()
        print('Топ за всё время: ')
        for key in products_sums_list:
            print(f'{i}.{key[0]} -> {products_sums_list[key]}')
            i += 1
            if i > 10:
                break
        i = 1
        print()
        print('======================================')
        print('Топ за 2017-ый год: ')
        for k,v in products_sums_list.items():
            if k[1] == 2017:
                print(f'    {i}.{k[0]} -> {v}')
                i += 1
                if i > 10:
                    break
        i = 1
        print()
        print('Топ за 2018-ый год: ')
        for k,v in products_sums_list.items():
            if k[1] == 2018:
                print(f'    {i}.{k[0]} -> {v}')
                i += 1
                if i > 10:
                    break
        i = 1
        print()
        print('Топ за 2019-ый год: ')
        for k,v in products_sums_list.items():
            if k[1] == 2019:
                print(f'    {i}.{k[0]} -> {v}')
                i += 1
                if i > 10:
                    break
        i = 1
        print()
        print('Топ за 2020-ый год: ')
        for k, v in products_sums_list.items():
            if k[1] == 2020:
                print(f'    {i}.{k[0]} -> {v}')
                i += 1
                if i > 10:
                    break
        i = 1
        print()
        print('Топ за 2021-ый год: ')
        for k, v in products_sums_list.items():
            if k[1] == 2021:
                print(f'    {i}.{k[0]} -> {v}')
                i += 1
                if i > 10:
                    break

    return f'{work_folder}{csv_name}.csv'

date = datetime.datetime.now()
datestr = date.strftime("%Y%m%d%H%M%S")
clear_folder(work_folder)
csv_name = get_csv(xlsx_file, work_folder)
csv_namev = csv_from_excel(work_folder, csv_name)