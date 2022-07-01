import csv
import datetime
import os
from random import choice, randint
import pandas as pd
import pyodbc
import tqdm
import time

original = "C:\\bigdata\\original\\"
work_folder = "C:\\bigdata\\"
months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
years = [2017, 2018, 2019, 2020, 2021]


def connect():
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=ASUSALEX\SQLEXPRESS;'
                          'Database=python_sql;'
                          'Trusted_Connection=yes;')
    conn.commit()
    return conn


def topyear(cursor):
    cursor.execute('SELECT top(1)SUM(quantity_sold*(retail_price-purchase_price)) AS "sumn" FROM dbo.data20220410131718'
                   ' where year = ^20*; ')


def drop_table(table_name, cursor):
    cursor.execute(f"drop table {table_name}")
    print("данные удалены")
    cursor.commit()


def clear_folder(path):
    print("чищу каталог")
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))


def get_csv(xlsx_file, folder):

    date = datetime.datetime.now()
    csv_name_i = date.strftime("%Y%m%d%H%M%S")
    print("открываю xlsx")
    data_xls = pd.read_excel(xlsx_file, "GroceryMar Pyat chips energ 10-")
    print("xlsx в csv")
    for i in tqdm.tqdm(range(20000567)):
        time.sleep(0.000000000001)
    data_xls.to_csv(f'{folder}{csv_name_i}.csv', encoding='utf-8', index=False)
    return csv_name_i


def csv_from_excel(folder, csv_name):
    print("шлифую csv")
    date = datetime.datetime.now()
    datestr = date.strftime("%Y%m%d%H%M%S")
    with open(f'{folder}{csv_name}.csv', encoding='utf-8') as csvf:
        data = str(csvf.read())
    with open(f"{folder}{datestr}v.csv", mode="w", encoding='utf-8') as w_file:
        csv_writer = csv.writer(w_file, delimiter=",", lineterminator="\r")
        for row in tqdm.tqdm(data.splitlines()):
            row = row.replace('"', '')
            row = list(row.split(','))
            row[0] = choice(months)
            row[1] = choice(years)
            if row[5] == '':
                row[5] = randint(50, 100)
            if row[6] == '':
                row[6] = randint(100.0, 1000.0)
            if row[7] == '':
                row[7] = float(row[6]) * 1.35
            csv_writer.writerow(row)
    copyfile(f"{folder}{datestr}v.csv")
    return f"{folder}{datestr}v.csv"


def copyfile(csv_name):
    with open(csv_name, mode="w", encoding='utf-8') as w_file:
        csv_writer = csv.writer(w_file, delimiter=",", lineterminator="\r")
        for row in tqdm.tqdm(data.splitlines()):
            csv_writer.writerow(row)

date = datetime.datetime.now()
datestr = date.strftime("%Y%m%d%H%M%S")
clear_folder("C:\\bigdata")
csv_name = get_csv("C:\\bigdata\\original\\bigdata2.xlsx", work_folder)
csv_namev = csv_from_excel(work_folder, csv_name)
print("подключаюсь к sql")
db_name = f"data{datestr}"
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=ASUSALEX\SQLEXPRESS;'
                      'Database=python_sql;'
                      'Trusted_Connection=yes;')
print("подключился")
cursor = conn.cursor()
cursor.execute(f'''
		CREATE TABLE {db_name} (
			month int,
			year int,
			id int,
			product_name nvarchar(200),
			supplier_name nvarchar(200),
			quantity_sold int,
			purchase_price float, 
			retail_price float
			)                     
            ''')
conn.commit()
data = pd.read_csv(csv_namev, on_bad_lines='skip') #понять, мешает или нет
df = pd.DataFrame(data)
for row in tqdm.tqdm(df.itertuples()):
    cursor.execute(f'''
                INSERT INTO {db_name} (month, year, id, product_name, supplier_name,
                                      quantity_sold, purchase_price, retail_price)
                VALUES (?,?,?,?,?,?,?,?)
                ''',
                   row[1],
                   row[2],
                   row[3],
                   row[4],
                   row[5],
                   row[6],
                   row[7],
                   row[8],
                   )
conn.commit()



print("всё")
