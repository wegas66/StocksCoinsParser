import time
from coinmarketcapapi import CoinMarketCapAPI, CoinMarketCapAPIError
import gspread
import pandas as pd
import yfinance as yf
from yahoo_fin import stock_info
from datetime import datetime, timedelta
import schedule

gc = gspread.service_account(filename='service_account.json')

cmc = CoinMarketCapAPI('')

sh = gc.open_by_url(
    'https://docs.google.com/spreadsheets/d/1a4Dxxxxxxxxxxxxxxxxxxxxxxxx/')

worksheet = sh.worksheet("Статистика (заполняем тут)")


def get_cryptoinfo(list_of_crypto):
    error = True
    while error:
        cryptos = {}
        try:
            cryptos_to_str = ','.join(list_of_crypto)
            r = cmc.cryptocurrency_quotes_latest(symbol=f'{cryptos_to_str}')
            error = False
            for name, value in r.data.items():
                cryptos[name] = value['quote']['USD']['price']
            return cryptos
        except Exception as e:
            bad = str(e).split(':')[2].replace('"', '').replace(' ', '')
            bad = bad.split(',')
            for n in bad:
                print(f'[{datetime.now()}] ' + n + ' - неправильный тикер')
                list_of_crypto.remove(n)
                cryptos[n] = 'Неправильный тикер'


def get_stocks(list_of_stocks):
    stocks_str = ' '.join(list_of_stocks)
    stocks = yf.Tickers(f'{stocks_str}').tickers
    list_stocks = {}
    for name, stock in stocks.items():
        try:
            list_stocks[name] = stock_info.get_live_price(name)
            if (list_stocks[name]>0) == False:
                list_stocks[name] = 'Неправильный тикер'
            else:
                list_stocks[name] = format(list_stocks[name], '.2f')
        except:
            list_stocks[name] = 'Неправильный тикер'
            print(f'[{datetime.now()}] '+ name + ' - неправильный тикер')
            continue
    raw_list_stocks = {}
    for name, value in list_stocks.items():
        try:
            raw_list_stocks[name] = float(value)
        except:
            raw_list_stocks[name] = value
    return raw_list_stocks


def get_table(worksheet):
    df = pd.DataFrame(worksheet.get_all_records())
    return df


def get_tickers_from_table(df, type_str):
    list_tickets = []
    for index, row in df.iterrows():
        if row['Криптовалюта/акции'] == type_str and row['Закрыта ли сделка'] == 'нет':
            list_tickets.append(row['Название инструмента'])
    return list(set(list_tickets))


def refresh_data_in_table(df, all_info):
    for i, row in df.iterrows():
        if row['Закрыта ли сделка'] == 'нет':
            try:
                price = all_info[df.loc[i, "Название инструмента"]]
            except:
                continue
            if price == 'Неправильный тикер':
                df.loc[i, 'Текущая цена'] = price
                continue
            if row['Тип сделки (покупка/продажа)'] == 'покупка':
                if not row['Стоп-лосс (если есть)'] == '':
                    if float(price) < float(row['Стоп-лосс (если есть)']):
                        df.loc[i,'Цена закрытия (если был сигнал на закрытие / стоп-лосс / тейкпрофит)'] = row['Стоп-лосс (если есть)']
                        df.loc[i,'Дата закрытия'] = str(datetime.now().strftime("%d.%m.%Y %H:%M"))
                        df.loc[i, 'Закрыта ли сделка'] = 'да '

                if not row['Тейк-профит (если есть)'] == '':
                    if float(price) > float(row['Тейк-профит (если есть)']):
                        df.loc[i,'Цена закрытия (если был сигнал на закрытие / стоп-лосс / тейкпрофит)'] = row['Тейк-профит (если есть)']
                        df.loc[i,'Дата закрытия'] = str(datetime.now().strftime("%d.%m.%Y %H:%M"))
                        df.loc[i, 'Закрыта ли сделка'] = 'да '

            elif row['Тип сделки (покупка/продажа)'] == 'продажа':
                if not row['Стоп-лосс (если есть)'] == '':
                    if float(price) > float(row['Стоп-лосс (если есть)']):
                        df.loc[i,'Цена закрытия (если был сигнал на закрытие / стоп-лосс / тейкпрофит)'] = row['Стоп-лосс (если есть)']
                        df.loc[i,'Дата закрытия'] = str(datetime.now().strftime("%d.%m.%Y %H:%M"))
                        df.loc[i, 'Закрыта ли сделка'] = 'да '

                if not row['Тейк-профит (если есть)'] == '':
                    if float(price) < float(row['Тейк-профит (если есть)']):
                        df.loc[i,'Цена закрытия (если был сигнал на закрытие / стоп-лосс / тейкпрофит)'] = row['Тейк-профит (если есть)']
                        df.loc[i,'Дата закрытия'] = str(datetime.now().strftime("%d.%m.%Y %H:%M"))
                        df.loc[i, 'Закрыта ли сделка'] = 'да '

            df.loc[i, 'Текущая цена'] = price
            df.loc[i, 'Дата актуализации котировки'] = str(datetime.now().strftime("%d.%m.%Y %H:%M"))

    return df


def do_all():
    print('[WORK STARTED]')
    df_raw = get_table(worksheet)
    df = df_raw[['Криптовалюта/акции', 'Название инструмента', 'Закрыта ли сделка',
                 'Валюта базового инструмента (руб/USD/BTC или другое)']]
    list_cryptos = get_tickers_from_table(df, 'Криптовалюта')
    list_stocks = get_tickers_from_table(df, 'Акции')
    info_stocks = get_stocks(list_stocks)
    info_crypto = get_cryptoinfo(list_cryptos)
    all_info = info_stocks | info_crypto
    new_df = refresh_data_in_table(df_raw, all_info)
    new_df.drop(new_df.columns[15:], axis=1, inplace=True)
    worksheet.update([new_df.columns.values.tolist()] + new_df.values.tolist())
    print('[WORK DONE]')
    print(f'[NEXT RUN IN {datetime.now()+timedelta(minutes=30)}]')


def main():
    do_all()
    schedule.every(30).minutes.do(do_all)
    while 1:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
