# coding=utf-8
import re
import json
import time
import datetime
from pprint import pprint
import psycopg2
import gevent
from gevent import monkey
monkey.patch_all()
import requests

DEBUG = False


class Postgres:
    def __init__(self, *args, **kwargs):
        if DEBUG:
            self.conn = psycopg2.connect(
                database="aicoin_ohlcv", user="postgres",
                password="123456", host="127.0.0.1", port="5432")
        else:
            self.conn = psycopg2.connect(
                database="aicoin_ohlcv", user="root",
                password="123456", host="172.17.100.100", port="5432")
        self.cursor = self.conn.cursor()

    def close_connection(self):
        """
        关闭连接
        """
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def create_table(self, create_table_sql, table_name, index_field=None):
        """
        创建表
        """
        try:
            self.cursor.execute(create_table_sql)
            if index_field:
                create_index_sql = 'CREATE UNIQUE INDEX IF NOT EXISTS {}_{} ON {} ({});'.format(
                    table_name, index_field, table_name, index_field)
                self.cursor.execute(create_index_sql)
        except Exception as e:
            print('Create Table Already Exists', e)
        finally:
            self.conn.commit()

    def drop_table(self, table_name):
        """
        删除表
        """
        drop_table_sql = '''DROP TABLE {};'''.format(table_name)
        try:
            self.cursor.execute(drop_table_sql)
        except Exception as e:
            print('Drop Table Error', e)
        finally:
            self.conn.commit()

    def insert_many(self, table_name, filed_name, data_list):
        """
        批量插入表数据

        例如:
        filed_name = "market_cap_by_available_supply, price_btc, price_usd, volume_usd, last_time"

        data_list = [(market_cap_by_available_supply, price_btc,
                      price_usd, volume_usd, last_time),...]
        """
        data_list_str = [str(('to_timestamp(' + str(d[0]) + ')',) +
                             d[1:6]).replace('\'', '')  # 仅取前6个字段，防止出现7个字段的情况
                         for d in data_list]
        # mts重复的时候忽略
        insert_table_sql = """insert into {tb_name} ({filed_name}) values {value} on conflict (mts) do nothing;""".format(
            tb_name=table_name,
            filed_name=filed_name,
            value=','.join(data_list_str))
        try:
            self.cursor.execute(insert_table_sql)
            print('insert success!')
        except Exception as e:
            print('Insert Data Error:{}\nSQL:{}'.format(e, data_list))
        finally:
            self.conn.commit()


class AicoinOhlcvScraper(object):
    """
    aicoin的历史k线抓取爬虫
    """

    def __init__(self):
        self.main_url = 'https://www.aicoin.net.cn/chart/'
        self.history_url = "https://www.aicoin.net.cn/chart/api/data/periodHistory"
        self.step_list = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '4h': 14400,
            '12h': 43200,
            '24h': 86400,
            '1w': 604800,
            '1M': 2592000
        }
        self.psg = Postgres()

    def convert_ts_2_datetime(self, ts):
        dt = datetime.datetime.fromtimestamp(ts)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def save_all_timeframe(self, symbol, ex, coin):
        """
        功能:
            跑一个交易对的所有timeframe
        参数:
            symbol: bitfinexbtcusd
            ex: bitfiex
            coin: btc_usd
        返回:
            交易对的所有timeframe k线数据,去重并且排序, list类型
        """
        tb_name = "t_{}_{}".format(ex, coin)
        create_table_sql = '''CREATE TABLE IF NOT EXISTS {} (id serial8 primary key, ''' +\
            '''mts timestamp(0), ''' +\
            '''open numeric, ''' +\
            '''high numeric, ''' +\
            '''low numeric, ''' +\
            '''close numeric, ''' +\
            '''volume_usd numeric, ''' +\
            '''create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP);'''
        create_table_sql = create_table_sql.format(tb_name)
        self.psg.create_table(create_table_sql, tb_name, 'mts')  # 尝试创建表
        field_name = "mts, open, high, low, close, volume_usd"
        for _, v in self.step_list.items():
            a, _ = self.get_all_ohlcv_by_symbol(symbol, v)
            # to timestamp
            # 批量插入数据
            self.psg.insert_many(
                tb_name, field_name,
                list(map(lambda x: tuple(x), a)))

    def symbol_list_api(self, ex='59D56005'):
        """
        获取所有的交易所、交易对及其对应的key
        """
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Host": "www.aicoin.net.cn",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
        }
        res = requests.get(self.main_url + ex, headers=headers)
        window = re.compile('window.COINS = \[.*?\];', re.S)
        dicts_pattern = re.compile('{.*?}', re.S)
        window_COINS = re.findall(window, res.text)[0]
        dicts = re.findall(dicts_pattern, window_COINS)
        return_list = []
        for d in dicts:
            loaded = json.loads(d)
            return_list.append(
                (loaded['symbol'], loaded['mid'], '{}_{}'.format(
                    loaded['coin'].lower(), loaded['symbol'].split(
                        loaded['mid'] + loaded['coin'].lower())[-1]))
            )
        return return_list

    def history_ohlcv_api(self, symbol, count, step, times=1):
        """
        往前滑动
        """
        headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
            "Connection": "keep-alive",
            "Content-Length": "0",
            "Host": "www.aicoin.net.cn",
            "Origin": "https://www.aicoin.net.cn",
            "Referer": "https://www.aicoin.net.cn/chart/D331D4E2",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
        }
        data = {
            "symbol": symbol,
            "step": step,
            "times": times,
            "count": count
        }
        r = requests.post(self.history_url, data=data,
                          headers=headers, timeout=6)
        time.sleep(1)
        return r.json()

    def ohlcv_api(self, symbol, step):
        """
        获取某个交易所的交易对K线
        """
        ohlcv_api = 'https://www.aicoin.net.cn/chart/api/data/period'

        headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
            "Connection": "keep-alive",
            "Host": "www.aicoin.net.cn",
            "Referer": "https://www.aicoin.net.cn/chart/D331D4E2",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
        }

        # api params
        parms = {
            'symbol': symbol,
            'step': step,
        }
        r1 = requests.get(ohlcv_api, params=parms, headers=headers)
        if r1.status_code != 200:
            print('获取k线数据失败')
            return []
        rj = r1.json()
        return rj['data'], rj['count']

    def get_all_ohlcv_by_symbol(self, symbol, step):
        """
        获取交易对的所有历史k线（首页请求+向右滑动方式）
        """
        times = 0
        d2 = [1]
        data, count = self.ohlcv_api(symbol, step)
        while d2 != []:
            times += 1
            d2 = self.history_ohlcv_api(symbol, count, step, times)
            if any(d2):
                data.extend(d2)
        return data, times

    def walk_all_symbols_to_save(self, all_symbols_tuple):
        """
        遍历所有的symbol,储存k线历史
        """
        len_all_symbols_tuple = len(all_symbols_tuple)
        step = 4
        for i in range(0, len_all_symbols_tuple, step):
            gevent.joinall([gevent.spawn(
                self.save_all_timeframe, symbol, ex, coin)
                for symbol, ex, coin in all_symbols_tuple[i:i + step]])
            time.sleep(1)
        self.psg.close_connection()


def main():
    scraper = AicoinOhlcvScraper()
    ll = scraper.symbol_list_api()
    pprint(ll)
    scraper.walk_all_symbols_to_save(ll)


if __name__ == '__main__':
    main()
