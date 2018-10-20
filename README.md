## aicoin的历史k线数据爬虫

aicoin主网: www.aicoin.net.cn/

## 1.抓取
aicoin目前支持161家交易所，13000+交易对
通过gevent+requests方式抓取

## 2.储存到postgresql
以交易对区分表格:

库名：aicoin_ohlcv

表名（举例）：bitfinex_btc_usd