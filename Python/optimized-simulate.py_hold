#!/usr/bin/env python

import os
import sys
import json
import pymongo
import math
from pymongo import MongoClient
from datetime import datetime
import dateutil.parser
import dateutil.parser as parser
import importlib
import logging
from schema import Schema, And, Use, Optional, SchemaError
from bson.objectid import ObjectId
import operator
import numpy
from matching_engine import MatchingEngine
import sim_config
from copy import copy
from match_result import MatchResult
import functools
import redis
import json
import numpy as np

try:
    profile
except NameError:
    def profile(x): return x


def check(conf_schema, conf):
    try:
        conf_schema.validate(conf)
        return True
    except SchemaError:
        return False

def find_trades(trades, filter):

    buy_trades = []
    sell_trades = []

    for trade in trades.find(filter=filter):

        # Corral the buy and sell trades into separate lists
        if trade['buy']:
            buy_trades.append(trade)
        else:
            sell_trades.append(trade)

        # Sort the buy trades into decreasing order
        buy_trades.sort(key=operator.itemgetter('r'),
                        reverse=True)

        # Sort the sell trades into increasing order
        sell_trades.sort(key=operator.itemgetter('r'),
                         reverse=False)

    return buy_trades, sell_trades

def get_buy_depth (rate: float, rates: list, quantities: list):

    depth = 0

    for r, q in zip (rates, quantities):

        if r > rate: break
        depth += r * q

    return depth

def get_sell_depth (rate: float, rates: list, quantities: list):

    depth = 0

    for r, q in zip (rates, quantities):

        if r < rate: break
        depth += r * q

    return depth

def apply_depth(depth: float, rates: list, quantities: list):

    i = 0
    in_depth = 0

    for r, q in zip (rates, quantities):

        in_depth += r * q
        if in_depth >= depth: break
        i += 1

    rates[len(rates)-i:] = []
    quantities[len(quantities)-i:] = []


@profile
def simulate():

    matching_engine: MatchingEngine = None
    CO_calls = 0
    matching_engine_calls = 0
    returncode = 0
    max_time_diffs = np.array([])
    last_ob_timestamp = None

    buy_trades_count = 0
    sell_trades_count = 0

    try:

        logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S')

        logging.debug(f'simulate args: {sys.argv}')

        if len(sys.argv) == 2:
            partition_id = ObjectId(sys.argv[1])
        else:
            assert False, 'Usage: simulate <Simulation ObjectId>'

        logging.debug('partition_id: ' + str(partition_id))

        assert os.environ['MONGODB'], 'MONGODB Not Defined'
        remote_mongo_client = MongoClient(os.environ['MONGODB'])

        assert os.environ['SIMULATOR_DB'], 'SIMULATOR_DB Not Defined'
        sim_db = remote_mongo_client[os.environ['SIMULATOR_DB']]
        sim_config.partition_config = sim_db.partitions.find_one(
            {
                "_id": partition_id
            }
        )

        env_id = sim_config.partition_config["envId"]
        exchange = sim_config.partition_config["exchange"]
        market = sim_config.partition_config["market"]
        fee = sim_config.partition_config['feeRate']
        QL = sim_config.partition_config['quantityLimit']
        IL = sim_config.partition_config['inventoryLimit']
        tick = sim_config.partition_config['tick']

        logging.debug('sim_config.partition_config:\n' +
                      str(sim_config.partition_config))

        if sim_config.partition_config == None:
            raise Exception('Unknown Trader Configuration')

        # If simId is 0, then it is a stand alone subprocess test run
        if sim_config.partition_config["simId"] == '0':
            sim_config.partition_config["simId"] = ObjectId()

        start = sim_config.partition_config["startTime"]
        end = sim_config.partition_config["endTime"]

        r = redis.Redis(
            host='localhost', 
            port=6379, 
            encoding=u'utf-8', 
            decode_responses=False, db=0)

        redis_low_score = int(start.timestamp()*1000)
        redis_high_score = int(end.timestamp()*1000)

        logging.debug(f'redis_low_score: {redis_low_score}')
        logging.debug(f'redis_high_score: {redis_high_score}')

        # Orderbooks
        ob_key = ":".join ([str(env_id), exchange, market])

        ob_ids = r.zrangebyscore(ob_key, redis_low_score, redis_high_score)
        logging.error('ob_ids: %r', len(ob_ids))

        pdf = sim_db.PDFs.find_one(
            filter={
                "name": sim_config.partition_config["pdf"]
            }
        )
        
        # PDF
        sim_config.pdf_x = list(map(lambda x:pow(10,x),pdf["x"]))
        assert len(sim_config.pdf_x) > 0
        sim_config.pdf_y = pdf["y"]
        assert len(sim_config.pdf_x) == len(sim_config.pdf_y)

        sim_config.rate_precision = - \
            int(numpy.log10(sim_config.partition_config['tick']))

        sim_config.quantity_precision = - \
            int(numpy.log10(sim_config.partition_config['tick']))

        assert  sim_config.partition_config["minNotional"], \
                "Min Notional Missing"

        matching_engine = MatchingEngine(

            assets=np.array([math.inf, 0], dtype=float),
            QL=QL,
            IL=IL,
            actual_fee_rate=sim_config.partition_config["actualFeeRate"],
            min_notional=sim_config.partition_config["minNotional"],
            trades_collection=sim_db.trades,
        )

        logging.debug(f'start: {start}')
        logging.debug(f'end: {end}')

        sim_config.init(sim_config.partition_config)

        logging.info('trader: ' + str(sim_config.partition_config["trader"]))

        trader = importlib.import_module(
            sim_config.partition_config["trader"].lower())

        total_orderbooks = 0
        last_ob_timestamp = None

        try:

            for ob_id in ob_ids:

                orderbook = {}

                ob_id = ob_id.decode()
                orderbook["_id"] = ob_id

                # orderbook["buy"]
                buy_rates_key = ":".join([str(orderbook['_id']),"buy_rates"])
                buy_quantities_key = ":".join([str(orderbook['_id']),"buy_quantities"])

                buy_rates = list(map(float, r.lrange (buy_rates_key, 0, -1)))
                buy_quantities = list(map(float, r.lrange (buy_quantities_key, 0, -1)))

                # orderbook["sell"]
                sell_rates_key = ":".join([str(orderbook['_id']),"sell_rates"])
                sell_quantities_key = ":".join([str(orderbook['_id']),"sell_quantities"])

                sell_rates = list(map(float, r.lrange (sell_rates_key, 0, -1)))
                sell_quantities = list(map(float, r.lrange (sell_quantities_key, 0, -1)))

                buy_trades_key = ":".join([str(orderbook['_id']),"buy_trades"])
                orderbook["buy_trades"] = json.loads (r.get (buy_trades_key))
                # logging.error('buy_trades:\n%r',  orderbook["buy_trades"])

                sell_trades_key = ":".join([str(orderbook['_id']),"sell_trades"])
                orderbook["sell_trades"] = json.loads (r.get (sell_trades_key))
                # logging.error('sell_trades:\n%r',  orderbook["sell_trades"])
               
                ob = r.hmget (str(ob_id), [
                    "e".encode(), 
                    "x".encode(), 
                    "m".encode(), 
                    "ts".encode(), 
                    "N".encode(), 
                    "V".encode(),
                    ])

                # logging.debug('ob.e: %r',  int(ob[0].decode()))
                orderbook["e"] = int(ob[0])

                # logging.debug('ob.x: %r',  str(ob[1].decode()))
                orderbook["x"] = str(ob[1].decode())

                # logging.debug('ob.m: %r',  str(ob[2].decode()))
                orderbook["m"] = str(ob[2].decode())

                # logging.debug('ob.ts: %r',  dateutil.parser.parse(ob[3].decode()))
                orderbook["ts"] = dateutil.parser.parse(ob[3])

                # logging.debug('ob.N: %r',  int(ob[4].decode()))
                orderbook["N"] = int(ob[4])

                # logging.debug('ob.V: %r',  str(ob[5].decode()))
                orderbook["V"] = str(ob[5].decode())

                # Record the 3 largest OB gaps
                if last_ob_timestamp != None:

                    ob_time_diff = orderbook["ts"] - last_ob_timestamp
                    max_time_diffs = np.sort(
                        np.append(max_time_diffs, ob_time_diff))[::-1][0:3]

                last_ob_timestamp = orderbook["ts"]

                total_orderbooks += 1

                sim_config.orderbook_id = orderbook['_id']

                buy_trades = orderbook["buy_trades"]
                assert all(buy_trades[i]["r"] >= buy_trades[i+1]["r"]
                           for i in range(len(buy_trades)-1)), 'Buy Trades Not Sorted'
                buy_trades_count += len(buy_trades)

                sell_trades = orderbook["sell_trades"]
                assert all(sell_trades[i]["r"] <= sell_trades[i+1]["r"]
                           for i in range(len(sell_trades)-1)), 'Sell Trades Not Sorted'
                sell_trades_count += len(sell_trades)

                assert len(buy_trades) > 0 or len(sell_trades) > 0

                if (__debug__ and
                        (
                            len(buy_trades) > 0 or
                            len(sell_trades) > 0
                        )
                    ):
                    logging.debug('len(buy_trades): ' + str(len(buy_trades)))
                    logging.debug('len(sell_trades): ' + str(len(sell_trades)))

                CO_calls += 1

                market_rate = (buy_rates[0] + sell_rates[0]) / 2.0

                depth = sim_config.partition_config["depth"]

                apply_depth (depth, buy_rates, buy_quantities)
                apply_depth (depth, sell_rates, sell_quantities)
                
                for x in buy_quantities:
                    x *= market_rate

                for x in sell_quantities:
                    x *= market_rate                

                buy_rate, sell_rate = trader.compute_orders(
                    fee,
                    QL,
                    tick,
                    sim_config.pdf_x,
                    sim_config.pdf_y,
                    buy_rates,
                    buy_quantities,
                    sell_rates,
                    sell_quantities,
                )

                if buy_rate > 0 and sell_rate > 0:

                    matching_engine_calls += 1

                    buy_match, sell_match = matching_engine.match(
                        buy_rate=buy_rate,
                        sell_rate=sell_rate,
                        buy_trades=buy_trades,
                        sell_trades=sell_trades,
                    )

                    funds, inventory = matching_engine.assets

                    if __debug__:
                        logging.debug(f'compute_orders return: {result}')
                        logging.debug('buy_match: ' + str(buy_match))
                        logging.debug('sell_match: ' + str(sell_match))

                    buy_depth = get_buy_depth (
                        buy_rate, 
                        buy_rates, 
                        buy_quantities)

                    sell_depth = get_sell_depth (
                        sell_rate, 
                        sell_rates, 
                        sell_quantities)
                   
                    sim_db.matchings.insert_one({
                        "runId": sim_config.partition_config["runId"],
                        "simVersion": sim_config.partition_config["simVersion"],
                        "e": sim_config.partition_config["envId"],
                        "x": sim_config.partition_config["exchange"],
                        "m": sim_config.partition_config["market"],
                        "s": sim_config.partition_config["simId"],
                        "p": sim_config.partition_config["_id"],
                        "depth": sim_config.partition_config["depth"],
                        "ob": orderbook['_id'],
                        "ts": datetime.now(),
                        "topBuy": buy_rates[0],
                        "buyRate": buy_rate,
                        "buyCount": len(buy_trades),
                        "buyMatch": str(buy_match).split('.')[1],
                        "buyDepth": buy_depth,
                        "buys": list(map((lambda x: x["r"]), buy_trades))
                        if len(buy_trades) > 0 else None,
                        "topSell": sell_rates[0],
                        "sellRate": sell_rate,
                        "sellCount": len(sell_trades),
                        "sellMatch": str(sell_match).split('.')[1],
                        "sellDepth": sell_depth,
                        "sells": list(map((lambda x: x["r"]), sell_trades))
                        if len(sell_trades) > 0 else None,
                        "funds": funds,
                        "inventory": inventory,
                    })

        except StopIteration:
            # logging.info('StopIteration Detected')
            returncode = 0

        except KeyError as err:
            logging.error('KeyError Detected: %r', err)
            logging.exception('Exception: %s', err)
            returncode = 1

    except Exception as err:
        logging.exception('Exception: %s', err)

    except:
        logging.exception('Unknown Exception')

    finally:

        logging.info(f'CO Calls: {CO_calls}')
        logging.info(f'buy_trades_count: {buy_trades_count}')
        logging.info(f'sell_trades_count: {sell_trades_count}')
        logging.info(f'Matching Engine Calls: {matching_engine_calls}')
        logging.info(
            f'buy_blocked_count: {matching_engine.buy_blocked_count}')
        logging.info(
            f'buy_no_trades_count: {matching_engine.buy_no_trades_count}')
        logging.info(
            f'buy_notion_failure_count: {matching_engine.buy_notion_failure_count}')
        logging.info(f'buy_match_count: {matching_engine.buy_match_count}')
        logging.info(
            f'buy_unmatchable_count: {matching_engine.buy_unmatchable_count}')

        logging.info(
            f'sell_blocked_count: {matching_engine.sell_blocked_count}')
        logging.info(
            f'sell_no_trades_count: {matching_engine.sell_no_trades_count}')
        logging.info(
            f'sell_notion_failure_count: {matching_engine.sell_notion_failure_count}')
        logging.info(f'sell_match_count: {matching_engine.sell_match_count}')
        logging.info(
            f'sell_unmatchable_count: {matching_engine.sell_unmatchable_count}')

        logging.info(
            'Largest Gaps Between Orderbooks (Hours:Minutes:Seconds)')

        for time_diff in max_time_diffs:
            total_minutes = time_diff.total_seconds() // 60
            logging.info(
                '    -> %04d:%02d:%02d',
                total_minutes // 60,
                total_minutes % 60,
                time_diff.total_seconds() % 60)

        logging.info(f'Total Orderbooks: {total_orderbooks}')
        logging.info(f'Last OB Timestamp: {last_ob_timestamp}')
        sys.exit(returncode)


if __name__ == '__main__':
    simulate()
