#!/usr/bin/env python

import os
import sys
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
from orderbooks import Orderbooks
import numpy as np
from match_result import MatchResult
import functools
import redis

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

    total_orderbooks = 0
    last_ob_timestamp = None

    matchings = []

    try:

        logging.basicConfig(
            format='[%(levelname)-5s] %(message)s',
            level=logging.INFO,
            datefmt='')

        logging.debug(f'simulate args: {sys.argv}')

        if len(sys.argv) == 2:
            partition_id = ObjectId(sys.argv[1])
        else:
            assert False, 'Usage: simulate <Simulation ObjectId>'

        logging.debug('partition_id: ' + str(partition_id))

        # Prep for local mongodb access
        assert os.environ['MONGODB'], 'MONGODB Not Defined'
        remote_mongo_client = MongoClient(os.environ['MONGODB'])

        assert os.environ['SIMULATOR_DB'], 'SIMULATOR_DB Not Defined'
        sim_db = remote_mongo_client[os.environ['SIMULATOR_DB']]

        # Prep for local mongodb access
        local_mongo_client = MongoClient()
        assert local_mongo_client, 'Unable to Connect to Local MongoDB Database'
        local_sim_db = local_mongo_client['sim']

        sim_config.partition_config = sim_db.partitions.find_one({"_id": partition_id })
        assert sim_config.partition_config, 'Unknown Trader Configuration'

        logging.debug('sim_config.partition_config:\n' +
                      str(sim_config.partition_config))

        # Convenience Destructuring
        env_id = sim_config.partition_config["envId"]
        exchange = sim_config.partition_config["exchange"]
        market = sim_config.partition_config["market"]
        fee = sim_config.partition_config['feeRate']
        QL = sim_config.partition_config['quantityLimit']
        IL = sim_config.partition_config['inventoryLimit']
        tick = sim_config.partition_config['tick']
        start = sim_config.partition_config["startTime"]
        end = sim_config.partition_config["endTime"]
        depth = sim_config.partition_config["depth"]

        # Load PDF
        pdf = sim_db.PDFs.find_one(
            filter={
                "name": sim_config.partition_config["pdf"]
            }
        )
        assert "x" in pdf and "y" in pdf
        pdf["x"] = list(map(lambda x:pow(10,x),pdf["x"]))
        sim_config.pdf_x = np.array(pdf["x"])
        assert len(sim_config.pdf_x) > 0
        sim_config.pdf_y = np.array(pdf["y"])
        assert len(sim_config.pdf_y) > 0
        assert sim_config.pdf_x.shape == sim_config.pdf_y.shape

        # Rate Precision
        sim_config.rate_precision = - \
            int(numpy.log10(sim_config.partition_config['tick']))

        # Matching Engine
        matching_engine = MatchingEngine(

            assets=np.array([math.inf, 0], dtype=float),
            QL=QL,
            IL=IL,
            actual_fee_rate=sim_config.partition_config["actualFeeRate"],
            min_notional=sim_config.partition_config["minNotional"],
            trades_collection=sim_db.trades,
        )

        sim_config.init(sim_config.partition_config)

        assert sim_config.partition_config["minNotional"], "Min Notional Missing"

        if __debug__:
            from co1 import Trader
            trader = Trader(sim_config)

        else:
            trader = importlib.import_module(
                sim_config.partition_config["trader"].lower()
                ).Trader(sim_config)

         # Connect to Redis
        r = redis.Redis(host='localhost', port=6379, encoding=u'utf-8', decode_responses=True, db=0)

         # Orderbooks
        redis_low_score = int(start.timestamp()*1000)
        redis_high_score = int(end.timestamp()*1000)

        logging.debug(f'redis_low_score: {redis_low_score}')
        logging.debug(f'redis_high_score: {redis_high_score}')

        ob_key = ":".join ([str(env_id), exchange, market])

        ob_ids = r.zrangebyscore(ob_key, redis_low_score, redis_high_score)
        logging.info('Orderbooks: %d', len(ob_ids))

        try:

            for ob_id in ob_ids:

                # Retrieve the orderbook's timestamp
                ob = r.hgetall(str(ob_id))
                assert ob, 'Unknown Orderbook Id'
                ob_ts = dateutil.parser.parse(ob["ts"])

                total_orderbooks += 1
                sim_config.orderbook_id = ob_id

                # buy_ob
                buy_rates_key = ":".join([ob_id,"buy_rates"])
                buy_quantities_key = ":".join([ob_id,"buy_quantities"])

                buy_rates = np.array (list(map(float, r.lrange (buy_rates_key, 0, -1))))
                buy_quantities = np.array (list(map(float, r.lrange (buy_quantities_key, 0, -1))))

                buy_ob = np.vstack((buy_rates, buy_quantities)).T

                # sell_ob
                sell_rates_key = ":".join([ob_id,"sell_rates"])
                sell_quantities_key = ":".join([ob_id,"sell_quantities"])

                sell_rates = np.array (list(map(float, r.lrange (sell_rates_key, 0, -1))))
                sell_quantities = np.array (list(map(float, r.lrange (sell_quantities_key, 0, -1))))

                sell_ob = np.vstack((sell_rates, sell_quantities)).T

                buy_ob = Orderbooks.apply_depth(depth, buy_ob)
                sell_ob = Orderbooks.apply_depth(depth, sell_ob)

                # buy_trades
                buy_trade_keys = \
                    r.zrangebyscore (":".join([ob_id,"buy_trades"]), 0, math.inf)
                logging.debug('buy_trade_keys:\n%r',  buy_trade_keys)

                buy_trades = []
                for x in buy_trade_keys:

                    trade = r.hgetall (x)
                    trade["e"] = int(trade["e"])
                    trade["ts"] = dateutil.parser.parse(trade["ts"])
                    trade["r"] = float (trade["r"])
                    trade["q"] = float (trade["q"])
                    buy_trades.append (trade)

                logging.debug ("buy_trades: %r", buy_trades)

                # sell_trades
                sell_trade_keys = \
                    r.zrangebyscore (":".join([ob_id,"sell_trades"]), 0, math.inf)
                logging.debug('sell_trade_keys:\n%r',  sell_trade_keys)

                sell_trades = []
                for x in sell_trade_keys:

                    trade = r.hgetall (x)
                    trade["e"] = int(trade["e"])
                    trade["ts"] = dateutil.parser.parse(trade["ts"])
                    trade["r"] = float (trade["r"])
                    trade["q"] = float (trade["q"])
                    sell_trades.append (trade)

                logging.debug ("sell_trades: %r", sell_trades)

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

                buy_rate, sell_rate = trader.compute_orders(buyob=buy_ob, sellob=sell_ob)

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

                    buy_depth = sum(map(
                        lambda x: x[1] if x[0] > buy_rate else 0,
                        buy_ob))

                    sell_depth = sum(
                        map(
                            lambda x: x[1] if x[0] < sell_rate else 0,
                            sell_ob))

                    matchings.append ({
                        "runId": sim_config.partition_config["runId"],
                        "simVersion": sim_config.partition_config["simVersion"],
                        "e": sim_config.partition_config["envId"],
                        "x": sim_config.partition_config["exchange"],
                        "m": sim_config.partition_config["market"],
                        "s": sim_config.partition_config["simId"],
                        "p": sim_config.partition_config["_id"],
                        "depth": sim_config.partition_config["depth"],
                        "ob": ob_id,
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

        # Send the matchings to the database
        if len (matchings) > 0:
            sim_db.matchings.insert_many(matchings)

        logging.debug(f'CO Calls: {CO_calls}')
        logging.debug(f'Matching Engine Calls: {matching_engine_calls}')

        logging.info("***   Buy Summary   ***")
        logging.info(f'Matches: {matching_engine.buy_match_count}')
        logging.info(
            f'Blocked: {matching_engine.buy_blocked_count}')
        logging.info(
            f'No Trades: {matching_engine.buy_no_trades_count}')
        logging.info(
            f'Notion Failures: {matching_engine.buy_notion_failure_count}')
        logging.info(
            f'Unmatchable: {matching_engine.buy_unmatchable_count}')

        logging.info("***   Sell Summary   ***")
        logging.info(f'Matches: {matching_engine.sell_match_count}')
        logging.info(
            f'Blocked: {matching_engine.sell_blocked_count}')
        logging.info(
            f'No Trades: {matching_engine.sell_no_trades_count}')
        logging.info(
            f'Notion Failures: {matching_engine.sell_notion_failure_count}')
        logging.info(
            f'Unmatchable: {matching_engine.sell_unmatchable_count}')

        sys.exit(returncode)


if __name__ == '__main__':
    simulate()
