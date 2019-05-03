#!/usr/bin/env python

import os
import sys
import json
import pymongo
import math
from pprint import pprint, pformat
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
from numpy import array
from match_result import MatchResult
import functools
import time
from get_object_size import get_object_size
import redis
from pickle import loads, dumps
import json

try:
    profile
except NameError:
    def profile(x): return x
        
orderbook_trades = {}


def get_trades(
    trades,
    envId: int,
    exchange: str,
    market: str,
    start: datetime,
    end: datetime,
):

    try:

        filter = {
            "e": envId,
            "x": exchange,
            "m": market,
            "ts": {
                "$gte": start,
                "$lte": end,
            }
        }

        for trade in trades.find(filter):

            if "ob" in trade:
                if trade["ob"] in orderbook_trades:
                    orderbook_trades[trade["ob"]].append(trade)
                else:
                    orderbook_trades[trade["ob"]] = [trade]

    except Exception as err:
        logging.exception(err)


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

if __name__ == '__main__':

    returncode = 0
    start_execution = time.time()
    max_time_diffs = []
    buy_trades_count = 0
    sell_trades_count = 0
    saved_orderbook_count = 0
    total_size_of_python_objects = 0

    try:

        logging.basicConfig(
            format='[%(levelname)-5s] %(message)s',
            level=logging.INFO,
            datefmt='')
        load_id = sys.argv[1]

        logging.debug(f'load {load_id}')

        assert os.environ['MONGODB'], 'MONGODB Not Defined'
        remote_mongo_client = MongoClient(os.environ['MONGODB'])
        if remote_mongo_client == None:
            raise Exception('Unable to Connect to Remote MongoDB')

        # Prep for remote mongodb access
        assert os.environ['SIMULATOR_DB'], 'SIMULATOR_DB Not Defined'
        sim_db = remote_mongo_client[os.environ['SIMULATOR_DB']]
        if sim_db == None:
            raise Exception('Unable to Connect to Database')

        # Prep for local mongodb access
        local_mongo_client = MongoClient()
        assert local_mongo_client, 'Unable to Connect to Local MongoDB'
        local_sim_db = local_mongo_client['sim']

        # Load and check configuration
        config = sim_db.loads.find_one(
            {"_id": ObjectId(load_id)}
        )
        assert config, 'Unknown Configuration'
        logging.debug("Load Configuration...\n%s", pformat(config, indent=4))

        input_orderbooks = remote_mongo_client.history.orderbooks

        # Ensure the output collection exists and is indexed
        try:
            sim_db.create_collection("orderbooks")

        except pymongo.errors.OperationFailure:
            pass  # Ignore error if collection already exists

        except pymongo.errors.CollectionInvalid:
            pass  # Ignore error if collection already exists

        sim_db.orderbooks.create_index(
            [
                ("e", pymongo.ASCENDING),
                ("x", pymongo.ASCENDING),
                ("m", pymongo.ASCENDING),
                ("ts", pymongo.ASCENDING),
            ], unique=True
        )

        logging.info (
            "{0:25}{1:10}".format( 
                "Channel:",
                ":".join([
                    str(config["envId"]),
                    config["exchange"],
                    config["market"]
                ])
            )
        )

        logging.info(
            "{0:25}{1:10}".format(
                'Start Time:', 
                str(config["timeFrame"]["startTime"])))
        logging.info(
            "{0:25}{1:10}".format(
                'End Time:', 
                str(config["timeFrame"]["endTime"])))

        trades = remote_mongo_client.history.trades
        get_trades(
            trades=trades,
            envId=config["envId"],
            exchange=config["exchange"],
            market=config["market"],
            start=config["timeFrame"]["startTime"],
            end=config["timeFrame"]["endTime"],
        )

        # Get first and last orderbook
        first_orderbook = Orderbooks.get_first_orderbook(
            envId=config["envId"],
            exchange=config["exchange"],
            market=config["market"],
            start=config["timeFrame"]["startTime"],
            orderbooks_collection=input_orderbooks
        )

        last_orderbook = Orderbooks.get_last_orderbook(

            envId=config["envId"],
            exchange=config["exchange"],
            market=config["market"],
            start=config["timeFrame"]["startTime"],
            end=config["timeFrame"]["endTime"],

            orderbooks_collection=input_orderbooks
        )

        if first_orderbook == None or last_orderbook == None:
            logging.info('No Orderbooks')
            os._exit(0)

        actual_start = first_orderbook["ts"]
        actual_end = last_orderbook["ts"]

        logging.info(
            "{0:25}{1:10}".format(
            'Actual Start:', 
            str(actual_start)))
        logging.info(
            "{0:25}{1:10}".format(
            'Actual End:', 
            str(actual_end)))

        assert actual_start < actual_end

            # Count the number of orderbooks
        number_orderbooks = Orderbooks.count_orderbooks(

            config["envId"],
            config["exchange"],
            config["market"],
            actual_start,
            actual_end,
            remote_mongo_client.history.orderbooks
        )

        logging.info(
            "{0:25}{1:10d}".format(
                'Orderbooks:',
                number_orderbooks)
        )

        # Count the number of corrupt orderbooks
        number_corrupt_orderbooks = Orderbooks.count_corrupt_orderbooks(

            config["envId"],
            config["exchange"],
            config["market"],
            actual_start,
            actual_end,
            remote_mongo_client.history.orderbooks
        )

        logging.info(
            "{0:25}{1:10d}".format(
                'Corrupt Orderbooks:',
                number_corrupt_orderbooks)
        )

        trades = remote_mongo_client.history.trades

        try:
            orderbooks = Orderbooks(
                orderbooks_collection=input_orderbooks,
                envId=config["envId"],
                exchange=config["exchange"],
                market=config["market"],
                depth=config["depth"],
                start=config["timeFrame"]["startTime"],
                end=config["timeFrame"]["endTime"],
            )

        except StopIteration:
            os._exit(0)

        orderbook = None

        total_orderbooks = 0
        total_trades = 0
        last_ob_timestamp = None
        corrupt_orderbooks = 0
        
        r = redis.Redis(
            host='localhost', 
            port=6379, 
            encoding=u'utf-8', 
            decode_responses=True, 
            db=0)

        try:

            while True:

                orderbook = orderbooks.next()

                # Record the 3 largest OB gaps
                if last_ob_timestamp != None:

                    ob_time_diff = orderbook["ts"] - last_ob_timestamp
                    max_time_diffs = np.sort(
                        np.append(max_time_diffs, ob_time_diff))[::-1][0:3]

                last_ob_timestamp = orderbook["ts"]

                total_orderbooks += 1

                sim_config.orderbook_id = orderbook['_id']

                # Get all trades associated with the orderbook
                filter = {
                    "e": orderbook['e'],
                    "x": orderbook['x'],
                    "m": orderbook['m'],
                    "ob": orderbook['_id'],
                }
                buy_trades, sell_trades = find_trades(trades, filter)

                for x in buy_trades:
                    x["_id"] = str(x["_id"])
                    x["ob"] = str(x["ob"])
                    x["ts"] = str(x["ts"])

                for x in sell_trades:
                    x["_id"] = str(x["_id"])
                    x["ob"] = str(x["ob"])
                    x["ts"] = str(x["ts"])

                logging.debug ("buy_trades: %r", buy_trades)
                logging.debug ("sell_trades: %r", sell_trades)

                if (__debug__ and
                            (
                                len(buy_trades) > 0 or
                                len(sell_trades) > 0
                            )
                        ):
                    logging.debug('len(buy_trades): ' + str(len(buy_trades)))
                    logging.debug('len(sell_trades): ' + str(len(sell_trades)))

                if len(buy_trades) > 0 or len(sell_trades) > 0:

                    saved_orderbook_count += 1

                    logging.debug ("buy_trades: %r", buy_trades)
                    logging.debug ("sell_trades: %r", sell_trades)

                    orderbook["s"] = True

                    depth = config["depth"]

                    orderbook['buy'] = \
                        Orderbooks.apply_depth(depth, orderbook['buy']) \
                        if config["trim"] else orderbook['buy']

                    orderbook['sell'] = \
                        Orderbooks.apply_depth(depth, orderbook['sell']) \
                        if config["trim"] else orderbook['sell']

                    buy_trades_count += len(buy_trades)
                    sell_trades_count += len(sell_trades)

                    insertObj = {
                        **orderbook,
                        **{"buy_trades": buy_trades},
                        **{"sell_trades": sell_trades}
                    }

                    # Can't save numpy arrays hence convert OBs to list
                    insertObj['buy'] = insertObj['buy'].tolist()
                    insertObj['sell'] = insertObj['sell'].tolist()

                    try:
                        local_sim_db.orderbooks.replace_one(
                            filter={"_id": insertObj["_id"]},
                            replacement=insertObj,
                            upsert=True)

                    except pymongo.errors.DuplicateKeyError:
                        pass

                    redis_key = str(orderbook['e']) + ':' + orderbook['x'] + ':' + orderbook['m']
                    redis_score = int(orderbook["ts"].timestamp()*1000)

                    r.zadd (redis_key, {str(orderbook['_id']): redis_score} )

                    # load buy_trades
                    score = 0
                    for t in buy_trades:
                        hmset_key = ":".join([str(orderbook['_id']), str(score), "buy_trades"])
                        r.hmset (
                            hmset_key, {
                            "_id": str(t["_id"]).encode(),
                            "e": t['e'],
                            "x": t['x'].encode(),
                            "m": t['m'].encode(),
                            "ts": str(t['ts']).encode(),
                            "ob": str(t['ob']).encode(),
                            "r": t['r'],
                            "q": t['q'],
                        }) 
                        r.zadd (
                            ":".join([
                                str(orderbook['_id']), 
                                "buy_trades",
                                ]), 
                            {hmset_key : score} )
                        score += 1

                    # load sell_trades
                    score = 0
                    for t in sell_trades:
                        hmset_key = ":".join([str(orderbook['_id']), str(score), "sell_trades"])
                        r.hmset (
                            hmset_key, {
                            "_id": str(t["_id"]).encode(),
                            "e": t['e'],
                            "x": t['x'].encode(),
                            "m": t['m'].encode(),
                            "ts": str(t['ts']).encode(),
                            "ob": str(t['ob']).encode(),
                            "r": t['r'],
                            "q": t['q'],
                        }) 
                        r.zadd (
                            ":".join([
                                str(orderbook['_id']), 
                                "sell_trades",
                                ]), 
                            {hmset_key : score} )
                        score += 1

                    r.hmset (str(orderbook['_id']), {
                        "e": orderbook['e'],
                        "x": orderbook['x'].encode(),
                        "m": orderbook['m'].encode(),
                        "ts": str(orderbook['ts']).encode(),
                        "N": orderbook['N'],
                        "V": str(orderbook['V']).encode(),
                    })

                    buy_rates_key = ":".join([str(orderbook['_id']),"buy_rates"])
                    r.delete (buy_rates_key)
                    r.rpush (buy_rates_key, *orderbook['buy'][:, 0])

                    buy_quantities_key = ":".join([str(orderbook['_id']),"buy_quantities"])
                    r.delete (buy_quantities_key)
                    r.rpush (buy_quantities_key, *orderbook['buy'][:, 1])

                    sell_rates_key = ":".join([str(orderbook['_id']),"sell_rates"])
                    r.delete (sell_rates_key)
                    r.rpush (sell_rates_key, *orderbook['sell'][:, 0])

                    sell_quantities_key = ":".join([str(orderbook['_id']),"sell_quantities"])
                    r.delete (sell_quantities_key)
                    r.rpush (sell_quantities_key, *orderbook['sell'][:, 1])


        except StopIteration:
            if orderbooks.corrupt_order_book_count > 0:
                logging.info(f'Corrupt Orderbook Count:  {orderbooks.corrupt_order_book_count}')

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

        logging.info('Orderbook Gaps (HH:MM:SS)')
        for gap in max_time_diffs:
            gap_minutes = gap.total_seconds() // 60
            gap_time = "{0:25}{1}:{2}:{3}".format (
                "",
                f'{int(gap_minutes // 60):02}',
                f'{int(gap_minutes % 60):02}',
                f'{int(gap.total_seconds() % 60):02}')  
            logging.info (gap_time)              

        load_time = round(time.time()-start_execution)
        load_minutes = load_time // 60
        load_time_formatted = "{0:25}{1}:{2}:{3}".format (
            "Load Time (HH:MM:SS):", 
            f'{load_minutes // 60:02}',
            f'{load_minutes % 60:02}',
            f'{load_time % 60:02}')

        logging.info (load_time_formatted)
        FORMAT = "{0:25}{1:10d}"
        logging.info(f'LOAD RESULTS')
        logging.info(FORMAT.format('    Buy Trades:',buy_trades_count))
        logging.info(FORMAT.format('    Sell Trades:',sell_trades_count))
        logging.info(FORMAT.format('    Saved OBs:',saved_orderbook_count))

 

        sys.exit(returncode)
