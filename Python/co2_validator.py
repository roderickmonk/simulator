import os
import sys
import json
import datetime as DT
import pymongo
import math
from pprint import pprint
from pymongo import MongoClient
from datetime import datetime
import dateutil.parser
import dateutil.parser as parser
import logging
import coloredlogs
from schema import Schema, And, Use, Optional, SchemaError
from bson.objectid import ObjectId
import operator
import sim_config
from copy import copy
import numpy as np
from operator import itemgetter
from co2 import Trader
import redis

import datetime
import timeit


class Timer:

    # Ref: https://stackoverflow.com/a/57931660/

    def __init__(self, round_ndigits: int = 0):
        self._round_ndigits = round_ndigits
        self._start_time = timeit.default_timer()

    def __call__(self) -> float:
        return timeit.default_timer() - self._start_time

    def __str__(self) -> str:
        return str(
            datetime.timedelta(seconds=round(self(), self._round_ndigits)))


class Co2Validator:

    trader = None
    r = None

    def __init__(self):

        self.r = redis.Redis(host='localhost',
                             port=6379,
                             encoding=u'utf-8',
                             decode_responses=True,
                             db=0)

        bot_id = self.r.get("testBotId")
        logging.error("bot_id: %s", bot_id)

        bot_config = self.r.hgetall(bot_id.encode())
        logging.error("bot_config: %r", bot_config)

        bot_config["quantityLimit"] = float(bot_config["quantityLimit"])
        bot_config["inventoryLimit"] = float(bot_config["quantityLimit"])
        bot_config["feeRate"] = float(bot_config["feeRate"])
        bot_config["tick"] = float(bot_config["tick"])
        bot_config["priceDepthLimit"] = float(bot_config["priceDepthLimit"])

        if bot_config["allowOrderConflicts"] == "true":
            bot_config["allowOrderConflicts"] = True
        else:
            bot_config["allowOrderConflicts"] = False

        self.trader = Trader(bot_config)

    def redis_get(self, cycle_time, field):
        raw = self.r.get(":".join([cycle_time, field]))
        raw = raw[0:-1]  # Remove the dangling comma
        raw = raw.split(',')
        raw = [float(i) for i in raw]
        return np.array(raw)

    def run(self):

        try:

            p = self.r.pubsub()
            p.psubscribe('*')

            while True:
                # print("Waiting...")
                message = p.get_message()

                if message != None:

                    if message['type'] == 'pmessage':

                        np.set_printoptions(precision=12)
                        np.set_printoptions(suppress=True)

                        rx_msg = json.loads(message["data"])

                        cycle_time, rust_buy_rate, rust_sell_rate = itemgetter(
                            'cycle_time', 'buy_rate', 'sell_rate')(rx_msg)

                        logging.error("Cycle Time: %r", cycle_time)

                        # buyOB
                        buy_rates = self.redis_get(cycle_time, "buy_rates")
                        buy_quantities = self.redis_get(cycle_time,
                                                   "buy_quantities")

                        assert buy_rates.size == buy_quantities.size

                        buyob = np.vstack((buy_rates, buy_quantities)).T
                        logging.debug('buyob:\n%r', buyob)

                        # sellOB
                        sell_rates = self.redis_get(cycle_time, "sell_rates")
                        sell_quantities = self.redis_get(cycle_time,
                                                    "sell_quantities")

                        assert sell_rates.size == sell_quantities.size

                        sellob = np.vstack((sell_rates, sell_quantities)).T
                        logging.debug('sellob:\n%r', sellob)

                        timer = Timer()
                        buy_rate, sell_rate = self.trader.compute_orders(
                            buyob, sellob)

                        logging.error(
                            "Elapsed Time: %d, best_buy: %14.8f, best_sell: %14.8f",
                            timer() * 1000, buy_rates[0], sell_rates[0])
                        logging.error(
                            "Elapsed Time: %d, buy_rate: %14.8f, sell_rate: %14.8f",
                            timer() * 1000, buy_rate, sell_rate)

        except StopIteration:
            assert False  # Must not be here

        except:
            assert False  # Must not be here


if __name__ == '__main__':

    logging.basicConfig(format='[%(levelname)-5s] %(message)s',
                        level=logging.INFO,
                        datefmt='')

    coloredlogs.install(level='DEBUG')

    co2 = Co2Validator()
    co2.run()

    print("That's All Folks")
