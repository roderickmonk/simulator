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
from timeit import default_timer as timer
from datetime import timedelta


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
        logging.debug("bot_id: %s", bot_id)

        bot_config = self.r.hgetall(bot_id.encode())
        logging.debug("bot_config: %r", bot_config)

        bot_config["quantityLimit"] = float(bot_config["quantityLimit"])
        bot_config["inventoryLimit"] = float(bot_config["quantityLimit"])
        bot_config["feeRate"] = float(bot_config["feeRate"])
        bot_config["tick"] = float(bot_config["tick"])
        bot_config["priceDepthLimit"] = float(bot_config["priceDepthLimit"])
        del bot_config["PDF"]
        del bot_config["tuningGeneration"]

        if bot_config["allowOrderConflicts"] == "true":
            bot_config["allowOrderConflicts"] = True
        else:
            bot_config["allowOrderConflicts"] = False

        s = json.dumps(bot_config, sort_keys=True, indent=4)
        logging.info("Bot Configuration\n" + s)

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

                        logging.debug("Cycle Time: %r", cycle_time)

                        # buyOB
                        buy_rates = self.redis_get(cycle_time, "buy_rates")
                        buy_quantities = self.redis_get(
                            cycle_time, "buy_quantities")

                        assert buy_rates.size == buy_quantities.size

                        buyob = np.vstack((buy_rates, buy_quantities)).T
                        logging.debug('buyob:\n%r', buyob)

                        # sellOB
                        sell_rates = self.redis_get(cycle_time, "sell_rates")
                        sell_quantities = self.redis_get(
                            cycle_time, "sell_quantities")

                        assert sell_rates.size == sell_quantities.size

                        sellob = np.vstack((sell_rates, sell_quantities)).T
                        logging.debug('sellob:\n%r', sellob)

                        timer = Timer()

                        buy_rate, sell_rate = self.trader.compute_orders(
                            buyob, sellob)

                        logging.info(
                            "Elapsed (msecs): %d\tBest Buy: %14.8f\tBest Sell: %14.8f\n"
                            +
                            "\t\t\t\t\t\t\tBuy Rate: %14.8f\tSell Rate: %14.8f",
                            timer() * 1000, buy_rates[0], sell_rates[0],
                            buy_rate, sell_rate)

                        # Compare PVs
                        buy_pv_ref = self.redis_get(cycle_time, "buy_pv")
                        sell_pv_ref = self.redis_get(cycle_time, "sell_pv")

                        if self.trader.buy_pv.size != buy_pv_ref.size:
                            logging.error("buy_pv: %r", self.trader.buy_pv)
                            logging.error("buy_pv_ref: %r", buy_pv_ref)
                            logging.error("buy_pv's Size Differ")
                            os._exit(0)

                        if self.trader.sell_pv.size != sell_pv_ref.size:
                            logging.error("sell_pv: %r", self.trader.buy_pv)
                            logging.error("sell_pv_ref: %r", sell_pv_ref)
                            logging.error("sell_pv's Size Differ")
                            os._exit(0)

                        assert self.trader.buy_pv.shape == buy_pv_ref.shape
                        assert self.trader.sell_pv.shape == sell_pv_ref.shape
                        assert self.trader.buy_pv.dtype == buy_pv_ref.dtype
                        assert self.trader.sell_pv.dtype == sell_pv_ref.dtype

                        if not np.allclose(self.trader.buy_pv, buy_pv_ref, atol=0.000000005):
                            logging.error("buy_pv: %r", self.trader.buy_pv)
                            logging.error("buy_pv_ref: %r", buy_pv_ref)
                            logging.error("buy_pv != buy_pv_ref")
                            os._exit(0)

                        if not np.allclose(self.trader.sell_pv, sell_pv_ref, atol=0.000000005):
                            logging.error("sell_pv: %r", self.trader.sell_pv)
                            logging.error("sell_pv_ref: %r", sell_pv_ref)
                            logging.error("sell_pv != sell_pv_ref")
                            os._exit(0)

                        # Compare EVs
                        buy_ev_ref = self.redis_get(cycle_time, "buy_ev")
                        sell_ev_ref = self.redis_get(cycle_time, "sell_ev")

                        EVs_identical = \
                            self.trader.buy_ev.size == buy_ev_ref.size and \
                            self.trader.sell_ev.size == sell_ev_ref.size and \
                            self.trader.buy_ev.shape == buy_ev_ref.shape and \
                            self.trader.sell_ev.shape == sell_ev_ref.shape and \
                            self.trader.buy_ev.dtype == buy_ev_ref.dtype and \
                            self.trader.sell_ev.dtype == sell_ev_ref.dtype and \
                            np.allclose(self.trader.buy_ev, buy_ev_ref, atol=0.000000005) and \
                            np.allclose(self.trader.sell_ev,
                                        sell_ev_ref, atol=0.000000005)

                        if not EVs_identical:

                            logging.error("EVs Not Identical")
                            logging.error("buy_ev: %r", self.trader.buy_ev)
                            logging.error("buy_ev_ref: %r", buy_ev_ref)
                            logging.error("sell_ev: %r", self.trader.sell_ev)
                            logging.error("sell_ev_ref: %r", sell_ev_ref)
                            os._exit(0)

        except StopIteration:
            assert False  # Must not be here

        except:
            assert False  # Must not be here


if __name__ == '__main__':

    logging.basicConfig(format='[%(levelname)-5s] %(message)s',
                        level=logging.INFO,
                        datefmt='')

    coloredlogs.install(level='INFO',
                        milliseconds=True,
                        fmt='%(asctime)s [%(levelname)-5s] %(message)s')

    co2 = Co2Validator()
    co2.run()

    print("That's All Folks")
