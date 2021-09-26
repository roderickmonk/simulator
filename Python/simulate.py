#!/usr/bin/env python
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

import importlib
import logging
import math
import operator
import os
import sys
from copy import copy
from datetime import datetime

import numpy as np
from bson.objectid import ObjectId
from common_sentient.get_pdf import get_pdf
from pymongo import MongoClient
from schema import SchemaError

import common_sentient.sim_config as sim_config
from common_sentient.matching_engine import MatchingEngine
from common_sentient.exceptions import InvalidConfiguration
from orderbooks import Orderbooks


def check(conf_schema, conf):
    try:
        conf_schema.validate(conf)
        return True
    except SchemaError:
        return False


def find_trades(trades, filter):

    buy_trades = []
    sell_trades = []

    for trade in trades.find(filter=filter, no_cursor_timeout=True):

        # Corral the buy and sell trades into separate lists
        if trade["buy"]:
            buy_trades.append(trade)
        else:
            sell_trades.append(trade)

        # Sort the buy trades into decreasing order
        buy_trades.sort(key=operator.itemgetter("r"), reverse=True)

        # Sort the sell trades into increasing order
        sell_trades.sort(key=operator.itemgetter("r"), reverse=False)

    return buy_trades, sell_trades


def simulate():

    matching_engine: MatchingEngine = None
    CO_calls = 0
    matching_engine_calls = 0
    returncode = 0

    buy_trades_count = 0
    sell_trades_count = 0

    matchings = []

    try:

        logging.basicConfig(
            format="[%(levelname)-5s] %(message)s", level=logging.INFO, datefmt=""
        )

        logging.info(f"simulate args: {sys.argv}")

        if len(sys.argv) == 2:
            partition_id = ObjectId(sys.argv[1])
        else:
            assert False, "Usage: simulate <Simulation ObjectId>"

        logging.debug("partition_id: " + str(partition_id))

        # Prep for remote mongodb access
        assert os.environ["MONGODB"], "MONGODB Not Defined"
        remote_mongo_client = MongoClient(os.environ["MONGODB"])

        assert os.environ["SIMULATOR_DB"], "SIMULATOR_DB Not Defined"
        sim_db = remote_mongo_client[os.environ["SIMULATOR_DB"]]

        sim_config.sim_configuration_db = remote_mongo_client["sim_configuration"]

        # Prep for local mongodb access
        assert os.environ["LOCALDB"], "LOCALDB Not Defined"
        local_mongo_client = MongoClient(os.environ["LOCALDB"])
        assert local_mongo_client, "Unable to Connect to Local MongoDB"
        local_sim_db = local_mongo_client["sim"]
        if local_sim_db == None:
            raise Exception("Unable to Connect to Local MongoDB")

        sim_config.partition_config = sim_db.partitions.find_one(
            {"_id": partition_id}
        )
        assert sim_config.partition_config, "Unknown Trader Configuration"

        logging.debug(
            "sim_config.partition_config:\n" + str(sim_config.partition_config)
        )

        # Convenience destructuring
        depth = sim_config.partition_config["depth"]

        pdf = get_pdf(
            remote_mongo_client["sim_configuration"]["tunings"],
            sim_config.partition_config["pdf"],
        )

        trader_config = dict(
            (key, sim_config.partition_config[key])
            for key in [
                "allowOrderConflicts",
                "feeRate",
                "quantityLimit",
                "precision",
            ]
        ) | {"pdf": pdf}

        logging.info(f"{trader_config=}")

        if "minNotional" not in sim_config.partition_config:
            raise InvalidConfiguration("Min Notional Missing")

        # Matching Engine
        matching_engine = MatchingEngine(
            assets=np.array([math.inf, 0], dtype=float),
            QL=sim_config.partition_config["quantityLimit"],
            IL=sim_config.partition_config["inventoryLimit"],
            actual_fee_rate=sim_config.partition_config["actualFeeRate"],
            min_notional=sim_config.partition_config["minNotional"],
            trades_collection=sim_db.trades,
        )

        sim_config.init(sim_config.partition_config)

        if __debug__:
            from traders.co1 import Trader
            trader = Trader(trader_config)
        else:
            trader = importlib.import_module(
                sim_config.partition_config["trader"].lower()
            ).Trader(trader_config)

        try:
            orderbooks = Orderbooks(
                ob_collection=local_sim_db.orderbooks,
                envId=sim_config.partition_config["envId"],
                exchange=sim_config.partition_config["exchange"].lower(),
                market=sim_config.partition_config["market"].lower(),
                depth=sim_config.partition_config["depth"],
                start=sim_config.partition_config["startTime"],
                end=sim_config.partition_config["endTime"],
            )

        except StopIteration:
            os._exit(0)

        try:

            while True:

                orderbook = orderbooks.next()

                sim_config.orderbook_id = orderbook["_id"]

                buy_trades = orderbook["buy_trades"]
                assert all(
                    buy_trades[i]["r"] >= buy_trades[i + 1]["r"]
                    for i in range(len(buy_trades) - 1)
                ), "Buy Trades Not Sorted"
                buy_trades_count += len(buy_trades)

                sell_trades = orderbook["sell_trades"]
                assert all(
                    sell_trades[i]["r"] <= sell_trades[i + 1]["r"]
                    for i in range(len(sell_trades) - 1)
                ), "Sell Trades Not Sorted"
                sell_trades_count += len(sell_trades)

                assert len(buy_trades) > 0 or len(sell_trades) > 0

                if __debug__ and (len(buy_trades) > 0 or len(sell_trades) > 0):
                    logging.debug("len(buy_trades): " + str(len(buy_trades)))
                    logging.debug("len(sell_trades): " + str(len(sell_trades)))

                CO_calls += 1

                buyob = (
                    Orderbooks.apply_depth(depth, orderbook["buy"])
                    if depth > 0
                    else orderbook["buy"]
                )

                sellob = (
                    Orderbooks.apply_depth(depth, orderbook["sell"])
                    if depth > 0
                    else orderbook["sell"]
                )

                result = trader.compute_orders(buyob=buyob, sellob=sellob)

                buy_rate, sell_rate = result

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
                        logging.debug(f"compute_orders return: {result}")
                        logging.debug("buy_match: " + str(buy_match))
                        logging.debug("sell_match: " + str(sell_match))

                    buy_depth = sum(
                        map(lambda x: x[1] if x[0] > buy_rate else 0, buyob)
                    )

                    sell_depth = sum(
                        map(lambda x: x[1] if x[0] < sell_rate else 0, sellob)
                    )

                    matchings.append(
                        {
                            "runId": sim_config.partition_config["runId"],
                            "simVersion": sim_config.partition_config["simVersion"],
                            "e": sim_config.partition_config["envId"],
                            "x": sim_config.partition_config["exchange"],
                            "m": sim_config.partition_config["market"],
                            "s": sim_config.partition_config["simId"],
                            "p": sim_config.partition_config["_id"],
                            "depth": sim_config.partition_config["depth"],
                            "allowOrderConflicts": sim_config.partition_config[
                                "allowOrderConflicts"
                            ],
                            "ob": orderbook["_id"],
                            "ts": datetime.now(),
                            "topBuy": orderbook["buy"][0][0],
                            "buyRate": buy_rate,
                            "buyCount": len(buy_trades),
                            "buyMatch": str(buy_match).split(".")[1],
                            "buyDepth": buy_depth,
                            "buys": list(map((lambda x: x["r"]), buy_trades))
                            if len(buy_trades) > 0
                            else None,
                            "topSell": orderbook["sell"][0][0],
                            "sellRate": sell_rate,
                            "sellCount": len(sell_trades),
                            "sellMatch": str(sell_match).split(".")[1],
                            "sellDepth": sell_depth,
                            "sells": list(map((lambda x: x["r"]), sell_trades))
                            if len(sell_trades) > 0
                            else None,
                            "funds": funds,
                            "inventory": inventory,
                        }
                    )

        except StopIteration:
            # logging.info('StopIteration Detected')

            # Send the matchings to the database
            if len(matchings) > 0:
                sim_db.matchings.insert_many(matchings)

            logging.info("{0:24}{1:8d}".format("CO Calls:", CO_calls))
            logging.info(
                "{0:24}{1:8d}".format("Matching Engine Calls:", matching_engine_calls)
            )

            logging.info("BUY SUMMARY")
            logging.info(
                "    {0:20}{1:8d}".format("Matches:", matching_engine.buy_match_count)
            )
            logging.info(
                "    {0:20}{1:8d}".format("Blocked:", matching_engine.buy_blocked_count)
            )
            logging.info(
                "    {0:20}{1:8d}".format(
                    "No Trades", matching_engine.buy_no_trades_count
                )
            )
            logging.info(
                "    {0:20}{1:8d}".format(
                    "Unmatchable:", matching_engine.buy_unmatchable_count
                )
            )
            logging.info(
                "    {0:20}{1:8d}".format(
                    "Notion Failures:", matching_engine.buy_notion_failure_count
                )
            )

            logging.info("SELL SUMMARY")
            logging.info(
                "    {0:20}{1:8d}".format("Matches:", matching_engine.sell_match_count)
            )
            logging.info(
                "    {0:20}{1:8d}".format(
                    "Blocked:", matching_engine.sell_blocked_count
                )
            )
            logging.info(
                "    {0:20}{1:8d}".format(
                    "No Trades:", matching_engine.sell_no_trades_count
                )
            )
            logging.info(
                "    {0:20}{1:8d}".format(
                    "Unmatchable:", matching_engine.sell_unmatchable_count
                )
            )
            logging.info(
                "    {0:20}{1:8d}".format(
                    "Notion Failures:", matching_engine.sell_notion_failure_count
                )
            )

            returncode = 0

        except KeyError as err:
            logging.error("KeyError Detected: %r", err)
            logging.exception("Exception: %s", err)
            returncode = 1

    except Exception as err:
        logging.exception("Exception: %s", err)

    except:
        logging.exception("Unknown Exception")

    finally:

        """
        # Send the matchings to the database
        if len (matchings) > 0:
            sim_db.matchings.insert_many(matchings)

        logging.info(
             "{0:24}{1:8d}".format("CO Calls:", CO_calls))
        logging.info(
             "{0:24}{1:8d}".format("Matching Engine Calls:", matching_engine_calls))

        logging.info("BUY SUMMARY")
        logging.info(
            "    {0:20}{1:8d}".format("Matches:", matching_engine.buy_match_count))
        logging.info(
            "    {0:20}{1:8d}".format("Blocked:", matching_engine.buy_blocked_count))
        logging.info(
            "    {0:20}{1:8d}".format("No Trades", matching_engine.buy_no_trades_count))
        logging.info(
            "    {0:20}{1:8d}".format("Unmatchable:", matching_engine.buy_unmatchable_count))
        logging.info(
            "    {0:20}{1:8d}".format("Notion Failures:",matching_engine.buy_notion_failure_count))

        logging.info("SELL SUMMARY")
        logging.info(
            "    {0:20}{1:8d}".format("Matches:", matching_engine.sell_match_count))
        logging.info(
            "    {0:20}{1:8d}".format("Blocked:", matching_engine.sell_blocked_count))
        logging.info(
            "    {0:20}{1:8d}".format("No Trades:", matching_engine.sell_no_trades_count))
        logging.info(
            "    {0:20}{1:8d}".format("Unmatchable:", matching_engine.sell_unmatchable_count))
        logging.info(
            "    {0:20}{1:8d}".format("Notion Failures:", matching_engine.sell_notion_failure_count))
        """

        sys.exit(returncode)


if __name__ == "__main__":
    simulate()
