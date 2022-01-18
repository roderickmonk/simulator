#!/usr/bin/env python
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

import importlib
import logging
import os
import sys
from datetime import datetime

from sentient_util import logger
import numpy as np
from bson.objectid import ObjectId
from devtools import debug
from pymongo import MongoClient
from schema import SchemaError
from sentient_util.exceptions import InvalidConfiguration
from sentient_util.get_pdf import get_pdf
from sentient_util.matching_engine import MatchingEngine, MatchingEngineResult
from sentient_util.pydantic_config import SimEnv0_Config, Trade
from sentient_traders.config import TraderConfig

from config import PartitionConfig
from orderbooks import Orderbooks


def check(conf_schema, conf):
    try:
        conf_schema.validate(conf)
        return True
    except SchemaError:
        return False


def simulate():

    # matching_engine: Optional[MatchingEngine] = None
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

        logger.setup(fmt="[%(levelname)-5s] %(message)s")
        logger.set_log_level("INFO")

        logging.debug(f"simulate args: {sys.argv}")

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

        # Prep for local mongodb access
        assert os.environ["LOCALDB"], "LOCALDB Not Defined"
        local_mongo_client = MongoClient(os.environ["LOCALDB"])
        assert local_mongo_client, "Unable to Connect to Local MongoDB"
        local_sim_db = local_mongo_client["sim"]
        if local_sim_db == None:
            raise Exception("Unable to Connect to Local MongoDB")

        config = sim_db.partitions.find_one({"_id": partition_id})
        if config is None:
            raise RuntimeError("Unknown Trader Configuration")

        trader_config = TraderConfig(**config)
        matching_engine = MatchingEngine(SimEnv0_Config(**config))
        config = PartitionConfig(**config)

        logging.debug(f"{trader_config=}")
        logging.debug(f"{config=}")
        logging.error(f"{matching_engine=}")

        if __debug__:
            from sentient_traders.co1 import Trader

            trader = Trader(trader_config=trader_config)

        else:
            trader_module = importlib.import_module(config.trader.lower())

            if (trader_class := getattr(trader_module, "Trader")) is None:
                raise TypeError(f"Trader not found")

            trader = trader_class(trader_config=trader_config)

        try:
            orderbooks = Orderbooks(
                ob_collection=local_sim_db.orderbooks, **dict(config)
            )

        except StopIteration as msg:
            logging.critical(f"{msg=}")
            raise

        try:

            while True:

                orderbook = orderbooks.next()

                orderbook_id = orderbook["_id"]

                buy_trades = [Trade(**trade) for trade in orderbook["buy_trades"]]
                for t in buy_trades:
                    t.ob = ObjectId(t.ob)
                    t.ob_id = ObjectId(t.ob)

                assert all(
                    buy_trades[i].r >= buy_trades[i + 1].r
                    for i in range(len(buy_trades) - 1)
                ), "Buy Trades Not Sorted"
                buy_trades_count += len(buy_trades)
                logging.debug(f"{buy_trades=}")

                sell_trades = [Trade(**trade) for trade in orderbook["sell_trades"]]
                for t in sell_trades:
                    t.ob = ObjectId(t.ob)
                    t.ob_id = ObjectId(t.ob)
                assert all(
                    sell_trades[i].r <= sell_trades[i + 1].r
                    for i in range(len(sell_trades) - 1)
                ), "Sell Trades Not Sorted"
                sell_trades_count += len(sell_trades)
                logging.debug(f"{sell_trades=}")

                if __debug__ and (len(buy_trades) > 0 or len(sell_trades) > 0):
                    logging.debug(f"{len(buy_trades)=}")
                    logging.debug(f"{len(sell_trades)=}")

                CO_calls += 1

                buyob = (
                    Orderbooks.apply_depth(config.depth, orderbook["buy"])
                    if config.depth > 0
                    else orderbook["buy"]
                )

                sellob = (
                    Orderbooks.apply_depth(config.depth, orderbook["sell"])
                    if config.depth > 0
                    else orderbook["sell"]
                )

                buy_rate, sell_rate = trader.compute_orders(buyob=buyob, sellob=sellob)

                logging.debug(f"{buy_rate=}, {sell_rate=}")

                if buy_rate > 0 and sell_rate > 0:

                    matching_engine_calls += 1

                    matching = matching_engine.match(
                        orderbook_id=orderbook_id,
                        buy_rate=buy_rate,
                        sell_rate=sell_rate,
                        buy_trades=buy_trades,
                        sell_trades=sell_trades,
                    )
                    funds, inventory = matching_engine.assets()

                    if __debug__:
                        logging.debug(
                            f"compute_orders return: {buy_rate=}, {sell_rate=}"
                        )
                        logging.debug("buy_match: " + str(matching.buy_match))
                        logging.debug("sell_match: " + str(matching.sell_match))

                    buy_depth = sum(
                        map(lambda x: x[1] if x[0] > buy_rate else 0, buyob)
                    )

                    sell_depth = sum(
                        map(lambda x: x[1] if x[0] < sell_rate else 0, sellob)
                    )

                    matchings.append(
                        {
                            "runId": config.runId,
                            "simVersion": config.simVersion,
                            "e": config.envId,
                            "x": config.exchange,
                            "m": config.market,
                            "s": config.simId,
                            "p": partition_id,
                            "depth": config.depth,
                            "allowOrderConflicts": config.allowOrderConflicts,
                            "ob": orderbook["_id"],
                            "ts": datetime.now(),
                            "topBuy": orderbook["buy"][0][0],
                            "buyRate": buy_rate,
                            "buyCount": len(buy_trades),
                            "buyMatch": matching.buy_match.value,
                            "buyDepth": buy_depth,
                            "buys": list(map((lambda x: x.r), buy_trades))
                            if len(buy_trades) > 0
                            else None,
                            "topSell": orderbook["sell"][0][0],
                            "sellRate": sell_rate,
                            "sellCount": len(sell_trades),
                            "sellMatch": matching.sell_match.value,
                            "sellDepth": sell_depth,
                            "sells": list(map((lambda x: x.r), sell_trades))
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
