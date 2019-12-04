import sys
import os
import argparse
import pymongo
from pymongo import MongoClient
import coloredlogs
import logging


def formatOutput(detail):

    market = detail["m"]

    buyCancel = detail["buyCancel"] if "buyCancel" in detail else 0
    buyRoutine = detail["buyRoutine"] if "buyRoutine" in detail else 0
    buyInteresting = detail["buyInteresting"] if "buyInteresting" in detail else 0

    sellCancel = detail["sellCancel"] if "sellCancel" in detail else 0
    sellRoutine = detail["sellRoutine"] if "sellRoutine" in detail else 0
    sellInteresting = detail["sellInteresting"] if "sellInteresting" in detail else 0

    buyCancel = 1
    totalBuy = buyCancel + buyRoutine + buyInteresting
    totalSell = sellCancel + sellRoutine + sellInteresting

    buyCancelRatio = buyRoutineRatio = buyInterestingRatio = ""
    if totalBuy != 0:
        buyCancelRatio = f"({float(buyCancel)/float(totalBuy):>3.2f})"
        buyRoutineRatio = f"({float(buyRoutine)/float(totalBuy):>3.2f})"
        buyInterestingRatio = f"({float(buyInteresting)/float(totalBuy):>3.2f})"

    sellCancelRatio = sellRoutineRatio = sellInterestingRatio = ""
    if totalSell != 0:
        sellCancelRatio = f"({float(sellCancel)/float(totalSell):>3.2f})"
        sellRoutineRatio = f"({float(sellRoutine)/float(totalSell):>3.2f})"
        sellInterestingRatio = f"({float(sellInteresting)/float(totalSell):>3.2f})"

    logging.critical(f"{market:<12}{'Buy':>12}{'Sell':>24}")
    logging.warning(
        f"Cancel:      {buyCancel:7d} {buyCancelRatio}\t\t{sellCancel:7d} {sellCancelRatio}")
    logging.warning(
        f"Routine:     {buyRoutine:7d} {buyRoutineRatio}\t\t{sellRoutine:7d} {sellRoutineRatio}")
    logging.warning(
        f"Interesting: {buyInteresting:7d} {buyInterestingRatio}\t\t{sellInteresting:7d} {sellInterestingRatio}")
    logging.warning(
        f"TOTALS:      {totalBuy:7d}\t\t\t{totalSell:7d}")


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG, format="%(message)s")
    coloredlogs.install(fmt="%(message)s")

    parser = argparse.ArgumentParser(
        description='Simple Trade Quality Statistics')
    parser.add_argument("-market", type=str, default=None,
                        help="Define Market of interest (format 'base-quote`)")
    parser.add_argument("-clear", type=bool, default=False,
                        help="Clear statistics for the selected market")
    parser.add_argument("-all", default=False, type=bool,
                        help="Display All Markets")

    args = parser.parse_args()
    market = args.market
    logging.debug(f"market: {market}")
    clear = args.clear
    logging.debug(f"clear: {clear}")
    all = args.all
    logging.debug(f"all: {all}")

    if not market and not all:
        parser.error("Either -m <market> or -all required, but not both")
        os._exit(1)
    elif market and all:
        parser.error("Either -m <market> or -all required, but not both")

    mongodb = MongoClient(
        "mongodb://1057405bcltd:bittrex@3.11.7.67:15165/admin")
    history_db = mongodb["history"]

    if market and not clear:

        detail = history_db["trade-quality"].find_one(
            {"e": 0, "x": "bittrex", "m": market})

        formatOutput(detail)

    elif market and clear:
        history_db["trade-quality"].delete_one(
            {"e": 0, "x": "bittrex", "m": market})

        logging.error(f"Market {market} Removed from trade-quality")

    elif all and clear:

        history_db["trade-quality"].delete_many({})

        logging.error(f"All trade-quality documents removed")


    elif all:
    
        details = list(history_db["trade-quality"].find().sort([("m", pymongo.ASCENDING)]))

        for detail in details:
            formatOutput(detail)
            logging.info ("-----------------------------------------------------------")

    else:
        logging.error("Software Anomaly")
