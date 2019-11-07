import numpy as np
from operator import sub
import os
from pymongo import MongoClient
from bson.objectid import ObjectId
import sys
import logging
from datetime import datetime, timedelta
import math
import pprint


def compareVector(x, y) -> bool:
    if len(x) != len(y):
        return False
    for x1, y1 in zip(x, y):
        if not math.isclose(x1, y1):
            return False
    return True


def compare2D(x, y, trace=False) -> bool:
    if len(x) != len(y):
        return False
    for i, (x1, y1) in enumerate(zip(x, y)):
        if len(x1) != len(y1):
            logging.error(f"\tLengths of Row {i} Differ")

            if trace:
                logging.error(f"R  row[{i}]: \t{x1}")
                logging.error(f"PY row[{i}]: \t{y1}")

            return False

        for j, (x2, y2) in enumerate(zip(x1, y1)):
            if not math.isclose(x2, y2):

                if trace:
                    logging.error(f"\tR  row[{i}]: \t{x1}")
                    logging.error(f"\tPY row[{i}]: \t{y1}")

                logging.error(f"\t[{i},{j}] Differs, ({x2} != {y2})")
                return False
    return True


if __name__ == '__main__':

    logging.basicConfig(format='%(message)s', level=logging.ERROR, datefmt='')

    logging.debug(f'sys.argv: {sys.argv}')

    assert os.environ['MONGODB'], 'MONGODB Not Defined'
    mongodb = MongoClient(os.environ['MONGODB'])
    config_db = mongodb["configuration"]

    r_tuning = mongodb["QA"].reference_tunings.find_one(filter={
        "name": sys.argv[1],
    })
    logging.debug("QA.tuning Keys:\n%r", list(r_tuning.keys()))

    py_tuning = mongodb["configuration"].tuning.find_one(filter={
        "name": sys.argv[1],
    })
    logging.debug("configuration.tuning Keys:\n%r", list(py_tuning.keys()))

    if not set(r_tuning.keys()) == set(py_tuning.keys()):
        logging.error("Keys Differ")
        os._exit(1)

    # depths
    logging.debug(r_tuning["depths"])
    logging.debug(py_tuning["depths"])
    assert len(r_tuning["depths"]) == len(
        py_tuning["depths"]), "len(depths) differs"
    assert compareVector(r_tuning["depths"], py_tuning["depths"])

    # price_depths
    assert len(r_tuning["price_depths"]) == len(
        py_tuning["price_depths"]), "len(price_depths) differs"
    assert compareVector(r_tuning["price_depths"], py_tuning["price_depths"])

    key = "values"
    logging.error(f'{key}:')
    if not len(r_tuning[key]) == len(py_tuning[key]):
        logging.critical(f'\tLength Differ')
    elif compare2D(r_tuning[key], py_tuning[key]):
        logging.critical(f'\tIdentical')

    """
    key = "remainingDepths"
    if key in r_tuning and key in py_tuning:
            logging.error(f'{key}:')
        if not len(r_tuning[key]) == len(py_tuning[key]):
            logging.critical(f'\tLength Differ')
        elif compare2D(r_tuning[key], py_tuning[key]):
            logging.critical(f'\tIdentical')

    key = "remainingPriceDepths"
    logging.error(f'{key}:')
    if not len(r_tuning[key]) == len(py_tuning[key]):
        logging.critical(f'\tLengths Differ')
    elif compare2D(r_tuning[key], py_tuning[key]):
        logging.critical(f'\tIdentical')

    key = "metaRemainingVolumes"
    logging.error(f'{key}:')
    # pprint.pprint ((r_tuning[key], py_tuning[key]))
    if not len(r_tuning[key]) == len(py_tuning[key]):
        logging.critical(f'\tLengths Differ')
    elif compare2D(r_tuning[key], py_tuning[key], True):
        logging.critical(f'\tIdentical')

    key = "metaPriceDepths"
    logging.error(f'{key}:')
    if not len(r_tuning[key]) == len(py_tuning[key]):
        logging.critical(f'\tLengths Differ')
    elif compare2D(r_tuning[key], py_tuning[key]):
        logging.critical(f'\tIdentical')
    """


    print("That's All Folks")
