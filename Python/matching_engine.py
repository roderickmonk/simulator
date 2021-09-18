import datetime
import logging
import math
from copy import copy
from datetime import datetime
from pprint import pprint
from typing import List

import numpy as np
from bson.objectid import ObjectId
from schema import And, Optional, Schema, SchemaError, Use

import sim_config
from match_result import MatchResult


class MatchingEngine:

    assets: np.array
    QL: float
    IL: float
    actual_fee_rate: float
    min_notional: float
    trades_collection = None
    buy_blocked_count: int = 0
    buy_no_trades_count: int = 0
    buy_notion_failure_count: int = 0
    buy_match_count: int = 0
    buy_unmatchable_count: int = 0
    sell_blocked_count: int = 0
    sell_match_count: int = 0
    sell_no_trades_count: int = 0
    sell_notion_failure_count: int = 0
    sell_unmatchable_count: int = 0

    sim_trades_idx: int = 0

    def __init__(
        self,
        *,
        QL: float,
        IL: float,
        assets: np.array([math.inf, 0], dtype=float),
        actual_fee_rate: float,
        min_notional=0.0005,
        trades_collection,
    ) -> None:

        assert QL > 0

        self.assets = assets
        self.QL = QL
        self.IL = IL
        self.actual_fee_rate = actual_fee_rate
        self.min_notional = min_notional
        self.trades_collection = trades_collection

    def __str__(self):
        return (
            str(self.__class__)
            + "\n"
            + "\n".join(
                ("{} = {}".format(item, self.__dict__[item]) for item in self.__dict__)
            )
        )

    def match(
        self,
        buy_rate: float,
        sell_rate: float,
        buy_trades: List[dict],
        sell_trades: List[dict],
    ):

        assert buy_rate > 0 and sell_rate > 0

        funds, inventory = self.assets

        if __debug__:
            logging.debug(
                "buy_rate: %f, sell_rate: %f, IL: %f", buy_rate, sell_rate, self.IL
            )

            logging.debug(
                "QL: %f funds: %f, IL - inventory * sell_rate: %f",
                self.QL,
                funds,
                self.IL - inventory * sell_rate,
            )

        # Take a snapshot of the assets;
        # both buy and sell sides start with the same snapshot
        start_assets = self.assets

        buy_result = self.buy(
            start_assets=start_assets,
            buy_rate=buy_rate,
            sell_rate=sell_rate,
            sell_trades=sell_trades,
        )

        sell_result = self.sell(
            start_assets=start_assets,
            sell_rate=sell_rate,
            buy_trades=buy_trades,
        )

        if buy_result == MatchResult.MATCHED or sell_result == MatchResult.MATCHED:

            _, i = self.assets
            logging.debug("i: %4.16f, b: %4.16f, s: %4.16f", i, buy_rate, sell_rate)

        return buy_result, sell_result

    def buy(
        self,
        start_assets: np.array,
        buy_rate: float,
        sell_rate: float,
        sell_trades: List[dict],
    ) -> MatchResult:

        if len(sell_trades) == 0:
            self.buy_no_trades_count += 1
            return MatchResult.NO_TRADES

        logging.debug("sell_trades: %r", sell_trades)

        funds, inventory = start_assets

        ceiling = self.IL - inventory * sell_rate
        quantity = min(self.QL, funds, ceiling)

        """
        if __debug__:
            logging.debug ('ceiling: ' + str(ceiling))
            logging.debug ('quantity: ' + str(quantity))
        """

        if ceiling <= 0 or funds <= 0:

            self.buy_blocked_count += 1
            return MatchResult.BLOCKED

        matched = False
        match = 0

        for trade in sell_trades:

            # Trades higher than rate are ignored
            if trade["r"] <= buy_rate:

                if quantity <= 0:
                    if matched:
                        return MatchResult.MATCHED
                    else:
                        self.buy_unmatchable_count += 1
                        return MatchResult.UNMATCHABLE

                base = min(quantity, trade["q"] * buy_rate)
                quote = base / buy_rate
                fee = quote * self.actual_fee_rate
                notion = quote * buy_rate

                # Ensure we meet the min notional
                if notion < self.min_notional:
                    if matched:
                        return MatchResult.MATCHED
                    else:
                        self.buy_notion_failure_count += 1
                        return MatchResult.MIN_NOTIONAL_FAILURE

                if __debug__:
                    logging.debug(
                        "actual_fee_rate: %f, base: %f, quote: %f, fee: %f",
                        self.actual_fee_rate,
                        base,
                        quote,
                        fee,
                    )

                logging.debug("BUY  r: %f, q: %f", buy_rate, quote)
                matched = True
                self.buy_match_count += 1

                self.trades_collection.insert_one(
                    {
                        "runId": sim_config.partition_config["runId"],
                        "simVersion": sim_config.partition_config["simVersion"],
                        "s": sim_config.partition_config["simId"],
                        "p": sim_config.partition_config["_id"],
                        "ceiling": ceiling,
                        "idx": self.sim_trades_idx,
                        "match": match,
                        "ts": datetime.now(),
                        "buy": True,
                        "o": sim_config.orderbook_id,
                        "t": trade["_id"],
                        "quantity": quantity,
                        "r": buy_rate,
                        "q": quote,
                        "b": -base,
                        "buyFee": fee,
                        "historyTrade": trade,
                    }
                )

                quantity -= base
                self.assets += [-base, quote - fee]

                self.sim_trades_idx += 1
                match += 1

        if matched:
            return MatchResult.MATCHED
        else:
            self.buy_unmatchable_count += 1
            return MatchResult.UNMATCHABLE

    def sell(
        self,
        start_assets: np.array,
        sell_rate: float,
        buy_trades: List[dict],
    ) -> MatchResult:

        _, inventory = start_assets

        if len(buy_trades) == 0:

            self.sell_no_trades_count += 1
            return MatchResult.NO_TRADES

        quantity = min(self.QL, inventory * sell_rate)

        if quantity <= 0:
            self.sell_blocked_count += 1
            return MatchResult.BLOCKED

        matched = False
        match = 0

        for trade in buy_trades:

            # Trades lower than rate are ignored
            if trade["r"] >= sell_rate:

                if quantity <= 0:

                    if matched:
                        return MatchResult.MATCHED

                    else:
                        self.sell_unmatchable_count += 1
                        return MatchResult.UNMATCHABLE

                base = min(quantity, trade["q"] * sell_rate)
                quote = base / sell_rate
                fee = base * self.actual_fee_rate
                notion = quote * sell_rate

                """
                if __debug__:
                    logging.debug(f'base: {base}, quote: {quote}, fee: {fee}, notion: {notion}')
                """

                # Ensure we meet the min notional
                if notion < self.min_notional:
                    if matched:
                        return MatchResult.MATCHED
                    else:
                        self.sell_notion_failure_count += 1
                        return MatchResult.MIN_NOTIONAL_FAILURE

                matched = True

                logging.debug("SELL r: %f, q: %f", sell_rate, quote)
                self.sell_match_count += 1

                logging.debug(
                    "SELL_TRADE: %2.8f, %s", sell_rate, sim_config.orderbook_id
                )

                self.trades_collection.insert_one(
                    {
                        "runId": sim_config.partition_config["runId"],
                        "simVersion": sim_config.partition_config["simVersion"],
                        "s": sim_config.partition_config["simId"],
                        "p": sim_config.partition_config["_id"],
                        "idx": self.sim_trades_idx,
                        "match": match,
                        "ts": datetime.now(),
                        "buy": False,
                        "o": sim_config.orderbook_id,
                        "t": trade["_id"],
                        "quantity": quantity,
                        "r": sell_rate,
                        "q": -quote,
                        "b": base,
                        "sellFee": fee,
                        "historyTrade": trade,
                    }
                )

                quantity -= base
                self.assets += [base - fee, -quote]

                self.sim_trades_idx += 1
                match += 1

        if matched:
            return MatchResult.MATCHED
        else:
            self.sell_unmatchable_count += 1
            return MatchResult.UNMATCHABLE
