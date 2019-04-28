import logging
import numpy as np
import timeit
import time
import os

try:
    profile
except NameError:
    profile = lambda x: x

class Trader:

    fee: float = None
    QL: float = None
    pdf_x: np.ndarray = None
    pdf_y: np.ndarray = None
    tick: float = None
    rate_precision: float = None

    def __init__(
            self,
            config: dict

    ) -> None:

        self.fee = config.partition_config["feeRate"]

        self.QL = config.partition_config["quantityLimit"]

        assert len(config.pdf_x) > 0
        assert len(config.pdf_y) > 0
        assert config.pdf_x.shape == config.pdf_y.shape

        self.pdf_x = config.pdf_x
        self.pdf_y = config.pdf_y

        self.tick = config.partition_config["tick"]
        self.rate_precision = config.rate_precision

    @profile
    def evol(self, pv: np.ndarray):
        """
        Calculate the expected volume for a potential order.

        The histrogam approximates the probability discreet distribution of
        market order (MO) sizes. For example, MOs of size = 0.1 BTC
        occur more frequently than those of size = 1.0 BTC.
        pv is the preceding volume; the amount of volume ahead of our order
        on the OB.
        """

        assert self.pdf_x.shape == self.pdf_y.shape

        ev = [np.sum(
            np.minimum(
                np.maximum(self.pdf_x - x, 0),
                self.QL,
            ) * self.pdf_y,
        ) for x in pv]

        return ev

    @profile
    def get_pv_and_rates(self, ob: np.ndarray, is_buy: bool):

        assert len(ob) > 0
        assert ob.shape[1] == 2

        r = np.around (np.array(ob[:,0] ), self.rate_precision)
        q = np.array(ob[:, 1])

        rates = r + self.tick if is_buy else r - self.tick
        rates = np.around (rates, self.rate_precision)
        pv = np.concatenate( ( [0], np.cumsum( (r * q) [slice(0,-1)])))
        index = np.where( np.logical_not( np.in1d( rates, r, assume_unique=False)))[0]

        return pv[index], rates[index]


    @profile
    def max_profit (
        self, 
        sell_ev: list, 
        sell_rates: np.ndarray, 
        buy_ev: list, 
        buy_rates: np.ndarray):

        logging.debug ("buy_rates: %r", buy_rates)
        logging.debug ("sell_rates: %r", sell_rates)

        max = 0.0
        buy_rate = -1.0
        sell_rate = -1.0

        for s in range(len(sell_ev)):
            for b in range (len(buy_ev)):
                expected_profit = \
                (sell_rates[s] * (1 - self.fee) - buy_rates[b] / (1 - self.fee)) * \
                    min(sell_ev[s], buy_ev[b])

                if expected_profit > max:
                    
                    max = expected_profit
                    buy_rate = buy_rates[b]
                    sell_rate = sell_rates[s]

                    logging.debug ("max: %3.10f, max buy: %3.10f, max sell: %3.10f", max, buy_rate, sell_rate)
                    logging.debug ("b: %d, s: %d", b, s)
                        
        return [             
            round(buy_rate,self.rate_precision),
            round(sell_rate,self.rate_precision)
        ]

    @profile
    def compute_orders2(
        self,
        buyob: np.ndarray,
        sellob: np.ndarray,
    ):

        start_time = time.time()

        try:

            buy_pv, buy_rates = self.get_pv_and_rates(ob=buyob, is_buy=True) 
            sell_pv, sell_rates = self.get_pv_and_rates(ob=sellob, is_buy=False)

            if __debug__:
                logging.debug('buy_pv:\n' + str(buy_pv))
                logging.debug('buy_rates:\n' + str(buy_rates))
                logging.debug('sell_pv:\n' + str(sell_pv))
                logging.debug('sell_rates:\n' + str(sell_rates))

            # an adjusted price for each order that factors in the fee

            buy_ev = self.evol(buy_pv)
            sell_ev = self.evol(sell_pv)

            return self.max_profit (
                sell_ev, 
                sell_rates,
                buy_ev,
                buy_rates)

        except Exception as err:

            logging.exception(err)
            raise

    @profile
    def compute_orders(
        self,
        buyob: np.ndarray,
        sellob: np.ndarray,
    ):

        start_time = time.time()

        try:

            buy_pv, buy_rates = self.get_pv_and_rates(ob=buyob, is_buy=True) 
            sell_pv, sell_rates = self.get_pv_and_rates(ob=sellob, is_buy=False)

            if __debug__:
                logging.debug('buy_pv:\n' + str(buy_pv))
                logging.debug('buy_rates:\n' + str(buy_rates))
                logging.debug('sell_pv:\n' + str(sell_pv))
                logging.debug('sell_rates:\n' + str(sell_rates))

            # an adjusted price for each order that factors in the fee

            buyrateswfee:  np.ndarray = buy_rates / (1 - self.fee)

            sellrateswfee = sell_rates * (1 - self.fee)

            logging.debug('buyrateswfee:\n%r', buyrateswfee)
            logging.debug('sellrateswfee:\n%r', sellrateswfee)

            buy_ev = self.evol(buy_pv)
            sell_ev = self.evol(sell_pv)

            logging.debug('buy_ev:\n%r', buy_ev)
            logging.debug('sell_ev:\n%r', sell_ev)

            # Get a matrix of the pairwise minimum 'evol' for all possible pairs
            # of orders. The idea is that the lower of the two values creates
            # a bottleneck, which is a notable innacuracy, though somewhat correct.
            minev = np.minimum.outer(buy_ev, sell_ev)

            # get a matrix of profit margins for all possible order pairs
            profit_margin = np.subtract.outer(
                sellrateswfee, buyrateswfee).transpose()

            # get the products of evol * (profit margin)
            expected_profit = minev * profit_margin

            logging.debug('minev:\n' + str(minev))
            logging.debug('profit_margin:\n' + str(profit_margin))
            logging.debug('expected_profit:\n' + str(expected_profit))
            logging.debug('expected_profit.max(): ' + str(expected_profit.max()))

            # If expected profit is <= 0, then cancel the current orders
            if expected_profit.max() <= 0:
                return -1, -1

            else:
                # select the best order pair(s)
                best_orders = np.array(
                    np.where(expected_profit == expected_profit.max())
                ).flatten()

                assert len(best_orders) >= 2
                assert len(buy_rates) >= best_orders[0]
                assert len(sell_rates) >= best_orders[1]

                buy_rate = buy_rates[best_orders[0]]
                sell_rate = sell_rates[best_orders[1]]

                return buy_rate.item(), sell_rate.item()

        except Exception as err:

            logging.exception(err)
            raise

        finally:
            pass
            rounded_end = ('{0:.8f}'.format(round(time.time()-start_time, 8)))

            if __debug__:
                logging.debug(
                    'Simulation Execution Time: %s seconds',
                    str(rounded_end))
