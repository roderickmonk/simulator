import numpy as np

# Calculate the expected volume for a potential order.
# 'mids' and 'density' provide the x and y values for a histogram.
# That histrogam approximates the probability distribution of
# Market order (MO) sizes. For example, MOs of size = 0.1 BTC
# occur more frequently than those of size = 1.0 BTC.
# pv is the preceding volume; the amount of volume ahead of our order
# on the OB.


def evol(pv, ql, mids, density):

    try:
        # First, we calculate how much volume of a MO will reach us,
        # depending on its size and how deep (the pv) our order is.
        mids = np.maximum(mids - pv, 0)
        # Apply the ql as the maximum size of a fill.
        mids = np.minimum(mids, ql)
        # Finaly, weight the fill sizes by the probabilities of the MO sizes,
        # and return the weighted mean.
        return(np.sum(mids * density))

    except Exception as err:

        print(err)
        logging.exception(err)


def compute_orders(buyob, sellob, ql, fees, mids, density):

    try:

        buyob = np.array(buyob)
        sellob = np.array(sellob)
        mids = np.array(mids)
        density = np.array(density)

        # mids is an array of midpoints for bars in a histogram.
        # The histogram is initialy in log10 scale, so here we transform back to linear scale.
        mids = [10**x for x in mids]

        # get some arrays that we need that hold:
        # prices for each potential order
        buyrates = buyob[:, 0] + 1e-8
        buypv = np.concatenate(([0], np.cumsum(buyob[:, 1][slice(0, -1)])))
        buyindex = np.where(np.logical_not(np.in1d(buyrates, buyob[:, 0])))[0]
        buyrates = buyrates[buyindex]
        # preceding volume for each potential order
        buypv = buypv[buyindex]
        # an adjusted price for each order that factors in the fee
        buyrateswfee = buyrates * (1 - fees)

        sellrates = sellob[:, 0] - 1e-8
        sellpv = np.concatenate(([0], np.cumsum(sellob[:, 1][slice(0, -1)])))
        sellindex = np.where(np.logical_not(
            np.in1d(sellrates, sellob[:, 0])))[0]
        sellrates = sellrates[sellindex]
        sellpv = sellpv[sellindex]
        sellrateswfee = sellrates * (1 - fees)

        # get evol for all possible orders
        buyev = [evol(x, ql, mids, density) for x in buypv]
        sellev = [evol(x, ql, mids, density) for x in sellpv]

        # get a matrix of the pairwise minimum 'evol' for all possible pairs
        # of orders. The idea is that the lower value of the two values creates a
        # bottleneck, which is a notable innacuracy, though somewhat correct.
        minev = np.minimum.outer(buyev, sellev)
        # get a matrix of profit margins for all possible order pairs
        profmarg = np.subtract.outer(sellrateswfee, buyrateswfee)
        profmarg = profmarg.transpose()
        # get the products of evol * (profit margin)
        expprofit = minev * profmarg

        # select the best order pair(s).
        bestorders = np.array(np.where(expprofit == expprofit.max())).flatten()
        return[0, None, buyrates[bestorders[0]], sellrates[bestorders[1]]]

    except Exception as err:

        print(err)
        logging.exception(err)
