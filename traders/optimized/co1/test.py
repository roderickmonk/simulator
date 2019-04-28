
import optimized_co1

A = [1.,2.,3.,4.]
B = [10., 11., 12., 13,]

"""
    double const fee,
    double const QL,
    double const tick,
    double const updateThreshold,
    double const placeThreshold,
    vector<double> const &tuning1,
    vector<double> const &tuning2,
    vector<double> const &buy_rates,
    vector<double> const &buy_quantities,
    vector<double> const &sell_rates,
    vector<double> const &sell_quantities,
    double const incumbent_buy_rate,
    double const incumbent_buy_quantity,
    double const incumbent_sell_rate,
    double const incumbent_sell_quantity)
"""

C = optimized_co1.compute_orders(0.0,1.0,2.0,3.0,4.0,A,A,A,A,A,A,5.0,6.0,7.0,8.0)

print(C)

