#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <vector>
#include "HftEngine.h"
#include <pybind11/iostream.h>

using namespace std;

// ----------------
// Regular C++ code
// ----------------
// status, explain, buy_rate, sell_rate
tuple<double, double> compute_orders(
    double &fee,
    double &QL,
    double &tick,
    vector<double> &tuning1,
    vector<double> &tuning2,
    vector<double> &buy_rates,
    vector<double> &buy_quantities,
    vector<double> &sell_rates,
    vector<double> &sell_quantities)
{

    vector<Order> buyEntries = {};
    for (uint i = 0; i < buy_rates.size(); ++i)
        buyEntries.push_back(make_pair(buy_rates[i], buy_quantities[i]));

    vector<Order> sellEntries = {};
    for (uint i = 0; i < sell_rates.size(); ++i)
        sellEntries.push_back(make_pair(sell_rates[i], sell_quantities[i]));

    OrderBook orderbook = make_pair(buyEntries, sellEntries);

    Order nullIncumbent = make_pair(0.0, 0.0);

    double dummy = 0.0;

    NextOrders nextOrders = ComputeOrders(
        fee,
        QL,
        tick,
        dummy,
        dummy,
        tuning1,
        tuning2,
        orderbook,
        nullIncumbent,
        nullIncumbent);

    return make_pair(get<0>(nextOrders), get<1>(nextOrders));
}

// ----------------
// Python interface
// ----------------

namespace py = pybind11;

PYBIND11_MODULE(optimized_co1, m)
{
    m.doc() = "pybind11 optimized_co1 plugin";
    m.def("compute_orders", &compute_orders, "Compute next buy/sell_rates");
}
