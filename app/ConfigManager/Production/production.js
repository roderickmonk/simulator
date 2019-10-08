"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("../common");
exports.operationalTraders = [
    { trader: "@1057405bcltd/hft_trader_co1", },
    { trader: "@1057405bcltd/hft_trader_evol_a_cycler", },
    { trader: "@1057405bcltd/hft_trader_co1_pdepth1", }
];
exports.productionSchema = {
    properties: {
        _id: {},
        envId: {
            type: "number",
            default: 0,
        },
        allowOrderConflicts: {
            type: "boolean",
            default: false,
        },
        archiveCycles: {
            type: "boolean",
            default: false,
        },
        archiveCyclesExpiry: {
            type: "number",
            default: 345600,
        },
        baseCurrency: {
            type: "string"
        },
        exchange: {
            type: "string"
        },
        account: common_1.accountSchema,
        backstop: {
            type: "number",
            default: 3.0,
        },
        bittrex: {
            type: "object",
            properties: {
                cleanCancelledOrdersInterval: {
                    type: "number",
                    default: 500
                },
                cleanCancelledOrdersTimeout: {
                    type: "number",
                    default: 5000
                },
            },
            default: {},
        },
        depth: {
            type: "number",
            default: 3,
        },
        endpoint: {
            type: "string"
        },
        exchangeVersion: {
            type: "string",
            default: "3",
        },
        feeRate: {
            type: "number",
            default: 0.0025
        },
        hftName: {
            type: "string",
        },
        logging: common_1.loggingSchema,
        sellDelay: {
            type: "number",
            default: 0,
        },
        sellAttempts: {
            type: "number",
            default: 1,
        },
        monitorOpenOrdersInterval: {
            type: "number",
            default: 5000,
        },
        minNotional: {
            type: "number",
            default: 0.0005,
        },
        minQuantityLimitFactor: {
            type: "number",
            default: 0.0,
        },
        pollTradesInterval: {
            type: "number",
            default: undefined,
        },
        priceDepthLimit: {
            type: "number",
            default: 2.0,
            minimum: 1.0,
        },
        quantityLimit: {
            type: "number",
        },
        queueLengthTolerance: {
            type: "number",
            default: 3,
        },
        inventoryLimit: {
            type: "number",
        },
        inventoryLimitSide: {
            enum: ["Buy", "Sell"],
            default: "Buy",
        },
        restartInterval: {
            type: "number",
            default: 0,
        },
        trader: {
            enum: exports.operationalTraders.map(x => x.trader),
            default: exports.operationalTraders.map(x => x.trader)[0]
        },
        PDF: {
            type: "string",
            default: "pdf1"
        },
        alternateMarketTracking: {
            type: "object",
            properties: {
                exchange: {
                    type: "string",
                    default: "binance",
                },
                market: {
                    type: "string",
                },
                buy: {
                    type: "object",
                    properties: {
                        blockingWindow: {
                            type: "number",
                        },
                        blockingExpiry: {
                            type: "number",
                        },
                        blockingVolume: {
                            type: "number",
                        },
                    },
                    required: [
                        "blockingWindow",
                        "blockingExpiry",
                        "blockingVolume"
                    ]
                },
                sell: {
                    type: "object",
                    properties: {
                        blockingWindow: {
                            type: "number",
                        },
                        blockingExpiry: {
                            type: "number",
                        },
                        blockingVolume: {
                            type: "number",
                        },
                    },
                    required: [
                        "blockingWindow",
                        "blockingExpiry",
                        "blockingVolume"
                    ]
                },
            },
            required: ["exchange", "buy", "sell"]
        },
        x: {
            type: "string",
        },
        tuningGeneration: {
            properties: {
                priceDepthStart: {
                    type: "number",
                },
                priceDepthEnd: {
                    type: "number",
                },
                priceDepthSamples: {
                    type: "number",
                },
                depthStart: {
                    type: "number",
                },
                depthEnd: {
                    type: "number",
                },
                depthSamples: {
                    type: "number",
                },
                window: {
                    type: "number",
                },
                recalculationInterval: {
                    type: "number",
                }
            },
            required: []
        },
        bots: {},
        additionalProperties: false,
        required: [
            "_id",
            "allowOrderConflicts",
            "archiveCycles",
            "archiveCyclesExpiry",
            "depth",
            "exchange",
            "hftName",
            "inventoryLimitSide",
            "backstop",
            "baseCurrency",
            "endpoint",
            "feeRate",
            "sellDelay",
            "sellAttempts",
            "quantityLimit",
            "logging",
            "account",
            "minQuantityLimitFactor",
            "monitorOpenOrdersInterval",
            "priceDepthLimit",
            "restartInterval",
            "queueLengthTolerance",
            "trader",
            "x",
        ]
    }
};
exports.productionBotSchema = {
    properties: {
        allowOrderConflicts: {
            type: "boolean",
        },
        averagingWindow: {
            properties: {
                cycle: {
                    type: "number",
                    default: 1000
                },
                http: {
                    type: "number",
                    default: 1000
                },
                orderBookChange: {
                    type: "number",
                    default: 40
                }
            },
            default: {},
        },
        archiveCycles: {
            type: "boolean",
        },
        archiveTraceCycles: {
            type: "boolean",
            default: false,
        },
        backstop: {
            type: "number",
        },
        bitfinex: {
            commandStreamingEnabled: {
                type: "boolean",
                default: true
            },
            useWalletAvailableData: {
                type: "boolean",
                default: true
            }
        },
        feeRate: {
            type: "number",
        },
        sellDelay: {
            type: "number",
        },
        sellAttempts: {
            type: "number",
        },
        minTradeSize: {
            type: "number",
            default: 0,
        },
        mode: {
            enum: [
                "off",
                "active",
                "demo",
                "monitor",
                "testing",
                "addon-testing"
            ],
            default: "off"
        },
        debug: {
            type: "boolean",
            default: false,
        },
        cancelRetryInterval: {
            type: "number",
            default: 500
        },
        depth: {
            type: "number",
        },
        envId: {
            type: "number",
            default: 0,
        },
        quantityLimit: {
            type: "number"
        },
        hftSimTesting: {
            type: "boolean",
            default: false,
        },
        inventoryLimit: {
            type: "number"
        },
        inventoryLimitSide: {
            enum: ["Buy", "Sell"],
        },
        minimumOrderSize: {
            type: "number",
            default: 0
        },
        minimumTotal: {
            type: "number",
            default: 0
        },
        minNotional: {
            type: "number",
        },
        priceDepthLimit: {
            type: "number",
            minimum: 1.0,
        },
        pricePrecision: {
            type: "number",
        },
        trader: {
            enum: exports.operationalTraders.map(x => x.trader),
        },
        tick: {
            type: "number",
            default: 0.00000001
        },
        PDF: {
            type: "string"
        },
        poloniex: {
            type: "object",
            properties: {
                getBalancesInterval: {
                    type: "number"
                },
                getAvailablesInterval: {
                    type: "number"
                },
                getOpenOrdersInterval: {
                    type: "number"
                }
            },
            required: [
                "getBalancesInterval",
                "getAvailablesInterval",
                "getOpenOrdersInterval"
            ]
        },
        compareTestActive: {
            type: "boolean",
            default: false,
        },
        compareTestExpiry: {
            type: "number",
            default: 3 * 86400000,
        },
        alternateMarketTracking: {
            type: "object",
            properties: {
                active: {
                    type: "boolean",
                },
                exchange: {
                    type: "string",
                },
                market: {
                    type: "string",
                },
                buy: {
                    type: "object",
                    properties: {
                        blockingWindow: {
                            type: "number",
                        },
                        blockingExpiry: {
                            type: "number",
                        },
                        blockingVolume: {
                            type: "number",
                        },
                    },
                    required: [
                        "blockingWindow",
                        "blockingExpiry",
                        "blockingVolume"
                    ]
                },
                sell: {
                    type: "object",
                    properties: {
                        blockingWindow: {
                            type: "number",
                        },
                        blockingExpiry: {
                            type: "number",
                        },
                        blockingVolume: {
                            type: "number",
                        },
                    },
                    required: [
                        "blockingWindow",
                        "blockingExpiry",
                        "blockingVolume"
                    ]
                },
            },
            required: ["exchange", "buy", "sell"],
        },
        tuningGeneration: {
            properties: {
                priceDepthStart: {
                    type: "number",
                },
                priceDepthEnd: {
                    type: "number",
                },
                priceDepthSamples: {
                    type: "number",
                },
                depthStart: {
                    type: "number",
                },
                depthEnd: {
                    type: "number",
                },
                depthSamples: {
                    type: "number",
                },
                window: {
                    type: "number",
                },
                recalculationInterval: {
                    type: "number",
                }
            },
            required: [
                "priceDepthStart",
                "priceDepthEnd",
                "priceDepthSamples",
                "depthStart",
                "depthEnd",
                "depthSamples",
                "window",
                "recalculationInterval",
            ]
        },
    },
    additionalProperties: false,
    required: [
        "allowOrderConflicts",
        "archiveCycles",
        "compareTestActive",
        "compareTestExpiry",
        "depth",
        "envId",
        "feeRate",
        "averagingWindow",
        "archiveTraceCycles",
        "backstop",
        "sellDelay",
        "sellAttempts",
        "quantityLimit",
        "hftSimTesting",
        "inventoryLimit",
        "inventoryLimitSide",
        "minNotional",
        "minimumOrderSize",
        "minimumTotal",
        "minTradeSize",
        "priceDepthLimit",
        "pricePrecision",
        "trader",
    ]
};
