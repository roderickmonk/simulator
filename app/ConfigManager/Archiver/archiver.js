"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("../common");
exports.archiverSchema = {
    properties: {
        _id: {},
        envId: {
            type: "number",
        },
        exchange: {
            type: "string"
        },
        exchangeVersion: {
            type: "string",
            default: "3",
        },
        baseCurrency: {
            type: "string"
        }, archiveOrderbooks: {
            type: "boolean",
        },
        archiveTrades: {
            type: "boolean",
        },
        orderbookSnapshotInterval: {
            type: "number",
            default: 3600000,
        },
        account: common_1.accountSchema,
        hftName: {
            type: "string",
        },
        logging: common_1.loggingSchema,
        pollTradesInterval: {
            type: "number",
            default: undefined,
        },
        queueLengthTolerance: {
            type: "number",
            default: 3,
        },
        pollingInterval: {
            type: "number",
            default: 2000,
        },
        restartInterval: {
            type: "number",
            default: 0,
        },
        x: {
            type: "string",
        },
        bots: {},
    },
    additionalProperties: false,
    required: [
        "envId",
        "archiveOrderbooks",
        "orderbookSnapshotInterval",
        "archiveTrades",
        "exchange",
        "hftName",
        "baseCurrency",
        "logging",
        "account",
        "pollingInterval",
        "restartInterval",
        "queueLengthTolerance",
        "x",
    ]
};
exports.archiverBotSchema = {
    properties: {
        archiveOrderbooks: {
            type: "boolean",
        },
        archiveTrades: {
            type: "boolean",
        },
        debug: {
            type: "boolean",
            default: false,
        },
        pollingInterval: {
            type: "number",
        },
    },
    additionalProperties: false,
    required: [
        "archiveOrderbooks",
        "archiveTrades",
        "pollingInterval",
    ]
};
