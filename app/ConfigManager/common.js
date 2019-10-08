"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.accountSchema = {
    type: "object",
    properties: {
        accountName: {
            type: "string"
        },
        apiKey: {
            type: "string"
        },
        apiSecret: {
            type: "string"
        }
    },
    required: ["accountName", "apiKey", "apiSecret"]
};
exports.loggingSchema = {
    type: "object",
    properties: {
        console: {
            type: "object",
            properties: {
                enable: {
                    type: "boolean",
                    default: true,
                },
                logLevel: {
                    enum: [
                        "trace",
                        "debug",
                        "info",
                        "warn",
                        "error",
                        "fatal"
                    ],
                    default: "fatal"
                },
            },
            default: {}
        },
        cloudWatch: {
            type: "object",
            properties: {
                enable: {
                    type: "boolean",
                    default: true
                },
                logLevel: {
                    enum: [
                        "trace",
                        "debug",
                        "info",
                        "warn",
                        "error",
                        "fatal"
                    ],
                    default: "info"
                },
            },
            default: {},
            required: [
                "enable",
                "logLevel",
            ]
        }
    },
    default: {}
};
