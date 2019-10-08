"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const validator_1 = require("./validator");
const Ajv = require('ajv');
const schema = {
    "type": "object",
    "properties": {
        "console": {
            "type": "object",
            "properties": {
                "enable": {
                    "type": "boolean",
                    "default": true,
                },
                "logLevel": {
                    "enum": ["trace", "debug", "info", "warn", "error", "fatal"],
                    "default": "warn"
                },
            },
            "required": ["enable", "logLevel"]
        },
        "cloudWatch": {
            "type": "object",
            "properties": {
                "enable": {
                    "type": "boolean",
                    "default": true,
                },
                "logLevel": {
                    "enum": ["trace", "debug", "info", "warn", "error", "fatal"],
                    "default": "trace"
                },
                "logGroupName": {
                    "type": "string"
                },
                "logStreamName": {
                    "type": "string"
                }
            },
            "required": ["enable", "logLevel", "logGroupName", "logStreamName"]
        }
    },
    "required": ["console", "cloudWatch"]
};
class LogConfig {
}
exports.LogConfig = LogConfig;
LogConfig.get = (collection) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const logConfig = yield collection.findOne({});
        validator_1.validator(schema, logConfig);
        return logConfig;
    }
    catch (err) {
        throw err;
    }
});
