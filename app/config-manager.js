"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Ajv = require("ajv");
const ajv = new Ajv;
const simSchema = {
    "properties": {
        "name": {
            "type": "string",
        },
        "envId": {
            "type": "number",
        },
        "minNotional": {
            "type": "number",
            "default": 0.00000001
        },
        "trim": {
            "type": "boolean",
        },
        "parallelSimulations": {
            "type": "number",
        },
        "saveRedis": {
            "type": "boolean",
            "default": false,
        },
        "onlyOrderbooksWithTrades": {
            "type": "boolean",
            "default": true,
        },
    },
    "required": [
        "name",
        "envId",
        "minNotional",
        "trim",
        "parallelSimulations",
        "saveRedis",
        "onlyOrderbooksWithTrades",
        "multiplyConfig",
    ]
};
exports.configValidator = (simConfig) => {
    try {
        const ajv = new Ajv({
            allErrors: true,
            useDefaults: true,
            verbose: false,
        });
        const validate = ajv.compile(simSchema);
        const valid = validate(simConfig);
        if (!valid || validate.errors) {
            const msg = `Invalid Configuration: ` +
                `${JSON.stringify(validate.errors, null, 4)}`;
            return Promise.reject(new Error(msg));
        }
        return Promise.resolve(true);
    }
    catch (err) {
        return Promise.reject(err);
    }
};
