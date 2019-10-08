"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const ajv_1 = __importDefault(require("ajv"));
const ajv = new ajv_1.default({
    allErrors: true,
    useDefaults: true,
    verbose: true
});
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
        const ajv = new ajv_1.default({
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
