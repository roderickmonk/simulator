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
        "simConfig": {
            "type": "array",
            "items": {
                "type": "object",
            }
        },
    },
    "required": [
        "name",
        "envId",
        "minNotional",
        "trim",
        "parallelSimulations",
        "simConfig",
    ]
};
exports.configValidator = (simConfig) => {
    try {
        const ajv = new Ajv({
            allErrors: true,
            useDefaults: true,
            verbose: true
        });
        const validate = ajv.compile(simSchema);
        const valid = validate(simConfig);
        if (!valid || validate.errors) {
            console.log(`Invalid Configuration: ` +
                `${JSON.stringify(validate.errors, null, 4)}`);
            return false;
        }
        else {
            return true;
        }
    }
    catch (err) {
        throw err;
    }
};
