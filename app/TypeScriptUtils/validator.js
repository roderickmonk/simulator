"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Ajv = require('ajv');
exports.validator = (schema, config) => {
    const ajv = new Ajv({ allErrors: true, useDefaults: true, verbose: false });
    const validate = ajv.compile(schema);
    const valid = validate(config);
    if (!valid || validate.errors) {
        throw new Error(validate.errors[0].message);
    }
};
