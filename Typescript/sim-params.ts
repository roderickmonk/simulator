export const simParams = {

    actualFeeRate: {
        type: "array",
        items: {
            type: "number",
        }
    },
    exchange: {
        type: "array",
        items: {
            type: "string"
        },
    },
    depth: {
        type: "array",
        items: {
            type: "number",
        }
    },
    feeRate: {
        type: "array",
        items: {
            type: "number",
        }
    },
    inventoryLimit: {
        type: "array",
        items: {
            type: "number",
        }
    },
    market: {
        type: "array",
        items: {
            type: "string",
        }
    },
    partitions: {
        type: "array",
        items: {
            type: "number",
        }
    },
    pdf: {
        type: "array",
        items: {
            type: "string",
        }
    },
    quantityLimit: {
        type: "array",
        items: {
            type: "number",
        }
    },
    timeFrame: {
        type: "array",
        items: {
            type: "object",
            properties: {
                startTime: {
                    type: "string",

                },
                endTime: {
                    type: "string",
                }
            }
        }
    },
    tick: {
        type: "array",
        items: {
            type: "number",
        }
    },
    trader: {
        type: "array",
        items: {
            type: "object",
            properties: {
                name: {
                    type: "string",
                },
                optimized: {
                    type: "boolean",
                    default: true,
                }
            }
        }
    },
}
