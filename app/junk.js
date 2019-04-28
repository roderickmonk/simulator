"use strict";
const arr = [
    { label: 'All', value: 'All' },
    { label: 'All', value: 'All' },
    { label: 'Alex', value: 'Ninja' },
    { label: 'Bill', value: 'Op1' },
    { label: 'Bill', value: 'Op2' },
    { label: 'Cill', value: 'iopop' }
];
var result = arr.reduce((unique, o) => {
    if (!unique.some(obj => obj.label === o.label && obj.value === o.value)) {
        unique.push(o);
    }
    return unique;
}, []);
console.log(result);
const junk = new Set(JSON.stringify(arr));
var jobsUnique = Array.from(new Set(JSON.stringify(arr)));
console.log({ jobsUnique });
console.log({ junk });
var some = [
    {
        name: "Guille", x: 1, last: "Foo"
    },
    {
        name: "Jorge", x: 2, last: "bar"
    },
    {
        name: "Pedro", x: 3, last: "Foo"
    },
    {
        name: "Guille", x: 4, last: "Ipsum"
    }
];
const stuff1 = some.reduce((x, y) => x.findIndex(e => e.name == y.name) < 0 ? [...x, y] : x, []);
console.log({ stuff1 });
const removeDuplicates = (myArr, prop) => {
    return myArr.filter((obj, pos, arr) => {
        return arr.map(mapObj => mapObj[prop]).indexOf(obj[prop]) === pos;
    });
};
const stuff2 = removeDuplicates(some, 'name');
console.log({ stuff2 });
const stuff3 = removeDuplicates(some, 'last');
console.log({ stuff3 });
