console.log("start");
import {Backend} from '../pkg/subzero.js';
console.log("importd");

let schema = '{}';
let backend = Backend.init(schema);
console.log("end");