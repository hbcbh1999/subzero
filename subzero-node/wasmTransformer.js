/* eslint-disable */

const path = require('path');
const fs = require('fs');

module.exports = {
    process(sourceText, sourcePath, options) {
        const bytes = fs.readFileSync(sourcePath);
        const base64 = bytes.toString('base64');
       
        return {
            code: `
                let buff = Buffer.from('${base64}', 'base64');
                module.exports = buff
            `,
        };
    },
};