/* eslint-disable */
module.exports = {
    process(sourceText, sourcePath, options) {
        return {
            code:
                "const sql = `" + sourceText.replaceAll('\\\\','\\\\\\\\') + "`;\n" +
                "module.exports = sql;"
        };
    },
};