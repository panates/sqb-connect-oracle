/* sqb-connect-oracle
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-connect-oracle/
 */

/* Internal module dependencies. */
const OraPool = require('./pool');
const metadata = require('./metadata');

module.exports = {

  createSerializer(config) {
    return require('sqb-serializer-oracle').createSerializer(config);
  },

  createPool(cfg) {
    if (cfg.dialect === 'oracle') {
      return new OraPool(cfg);
    }
  },

  metaDataSelectSchemas(...args) {
    return metadata.selectSchemas.apply(undefined, args);
  },

  metaDataSelectTables(...args) {
    return metadata.selectTables.apply(undefined, args);
  },

  metaDataSelectColumns(...args) {
    return metadata.selectColumns.apply(undefined, args);
  },

  metaDataSelectPrimaryKeys(...args) {
    return metadata.selectPrimaryKeys.apply(undefined, args);
  },

  metaDataSelectForeignKeys(...args) {
    return metadata.selectForeignKeys.apply(undefined, args);
  }

};
