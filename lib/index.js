/* sqb-connect-oracle
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-connect-oracle/
 */

/**
 * Module dependencies.
 * @private
 */
const OraAdapter = require('./OraAdapter');
const OraMetaOperator = require('./OraMetaOperator');

module.exports = Object.assign({}, require('sqb-serializer-oracle'));

module.exports.createAdapter = function(config) {
  /* istanbul ignore else */
  if (config.dialect === 'oracle') {
    return new OraAdapter(config);
  }
};

module.exports.createMetaOperator = function(config) {
  /* istanbul ignore else */
  if (config.dialect === 'oracle') {
    return new OraMetaOperator();
  }
};
