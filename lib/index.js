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
const OraDriver = require('./driver');

module.exports = {

  createSerializer: require('sqb-serializer-oracle').createSerializer,

  createDriver: function(config) {
    if (config.dialect === 'oracle') {
      return new OraDriver(config);
    }
  }

};
