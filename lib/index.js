/* sqb-connect-oracle
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-connect-oracle/
 */

/* Internal module dependencies. */
const createConnector = require('./connector');

module.exports = {

  createSerializer: function(config) {
    return require('sqb-serializer-oracle').createSerializer(config);
  },

  createConnector: createConnector

};
