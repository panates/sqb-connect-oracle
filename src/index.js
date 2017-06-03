/* SQB-connect-oracledb
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-oracledb/
 */

/* Internal module dependencies. */
const OracledbPool = require('./pool');
const OracledbConnection = require('./connection');

module.exports = {
  OracledbPool,
  OracledbConnection
};
