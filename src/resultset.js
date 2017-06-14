/* SQB-connect-oracledb
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-oracledb/
 */

/* External module dependencies. */
const sqb = require('sqb');
const ResultSet = sqb.ResultSet;
//const debug = require('debug')('OracledbResultSet');

/**
 * @class
 * @extends DbPool
 */
class OracledbResultSet extends ResultSet {

  constructor(connection, options, response) {
    super(connection, options);
    this._nested = response.resultSet;
    this.metaData = response.metaData;
  }

  //noinspection JSUnusedGlobalSymbols
  _close(callback) {
    if (this._nested) {
      this._nested.close(err => {
        callback(err);
      });
    }
  }

  //noinspection JSUnusedGlobalSymbols
  _fetchDbRows(rowCount, callback) {
    const self = this;
    if (self._nested) {
      self._nested.getRows(rowCount, callback);
    } else callback(undefined, []);
  }

}

module.exports = OracledbResultSet;
