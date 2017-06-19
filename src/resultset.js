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
const debug = require('debug')('OracledbResultSet');

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
    this._closeCursor(err => callback(err));
  }

  //noinspection JSUnusedGlobalSymbols
  _fetchDbRows(rowCount, callback) {
    const self = this;
    if (self._nested) {
      self._nested.getRows(rowCount, (err, rows) => {
        if (err || (rows && rows.length))
          callback(err, rows);
        else
          /* It is better to close nested resultset, becaouse all records fetched.  */
          self._closeCursor(err => callback(err));
      });
    } else callback();
  }

  _closeCursor(callback) {
    const self = this;
    if (self._nested)
      self._nested.close(err => {
        if (!err)
          self._nested = undefined;
        else if (process.env.DEBBUG)
          debug('Nested Oracle ResultSet closed');
        callback(err);
      });
    else callback();
  }

}

module.exports = OracledbResultSet;
