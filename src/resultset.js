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

/**
 * @class
 * @extends DbPool
 */
class OracledbResultSet extends ResultSet {

  constructor(connection, options, response) {
    super(connection, options);
    //console.log(response);
    this._nested = response.resultSet;
    this.metaData = response.metaData;
  }

  //noinspection JSUnusedGlobalSymbols
  close(callback) {
    if (this._nested) {
      this._nested.close(err => {
        if (err)
          callback(err);
        else super.close(callback);
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
