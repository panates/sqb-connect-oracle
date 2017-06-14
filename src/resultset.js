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
    /*console.log('****************');
    console.log(response.metaData);
    console.log('****************');

    /*
    response.metaData.forEach((item, idx) => {
      const o = metaData[item.name] = {index: idx};
      // fetchType
      let a = fetchTypeMap[item.fetchType];
      if (a) o.jsType = a;
      a = dbTypeMap[item.dbType];
      // dbType
      if (a) o.dbType = a;
      if (item.byteSize) o.byteSize = item.byteSize;
      if (!item.nullable) o.required = true;
      if (item.precision) o.precision = item.precision;
      if (item.precision > 0) o.precision = item.precision;
      else item.dbType = 'FLOAT';
    });
    */
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
