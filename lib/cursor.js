/* sqb-connect-oracle
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-connect-oracle/
 */

/* External module dependencies. */
const debug = require('debug')('sqb:oracledb:Cursor');

/**
 * @class
 * @extends Cursor
 */
class OraCursor {

  constructor(resultSet, options) {
    this.resultSet = resultSet;
    this._rowNumberIdx = options.rowNumberIdx;
    this._rowNumberName = options.rowNumberName;
    this._rownum = 0;
  }

  get rowNum() {
    return this._rownum;
  }

  get bidirectional() {
    return false;
  }

  //noinspection JSUnusedGlobalSymbols
  close(callback) {
    const self = this;
    if (self.resultSet)
      self.resultSet.close(err => {
        if (!err)
          self.resultSet = undefined;
        else if (process.env.DEBBUG)
          debug('Closed');
        callback(err);
      });
    else callback();
  }

  //noinspection JSUnusedGlobalSymbols
  fetch(fromRow, toRow, callback) {
    if (toRow < fromRow)
      return callback(new Error('Oracledb cursor does not support moving back'));

    const self = this;
    if (self.resultSet) {
      if (fromRow - 1 > self.rowNum) {
        self.seek(fromRow - self.rowNum - 1, (err) => {
          if (err)
            return callback(err);
          const c = toRow - self._rownum;
          self._fetchRows(c, (err, rows) => callback(err, rows));
        });
      } else {
        const c = toRow - fromRow + 1;
        self._fetchRows(c, (err, rows) => callback(err, rows));
      }
    } else callback();
  }

  seek(step, callback) {
    if (step < 0)
      return callback(new Error('Oracledb cursor does not support moving back'));

    const self = this;

    function seekNext() {
      const c = Math.min(step, 100);
      self._fetchRows(c, (err, rows) => {
        if (err)
          callback(err);
        step -= rows.length;
        if (step <= 0)
          callback();
        else process.nextTick(() => seekNext());
      });
    }

    seekNext();
  }

  _fetchRows(nRows, callback) {
    const self = this;
    self.resultSet.getRows(nRows, (err, rows) => {
      if (err)
        return callback(err);
      if (rows && rows.length) {
        self._rownum += rows.length;
        // remove row$number fields
        if (self._rowNumberName) {
          for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            if (Array.isArray(row))
              row.splice(self._rowNumberIdx, 1);
            else
              delete row[self._rowNumberName];
          }
        }
        callback(undefined, rows);
      } else
      /* It is better to close nested resultset, becaouse all records fetched.  */
        self.close(err => callback(err));
    });
  }

}

module.exports = OraCursor;
