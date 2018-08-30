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

/**
 *
 * @param {Object} resultSet
 * @param {Object} options
 * @constructor
 */

class OraCursor {

  constructor(resultSet, options) {
    this._cursor = resultSet;
    this._rowNumberIdx = options.rowNumberIdx;
    this._rowNumberName = options.rowNumberName;
  }

  get isClosed() {
    return !this._cursor;
  }

  close() {
    if (!this._cursor)
      return Promise.resolve();
    return new Promise((resolve, reject) => {
      this._cursor.close(err => {
        if (err)
          return reject(err);
        this._cursor = undefined;
        resolve();
      });
    });
  }

  fetch(nRows) {
    return new Promise((resolve, reject) => {
      this._cursor.getRows(nRows, (err, rows) => {
        if (err)
          return reject(err);
        if (!(rows && rows.length))
          return resolve();
        /* remove row$number fields */
        if (this._rowNumberName) {
          for (const row of rows) {
            if (Array.isArray(row))
              row.splice(this._rowNumberIdx, 1);
            else
              delete row[this._rowNumberName];
          }
        }
        resolve(rows);
      });
    });
  }

}

/**
 * Expose `OraCursor`.
 */
module.exports = OraCursor;
