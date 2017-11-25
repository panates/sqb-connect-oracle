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
 * Expose `OraCursor`.
 */
module.exports = OraCursor;

/**
 *
 * @param {Object} resultSet
 * @param {Object} options
 * @constructor
 */

function OraCursor(resultSet, options) {
  this._cursor = resultSet;
  this._rowNumberIdx = options.rowNumberIdx;
  this._rowNumberName = options.rowNumberName;
}

const proto = OraCursor.prototype = {
  get bidirectional() {
    return false;
  },

  get isClosed() {
    return !this._cursor;
  }
};
proto.constructor = OraCursor;

proto.close = function(callback) {
  const self = this;
  if (!self._cursor)
    return callback();
  self._cursor.close(function(err) {
    if (!err) {
      self._cursor = undefined;
    }
    callback(err);
  });
};

proto.fetch = function(nRows, callback) {
  const self = this;
  self._cursor.getRows(nRows, function(err, rows) {
    if (err)
      return callback(err);
    if (!(rows && rows.length))
      return callback();
    // remove row$number fields
    if (self._rowNumberName) {
      rows.forEach(function(row) {
        if (Array.isArray(row))
          row.splice(self._rowNumberIdx, 1);
        else
          delete row[self._rowNumberName];
      });
    }
    callback(undefined, rows);

  });
};
