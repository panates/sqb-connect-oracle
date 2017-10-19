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
const debug = require('debug')('sqb:oracledb:Cursor');

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
  this.resultSet = resultSet;
  this._rowNumberIdx = options.rowNumberIdx;
  this._rowNumberName = options.rowNumberName;
  this._rownum = 0;
}

const proto = OraCursor.prototype = {
  get rowNum() {
    return this._rownum;
  },

  get bidirectional() {
    return false;
  }

};
proto.constructor = OraCursor;

//noinspection JSUnusedGlobalSymbols
proto.close = function(callback) {
  const self = this;
  if (self.resultSet)
    self.resultSet.close(function(err) {
      if (!err)
        self.resultSet = undefined;
      else if (process.env.DEBBUG)
        debug('Closed');
      callback(err);
    });
  else callback();
};

//noinspection JSUnusedGlobalSymbols
proto.fetch = function(fromRow, toRow, callback) {
  if (toRow < fromRow)
    return callback(new Error('Oracledb cursor does not support moving back'));

  const self = this;
  if (self.resultSet) {
    if (fromRow - 1 > self.rowNum) {
      self.seek(fromRow - self.rowNum - 1, function(err) {
        if (err)
          return callback(err);
        const c = toRow - self._rownum;
        self._fetchRows(c, function(err, rows) {
          callback(err, rows);
        });
      });
    } else {
      const c = toRow - fromRow + 1;
      self._fetchRows(c, function(err, rows) {
        callback(err, rows);
      });
    }
  } else callback();
};

proto.seek = function(step, callback) {
  if (step < 0)
    return callback(new Error('Oracledb cursor does not support moving back'));

  const self = this;

  function seekNext() {
    const c = Math.min(step, 100);
    self._fetchRows(c, function(err, rows) {
      if (err)
        callback(err);
      step -= rows.length;
      if (step <= 0)
        callback();
      else process.nextTick(seekNext);
    });
  }

  seekNext();
};

proto._fetchRows = function(nRows, callback) {
  const self = this;
  self.resultSet.getRows(nRows, function(err, rows) {
    if (err)
      return callback(err);
    if (rows && rows.length) {
      self._rownum += rows.length;
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
    } else
    /* It is better to close nested resultset, becaouse all records fetched.  */
      self.close(callback);
  });
};
