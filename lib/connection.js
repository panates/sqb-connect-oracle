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
const oracledb = require('oracledb');
const OraCursor = require('./cursor');

/**
 * Expose `OraConnection`.
 */
module.exports = OraConnection;

/**
 *
 * @param {Object} intlcon
 * @constructor
 */
function OraConnection(intlcon) {
  this.intlcon = intlcon;
}

const proto = OraConnection.prototype = {
  /**
   * @override
   * @return {boolean}
   */
  get isClosed() {
    return !this.intlcon;
  }
};
proto.constructor = OraConnection;

/**
 * @override
 */
proto.close = function(callback) {
  if (this.intlcon) {
    const conn = this.intlcon;
    this.intlcon = undefined;
    conn.close(callback);
  }
};

/**
 *
 * @param {string} sql
 * @param {Array|Object} params
 * @param {Object} options
 * @param {Function} callback
 * @private
 */
proto.execute = function(sql, params, options, callback) {

  if (this.isClosed) {
    callback(new Error('Can not execute while connection is closed'));
    return;
  }

  const self = this;
  const oraOptions = {
    autoCommit: options.autoCommit,
    extendedMetaData: true,
    _cursor: options.cursor,
    outFormat: options.objectRows ? oracledb.OBJECT : oracledb.ARRAY
  };
  if (options.cursor)
    oraOptions.prefetchRows = options.fetchRows;
  else
    oraOptions.maxRows = options.fetchRows;

  if (options.returningParams) {
    const rprms = options.returningParams;
    if (Array.isArray(params))
      params = params.slice();
    else params = Object.assign({}, params);
    Object.getOwnPropertyNames(rprms).forEach(function(n) {
      const o = {dir: oracledb.BIND_OUT};
      switch (rprms[n]) {
        case 'string':
          o.type = oracledb.STRING;
          break;
        case 'number':
          o.type = oracledb.NUMBER;
          break;
        case 'date':
          o.type = oracledb.DATE;
          break;
        case 'blob':
          o.type = oracledb.BLOB;
          break;
        case 'clob':
          o.type = oracledb.CLOB;
          break;
        case 'buffer':
          o.type = oracledb.BUFFER;
          break;
      }
      if (Array.isArray(params))
        params.push(o);
      else params[n] = o;
    });
  }

  self.intlcon.action = options.action || '';
  self.intlcon.clientId = options.clientId || '';
  self.intlcon.module = options.module || '';
  self.intlcon.execute(sql, params || [], oraOptions, function(err2, response) {
    if (err2)
      return callback(err2);

    const out = {};
    // Create array of field metadata
    var rowNumberIdx = -1;
    var rowNumberName = '';
    if (response.metaData) {
      out.fields = [];
      response.metaData.forEach(function(v, idx) {
        if (v.name.toLowerCase() === 'row$number') {
          rowNumberIdx = idx;
          rowNumberName = v.name;
          return;
        }
        const o = {
          index: idx,
          name: v.name,
          dataType: fetchTypeMap[v.fetchType],
          fieldType: dbTypeMap[v.dbType] || v.dbType
        };
        if (o.dataType === 'String' && o.dbType === 'CHAR')
          o.fixedLength = true;
        // others
        if (v.byteSize) o.size = v.byteSize;
        if (v.nullable) o.nullable = v.nullable;
        if (v.precision) o.precision = v.precision;
        out.fields.push(o);
      });
    }
    if (response.rows) {
      out.rows = response.rows;
      // remove row$number fields
      if (rowNumberName) {
        for (var i = 0; i < out.rows.length; i++) {
          const row = out.rows[i];
          if (Array.isArray(row))
            row.splice(rowNumberIdx, 1);
          else
            delete row[rowNumberName];
        }
      }
    }
    if (response.outBinds) {
      out.returns = {};
      Object.getOwnPropertyNames(response.outBinds).forEach(function(n) {
        const v = response.outBinds[n];
        out.returns[n.replace('returning$', '')] = v.length === 1 ? v[0] : v;
      });
    }
    if (response.rowsAffected)
      out.rowsAffected = response.rowsAffected;
    if (response._cursor)
      out.cursor =
          new OraCursor(response._cursor, {
            rowNumberIdx: rowNumberIdx,
            rowNumberName: rowNumberName
          });
    callback(undefined, out);
  });
};

proto.commit = function(arg) {
  return this.intlcon.commit.apply(this.intlcon, arguments);
};

proto.rollback = function() {
  return this.intlcon.rollback.apply(this.intlcon, arguments);
};

proto.test = function(callback) {
  const self = this;
  self.intlcon.execute('select 1 from dual', [], {}, function(err) {
    return callback(err);
  });
};

const fetchTypeMap = {
  2001: 'String',
  2002: 'Number',
  2003: 'Date',
  2004: 'Cursor',
  2005: 'Buffer',
  2006: 'Clob',
  2007: 'Blob'
};

const dbTypeMap = {
  1: 'VARCHAR',
  2: 'NUMBER',
  12: 'DATE',
  23: 'RAW',
  96: 'CHAR',
  100: 'BINARY_FLOAT,',
  101: 'BINARY_DOUBLE',
  104: 'ROWID',
  112: 'CLOB',
  113: 'BLOB',
  187: 'TIMESTAMP',
  188: 'TIMESTAMP_TZ',
  232: 'TIMESTAMP_LTZ'
};
