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
const OraCursor = require('./OraCursor');

class OraConnection {

  /**
   *
   * @param {Object} client
   * @constructor
   */
  constructor(client) {
    this.intlcon = client;
    this._params = {};
  }

  /**
   * @return {boolean}
   */
  get isClosed() {
    return !this.intlcon;
  }

  /**
   *
   * @param {String} param
   * @return {*}
   */
  get(param) {
    return this._params[param];
  }

  get serverVersion() {
    return this._params.server_version;
  }

  close() {
    if (!this.intlcon)
      return Promise.resolve();

    return new Promise((resolve, reject) => {
      this.intlcon.close((err) => {
        if (err)
          return reject(err);
        this.intlcon = null;
        resolve();
      });
    });
  }

  /**
   *
   * @param {string} query
   * @param {string} query.sql
   * @param {Array} query.values
   * @param {Object} options
   * @param {Integer} [options.fetchRows]
   * @param {Boolean} [options.cursor]
   * @param {Boolean} [options.autoCommit]
   * @private
   * @return {Promise<Object>}
   */
  execute(query, options) {

    if (this.isClosed)
      return Promise.reject(new Error('Can not execute while connection is closed'));

    return Promise.resolve().then(() => {

      const oraOptions = {
        autoCommit: options.autoCommit || !this._inTransaction,
        extendedMetaData: true,
        resultSet: options.cursor,
        outFormat: options.objectRows ? oracledb.OBJECT : oracledb.ARRAY
      };
      if (options.cursor)
        oraOptions.fetchArraySize = options.fetchRows;
      else
        oraOptions.maxRows = options.fetchRows;
      let params = query.values;

      if (query.returningFields) {
        const rprms = query.returningFields;
        if (Array.isArray(params))
          params = params.slice();
        else params = Object.assign({}, params);
        for (const n of Object.keys(rprms)) {
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
        }
      }

      this.intlcon.action = options.action || '';
      this.intlcon.clientId = options.clientId || '';
      this.intlcon.module = options.module || '';

      /* Execute query */
      return this.intlcon.execute(
          query.sql, params || [], oraOptions)
          .then(response => {

            if (options.autoCommit)
              this._inTransaction = false;

            const out = {};
            // Create array of field metadata
            let rowNumberIdx = -1;
            let rowNumberName = '';
            if (response.metaData) {
              out.fields = [];
              for (const [idx, v] of response.metaData.entries()) {
                if (v.name.toLowerCase() === 'row$number') {
                  rowNumberIdx = idx;
                  rowNumberName = v.name;
                  continue;
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
              }
            }
            if (response.rows) {
              out.rows = response.rows;
              // remove row$number fields
              if (rowNumberName) {
                for (const row of out.rows) {
                  if (Array.isArray(row))
                    row.splice(rowNumberIdx, 1);
                  else
                    delete row[rowNumberName];
                }
              }
            }
            if (response.outBinds) {
              out.rows = [{}];
              const row = out.rows[0];
              for (const n of Object.getOwnPropertyNames(response.outBinds)) {
                const v = response.outBinds[n];
                row[n.replace('returning$', '')] =
                    v.length === 1 ? v[0] : v;
              }
            }
            if (response.rowsAffected)
              out.rowsAffected = response.rowsAffected;
            if (response.resultSet)
              out.cursor =
                  new OraCursor(response.resultSet, {
                    rowNumberIdx: rowNumberIdx,
                    rowNumberName: rowNumberName
                  });
            return out;
          });
    });

  }

  startTransaction() {
    this._inTransaction = true;
    return Promise.resolve();
  }

  commit() {
    return new Promise((resolve, reject) => {
      this.intlcon.commit(err => {
        if (err)
          return reject(err);
        this._inTransaction = false;
        resolve();
      });
    });
  }

  rollback() {
    return new Promise((resolve, reject) => {
      this.intlcon.rollback(err => {
        if (err)
          return reject(err);
        this._inTransaction = false;
        resolve();
      });
    });
  }

  test() {
    return this.intlcon.execute('select 1 from dual', [], {});
  }

}

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
  8: 'LONG',
  12: 'DATE',
  23: 'RAW',
  24: 'LONG_RAW',
  68: 'UNSIGNED INT',
  96: 'CHAR',
  100: 'BINARY_FLOAT,',
  101: 'BINARY_DOUBLE',
  104: 'ROWID',
  108: 'UDT',
  111: 'REF',
  112: 'CLOB',
  113: 'BLOB',
  114: 'BFILE',
  116: 'RSET',
  187: 'TIMESTAMP',
  188: 'TIMESTAMP_TZ',
  232: 'TIMESTAMP_LTZ'
};

/**
 * Expose `OraConnection`.
 */
module.exports = OraConnection;
