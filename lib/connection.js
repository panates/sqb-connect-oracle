/* sqb-connect-oracle
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-connect-oracle/
 */

/* Internal module dependencies. */
const OraMetaData = require('./metadata');
const OraCursor = require('./cursor');

/* External module dependencies. */
const oracledb = require('oracledb');

/**
 * @class
 * @public
 */

class OraConnection {

  constructor(intlcon) {
    this.intlcon = intlcon;
    this.sessionId = this.intlcon._sessionId;
  }

  /**
   * @override
   * @return {boolean}
   */
  get isClosed() {
    return !this.intlcon;
  }

  //noinspection JSUnusedGlobalSymbols
  /**
   * @override
   */
  close(callback) {
    //console.log(new Error().stack);
    if (this.intlcon) {
      const obj = this.intlcon;
      this.intlcon = undefined;
      obj.close(callback);
    }
  }

  //noinspection JSUnusedGlobalSymbols
  /**
   *
   * @param {string} sql
   * @param {Array|Object} params
   * @param {Object} options
   * @param {Function} callback
   * @private
   */
  execute(sql, params, options, callback) {

    if (this.isClosed) {
      callback(new Error('Can not execute while connection is closed'));
      return;
    }

    //noinspection JSUnresolvedVariable
    const self = this;
    const oraOptions = {
      autoCommit: options.autoCommit,
      extendedMetaData: options.extendedMetaData,
      resultSet: options.cursor,
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

      Object.getOwnPropertyNames(rprms).forEach(n => {
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
    self.intlcon.execute(sql, params || [], oraOptions, (err2, response) => {
      if (err2) {
        err2.sql = sql;
        err2.params = params;
        err2.options = options;
        return callback(err2);
      }
      const out = {};
      // Create array of field metadata
      let rowNumberIdx = -1;
      let rowNumberName = '';
      if (response.metaData) {
        out.metaData = [];
        response.metaData.forEach((v, idx) => {
          if (v.name.toLowerCase() === 'row$number') {
            rowNumberIdx = idx;
            rowNumberName = v.name;
            return;
          }
          const o = {index: idx, name: v.name};
          // fetchType
          let a = fetchTypeMap[v.fetchType];
          if (a) o.jsType = a;
          // dbType
          a = dbTypeMap[v.dbType];
          if (o.jsType === 'String')
            o.fixelLength = a === 'CHAR';
          if (a) o.dbType = a;
          // others
          if (v.byteSize) o.size = v.byteSize;
          if (v.nullable) o.nullable = v.nullable;
          if (v.precision) o.precision = v.precision;
          if (v.precision > 0)
            o.precision = v.precision;
          else v.dbType = 'FLOAT';
          out.metaData.push(o);
        });
      }
      if (response.rows) {
        out.rows = response.rows;
        // remove row$number fields
        if (rowNumberName) {
          for (let i = 0; i < out.rows.length; i++) {
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
        Object.getOwnPropertyNames(response.outBinds).forEach(n => {
          const v = response.outBinds[n];
          out.returns[n.replace('returning$', '')] = v.length === 1 ? v[0] : v;
        });
      }
      if (response.rowsAffected)
        out.rowsAffected = response.rowsAffected;
      if (response.resultSet)
        out.cursor =
            new OraCursor(response.resultSet, {rowNumberIdx, rowNumberName});
      callback(undefined, out);
    });
  }

  //noinspection JSUnusedGlobalSymbols
  commit(...args) {
    return this.intlcon.commit(...args);
  }

  //noinspection JSUnusedGlobalSymbols
  rollback(...args) {
    return this.intlcon.rollback(...args);
  }

  meta() {
    return new OraMetaData(this);
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

module.exports = OraConnection;
