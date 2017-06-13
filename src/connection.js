/* SQB-connect-oracledb
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-oracledb/
 */

/* Internal module dependencies. */
const OracledbMetaData = require('./metadata');
const OracledbResultSet = require('./resultset');

/* External module dependencies. */
const {Connection} = require('sqb');
const assert = require('assert');
//noinspection SpellCheckingInspection,NpmUsedModulesInstalled
const oracledb = require('oracledb');

/**
 * @class
 * @public
 */

class OracledbConnection extends Connection {

  constructor(dbpool, intlcon) {
    super(dbpool);
    this.intlcon = intlcon;
  }

  acquire() {
    super.acquire();
  }

  //noinspection JSUnusedGlobalSymbols
  get sessionId() {
    return this.intlcon && this.intlcon._sessionId;
  }

  /**
   * @override
   * @return {boolean}
   */
  get closed() {
    return !this.intlcon;
  }

  //noinspection JSUnusedGlobalSymbols
  /**
   * @override
   */
  prepare(statement, params, options) {
    return super.prepare(...// eslint-disable-next-line
        arguments);
  }

  //noinspection JSUnusedGlobalSymbols
  /**
   * @override
   */
  _close() {
    super._close();
    if (this.intlcon) {
      const obj = this.intlcon;
      this.intlcon = undefined;
      obj.close();
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
  _execute(sql, params, options, callback) {

    //noinspection JSUnresolvedFunction,Eslint
    super._execute.apply(this, arguments);

    if (this.closed) {
      callback(new Error('Can not execute while connection is closed'));
      return;
    }

    //noinspection JSUnresolvedVariable
    const self = this;
    const oraOptions = {
      autoCommit: options.autoCommit,
      extendedMetaData: true, // options.extendedMetaData,
      maxRows: options.maxRows,
      prefetchRows: options.prefetchRows,
      resultSet: !!options.resultSet,
      outFormat: !options.resultSet && options.objectRows ?
          oracledb.OBJECT : oracledb.ARRAY
    };

    self.intlcon.action = options.action || '';
    self.intlcon.clientId = options.clientId || '';
    self.intlcon.module = options.module || '';
    self.intlcon.execute(sql, params ||
        [], oraOptions, function(err2, response) {
      if (err2) {
        err2.sql = sql;
        err2.params = params;
        err2.options = options;
        callback(err2);
      } else {
        const out = {};
        let metaData;
        if (response.metaData) {
          metaData = {};
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
        }
        if (options.resultSet && response.resultSet) {
          response.metaData = metaData;
          out.resultSet =
              new OracledbResultSet(self, options.resultSet, response);
        } else {
          if (response.rows)
            out.rows = response.rows;
          if (metaData)
            out.metaData = metaData;
        }
        if (response.rowsAffected)
          out.rowsAffected = response.rowsAffected;
        if (options.debug) {
          out.sql = sql;
          out.params = params;
          out.options = options;
        }
        callback(undefined, out);
      }
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
    return new OracledbMetaData(this);
  }

}

const fetchTypeMap = {
  2001: 'String',
  2002: 'Number',
  2003: 'Date',
  2004: 'ResultSet',
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

module.exports = OracledbConnection;
