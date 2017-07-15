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
const {Connection, FieldsMeta} = require('sqb');
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
    this._sessionId = this.intlcon._sessionId;
  }

  acquire() {
    super.acquire();
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
  _close(callback) {
    super._close();
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
      extendedMetaData: options.extendedMetaData,
      maxRows: options.maxRows,
      prefetchRows: options.prefetchRows,
      resultSet: options.resultSet,
      outFormat: options.objectRows ? oracledb.OBJECT : oracledb.ARRAY
    };

    self.intlcon.action = options.action || '';
    self.intlcon.clientId = options.clientId || '';
    self.intlcon.module = options.module || '';
    self.intlcon.execute(sql, params || [], oraOptions, (err2, response) => {
      if (err2) {
        err2.sql = sql;
        err2.params = params;
        err2.options = options;
        callback(err2);
      } else {
        // Create array of field metadata
        let metaData;
        if (response.metaData) {
          metaData = new FieldsMeta(options);
          response.metaData.forEach(v => {
            const o = {name: v.name};
            // fetchType
            let a = fetchTypeMap[v.fetchType];
            if (a) o.jsType = a;
            // dbType
            a = dbTypeMap[v.dbType];
            if (o.jsType === 'String')
              o.fixelLength = a === 'CHAR';
            if (a) o.dbType = a;
            if (v.byteSize) o.size = v.byteSize;
            if (v.nullable) o.nullable = v.nullable;
            if (v.precision) o.precision = v.precision;
            if (v.precision > 0) o.precision = v.precision;
            else v.dbType = 'FLOAT';
            metaData.add(o);
          });
        }
        const out = {};
        if (response.resultSet) {
          out.resultSet =
              new OracledbResultSet(self, options, response.resultSet, metaData);
        } else {
          if (metaData)
            out.metaData = metaData;
          if (response.rows)
            out.rows = response.rows;
        }
        if (response.rowsAffected)
          out.rowsAffected = response.rowsAffected;
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
