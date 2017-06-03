/* SQB-connect-oracledb
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-oracledb/
 */

/* Internal module dependencies. */
const OracledbMetaData = require('./metadata');

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
  _close() {
    super._close();
    if (this.intlcon) {
      this.intlcon.close();
      this.intlcon = undefined;
    }
  }

  //noinspection JSUnusedGlobalSymbols
  /**
   *
   * @param sql
   * @param params
   * @param options
   * @param callback
   * @private
   */
  _execute(sql, params, options, callback) {

    //noinspection JSUnresolvedFunction
    super._execute.apply(this, arguments);

    assert.ok(!this.closed);

    //noinspection JSUnresolvedVariable
    const oraOptions = {
      autoCommit: options.autoCommit,
      extendedMetaData: options.extendedMetaData,
      maxRows: options.maxRows,
      prefetchRows: options.prefetchRows,
      resultSet: !!options.resultSet,
      outFormat: options.objectRows ? oracledb.OBJECT : oracledb.ARRAY
    };

    this.intlcon.action = options.action || '';
    this.intlcon.clientId = options.clientId || '';
    this.intlcon.module = options.module || '';
    this.intlcon.execute(sql, params || [], oraOptions, function(err2, result) {
      if (err2) {
        err2.sql = sql;
        err2.params = params;
        err2.options = options;
        callback(err2);
      } else {
        const out = {};
        out.rows = result.rows;
        out.metaData = result.metaData;
        if (options.showSql) {
          out.sql = sql;
          out.params = params;
          out.options = options;
        }
        callback(undefined, out);
      }
    });
  }

  //noinspection JSUnusedGlobalSymbols
  commit() {
    return this.intlcon.commit.apply(this.intlcon, arguments);
  }

  //noinspection JSUnusedGlobalSymbols
  rollback() {
    return this.intlcon.rollback.apply(this.intlcon, arguments);
  }

  meta() {
    return new OracledbMetaData(this);
  }

}

module.exports = OracledbConnection;