/* SQB-connect-oracledb
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-oracledb/
 */

/* Internal module dependencies. */
const OraConnection = require('./connection');
const OracledbMetaData = require('./metadata');

/* External module dependencies. */
const sqb = require('sqb');
const DbPool = sqb.DbPool;
//noinspection SpellCheckingInspection
const oracledb = require('oracledb');

/* External module dependencies. */
const OracleSerializer = require('sqb-serializer-oracle');

/**
 * @class
 * @extends DbPool
 */
class OracledbPool extends DbPool {

  constructor(config) {
    super(config);
    //noinspection JSUnusedGlobalSymbols
    this.serializer = new OracleSerializer({
      namedParams: true,
      prettyPrint: config.prettyPrint
    });
  }

  //noinspection JSUnusedGlobalSymbols
  /**
   *
   * @return {string}
   * @override
   */
  get schema() {
    return super.schema || this.user;
  }

  //noinspection JSUnusedGlobalSymbols
  meta() {
    return new OracledbMetaData(this);
  }

  //noinspection JSUnusedGlobalSymbols
  /**
   *
   * @param {Function<Error, Connection>} callback
   * @protected
   * @override
   */
  _getConnection(callback) {
    const self = this;
    this._getOracleConnection(function(err, conn) {
      if (err)
        callback(err);
      else
        callback(undefined, new OraConnection(self, conn));
    });
  }

  /**
   *
   * @param {Function} callback
   * @private
   */
  _getOracleConnection(callback) {
    const self = this;
    this._getOraclePool(async (err, pool) => {
      if (err)
        callback(err);
      else {
        try {
          //noinspection JSUnresolvedFunction
          const internalConnection = await pool.getConnection();
          //noinspection JSUnresolvedVariable
          self.serverVersion =
              Math.trunc(internalConnection.oracleServerVersion / 100000000);
          if (!internalConnection._sessionId)
            internalConnection._sessionId =
                (await internalConnection.execute('select sid from v$mystat where rownum <=1', [], {})).rows[0][0];
          //noinspection JSUnresolvedVariable
          if (self.config.schema &&
              internalConnection.currentSchema !== self.config.schema)
            await internalConnection.execute('ALTER SESSION SET CURRENT_SCHEMA = ' +
                self.config.schema, [], {autoCommit: true});
          callback(undefined, internalConnection);
        } catch (e) {
          callback(e);
        }
      }
    });
  }

  /**
   *
   * @param {Function} callback
   * @private
   */
  _getOraclePool(callback) {
    if (this._pool) {
      callback(undefined, this._pool);
      return;
    }

    if (this._poolQueue) {
      this._poolQueue.push(callback);
      return;
    }

    const self = this;
    const cfg = self.config;
    self._poolQueue = [];
    self._poolQueue.push(callback);

    //noinspection JSUnresolvedFunction,JSUnresolvedVariable
    oracledb.createPool({
      user: cfg.user,
      password: cfg.password,
      connectString: cfg.connectString,
      poolIncrement: cfg.pool.increment,
      poolMax: cfg.pool.max,
      poolMin: cfg.pool.min,
      poolTimeout: cfg.pool.idleTimeout

    }, function(err, pool) {
      if (pool)
        self._pool = pool;
      self._poolQueue.every(function(fn) {
        if (err)
          fn(err);
        else fn(undefined, pool);
        return true;
      });
      delete self._poolQueue;
    });
  }

}

DbPool.register('oracledb', OracledbPool);

module.exports = OracledbPool;
