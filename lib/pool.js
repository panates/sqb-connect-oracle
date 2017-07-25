/* SQB-connect-oracle
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-oracle/
 */

/* Internal module dependencies. */
const OraConnection = require('./connection');

/* External module dependencies. */
const assert = require('assert');
const oracledb = require('oracledb');

/* External module dependencies. */
//const OracleSerializer = require('sqb-serializer-oracle');
const waterfall = require('putil-waterfall');

/**
 * @class
 */
class OraPool {

  constructor(config) {
    this._config = config;
    config.namedParams = true;
    this.schema = config.schema || config.user;
  }

  // noinspection JSUnusedGlobalSymbols
  /**
   * Terminates the connection pool.
   * @param {Function} callback
   * @protected
   * @abstract
   */
  close(callback) {
    if (!this._nastedPool)
      callback();
    else this._nastedPool.close((err) => {
      if (!err)
        this._nastedPool = undefined;
      callback(err);
    });
  }

  //noinspection JSUnusedGlobalSymbols
  /**
   *
   * @param {Function<Error, Connection>} callback
   * @protected
   * @override
   */
  connect(callback) {
    const self = this;
    self._getOracleConnection((err, conn) => {
      if (err)
        return callback(err);
      callback(undefined, new OraConnection(conn));
    });
  }

  /**
   *
   * @param {Function} callback
   * @private
   */
  _getOracleConnection(callback) {
    const self = this;
    this._getOraclePool((err, nastedPool) => {
      if (err)
        callback(err);
      else {
        try {
          //noinspection JSUnresolvedFunction
          nastedPool.getConnection((err, ncon) => {
            if (err)
              return callback(err);
            self.serverVersion =
                Math.trunc(ncon.oracleServerVersion / 100000000);

            waterfall([

                  function(next) {
                    ncon.execute('select sid from v$mystat where rownum <= 1', [], {},
                        (err, result) => {
                          if (result && result.rows)
                            ncon._sessionId = result.rows[0][0];
                          next(err);
                        });

                  },

                  function(next) {
                    if (!self._config.schema) return next();
                    const schema = String(self._config.schema);
                    // Sql injection check
                    assert(schema.match(/^\w+$/));
                    ncon.execute('alter SESSION set CURRENT_SCHEMA = ' + schema,
                        [], {autoCommit: true},
                        (err) => next(err));
                  }
                ],

                function(err) {
                  if (err) {
                    if (ncon)
                      ncon.close((err2) => callback(err2 || err));
                  } else
                    callback(undefined, ncon);
                });

          });
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
    if (this._nastedPool) {
      callback(undefined, this._nastedPool);
      return;
    }

    if (this._poolQueue) {
      this._poolQueue.push(callback);
      return;
    }

    const self = this;
    const cfg = self._config;
    self._poolQueue = [];
    self._poolQueue.push(callback);

    //noinspection JSUnresolvedFunction,JSUnresolvedVariable
    oracledb.createPool({
      user: cfg.user,
      password: cfg.password,
      connectString: cfg.database || cfg.connectString,
      poolIncrement: cfg.pool.increment,
      poolMax: cfg.pool.max,
      poolMin: cfg.pool.min,
      poolTimeout: cfg.pool.idleTimeout

    }, function(err, pool) {
      if (pool)
        self._nastedPool = pool;

      function callQueue() {
        const fn = self._poolQueue.shift();
        if (!fn) return;
        if (err)
          fn(err);
        else fn(undefined, pool);
        process.nextTick(() => callQueue());
      }

      callQueue();
    });
  }

}

module.exports = OraPool;
