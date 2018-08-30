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
const waterfall = require('putil-waterfall');
const OraConnection = require('./OraConnection');

class OraAdapter {

  /**
   * @constructor
   * @param {Object} config
   */
  constructor(config) {
    // Sql injection check
    if (config.schema && !config.schema.match(/^\w+$/))
      throw new Error('Invalid schema name');
    this.config = Object.assign({}, config);
    this.paramType = 0; //  COLON
  }

  createConnection() {
    const config = this.config;
    let connection;
    let client;
    return new Promise((resolve, reject) => {
      waterfall([

            /* Establish oracle connection */
            (next) => {
              const cfg = {};
              // Authentication options
              if (config.externalAuth)
                cfg.externalAuth = config.externalAuth;
              else {
                cfg.user = config.user;
                cfg.password = config.password;
              }
              // Connection options
              if (config.connectString)
                cfg.connectString = config.connectString;
              else if (config.host)
                cfg.connectString = config.host +
                    ':' + (config.port || '1521') +
                    (config.database ? '/' + config.database : '') +
                    (config.serverType ? ':' + config.serverType : '') +
                    (config.instanceName ? '/' + config.instanceName : '');
              // Get oracle connection
              oracledb.getConnection(cfg, next);
            },

            /* Create OraConnection */
            (next, ncon) => {
              client = ncon;
              connection = new OraConnection(ncon);
              const m = String(ncon.oracleServerVersion)
                  .match(/(\d{2})(\d{2})(\d{2})(\d{2})/);
              connection._params.server_version =
                  parseInt(m[1], 10) + '.' + parseInt(m[2], 10) + '.' +
                  parseInt(m[3], 10) + '.' + parseInt(m[4], 10);
              next();
            },

            /* Retrieve sessionId */
            (next) => {
              client.execute('select sid from v$mystat where rownum <= 1', [], {},
                  (err, result) => {
                    if (result && result.rows)
                      connection.sessionId = result.rows[0][0];
                    next(err);
                  });
            },

            /* Set default schema */
            (next) => {
              if (config.schema) {
                client.execute(
                    'alter SESSION set CURRENT_SCHEMA = ' + config.schema,
                    [], {autoCommit: true}, next);
              } else next();
            }
          ],

          function(err) {
            if (err) {
              if (client)
                return client.close(err2 => reject(err || err2));
              return reject(err);
            }
            resolve(connection);
          });
    });
  }

}

/**
 * Expose `OraAdapter`.
 */

module.exports = OraAdapter;
