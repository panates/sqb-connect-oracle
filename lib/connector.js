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
const OraConnection = require('./connection');

/**
 * Expose `createConnector`.
 */

/**
 *
 * @param {Object} config
 * @return {Function}
 */
module.exports = function createConnector(config) {
  if (config.dialect === 'oracle') {
    // Sql injection check
    if (config.schema && !config.schema.match(/^\w+$/))
      throw new Error('Invalid schema name');

    return function(callback) {
      var connection;
      waterfall([
            // Get oracle connection
            function(next) {
              oracledb.getConnection({
                user: config.user,
                password: config.password,
                connectString: config.database || config.connectString
              }, next);
            },
            // Create connection
            function(next, ncon) {
              connection = new OraConnection(ncon);
              const m = String(ncon.oracleServerVersion)
                  .match(/(\d{2})(\d{2})(\d{2})(\d{2})/);
              connection.serverVersion =
                  parseInt(m[1], 10) + '.' + parseInt(m[2], 10) + '.' +
                  parseInt(m[3], 10) + '.' + parseInt(m[4], 10);
              next();
            },
            // set sessionId
            function(next) {
              connection.intlcon.execute('select sid from v$mystat where rownum <= 1', [], {},
                  function(err, result) {
                    if (result && result.rows)
                      connection.sessionId = result.rows[0][0];
                    next(err);
                  });
            },
            // set default schema
            function(next) {
              if (config.schema) {
                connection.intlcon.execute(
                    'alter SESSION set CURRENT_SCHEMA = ' + config.schema,
                    [], {autoCommit: true}, next);
              } else next();
            }
          ],

          function(err) {
            if (err) {
              if (connection)
                connection.intlcon.close(function(err2) {
                  callback(err || err2);
                });
            } else
              callback(undefined, connection);
          });
    };
  }
};