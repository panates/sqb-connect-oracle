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
const OraMetaOperator = require('./OraMetaOperator');

const PARAMTYPE_COLON = 0;

/**
 * Expose `OraDriver`.
 */

module.exports = OraDriver;

function OraDriver(config) {
  // Sql injection check
  if (config.schema && !config.schema.match(/^\w+$/))
    throw new Error('Invalid schema name');
  this.config = Object.assign({}, config);
  this.paramType = PARAMTYPE_COLON;
  this.supportsSchemas = true;
  this.metaData = new OraMetaOperator();
}

const proto = OraDriver.prototype;

proto.createConnection = function(callback) {
  const config = this.config;
  var connection;
  waterfall([
        // Get oracle connection
        function(next) {
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
        // Create Connection
        function(next, ncon) {
          connection = new OraConnection(ncon);
          const m = String(ncon.oracleServerVersion)
              .match(/(\d{2})(\d{2})(\d{2})(\d{2})/);
          connection.serverVersion =
              parseInt(m[1], 10) + '.' + parseInt(m[2], 10) + '.' +
              parseInt(m[3], 10) + '.' + parseInt(m[4], 10);
          next();
        },
        // get sessionId
        function(next) {
          connection.client.execute('select sid from v$mystat where rownum <= 1', [], {},
              function(err, result) {
                if (result && result.rows)
                  connection.sessionId = result.rows[0][0];
                next(err);
              });
        },
        // set default schema
        function(next) {
          if (config.schema) {
            connection.client.execute(
                'alter SESSION set CURRENT_SCHEMA = ' + config.schema,
                [], {autoCommit: true}, next);
          } else next();
        }
      ],

      function(err) {
        if (err && connection) {
          return connection.client.close(function(err2) {
            callback(err || err2);
          });
        }
        callback(err, connection);
      });
};