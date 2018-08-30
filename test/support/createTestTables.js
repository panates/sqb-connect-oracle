const waterfall = require('putil-waterfall');
const tableRegions = require('./table_regions');
const tableAirports = require('./table_airports');

function createTestTables(client) {

  return waterfall([

        /* Drop stables */
        () => {
          return waterfall.every([tableAirports, tableRegions], (next, table) => {
            client.execute('drop table ' + table.name, (err) => {
              if (!err || err.message.indexOf('ORA-00942') >= 0)
                return next();
              next(err);
            });
          });
        },

        /* Create user */
        (next) => {
          client.execute('CREATE USER sqb_test IDENTIFIED BY test', (err) => {
            if (!err || err.message.indexOf('ORA-01920') >= 0)
              return next();
            next(err);
          });
        },

        /* Grant */
        (next) => client.execute('GRANT CONNECT, RESOURCE TO sqb_test', next),

        /* Grant */
        (next) => client.execute('GRANT CREATE SESSION TO sqb_test', next),

        /* Grant */
        (next) => client.execute('GRANT UNLIMITED TABLESPACE TO sqb_test', next),

        /* Create tables */
        () => {
          /* Iterate every table */
          return waterfall.every([tableRegions, tableAirports],
              (next, table) => {

                return waterfall([
                  /* Create table */
                  (next) => {
                    return waterfall.every(table.createSql, (next, sql) => {
                      client.execute(sql, [], {}, next);
                    }, next);
                  },

                  /* Insert rows */
                  () => {
                    return waterfall.every(table.rows, (next, row) => {
                      client.execute(table.insertSql, row, {
                        autoCommit: true,
                      }, (err) => {
                        if (err)
                          return next(err);
                        next();
                      });
                    });
                  }
                ]);
              });
        }
      ]
  );
}

module.exports = createTestTables;
