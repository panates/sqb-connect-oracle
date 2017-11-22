# SQB-connect-oracle

[![NPM Version][npm-image]][npm-url]
[![NPM Downloads][downloads-image]][downloads-url]
[![Dependencies][dependencies-image]][dependencies-url]
[![DevDependencies][devdependencies-image]][devdependencies-url]
[![PeerDependencies][peerdependencies-image]][peerdependencies-url]

Oracle connection adapter for [SQB](https://github.com/panates/sqb).

## Configuring

### Authentication options

#### Internal Authentication

Applications using internal authentication stores credentials manually and passes `user` and `password` properties in configuration object.

```js
sqb.use('sqb-connect-oracle');
const pool = sqb.pool({
  dialect: 'oracle',
  user: 'anyuser',
  password: 'anypassword',
  ....
})
```

#### External Authentication

External Authentication allows applications to use an external password store (such as Oracle Wallet), the Secure Socket Layer (SSL), or the operating system to validate user access. One of the benefits is that database credentials do not need to be hard coded in the application.

To use external authentication, set the `externalAuth` property to true. (Default false)

```js
sqb.use('sqb-connect-oracle');
const pool = sqb.pool({
  dialect: 'oracle',
  externalAuth: true
  ....
})
```

### Connection configuration options

#### Configure using connection parameters

```js
sqb.use('sqb-connect-oracle');
const pool = sqb.pool({
  dialect: 'oracle',
  ** Authentication options here 
  host: 'localhost',
  port: 1521,
  database: 'SALES'
})
```

- `host`: Hostname to connect to
- `port`: Port to connect to (default: 1521)
- `database`: Database (service name) to connect to (Optional)
- `serverType`: Type of server (Optional)
- `instanceName`: Instance name (Optional)


#### Configure using easy connection syntax

An Easy Connect string is often the simplest to use. With Oracle Database 12c the syntax is:

`[//]host[:port][/database][:serverType][/instanceName]`

```js
sqb.use('sqb-connect-oracle');
const pool = sqb.pool({
  dialect: 'oracle',
  ** Authentication options here 
  connectString: 'localhost:1521/SALES'
})
```

#### Configure using Net service name

A Net Service Name, such as sales in the example below, can be used to connect:

```js
sqb.use('sqb-connect-oracle');
const pool = sqb.pool({
  dialect: 'oracle',
  ** Authentication options here 
  connectString: 'sales'
})
```

This could be defined in a directory server, or in a local tnsnames.ora file, for example:
```
sales =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = mymachine.example.com)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = orcl)
    )
  )
```

#### Configure using full connection strings

```js
sqb.use('sqb-connect-oracle');
const pool = sqb.pool({
  dialect: 'oracle',
  ** Authentication options here 
  connectString: '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=mymachine.example.com)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=orcl)))'
})
```

### Additional parameters

```js
sqb.use('sqb-connect-oracle');
const pool = sqb.pool({
  dialect: 'oracle',
  ** Connection options here 
  schema: 'otherschema'
})
```

- `schema`: Sets default schema for session

## Node Compatibility

  - node `>= 4.x`;
  
### License
[MIT](LICENSE)

[npm-image]: https://img.shields.io/npm/v/sqb-connect-oracle.svg
[npm-url]: https://npmjs.org/package/sqb-connect-oracle
[downloads-image]: https://img.shields.io/npm/dm/sqb-connect-oracle.svg
[downloads-url]: https://npmjs.org/package/sqb-connect-oracle
[dependencies-image]: https://david-dm.org/panates/sqb-connect-oracle.svg
[dependencies-url]:https://david-dm.org/panates/sqb-connect-oracle#info=dependencies
[devdependencies-image]: https://david-dm.org/panates/sqb-connect-oracle/dev-status.svg
[devdependencies-url]:https://david-dm.org/panates/sqb-connect-oracle?type=dev
[peerdependencies-image]: https://david-dm.org/panates/sqb-connect-oracle/peer-status.svg
[peerdependencies-url]:https://david-dm.org/panates/sqb-connect-oracle?type=peer
