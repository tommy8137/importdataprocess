const pg = require('pg')

const Postgres = (dbconfig) => {
  const pool = new pg.Pool({
    user: dbconfig.pgName, // env var: PGUSER
    database: dbconfig.pgDb, // env var: PGDATABASE
    password: dbconfig.pgPw, // env var: PGPASSWORD
    host: dbconfig.pgIp, // Server hosting the postgres database
    port: dbconfig.pgPort, // env var: PGPORT
    idleTimeoutMillis: dbconfig.idleTimeoutMillis, // how long a client is allowed to remain idle before being closed
    max: 20, 
    application_name: `wieprocure-data-processing-${process.pid}`
  })
  pool.on('error', function (err) {
    console.log('Database error!', err)
  })
  
  return {
    pool: pool,
    Query: (sql, params) => {
      try {
        return new Promise((resolve, reject) => {
          pg.defaults.parseInt8 = true
          pool.connect(function(err, client, done) {
            if (err) {
              console.log(`error === ${err}`)
              reject({ message: 'could not connect to postgres' })
            }
            if(client == null || typeof client.query != 'function') {
              new Error('property query null')
            } else {
              client.query(sql, params, function(err, result) {
                done()
                if (err) {
                  console.log(`error === ${err}`)
                  reject({ message: 'error running query' })
                }
                resolve(result)
              })
            }
          })
        })
      } catch (e) {
        console.log(`error === ${e}`)
        // throwApiError('DB_ERROR', errorCode.DB_ERROR )
      }
    },
  }
}

module.exports = Postgres