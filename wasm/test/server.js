// ESM
const Fastify = require('fastify')
const {Backend} = require('core-wasm')
const {Pool} = require('pg')

const schema = {
  "schemas":[
      {
          "name":"public",
          "objects":[
              {
                  "kind":"function",
                  "name":"myfunction",
                  "volatile":"v",
                  "composite":false,
                  "setof":true,
                  "return_type":"int4",
                  "return_type_schema":"pg_catalog",
                  "parameters":[
                      {
                          "name":"a",
                          "type":"integer",
                          "required":true,
                          "variadic":false
                      }
                  ]
              },
              {
                  "kind":"view",
                  "name":"tasks",
                  "columns":[
                      {
                          "name":"id",
                          "data_type":"int",
                          "primary_key":true
                      },
                      {
                          "name":"name",
                          "data_type":"text"
                      }
                  ],
                  "foreign_keys":[
                      {
                          "name":"project_id_fk",
                          "table":["api","tasks"],
                          "columns": ["project_id"],
                          "referenced_table":["api","projects"],
                          "referenced_columns": ["id"]
                      }
                  ]
              },
              {
                  "kind":"table",
                  "name":"projects",
                  "columns":[
                      {
                          "name":"id",
                          "data_type":"int",
                          "primary_key":true
                      }
                  ],
                  "foreign_keys":[],
                  "column_level_permissions":{
                      "role": {
                          "get": ["id","name"]
                      }
                  },
                  "row_level_permissions": {
                      "role": {
                          "get": [
                              {"single":{"field":{"name":"id"},"filter":{"op":["eq",["10","int"]]}}}
                          ]
                      }
                  }
              }
          ]
      }
  ]
}

const {DB_URI} = process.env
const db_pool = new Pool({connectionString: DB_URI})

const backend = Backend.init(JSON.stringify(schema))

const fastify = Fastify({logger: false})

fastify.get('/', async (request, reply) => {
  let query = null
  try {
    query = backend.get_query("public", "tasks", "get", "/tasks", [["select","id"]], "", {}, {}, "postgresql")
  } catch (err) {
    fastify.log.error(err)
    return { error: err }
  }

  const client = await db_pool.connect()

  try {
    await client.query('BEGIN')
    const res = await client.query(query, [])
    await client.query('COMMIT')
    const result = res.rows[0]
    reply
      .code(200)
      .header('Content-Type', 'application/json; charset=utf-8')
      .send(result.body)
  } catch (err) {
    await client.query('ROLLBACK')
    fastify.log.error(err)
    return { error: err }
  } finally {
    client.release()
  }


})

/**
 * Run the server!
 */
const start = async () => {
  try {
    await fastify.listen( { port: 3000, host: '0.0.0.0' })
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}
start()