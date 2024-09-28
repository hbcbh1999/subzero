subZero REST is a self contained zero-dependency library that allows developers to implement their own customizable backend APIs on top of any database.

The resulting REST API is [PostgREST](https://postgrest.org) compatible.

Currently PostgreSQL, SQLite, MySQL* and ClickHouse* are supported as target databases.

For more information on how to use it see [https://subzero.cloud](https://subzero.cloud).

The library licensed under [AGPLv3 license](http://www.gnu.org/licenses/agpl-3.0.html)

# Usage

## node / express
The code below fully implements a backend server with functionality similar to PostgREST which you can extend with your own custom routes, middleware, etc.

```typescript
import dotenv from 'dotenv';
import { expand } from 'dotenv-expand';
import morgan from 'morgan';
import path from 'path';
import fs from 'fs';
import debug from 'debug';
import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import cookieParser from 'cookie-parser';
import passport from 'passport';
import { Strategy as JwtStrategy, ExtractJwt } from 'passport-jwt';
import { init as restInit, getRequestHandler, onSubzeroError } from '@subzerocloud/rest';
import pg from 'pg';
// for sqlite use the following import instead 
// import Client from 'better-sqlite3';
// import manually defined permissions with a json file
// import permissions from './permissions'
// note that better-sqlite3, sqlite3, @sqlite.org/sqlite-wasm, @libsql/client (turso) are supported

// read env
const env = dotenv.config();
expand(env);

const {
    DB_URI,
    DATABASE_URL,
    JWT_SECRET,
    DB_ANON_ROLE,
    DB_SCHEMAS,
    STATIC_DIR,
    NODE_ENV,
    PORT,
    DB_POOL_MAX,
} = process.env;

const dbAnonRole = DB_ANON_ROLE || 'anon';
const dbSchemas = DB_SCHEMAS ? DB_SCHEMAS.split(',') : ['public'];
const staticDir = STATIC_DIR || path.resolve(__dirname, 'public')

// Create a database connection pool
const dbPool = new pg.Pool({
    connectionString: DB_URI,
    max: DB_POOL_MAX ? parseInt(DB_POOL_MAX) : 10,
});
// for sqlite use the following instead
// const dbPool = new Client(DB_URI.replace('sqlite://',''));

// Create the Express application
const app = express();

// set up logging
const logger = morgan(NODE_ENV === 'production' ? 'combined' : 'dev');
app.use(logger);

// set up CORS
app.use(
    cors({
        exposedHeaders: [
            'content-range',
            'range-unit',
            'content-length',
            'content-type',
            'x-client-info',
        ],
    }),
);

// Configure Express to parse incoming JSON data and cookies
app.use(express.json());
app.use(cookieParser());

// set up passport for JWT auth
passport.use(
    new JwtStrategy(
        {
            // we use custom function because we want to extract the token from the cookie
            // if it's not present in the Authorization header
            jwtFromRequest: (req: Request) => {
                let token: string | null = ExtractJwt.fromAuthHeaderAsBearerToken()(req);
                // Extract token from the access_token cookie
                if (!token && req.cookies && req.cookies.access_token) {
                    token = req.cookies.access_token;
                }
                return token;
            },
            secretOrKey: JWT_SECRET,
        },
        async (jwt_payload, done) => {
            try {
                return done(null, jwt_payload);
            } catch (err) {
                return done(err, false);
            }
        }
    )
);
app.use(passport.initialize());

// helper middleware to authenticate requests
const isAuthenticated = passport.authenticate('jwt', { session: false });

// add your custom routes here

// The rest module provides a PostgREST-compatible API for accessing the database
// This will be the entry point for all REST requests
const restHandler = getRestHandler(dbSchemas, {debugFn: debug('subzero:rest')});
app.use('/rest/v1', isAuthenticated, restHandler);

// Serve static files from the 'public' directory
if (staticDir && fs.existsSync(staticDir)) {
    app.use(express.static(staticDir));
}

// register error handlers
app.use(onSubzeroError);

async function init() {
    // Initialize the rest module on startup
    // This is where the database schema is introspected
    await restInit(
        app, // Express app (this is used to store the global subzero instance)
        'postgresql', // Database type, can be 'postgresql', 'sqlite'
        dbPool, // Database connection pool
        dbSchemas, // Database schemas to expose
        {
            // use this when you want to delegate permissions to the database
            useInternalPermissionsCheck: false,
            // use this when you want to use manually defined permissions (usually with sqlite)
            //permissions,
            debugFn: debug('subzero:rest'),
        }
    );
}

// in dev mode let vite (and the subzero plugin for vite) handle the server start
function gracefulShutdown() {
    // Perform any necessary cleanup operations here
    console.log("Shutting down gracefully...");
    process.exit();
}

const port = PORT || 3000;
const server = app.listen(port, async () => {
    try {
        await init();
    } catch (e) {
        server.close();
        console.error(e);
        process.exit(1);
    }
    process.on('SIGINT', gracefulShutdown);
    process.on('SIGTERM', gracefulShutdown);
    console.log(`Listening on port ${port}...`);
});

```

## Lambda / Cloudflare Workers / Vercel

The code is similar in structure to the node example above.
Below we list only the parts that need to be changed.

```typescript
// switch import to @subzerocloud/rest-web
import { init as restInit, getRequestHandler, onSubzeroError} from '@subzerocloud/rest-web'

// use a different router instead of express
import { Router } from 'itty-router';
const router = Router();

// partial mock of express app, this is used by subzero rest module
const settings = {};
const app = {
    set: function (name: string, value: any) {
        settings[name] = value;
    },
    get: function(name: string) {
        return settings[name];
    },
    use: function (fn) {
        router.all('*', fn);
    },
}

// monkey-patch the web api Request object to match the express Request object
const withExpressRequest = (req) => {
    const url = new URL(req.url);
    req.get = (name) => {
        switch (name) {
            case 'host':
                return url.host;
            default:
                return req.headers.get(name);
        }

    };
    req.protocol = url.protocol.replace(':', '');
    req.originalUrl = url.pathname + url.search;
    req.path_prefix = '/rest/v1/';
    req.user = { role: dbAnonRole };
    req.app = app
}
router.all('*', withExpressRequest);

let subzeroInitialized = false;
router.all('*', async (req, event) => {
    if (!subzeroInitialized) {
        await init();
        subzeroInitialized = true;
    }
});

// attach the rest handler to the router
const restHandler = getRestHandler(dbSchemas, {debugFn: debug('subzero:rest')});
router.all('/rest/v1/*', isAuthenticated, restHandler);


// use this code to handle the request (select the one that matches your platform)

// cloudflare workers
export default { ...router } // this looks pointless, but trust us

// bun
export default router;

// vercel/next.js
export const GET = router.fetch
export const POST = router.fetch
```

[*] MySQL and ClickHouse support is implemented only in the core library and you would have to use lower level functions. The higher level functions (`init`, `getRestHandler`, etc) do not yet support these databases.