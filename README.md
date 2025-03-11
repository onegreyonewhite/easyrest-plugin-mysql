# EasyREST MySQL Plugin

The **EasyREST MySQL Plugin** is an external plugin for [EasyREST](https://github.com/onegreyonewhite/easyrest) that enables EasyREST to connect to and perform CRUD operations on MySQL databases. This plugin implements the `easyrest.DBPlugin` interface using a MySQL connection pool, session variable injection for context propagation, and transactional stored procedure execution.

**Key Features:**

- **CRUD Operations:** Supports SELECT, INSERT, UPDATE, and DELETE queries.
- **Context Injection:** If query fields or conditions reference context variables (using the `erctx.` prefix), the plugin injects these values into session variables.
- **Stored Procedure Calls:** Executes stored procedures within a transaction, rolling back on error.
- **Connection Pooling:** Uses a MySQL connection pool for optimal performance, with special handling for session variables to avoid race conditions.
- **Deterministic SQL Generation:** Ensures predictable SQL statements by sorting map keys where necessary.
- **UTF8MB4 Support:** The plugin currently operates strictly with `utf8mb4` and `utf8mb4_general_ci` settings (subject to change in the future).

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [MySQL Setup using Docker](#mysql-setup-using-docker)
- [SQL Schema and Stored Procedure](#sql-schema-and-stored-procedure)
- [Environment Variables for EasyREST](#environment-variables-for-easyrest)
- [Building the Plugin](#building-the-plugin)
- [Running EasyREST Server with the Plugin](#running-easyrest-server-with-the-plugin)
- [Testing API Endpoints](#testing-api-endpoints)
- [License](#license)

---

## Prerequisites

- [Docker](https://www.docker.com) installed on your machine.
- [Go 1.23.6](https://golang.org/dl/) or later.
- Basic knowledge of MySQL and Docker.

---

## MySQL Setup using Docker

Run MySQL in a Docker container on a non-standard port (e.g., **3307**) with a pre-created database. Open your terminal and execute:

```bash
docker run --name mysql-easyrest -p 3307:3306 \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=easyrestdb \
  -d mysql:8
```

This command starts a MySQL 8 container:
- **Container Name:** `mysql-easyrest`
- **Host Port:** `3307` (mapped to MySQL’s default port 3306 in the container)
- **Root Password:** `root`
- **Database Created:** `easyrestdb`

---

## SQL Schema and Stored Procedure

Create a SQL script (e.g., `schema.sql`) with the following content. This script creates a `users` table with an auto-increment primary key, a `name` column, and a `created_at` timestamp that defaults to the current time. It also defines a stored procedure (implemented as a function in this example) named `doSomething` which returns a processed message.

```sql
-- schema.sql

-- Create the 'users' table
CREATE TABLE IF NOT EXISTS users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a function 'doSomething'
DELIMITER //
CREATE FUNCTION doSomething(jsonParam TEXT)
RETURNS TEXT
DETERMINISTIC
BEGIN
    RETURN CONCAT('Processed: ', jsonParam);
END;
//
DELIMITER ;
```

To execute this script on the running MySQL server, run:

```bash
docker exec -i mysql-easyrest mysql -uroot -proot easyrestdb < schema.sql
```

---

## Environment Variables for EasyREST

Configure EasyREST to use this MySQL database via the MySQL plugin. Set the following environment variables before starting the EasyREST server:

```bash
export ER_DB_MYSQL="mysql://root:root@tcp(localhost:3307)/easyrestdb?parseTime=true"
export ER_TOKEN_SECRET="your-secret-key"
export ER_TOKEN_USER_SEARCH="sub"
export ER_DEFAULT_TIMEZONE="GMT"
```

- **ER_DB_MYSQL:** The URI for the MySQL database. The plugin will be selected when the URI scheme is `mysql://`.
- **ER_TOKEN_SECRET & ER_TOKEN_USER_SEARCH:** Used by EasyREST for JWT authentication.
- **ER_DEFAULT_TIMEZONE:** Default timezone for context propagation.

---

## Building the Plugin

Clone the repository for the EasyREST MySQL Plugin and build the plugin binary. In the repository root, run:

```bash
go build -o easyrest-plugin-mysql mysql_plugin.go
```

This produces the binary `easyrest-plugin-mysql` which must be in your PATH or referenced by the EasyREST server.

---

## Running EasyREST Server with the Plugin

Download and install the pre-built binary for the EasyREST Server from the [EasyREST Releases](https://github.com/onegreyonewhite/easyrest/releases) page. Once installed, set the environment variables (as above) and run the server binary:

```bash
./easyrest-server
```

The server will detect the `ER_DB_MYSQL` environment variable and launch the MySQL plugin via RPC.

---

## Testing API Endpoints

1. **Creating a JWT Token:**

   EasyREST uses JWT for authentication. Use your preferred method to generate a token with the appropriate claims (including the subject claim specified by `ER_TOKEN_USER_SEARCH`). For example, you might generate a token using a script or an [online tool](https://jwt.io/) and then export it:

   ```bash
   export TOKEN="your_generated_jwt_token"
   ```

2. **Calling the API:**

   With the server running, you can test the endpoints. For example, to fetch users:

   ```bash
   curl -H "Authorization: Bearer $TOKEN" "http://localhost:8080/api/mysql/users/?select=id,name,created_at"
   ```
   
   Create users (setup name from JWT-token `sub` claim):

   ```bash
   curl -X POST "http://localhost:8080/api/mysql/users/" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '[{"name": "erctx.claims_sub"},{"name": "request.claims.sub"}]'
   ```

   To call the stored procedure (or function):

   ```bash
   curl -X POST "http://localhost:8080/api/mysql/rpc/doSomething/" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"jsonParam": "test"}'
   ```

---

## License

EasyREST MySQL Plugin is licensed under the Apache License 2.0.  
See the file "LICENSE" for more information.

© 2025 Sergei Kliuikov
