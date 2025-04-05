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
- [Performance Optimizations](#performance-optimizations)
- [MySQL Setup using Docker](#mysql-setup-using-docker)
- [SQL Schema and Stored Procedure](#sql-schema-and-stored-procedure)
- [Environment Variables for EasyREST](#environment-variables-for-easyrest)
- [Building the Plugin](#building-the-plugin)
- [Running EasyREST Server with the Plugin](#running-easyrest-server-with-the-plugin)
- [Testing API Endpoints](#testing-api-endpoints)
- [Working with Tables, Views, and Context Variables](#working-with-tables-views-and-context-variables)
- [License](#license)

---

## Prerequisites

- [Docker](https://www.docker.com) installed on your machine.
- [Go 1.23.6](https://golang.org/dl/) or later.
- Basic knowledge of MySQL and Docker.

## Performance Optimizations

The plugin includes several performance optimizations:

1. **Connection Pool Management:**
   - Configurable maximum open connections (default: 100)
   - Configurable idle connections (default: 20)
   - Connection lifetime management
   - Idle connection timeout

2. **Bulk Operations:**
   - Memory-efficient result handling
   - Pre-allocated memory for large result sets
   - Batch processing for large operations

3. **Transaction Optimizations:**
   - Automatic transaction management
   - Configurable transaction timeouts
   - Proper connection release

4. **Query Parameters:**
   - `maxOpenConns` - Maximum number of open connections (default: 100)
   - `maxIdleConns` - Maximum number of idle connections (default: 20)
   - `connMaxLifetime` - Connection reuse time in minutes (default: 5)
   - `connMaxIdleTime` - Connection idle time in minutes (default: 10)
   - `timeout` - Query timeout in seconds (default: 30)
   - `parseTime` - Parse MySQL TIME/TIMESTAMP/DATETIME as time.Time (recommended: true)

Example URI with all optimization parameters:

- **Via Environment Variable:**
  ```bash
  export ER_DB_MYSQL="mysql://root:root@localhost:3307/easyrestdb?maxOpenConns=100&maxIdleConns=20&connMaxLifetime=5&connMaxIdleTime=10&timeout=30&parseTime=true"
  ```
- **Via Configuration File:**
  ```yaml
  plugins:
    mysql: # Or any name you choose for the plugin instance
      uri: mysql://root:root@localhost:3307/easyrestdb?maxOpenConns=100&maxIdleConns=20&connMaxLifetime=5&connMaxIdleTime=10&timeout=30&parseTime=true
      path: ./easyrest-plugin-mysql # Path to the plugin binary
  ```

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
- **Host Port:** `3307` (mapped to MySQL's default port 3306 in the container)
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
# --- Database Connection ---
# The URI for the MySQL database. The plugin will be selected when the URI scheme is 'mysql://'.
export ER_DB_MYSQL="mysql://root:root@localhost:3307/easyrestdb?parseTime=true"

# --- JWT Authentication ---
# Secret for HS* algorithms OR path to public key file for RS*/ES*/PS* algorithms
export ER_TOKEN_SECRET="your-secret-key"
# export ER_TOKEN_PUBLIC_KEY="/path/to/public.key"

# Claim in the JWT token to use as the user identifier
export ER_TOKEN_USER_SEARCH="sub"

# --- General Settings ---
# Default timezone for context propagation.
export ER_DEFAULT_TIMEZONE="GMT"

# Optional: Enable/disable scope checking (default: true if token secret/key is set)
# export ER_CHECK_SCOPE="true"
```

**Note:** If both a configuration file and environment variables are present, the configuration file settings take precedence for overlapping parameters. The `path` for the plugin can only be set via the configuration file.

---

## Building the Plugin

Clone the repository for the EasyREST MySQL Plugin and build the plugin binary. In the repository root, run:

```bash
go build -o easyrest-plugin-mysql mysql_plugin.go
```

This produces the binary `easyrest-plugin-mysql` which must be in your PATH or referenced by the EasyREST server.

---

## Running EasyREST Server with the Plugin

Download and install the pre-built binary for the EasyREST Server from the [EasyREST Releases](https://github.com/onegreyonewhite/easyrest/releases) page. Once installed, ensure the configuration is set up (either via `config.yaml` or environment variables) and run the server binary.

**Using a Configuration File (Recommended):**

1. **Create the `config.yaml` file:** Save the configuration example above (or your customized version) as `config.yaml` (or any name you prefer).
2. **Place the plugin binary:** Ensure the compiled `easyrest-plugin-mysql` binary exists at the location specified in the `path` field within `config.yaml` (e.g., in the same directory as the `easyrest-server` binary if `path: ./easyrest-plugin-mysql` is used).
3. **Run the server:** Execute the EasyREST server binary, pointing it to your configuration file using the `--config` flag:

    ```bash
    ./easyrest-server --config config.yaml
    ```

    *   The server reads `config.yaml`.
    *   It finds the `plugins.mysql` section.
    *   It sees the `mysql://` scheme in the `uri` and knows to use a MySQL plugin.
    *   It uses the `path` field (`./easyrest-plugin-mysql`) to locate and execute the plugin binary.
    *   The server then communicates with the plugin via RPC to handle requests to `/api/mysql/...`.

**Using Environment Variables:**

1. **Set Environment Variables:** Define the necessary `ER_` variables as shown previously.
2. **Place the plugin binary:** The `easyrest-plugin-mysql` binary *must* be located either in the same directory as the `easyrest-server` binary or in a directory included in your system's `PATH` environment variable.
3. **Run the server:**
    ```bash
    ./easyrest-server
    ```
    *   The server detects `ER_DB_MYSQL` starting with `mysql://`.
    *   It searches for an executable named `easyrest-plugin-mysql` in the current directory and in the system `PATH`.
    *   If found, it executes the plugin binary and communicates via RPC.

    **Limitation:** You cannot specify a custom path or name for the plugin binary when using only environment variables.

---

## Testing API Endpoints

1. **Creating a JWT Token:**

   EasyREST uses JWT for authentication. Use your preferred method to generate a token with the appropriate claims (including the subject claim specified by `ER_TOKEN_USER_SEARCH`). For example, you might generate a token using a script or an [online tool](https://jwt.io/) and then export it:

   ```bash
   export TOKEN="your_generated_jwt_token"
   ```

2. **Calling the API:**

   With the server running, you can test the endpoints. The API path depends on the name you gave the plugin instance in your `config.yaml` (e.g., `mysql`) or defaults to `mysql` if using `ER_DB_MYSQL`. Assuming the name is `mysql`:

   ```bash
   # Fetch users
   curl -H "Authorization: Bearer $TOKEN" "http://localhost:8080/api/mysql/users/?select=id,name,created_at"

   # Create users (setup name from JWT-token 'sub' claim)
   curl -X POST "http://localhost:8080/api/mysql/users/" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '[{"name": "erctx.claims_sub"},{"name": "request.claims.sub"}]'

   # Call the stored procedure (or function)
   curl -X POST "http://localhost:8080/api/mysql/rpc/doSomething/" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"jsonParam": "test"}'
   ```

---

## Working with Tables, Views, and Context Variables

This plugin allows you to interact with your MySQL database tables and views directly through the EasyREST API. It also provides a powerful mechanism for injecting context from the incoming request (like user information from a JWT) directly into your SQL session.

### Interacting with Tables and Views

Once the plugin is configured and connected to your database, EasyREST exposes API endpoints for your tables and views under the path `/api/{plugin_name}/{table_or_view_name}/` (e.g., `/api/mysql/users/`).

-   **Read (SELECT):** Use `GET` requests. You can specify fields (`?select=col1,col2`), filters (`?where=...`), ordering (`?orderBy=...`), grouping (`?groupBy=...`), limit (`?limit=...`), and offset (`?offset=...`). Views are accessible via `GET` just like tables.
-   **Create (INSERT):** Use `POST` requests with a JSON array of objects in the request body.
-   **Update (UPDATE):** Use `PATCH` requests. Provide the data to update in the request body and specify the rows to update using `?where=...` query parameters.
-   **Delete (DELETE):** Use `DELETE` requests, specifying rows to delete using `?where=...` query parameters.

Refer back to the [Testing API Endpoints](#testing-api-endpoints) section for basic `curl` examples.

### Schema Introspection and Type Mapping

The plugin automatically introspects your database schema (tables and views) and makes it available via the `/api/{plugin_name}/schema` endpoint. This schema reflects the columns and their basic types.

**Example Table Creation:**

```sql
-- Example table definition
CREATE TABLE IF NOT EXISTS products (
  id INT AUTO_INCREMENT PRIMARY KEY,
  sku VARCHAR(50) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  description TEXT NULL,
  price DECIMAL(10, 2) DEFAULT 0.00,
  stock_count INT DEFAULT 0,
  is_active BOOLEAN DEFAULT TRUE,
  created_by VARCHAR(100), -- Could store user ID/sub
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

When the plugin reads this schema, it maps MySQL data types to simpler, JSON-friendly types:

| MySQL Data Type Category        | Example Types                               | Schema Type | Notes                                    |
| :------------------------------ | :------------------------------------------ | :---------- | :--------------------------------------- |
| Integer Types                   | `INT`, `TINYINT`, `BIGINT`                  | `integer`   |                                          |
| Fixed-Point / Floating-Point  | `DECIMAL`, `NUMERIC`, `FLOAT`, `DOUBLE`     | `number`    |                                          |
| String Types                    | `VARCHAR`, `CHAR`, `TEXT`, `ENUM`, `JSON`   | `string`    |                                          |
| Binary Types                    | `BLOB`, `BINARY`, `VARBINARY`               | `string`    | Returned as base64 or hex string usually |
| Date/Time Types                 | `DATE`, `DATETIME`, `TIMESTAMP`, `TIME`     | `string`    | Formatted as string (e.g., `YYYY-MM-DD`) |
| Boolean                         | `BOOLEAN`, `BOOL` (`TINYINT(1)`)            | `integer`   | Typically represented as 0 or 1          |

The `schema` endpoint also indicates which fields are nullable (`x-nullable: true`) and which are part of the primary key (`readOnly: true`, implying they aren't required in inserts/updates via the API if auto-generated).

### Using Context Variables (`erctx.` / `request.`)

A key feature is the ability to use data from the EasyREST request context within your SQL queries without explicitly passing it in every API call's `where` clause or body. This is done using special prefixes in your API request data:

-   `erctx.<path>`: Accesses data from the general EasyREST context (like JWT claims).
-   `request.<path>`: Accesses data specific to the current request (often overlaps with `erctx`).

**How it Works:**

1.  **API Request:** You send a request, potentially including these special values:
    ```bash
    # Example: Create a product, setting created_by from JWT's 'sub' claim
    curl -X POST "http://localhost:8080/api/mysql/products/" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d '[{"sku": "PROD001", "name": "My Product", "created_by": "erctx.claims_sub"}]'
    ```
2.  **Plugin Injection:** Before executing the main SQL query (INSERT, SELECT, UPDATE, DELETE, or CALL), the plugin takes *all* available context variables, flattens their keys (e.g., `claims.sub` becomes `claims_sub`), and sets them as MySQL session variables using `SET @erctx_<key> = ?, @request_<key> = ?`. For the example above, it would effectively run:
    ```sql
    SET @erctx_claims_sub = 'user123', @request_claims_sub = 'user123', /* ... other context vars ... */ ;
    ```
    *(Assuming the JWT `sub` claim was `user123`)*
3.  **SQL Execution:** Your main SQL query (or trigger, or view logic) can now reference these session variables (e.g., `@request_claims_sub`). These variables exist only for the duration of the connection used for that specific API request.

This is extremely useful for:

-   **Audit Trails:** Automatically recording which user created or modified a row.
-   **Row-Level Security:** Filtering data based on the user making the request within views or triggers.
-   **Multi-Tenancy:** Scoping queries to a specific tenant ID derived from the user's context.

### Examples in SQL

Here are conceptual examples of how to leverage session variables set from the context:

**1. Trigger for Ownership/Validation:**

Imagine you want to ensure only the user who created a product (stored in `created_by`) can update its `name` or `description`.

```sql
DELIMITER //

CREATE TRIGGER before_product_update
BEFORE UPDATE ON products
FOR EACH ROW
BEGIN
    -- Check if the user making the request (@request_claims_sub)
    -- is the one who created the product.
    -- Allow update only if they match OR if the user is an admin (e.g., from a role claim)
    IF OLD.created_by != @request_claims_sub AND @request_claims_role != 'admin' THEN
        -- If trying to update restricted fields
        IF NEW.name != OLD.name OR NEW.description != OLD.description THEN
             SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Authorization failed: You can only modify products you created.';
        END IF;
    END IF;
END;
//

DELIMITER ;
```
*Note: This assumes your JWT contains a `sub` claim (mapped to `@request_claims_sub`) and potentially a `role` claim (mapped to `@request_claims_role`). You need `token_user_search: sub` in your EasyREST config.*

**2. Stored Function for User-Specific Data (Alternative to View):**

**Important Note:** MySQL **does not allow** the direct use of session variables (like `@request_claims_sub`) within a `CREATE VIEW` statement. Attempting to do so will result in `ERROR 1351 (HY000)`. Views must have static definitions.

To achieve row-level security based on the current user context, use a **Stored Function** instead. This function *can* access session variables set by the plugin.

Create a function that returns products filtered by the `created_by` field matching the current user's `sub` claim.

```sql
DELIMITER //

CREATE FUNCTION getMyProducts()
RETURNS JSON
DETERMINISTIC
READS SQL DATA -- Important: Indicates the function reads data
BEGIN
    DECLARE result JSON;
    SELECT JSON_ARRAYAGG(JSON_OBJECT(
            'id', id,
            'sku', sku,
            'name', name,
            'price', price,
            'stock_count', stock_count,
            'is_active', is_active,
            'created_at', created_at
           ))
    INTO result
    FROM products
    WHERE created_by = @request_claims_sub; -- Session variable works here!

    RETURN COALESCE(result, JSON_ARRAY()); -- Return empty array if no results
END;
//

DELIMITER ;
```

Now, you can call this function using the EasyREST RPC endpoint:

```bash
# Call the function to get products for the current user
curl -X POST "http://localhost:8080/api/mysql/rpc/getMyProducts/" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{}' # No parameters needed for this function
```

The plugin will inject the context (setting `@request_claims_sub`), call the `getMyProducts()` function, and return the JSON array of products belonging to the user.

**Alternative: Application-Level Filtering:**
You can always achieve the same result by requiring the client to specify the filter in the `GET` request:
```bash
curl -H "Authorization: Bearer $TOKEN" \
"http://localhost:8080/api/mysql/products/?select=id,sku,name&where=created_by%3Derctx.claims_sub"
```
This uses the `erctx.claims_sub` directly in the `where` parameter, which the plugin resolves before executing the `SELECT` query.

---

## License

EasyREST MySQL Plugin is licensed under the Apache License 2.0.  
See the file "LICENSE" for more information.

© 2025 Sergei Kliuikov
