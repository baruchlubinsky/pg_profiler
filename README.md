Database setup
==

Run postgres.

Setup the database:

<code>
   $ createuser -l metrics_server
   $ createdb -O metrics_server metrics_server
</code>

To interact with the database run `./db_console`.
