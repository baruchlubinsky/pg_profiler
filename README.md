Database setup
==

Run postgres.

Setup the database:

<pre>
$ createuser -l metrics_server
$ createdb -O metrics_server metrics_server
</pre>

To interact with the database run `./db_console`.
