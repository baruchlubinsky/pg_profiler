=Database setup=

Run postgres.

Setup the database:

$ createuser -l metrics_server
$ createdb -O metrics_server metrics_server

To interact with the database run `./db_console`.