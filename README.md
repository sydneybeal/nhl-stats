# Intro

After you have cloned this repository to a directory called `data-eng-challenge`, created and activated a python 3.7+ virtualenv, you can run `make init` to install the libraries used for development.

* Note: needed to change requirements.txt to upgrade pandas version to 1.2.4

#### step1
If you look into the code at [nhldata/app.py], in the `main()` function you will see the simple steps of operation.  Fetch the schedule for a given date, get the player stats for those games, write them to S3 (simulated using a Min.io docker container when running `step1`).

The main function also has a 5-min retry loop to account for server errors, when testing you will want to comment out the line `wait_seconds = 60 * 5` and use the line `wait_seconds = 5` instead.

When you are done, running `make step1` should successfully bring up a database (localhost:5432), a minio server ([http://localhost:9000]), run your job and you will see the resultant CSV files in [s3_data/data-bucket/]. *Notee that you will need to type your admin password for a sudo command shortly after running step1.*

Leave this running and use a new terminal for part two as this the running postgres instance you'll be working with.

* Note: You will need to open terminal and chmod -R 755 all of the s3_data/data-bucket directory with the `.csv`s the first time you run the job

#### step2
If the `run_sql` step fails, it may be due to some incompatibility with the syntax and your bash version. 
* In the `catalog_data` section of `Makefile`, you can try changing 
```
find s3_data/data-bucket -name "*.csv" | xargs printf '\\''copy game_stats from '\''%s'\'' WITH (FORMAT csv, HEADER);\n' >> s3_data/load_data.sql
```
to
```
find s3_data/data-bucket -name "*.csv" -exec echo "\copy game_stats from '{}' WITH (FORMAT csv, HEADER);" \; >> s3_data/load_data.sql
```
The `make_sql` step may also not be able to find the db server. Check the logs from the terminal output of step1, you may need to update the `make_sql` part of `Makefile`: `$$(basename $(PWD))-db-1` instead of `$$(basename $(PWD))_db_1`

The [dbt/] directory contains all the configuration for dbt. Tables are documented in `*.yml` files.  This is also where data quality tests for the schema are documented. 

When `dbt test` or in this case `make dbt_test` is executed, dbt will generate tests for those columns based on that configuration and verify that no rows exist that fail the test.

The file [base/nhl/nhl_players.sql] creates a base table of players and their running point tallies. Note the jinja syntax `{{ ref('player_game_stats') }}` to reference other tables by their file name.

The file [mart/nhl/points_leaders.yml] selects the top point scorer (or all the tied point leaders) per team.  And for this table we're only interested in teams where they have a player with at least 1 point.

When you are developing or finished you can run 
* `make dbt_run` to build/rebuild your tables and views when you change the sql
* `make dbt_test` to run the schema tests defined in the yml files.  You should need to change the yml this part should just pass when your SQL is correct.

The final bit of polish is `make points_leaders` which is simulating a user running a report on that data and will chose the top 10 points leaders`

## Finally
As stated above, the steps we are looking to run, which simulate a running job, some data load mechanism and final shaping for use are:
1. `make step1`  -- leaving the terminal open
1. `make step2 points_leaders`  

## Extras
**NHL API Spec**
* [https://raw.githubusercontent.com/erunion/sport-api-specifications/master/nhl/nhl.yaml]
* [Head to the swagger editor](https://editor.swagger.io/) and from the menu select `File -> Import URL` and paste in the aabove swagger address.


**Docker**
* [https://www.docker.com/products/docker-desktop] mac, windows
* [https://docs.docker.com/engine/install/] linux (under "server")


**DBT**
* [https://docs.getdbt.com]

**Minio**
Self hosted, distributed object store with an S3 api
* [https://min.io/]
* [Using with boto3](https://docs.min.io/docs/how-to-use-aws-sdk-for-python-with-minio-server.html)
* [Using with the awscli](https://docs.min.io/docs/aws-cli-with-minio.html)


