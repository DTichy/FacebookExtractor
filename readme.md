# Facebook Extractor

This tools helps to extract posts, comments, subcomments and all reactions, aggregated as total sum, from a given Facebook Page and saves them in a SQL Server Database.

## Getting Started

Pull the repo and make sure, that all python packages are available.
Execute the script sql/sqlserver/CreateSocialMediaCollector_SQLServer.sql, to set up the database.
Configure the config/app.default.config file and optional rename it.
Fill up a list with id, use the example file config/ids_example.csv

Start the program with

```
python .\src\main.py [PATH TO CONFIG FILE] [PATH TO ID LIST] [STARTDATETIME (Format YYYYMMDDHHmm)] [ENDDATETIME (Format YYYYMMDDHHmm)]
```

for example - to extract all posts from given page list "ids.csv" in between 2018-01-01-0000 and 2018-01-01-2359 use:

```
python .\src\main.py .\config\app.default.config .\config\ids_example.csv 201801010000 201801012359
```

#### Hint
Comment data will be saved in a temporary table, called "FacebookCommentCurrent", and can be shifted to the main table "FacebookComment" over the T-Sql Procedure:

```
exec PersistCurrentData
```

This will not slow down the inserts after the table grows bigger. I would recommend to set up a job, to save the data periodically.

### Prerequisites

Python 3.6
See requirements.txt

### Additional Information

Program only works with Facebook pages and it extracts all posts in the given time period and all comments and subcomments from those posts, over the GraphApi.
Same pages and time periods can be extracted multiple times, but no historization will take place in the database. Data will be updated, if existing post, comment or subcomment in database differs. Every data record will save the date and time of the extraction.

### Open Todos

Some todos are still open
- Refactor code
- Implement user info for pagination wait
- Add multiple data destinations (Postgres, CSV, ...)


## License

This project is licensed under the GPL-3.0 License - see the [LICENSE.md](LICENSE.md) file for details

