# Data Caterer - Docker Compose

If you want to try out data caterer generating data for various data sources, you do use the following docker-compose file.

All you need to do is define which data source you want to run with via a command like below:

```shell
DATA_SOURCE=postgres docker-compose up -d datacaterer
```

You can change `DATA_SOURCE` to one of the following:  
- postgres
- mysql
- cassandra
- solace
- kafka
- http
