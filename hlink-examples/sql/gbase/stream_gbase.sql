CREATE TABLE source
(
    id             int,
    name           varchar,
    price          decimal,
    money          double,
    message        string,
    age            tinyint,
    todayTimestamp timestamp,
    todayDate      date,
    todayTime      time
) WITH (
      'connector' = 'gbase-x',
      'url' = 'jdbc:gbase://gbase:5258/dev_db',
      'table-name' = 'sink',
      'schema' = 'dev_db',
      'username' = 'dev',
      'password' = 'dev123',
      'sink.buffer-flush.max-rows' = '1',
      'sink.allReplace' = 'false'
      );

CREATE TABLE sink
(
    id             int,
    name           varchar,
    price          decimal,
    money          double,
    message        string,
    age            tinyint,
    todayTimestamp timestamp,
    todayDate      date,
    todayTime      time
) WITH (
      'connector' = 'console',
      'number-of-rows' = '50'
      );

insert into source
select *
from sink;
