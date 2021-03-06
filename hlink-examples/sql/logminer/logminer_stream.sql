CREATE TABLE source
(
    ID             bigint,
    TTIME          DATE,
    TTIMESTAMP     TIMESTAMP
) WITH (
      'connector' = 'oraclelogminer-x'
      ,'jdbcUrl' = 'jdbc:oracle:thin:@127.0.0.1:1521:xe'
      ,'username' = 'username'
      ,'password' = 'password'
      ,'cat' = 'insert,delete,update'
      ,'table' = 'schema.table'
      ,'timestamp-format.standard' = 'SQL'
      );

CREATE TABLE sink
(
    ID             bigint,
    TTIME          DATE,
    TTIMESTAMP     TIMESTAMP
) WITH (
      'connector' = 'console'
      );

insert into sink
select *
from source u;
