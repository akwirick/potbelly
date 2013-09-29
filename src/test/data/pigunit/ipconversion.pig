REGISTER target/potbelly-0.1.jar;
DEFINE IPToInt com.akwirick.potbelly.IPToInt;

A = LOAD 'string_ips.txt' AS (ip:CHARARRAY);
B = FOREACH A GENERATE IPToInt(ip);
DUMP B;
