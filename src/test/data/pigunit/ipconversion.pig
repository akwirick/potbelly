A = LOAD '$input' AS (string_address:CHARARRAY)
B = FOREACH A GENERATE com.akwirick.pig.IPToInt(string_address)
STORE B INTO '$output';