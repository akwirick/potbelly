#Potbelly
This is a small library of [Pig](http://pig.apache.org/) UDFs for simplifying specific data processing use cases.  At this time, it contains only one eval function to convert IPV4 addresses from dot notation to Long Integers.

## Build and Usage
The UDF should build with the provided Maven POM file.  You may want to experiment with the dependency settings to tailor the build to your specific Pig and Hadoop environments.  

TO use the UDF you will need to register the JAR file and define the UDF at the top of your Pig script.  For example:

```
pig -x local

---Pig loads up---

REGISTER potbelly-0.1.jar
define IPToInt com.akwirick.potbelly.IPToInt
A = load 'string_ips.txt' as (addresses:CHARARRAY)
B = FOREACH A GENERATE IPToInt(addresses)
DUMP B;
```

## Tests - STILL IN PROGRESS
Unit testing ensures that the basic functionality of the UDF is intact.  Pigtest is still pretty rough, so if you experience problems feel free to send me a message.
