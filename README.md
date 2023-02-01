# MME Market Data Server (MDS) Receiver

## Setup
- Create `/lib` and copy all jars from nasdaq package (copy all the files from the inner folders to /lib ) 
and delete `slf4j-log4j12.jar`, and `log4j.jar`(we will use log4j2 defined in Gradle)
- Create `/config/application.json` from the example
- The dir structure:
```
/app_name
  /config
  /log
  the_jar.jar
```
- To run (async logging enabled):
```
java -jar the_jar.jar -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector
```