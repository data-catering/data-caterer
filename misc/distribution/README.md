#### Distribution

##### Docker

```shell
gradle clean :api:shadowJar :app:shadowJar
docker build --build-arg "APP_VERSION=0.7.0" --build-arg "SPARK_VERSION=3.5.0" --no-cache -t datacatering/data-caterer:0.7.0 .
docker run -d -i -p 9898:9898 -e DEPLOY_MODE=standalone -v data-caterer-data:/opt/data-caterer --name datacaterer datacatering/data-caterer:0.7.0
#open localhost:9898
```

##### Java 17 VM Options

```shell
--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
```
-Dlog4j.configurationFile=classpath:log4j2.properties
