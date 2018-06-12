# Balala

 
**[Anima](https://github.com/biezhi/anima)**

`Balala` is Operating database Tools in vertx, Its source code comes from Anima。
But it doesn't have some of the functions of Anima,like 'join ...','limit','all',like stream...... 
It just encapsulates a Future on Anima,So refer to Anima documents

**[Document](https://github.com/biezhi/anima/wiki)**


> 📕 create sqlClinet instance,You can use JDBCClient  or MySQL / PostgreSQL client
```java
JsonObject mySQLClientConfig = new JsonObject()
            .put("url", "jdbc:mysql://127.0.0.1:3306/demo")
            .put("user","root")
            .put("password","123456");
        SQLClient mySQLClient = JDBCClient.createShared(vertx, mySQLClientConfig);
        Balala.open(mySQLClient);

```

IF you use MySQL / PostgreSQL client,batchSave can not use
