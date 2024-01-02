//package com.github.pflooky.datacaterer.java.api;
//
//public class ExampleJavaPlanRun extends PlanRun {
//    {
//        var myJson = json("minimal_json", "app/src/test/resources/sample/json/minimal")
//                .schema(schema().addFields(field().name("account_id"), field().name("peter")));
//        var myPostgres = postgres("my_postgres", "url")
//                .table("my.table")
//                .schema(field().name("account_id"))
//                .count(count()
//                        .recordsPerColumn(10, "account_id", "name")
//                        .generator(generator().min(10).max(100))
//                );
//
//        execute(myJson);
//    }
//}
