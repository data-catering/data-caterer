//package com.github.pflooky.datacaterer.java.api;
//
//import io.github.datacatering.datacaterer.api.model.ArrayType;
//import io.github.datacatering.datacaterer.api.model.Constants;
//import io.github.datacatering.datacaterer.api.model.DateType;
//import io.github.datacatering.datacaterer.api.model.DoubleType;
//import io.github.datacatering.datacaterer.api.model.IntegerType;
//import io.github.datacatering.datacaterer.api.model.TimestampType;
//
//import java.sql.Date;
//import java.sql.Timestamp;
//import java.util.List;
//import java.util.Map;
//
//public class DocumentationJavaPlanRun extends PlanRun {
//    {
//        String baseFolder = "src/test/resources/sample/java/documentation";
//        String[] accountStatus = {"open", "closed", "pending", "suspended"};
//        var jsonTask = json("account_info", baseFolder + "/json", Map.of(Constants.SAVE_MODE(), "overwrite"))
//                .schema(
//                        field().name("account_id").regex("ACC[0-9]{8}"),
//                        field().name("year").type(IntegerType.instance()).sql("YEAR(date)"),
//                        field().name("balance").type(DoubleType.instance()).min(10).max(1000),
//                        field().name("date").type(DateType.instance()).min(Date.valueOf("2022-01-01")),
//                        field().name("status").sql("element_at(sort_array(update_history.updated_time), 1)"),
//                        field().name("update_history")
//                                .type(ArrayType.instance())
//                                .schema(
//                                        field().name("updated_time").type(TimestampType.instance()).min(Timestamp.valueOf("2022-01-01 00:00:00")),
//                                        field().name("status").oneOf(accountStatus)
//                                ),
//                        field().name("customer_details")
//                                .schema(
//                                        field().name("name").sql("_join_txn_name"),
//                                        field().name("age").type(IntegerType.instance()).min(18).max(90),
//                                        field().name("city").expression("#{Address.city}")
//                                ),
//                        field().name("_join_txn_name").expression("#{Name.name}").omit(true)
//                )
//                .count(count().records(100));
//
//        var csvTxns = csv("transactions", baseFolder + "/csv", Map.of(Constants.SAVE_MODE(), "overwrite", "header", "true"))
//                .schema(
//                        field().name("account_id"),
//                        field().name("txn_id"),
//                        field().name("name"),
//                        field().name("amount").type(DoubleType.instance()).min(10).max(100),
//                        field().name("merchant").expression("#{Company.name}")
//                )
//                .count(
//                        count()
//                                .records(100)
//                                .recordsPerColumnGenerator(generator().min(1).max(2), "account_id", "name")
//                );
//
//        var foreignKeySetup = plan()
//                .addForeignKeyRelationship(
//                        jsonTask, List.of("account_id", "_join_txn_name"),
//                        List.of(Map.entry(csvTxns, List.of("account_id", "name")))
//                );
//
//        var postgresAcc = postgres("my_postgres", "jdbc:...")
//                .table("public.accounts")
//                .schema(
//                        field().name("account_id")
//                );
//        var postgresTxn = postgres(postgresAcc)
//                .table("public.transactions")
//                .schema(
//                        field().name("account_id").type(DoubleType.instance()).enableEdgeCases(true).edgeCaseProbability(0.1)
//                );
//        plan().addForeignKeyRelationship(
//                postgresAcc, List.of("account_id", "name"),
//                List.of(Map.entry(postgresTxn, List.of("account_id", "name")))
//        );
//
//        var csvTask = csv("my_csv", "s3a://my-bucket/csv/accounts")
//                .schema(
//                        field().name("account_id")
//      );
//
//        var s3Configuration = configuration()
//                .runtimeConfig(Map.of(
//                        "spark.hadoop.fs.s3a.directory.marker.retention", "keep",
//                        "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true",
//                        "spark.hadoop.fs.defaultFS", "s3a://my-bucket",
//                        //can change to other credential providers as shown here
//                        //https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Changing_Authentication_Providers
//                        "spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
//                        "spark.hadoop.fs.s3a.access.key", "access_key",
//                        "spark.hadoop.fs.s3a.secret.key", "secret_key"
//                ));
//
//        execute(s3Configuration, csvTask);
//
//        execute(foreignKeySetup, configuration().generatedReportsFolderPath(baseFolder + "/report"), jsonTask, csvTxns);
//    }
//}
