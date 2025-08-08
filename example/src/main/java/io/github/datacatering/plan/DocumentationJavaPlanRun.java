package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.api.model.ArrayType;
import io.github.datacatering.datacaterer.api.model.Constants;
import io.github.datacatering.datacaterer.api.model.DateType;
import io.github.datacatering.datacaterer.api.model.DoubleType;
import io.github.datacatering.datacaterer.api.model.IntegerType;
import io.github.datacatering.datacaterer.api.model.TimestampType;
import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class DocumentationJavaPlanRun extends PlanRun {
    {
        String baseFolder = "/opt/app/data";
        String[] accountStatus = {"open", "closed", "pending", "suspended"};
        var jsonTask = json("account_info", baseFolder + "/json", Map.of(Constants.SAVE_MODE(), "overwrite"))
                .fields(
                        field().name("account_id").regex("ACC[0-9]{8}"),
                        field().name("year").type(IntegerType.instance()).sql("YEAR(date)"),
                        field().name("balance").type(DoubleType.instance()).min(10).max(1000),
                        field().name("date").type(DateType.instance()).min(Date.valueOf("2022-01-01")),
                        field().name("status").sql("element_at(sort_array(update_history.updated_time), 1)"),
                        field().name("update_history")
                                .type(ArrayType.instance())
                                .fields(
                                        field().name("updated_time").type(TimestampType.instance()).min(Timestamp.valueOf("2022-01-01 00:00:00")),
                                        field().name("status").oneOf((Object[]) accountStatus)
                                ),
                        field().name("customer_details")
                                .fields(
                                        field().name("name").sql("_join_txn_name"),
                                        field().name("age").type(IntegerType.instance()).min(18).max(90),
                                        field().name("city").expression("#{Address.city}")
                                ),
                        field().name("_join_txn_name").expression("#{Name.name}").omit(true)
                )
                .count(count().records(100))
                .validations(
                        validation().expr("year >= 2022")
                );

        var csvTxns = csv("transactions", baseFolder + "/csv", Map.of(Constants.SAVE_MODE(), "overwrite", "header", "true"))
                .fields(
                        field().name("account_id"),
                        field().name("txn_id"),
                        field().name("name"),
                        field().name("amount").type(DoubleType.instance()).min(10).max(100),
                        field().name("merchant").expression("#{Company.name}")
                )
                .count(
                        count()
                                .records(100)
                                .recordsPerFieldGenerator(generator().min(1).max(2), "account_id", "name")
                )
                .validations(
                        validation().expr("LENGTH(merchant) > 0").description("Merchant should not be empty"),
                        validation().expr("amount > 15").description("Most amounts should be greater than $15").errorThreshold(5)
                );

        var postgresValidateTask = postgres("my_postgres", "jdbc:postgresql://host.docker.internal:5432/customer")
                .table("account", "accounts")
                .validations(
                        validation().field("account_id").isNull(true),
                        validation().field("name").matches("[A-Z][a-z]+ [A-Z][a-z]+").errorThreshold(0.2).description("Some names have different formats"),
                        validation().field("balance").greaterThan(0, false).errorThreshold(10).description("Account can have negative balance if overdraft"),
                        validation().expr("CASE WHEN status == 'closed' THEN isNotNull(close_date) ELSE isNull(close_date) END"),
                        validation().unique("account_id", "name"),
                        validation().groupBy("account_id", "name").max("login_retry").lessThan(10)
                );

        var foreignKeySetup = plan()
                .addForeignKeyRelationship(
                        jsonTask, List.of("account_id", "_join_txn_name"),
                        List.of(Map.entry(csvTxns, List.of("account_id", "name")))
                );

        var conf = configuration()
                .generatedReportsFolderPath(baseFolder + "/report")
                .enableSinkMetadata(true)
                .enableValidation(true);

        execute(foreignKeySetup, conf, jsonTask, csvTxns);
    }
}
