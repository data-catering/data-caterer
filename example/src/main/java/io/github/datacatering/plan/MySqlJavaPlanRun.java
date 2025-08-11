package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.api.model.DoubleType;
import io.github.datacatering.datacaterer.api.model.IntegerType;
import io.github.datacatering.datacaterer.api.model.TimestampType;
import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.sql.Date;
import java.util.Map;

public class MySqlJavaPlanRun extends PlanRun {
    {
        String baseFolder = "/opt/app/data";
        
        var accountTask = mysql("customer_mysql", "jdbc:mysql://host.docker.internal:3306/customer")
                .table("account", "accounts")
                .fields(
                        field().name("account_number"),
                        field().name("amount").type(DoubleType.instance()).min(1).max(1000),
                        field().name("created_by_fixed_length").sql("CASE WHEN account_status IN ('open', 'closed') THEN 'eod' ELSE 'event' END"),
                        field().name("name").expression("#{Name.name}"),
                        field().name("open_timestamp").type(TimestampType.instance()).min(Date.valueOf("2022-01-01")),
                        field().name("account_status").oneOf("open", "closed", "suspended", "pending")
                )
                .count(count().records(1000));

        var balancesTask = mysql("customer_mysql", "jdbc:mysql://host.docker.internal:3306/customer")
                .table("account", "balances")  
                .fields(
                        field().name("account_number"),
                        field().name("balance").type(DoubleType.instance()).min(0).max(10000),
                        field().name("balance_date").type(TimestampType.instance()),
                        field().name("account_status")
                )
                .count(count().recordsPerFieldGenerator(generator().min(1).max(5), "account_number"));

        var foreignKeySetup = plan()
                .addForeignKeyRelationship(
                        accountTask, java.util.List.of("account_number", "account_status"),
                        java.util.List.of(java.util.Map.entry(balancesTask, java.util.List.of("account_number", "account_status")))
                );

        var conf = configuration()
                .generatedReportsFolderPath(baseFolder + "/report")
                .enableUniqueCheck(true)
                .enableSinkMetadata(true);

        execute(foreignKeySetup, conf, accountTask, balancesTask);
    }
}