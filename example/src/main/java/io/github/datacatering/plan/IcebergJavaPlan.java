package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.api.model.DateType;
import io.github.datacatering.datacaterer.api.model.DecimalType;
import io.github.datacatering.datacaterer.api.model.DoubleType;
import io.github.datacatering.datacaterer.api.model.TimestampType;
import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

import java.util.List;
import java.util.Map;

public class IcebergJavaPlan extends PlanRun {
    {
        var accountTask = iceberg("customer_accounts", "/opt/app/data/customer/iceberg", "account.accounts")
                .fields(
                        field().name("account_id").regex("ACC[0-9]{8}").unique(true),
                        field().name("balance").type(new DecimalType(5, 2)).min(1).max(1000),
                        field().name("created_by").sql("CASE WHEN status IN ('open', 'closed') THEN 'eod' ELSE 'event' END"),
                        field().name("name").expression("#{Name.name}"),
                        field().name("open_time").type(TimestampType.instance()).min(java.sql.Date.valueOf("2022-01-01")),
                        field().name("status").oneOf("open", "closed", "suspended", "pending")
                )
                .count(count().records(100));

        var transactionTask = iceberg("customer_transactions", "/opt/app/data/customer/iceberg", "account.transactions")
                .fields(
                        field().name("account_id"),
                        field().name("full_name"),
                        field().name("amount").type(DoubleType.instance()).min(1).max(100),
                        field().name("time").type(TimestampType.instance()).min(java.sql.Date.valueOf("2022-01-01")),
                        field().name("date").type(DateType.instance()).sql("DATE(time)")
                )
                .count(count().recordsPerFieldGenerator(generator().min(0).max(5), "account_id", "full_name"));

        var config = configuration()
                .generatedReportsFolderPath("/opt/app/data/report")
                .enableUniqueCheck(true);

        var myPlan = plan().addForeignKeyRelationship(
                accountTask, List.of("account_id", "name"),
                List.of(Map.entry(transactionTask, List.of("account_id", "full_name")))
        );

        execute(myPlan, config, accountTask, transactionTask);
    }
}
