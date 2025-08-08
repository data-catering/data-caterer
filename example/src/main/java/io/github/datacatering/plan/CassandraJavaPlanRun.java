package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.api.model.DoubleType;
import io.github.datacatering.datacaterer.api.model.TimestampType;
import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

public class CassandraJavaPlanRun extends PlanRun {
    {
        var accountTask = cassandra("customer_cassandra", "host.docker.internal:9042")
                .table("account", "accounts")
                .fields(
                        field().name("account_id").regex("ACC[0-9]{8}").primaryKey(true),
                        field().name("amount").type(DoubleType.instance()).min(1).max(1000),
                        field().name("created_by").sql("CASE WHEN status IN ('open', 'closed') THEN 'eod' ELSE 'event'"),
                        field().name("name").expression("#{Name.name}"),
                        field().name("open_time").type(TimestampType.instance()),
                        field().name("status").oneOf("open", "closed", "suspended", "pending")
                );

        var config = configuration()
                .generatedReportsFolderPath("/opt/app/data/report")
                .enableUniqueCheck(true);

        execute(config, accountTask);
    }
}
