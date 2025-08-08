package io.github.datacatering.plan;

import io.github.datacatering.datacaterer.javaapi.api.PlanRun;

public class PostgresAutoJavaPlan extends PlanRun {
    {
        var accountTask = postgres("customer_accounts", "jdbc:postgresql://host.docker.internal:5432/customer");

        var config = configuration().enableGeneratePlanAndTasks(true);
        execute(config, accountTask);
    }
}
