import {createAccordionItem} from "./shared.js";

let historyContainer = document.getElementById("history-container");
const tableHeadersWithKey = [{
    field: "status",
    title: "Status",
    sortable: true,
}, {
    field: "id",
    title: "Run ID",
    sortable: true,
}, {
    field: "createdTs",
    title: "Created Time",
    sortable: true,
}, {
    field: "updatedTs",
    title: "Last Update Time",
    sortable: true,
}, {
    field: "failedReason",
    title: "Fail Reason",
    sortable: true,
}]

Promise.resolve({
    "planExecutionByPlan": [
        {
            "executions": [
                {
                    "id": "16f75cf0-16d1-4a08-be65-e0a4ef803e26",
                    "runs": [
                        {
                            "createdTs": "2024-02-19T10:16:04+08:00",
                            "failedReason": "",
                            "id": "16f75cf0-16d1-4a08-be65-e0a4ef803e26",
                            "name": "my-plan",
                            "runBy": "admin",
                            "status": "started",
                            "updatedBy": "admin",
                            "updatedTs": "2024-02-19T10:16:04+08:00"
                        }, {
                            "createdTs": "2024-02-19T10:16:04+08:00",
                            "failedReason": "",
                            "id": "16f75cf0-16d1-4a08-be65-e0a4ef803e26",
                            "name": "my-plan",
                            "runBy": "admin",
                            "status": "parsed_plan",
                            "updatedBy": "admin",
                            "updatedTs": "2024-02-19T10:16:05+08:00"
                        }, {
                            "createdTs": "2024-02-19T10:16:04+08:00",
                            "failedReason": "java.io.IOException: Failed to open native connection to Cassandra at {localhost:9042} :: Could not reach any contact point, make sure you've provided valid addresses (showing first 2 nodes, use getAllErrors() for more): Node(endPoint=localhost/127.0.0.1:9042, hostId=null, hashCode=25537d2f): [com.datastax.oss.driver.api.core.connection.ConnectionInitException: [s0|control|connecting...] Protocol initialization request, step 1 (OPTIONS): failed to send request (com.datastax.oss.driver.shaded.netty.channel.StacklessClosedChannelException)], Node(endPoint=localhost/0:0:0:0:0:0:0:1:9042, hostId=null, hashCode=d5ff18f): [com.datastax.oss.driver.api.core.connection.ConnectionInitException: [s0|control|connecting...] Protocol initialization request, step 1 (OPTIONS): failed to send request (com.datastax.oss.driver.shaded.netty.channel.StacklessClosedChannelException)]",
                            "id": "16f75cf0-16d1-4a08-be65-e0a4ef803e26",
                            "name": "my-plan",
                            "runBy": "admin",
                            "status": "failed",
                            "updatedBy": "admin",
                            "updatedTs": "2024-02-19T10:16:18+08:00"
                        }
                    ]
                }, {
                    "id": "e83c4eba-6e85-40c6-8409-c1c4c84e0c5d",
                    "runs": [
                        {
                            "createdTs": "2024-02-23T13:59:34+08:00",
                            "failedReason": "",
                            "id": "e83c4eba-6e85-40c6-8409-c1c4c84e0c5d",
                            "name": "my-plan",
                            "runBy": "admin",
                            "status": "started",
                            "updatedBy": "admin",
                            "updatedTs": "2024-02-23T13:59:34+08:00"
                        }, {
                            "createdTs": "2024-02-23T13:59:34+08:00",
                            "failedReason": "assertion failed: Field name cannot be empty, data-source-name=my-csv",
                            "id": "e83c4eba-6e85-40c6-8409-c1c4c84e0c5d",
                            "name": "my-plan",
                            "runBy": "admin",
                            "status": "failed",
                            "updatedBy": "admin",
                            "updatedTs": "2024-02-23T13:59:34+08:00"
                        }
                    ]
                }, {
                    "id": "d8c3cd64-595c-4f88-876d-bd63f1898e3f",
                    "runs": [
                        {
                            "createdTs": "2024-02-20T16:17:07+08:00",
                            "failedReason": "",
                            "id": "d8c3cd64-595c-4f88-876d-bd63f1898e3f",
                            "name": "my-plan",
                            "runBy": "admin",
                            "status": "started",
                            "updatedBy": "admin",
                            "updatedTs": "2024-02-20T16:17:07+08:00"
                        }, {
                            "createdTs": "2024-02-20T16:17:07+08:00",
                            "failedReason": "",
                            "id": "d8c3cd64-595c-4f88-876d-bd63f1898e3f",
                            "name": "my-plan",
                            "runBy": "admin",
                            "status": "parsed_plan",
                            "updatedBy": "admin",
                            "updatedTs": "2024-02-20T16:17:07+08:00"
                        }, {
                            "createdTs": "2024-02-20T16:17:07+08:00",
                            "failedReason": "",
                            "id": "d8c3cd64-595c-4f88-876d-bd63f1898e3f",
                            "name": "my-plan",
                            "runBy": "admin",
                            "status": "finished",
                            "updatedBy": "admin",
                            "updatedTs": "2024-02-20T16:17:14+08:00"
                        }
                    ]
                }, {
                    "id": "95d0f8d8-aa40-4146-bd0d-188244da90ac",
                    "runs": [
                        {
                            "createdTs": "2024-02-21T09:31:20+08:00",
                            "failedReason": "",
                            "id": "95d0f8d8-aa40-4146-bd0d-188244da90ac",
                            "name": "my-plan",
                            "runBy": "admin",
                            "status": "started",
                            "updatedBy": "admin",
                            "updatedTs": "2024-02-21T09:31:21+08:00"
                        }, {
                            "createdTs": "2024-02-21T09:31:20+08:00",
                            "failedReason": "Column name defined in foreign key relationship does not exist, data-source=my-csv, column-name=a,s,d",
                            "id": "95d0f8d8-aa40-4146-bd0d-188244da90ac",
                            "name": "my-plan",
                            "runBy": "admin",
                            "status": "failed",
                            "updatedBy": "admin",
                            "updatedTs": "2024-02-21T09:31:21+08:00"
                        }
                    ]
                }
            ],
            "name": "my-plan"
        }
    ]
})
    .then(body => {
        let planHistories = Object.values(body.planExecutionByPlan);
        for (const planHistory of planHistories) {
            const planName = planHistory.name;
            const planHistoryById = planHistory.executions;
            const planRunsByIdTableId = planName + "-runs-table";

            let planRunsByIdTable = document.createElement("table");
            planRunsByIdTable.setAttribute("id", planRunsByIdTableId + "-element");
            planRunsByIdTable.setAttribute("data-toggle", "table");
            const planHistoryByIdValues = Object.values(planHistoryById);
            const lastUpdatePerId = [];
            for (const runUpdatesById of planHistoryByIdValues) {
                let runUpdates = runUpdatesById.runs;
                lastUpdatePerId.push(runUpdates[runUpdates.length - 1]);
            }

            $(planRunsByIdTable).bootstrapTable({
                sortStable: true,
                columns: tableHeadersWithKey,
                data: Object.values(lastUpdatePerId),
            });

            let planHistoryContainer = createAccordionItem(planName, planName, "", planRunsByIdTable);
            historyContainer.append(planHistoryContainer);
        }
    });
