import {createAccordionItem, createToast} from "../shared.js";

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

fetch("http://localhost:9090/run/history", {
    method: "GET"
})
    .then(r => {
        if (r.ok) {
            return r.json();
        } else {
            r.text().then(text => {
                new bootstrap.Toast(
                    createToast(`Plan run history`, `Failed to get plan run history! Error: ${text}`, "fail")
                ).show();
                throw new Error(text);
            });
        }
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
