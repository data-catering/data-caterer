import {createAccordionItem, createButton, createButtonGroup, createToast} from "../shared.js";
import {apiFetch} from "../config.js";

let historyContainer = document.getElementById("history-container");
const tableHeaders = [{
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
    field: "timeTaken",
    title: "Time Taken (s)",
    sortable: true,
}, {
    field: "generationSummary",
    title: "Data Generated",
}, {
    field: "validationSummary",
    title: "Data Validated",
}, {
    field: "failedReason",
    title: "Fail Reason",
}, {
    field: "reportLink",
    title: "Report",
}];

apiFetch("/run/history", {
    method: "GET"
})
    .then(r => {
        if (r.ok) {
            return r.json();
        } else {
            r.text().then(text => {
                createToast(`Plan run history`, `Failed to get plan run history! Error: ${text}`, "fail");
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
            planRunsByIdTable.setAttribute("data-sort-name", "createdTs");
            planRunsByIdTable.setAttribute("data-sort-order", "desc");
            const planHistoryByIdValues = Object.values(planHistoryById);
            const lastUpdatePerId = [];

            for (const runUpdatesById of planHistoryByIdValues) {
                let runUpdates = runUpdatesById.runs;
                let latestRunUpdate = runUpdates[runUpdates.length - 1];
                latestRunUpdate["createdTs"] = new Date(latestRunUpdate["createdTs"]).toISOString();
                latestRunUpdate["updatedTs"] = new Date(latestRunUpdate["updatedTs"]).toISOString();
                let reportHref = `/report/${latestRunUpdate["id"]}/index.html`;
                latestRunUpdate["reportLink"] = latestRunUpdate["reportLink"] === "" ? "" : `<a href=${reportHref} target="_blank" rel="noopener noreferrer">Report</a>`;
                let generationSummary = Array.from(latestRunUpdate["generationSummary"])
                    .filter(g => g.length > 3 && g[0] !== "")
                    .map(g => `${g[0]} -> ${g[3]}`)
                    .join("<br>");
                latestRunUpdate["generationSummary"] = generationSummary.length > 0 ? generationSummary : "";
                let validationSummary = Array.from(latestRunUpdate["validationSummary"])
                    .filter(v => v.length > 3 && v[0] !== "")
                    .map(v => v[3])
                    .join("<br>");
                latestRunUpdate["validationSummary"] = validationSummary.length > 0 ? validationSummary : "";
                latestRunUpdate["failedReason"] = latestRunUpdate["failedReason"].length > 500 ? latestRunUpdate["failedReason"].slice(0, 500) : latestRunUpdate["failedReason"];
                lastUpdatePerId.push(latestRunUpdate);
            }

            let planHistoryContainer = createAccordionItem(planName, planName, "", planRunsByIdTable);

            let editButton = createButton(`plan-edit-${planRunsByIdTableId}`, "Plan edit", "btn btn-primary", "Edit");
            editButton.addEventListener("click", function() {
                location.href = `/?plan-name=${planName}`;
            });
            let buttonGroup = createButtonGroup(editButton);
            let header = planHistoryContainer.querySelector(".accordion-header");
            let divContainer = document.createElement("div");
            divContainer.setAttribute("class", "d-flex align-items-center");
            divContainer.append(header.firstChild, buttonGroup);
            header.replaceChildren(divContainer);
            historyContainer.append(planHistoryContainer);

            $(planRunsByIdTable).bootstrapTable({
                sortStable: true,
                columns: tableHeaders,
                data: Object.values(lastUpdatePerId),
                rowStyle: function (row, index) {
                    if (row["status"] === "failed") {
                        return { classes: "table-danger" }
                    } else if (row["status"] === "finished") {
                        return { classes: "table-success" }
                    } else {
                        return { classes: "table-warning" }
                    }
                }
            });
        }
    });

