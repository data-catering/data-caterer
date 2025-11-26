import {
    createAccordionItem,
    createButton,
    createButtonGroup,
    createToast,
    executePlan,
    syntaxHighlight
} from "../shared.js";
import {apiFetch} from "../config.js";

const planList = document.getElementById("plan-list");
let numPlans = 0;

getExistingPlans();

function getExistingPlans() {
    apiFetch("/plans", {
        method: "GET"
    })
        .then(r => {
            if (r.ok) {
                return r.json();
            } else {
                r.text().then(text => {
                    createToast(`Get plans`, `Failed to get plans! Error: ${text}`, "fail");
                    throw new Error(text);
                });
            }
        })
        .then(respJson => {
            let plans = respJson.plans;
            for (let plan of plans) {
                numPlans += 1;
                let planName = plan.plan.name;
                let accordionItem = createAccordionItem(numPlans, planName, "", syntaxHighlight(plan));

                let editButton = createButton(`plan-edit-${numPlans}`, "Plan edit", "btn btn-primary", "Edit");
                let executeButton = createButton(`plan-execute-${numPlans}`, "Plan execute", "btn btn-primary", "Execute");
                let deleteButton = createButton(`plan-delete-${numPlans}`, "Plan delete", "btn btn-danger", "Delete");

                editButton.addEventListener("click", function() {
                    location.href = `/?plan-name=${planName}`;
                });
                executeButton.addEventListener("click", function () {
                    let runId = crypto.randomUUID();
                    plan.id = runId;
                    executePlan(plan, planName, runId);
                });
                deleteButton.addEventListener("click", async function () {
                    await apiFetch(`/plan/${planName}`, {method: "DELETE"});
                    createToast(plan.name, `Plan ${planName} deleted!`, "success");
                    planList.removeChild(accordionItem);
                });

                let buttonGroup = createButtonGroup(editButton, executeButton, deleteButton);
                let header = accordionItem.querySelector(".accordion-header");
                let divContainer = document.createElement("div");
                divContainer.setAttribute("class", "d-flex align-items-center");
                divContainer.append(header.firstChild, buttonGroup);
                header.replaceChildren(divContainer);
                planList.append(accordionItem);
            }
        });
}
