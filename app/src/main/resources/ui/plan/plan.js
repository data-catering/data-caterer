import {createAccordionItem, createButton, createButtonGroup, createToast} from "../shared.js";

const planList = document.getElementById("plan-list");
let numPlans = 0;

getExistingPlans();

function getExistingPlans() {
    fetch("http://localhost:9898/plans", {
        method: "GET"
    })
        .then(r => {
            if (r.ok) {
                return r.json();
            } else {
                r.text().then(text => {
                    new bootstrap.Toast(
                        createToast(`Get plans`, `Failed to get plans! Error: ${text}`, "fail")
                    ).show();
                    throw new Error(text);
                });
            }
        })
        .then(respJson => {
            let plans = respJson.plans;
            for (let plan of plans) {
                numPlans += 1;
                let accordionItem = createAccordionItem(numPlans, plan.name, JSON.stringify(plan));

                let editButton = createButton(`plan-edit-${numPlans}`, "Plan edit", "btn btn-primary", "Edit");
                let executeButton = createButton(`plan-execute-${numPlans}`, "Plan execute", "btn btn-primary", "Execute");
                let deleteButton = createButton(`plan-delete-${numPlans}`, "Plan delete", "btn btn-danger", "Delete");

                editButton.addEventListener("click", function() {
                    location.href = `http://localhost:9898/?plan-name=${plan.name}`;
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
