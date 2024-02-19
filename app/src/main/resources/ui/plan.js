import {createAccordionItem} from "./shared.js";

const planList = document.getElementById("plan-list");
let numPlans = 0;

getExistingPlans();

function getExistingPlans() {
    fetch("http://localhost:9090/plan/saved", {
        method: "GET"
    })
        .then(r => r.json())
        .then(respJson => {
            let plans = respJson.plans;
            for (let plan of plans) {
                numPlans += 1;
                let accordionItem = createAccordionItem(numPlans, plan.name, JSON.stringify(plan));
                planList.append(accordionItem);
            }
        });
}
