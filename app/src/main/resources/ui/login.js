import {createToast} from "./shared.js";
import {apiFetch} from "./config.js";

const loginButton = document.getElementById("login-button");
const loginSaveButton = document.getElementById("login-save-button");
const loginCloseButton = document.getElementById("login-close-button");
const usernameInput = document.getElementById("username");
const tokenInput = document.getElementById("token");

export function initLoginButton() {
    loginButton.addEventListener("click", async function () {
        showLoginBox();
    });
}

export function initLoginSaveButton() {
    loginSaveButton.addEventListener("click", async function () {
        await saveCredentials();
    });
}

export function initLoginCloseButton() {
    loginCloseButton.addEventListener("click", async function () {
        closeLoginBox();
    });
}

export function showLoginBox(isFocusUsername = true) {
    document.getElementById("login-box").style.display = "block";
    const focusElement = isFocusUsername ? usernameInput : tokenInput;
    focusElement.focus();
    focusElement.select();
}

export function closeLoginBox() {
    document.getElementById("login-box").style.display = "none";
}

export async function saveCredentials() {
    const username = usernameInput.value;
    const token = tokenInput.value;

    // send to save credentials endpoint
    await apiFetch(`/credentials`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({"userId": username, "token": token})
    })
        .catch(err => {
            createToast("Login", `Login failed!`, "fail");
            throw new Error("Login failed");
        })
        .then(r => {
            console.log(r);
            if (r.ok) {
                createToast("Login", `Login successful!`, "success");
                setIsValid(usernameInput);
                setIsValid(tokenInput);
                closeLoginBox();
            } else {
                r.text().then(text => {
                    if (r.status === 401) {
                        if (text.includes("User not found")) {
                            setIsValid(usernameInput, false);
                            setIsValid(tokenInput);
                            showLoginBox();
                        } else if (text.includes("Invalid user credentials")) {
                            setIsValid(tokenInput, false);
                            setIsValid(usernameInput);
                            showLoginBox(false);
                        }
                    }
                    createToast("Login", `Login failed. Error: ${text}`, "fail");
                    throw new Error(text);
                });
            }
        });
}

function setIsValid(element, isValid = true) {
    if (isValid) {
        element.classList.remove("is-invalid");
        element.classList.add("is-valid");
    } else {
        element.classList.remove("is-valid");
        element.classList.add("is-invalid");
    }
}
