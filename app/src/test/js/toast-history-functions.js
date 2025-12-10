/**
 * Toast history functions extracted for testing.
 * This module contains the core toast logic that can be tested independently.
 */

// Toast history management
const toastHistory = [];
const MAX_TOAST_HISTORY = 50;

export function getToastHistory() {
    return toastHistory;
}

export function clearToastHistory() {
    toastHistory.length = 0;
    updateToastHistoryBadge();
}

function addToHistory(header, message, type, timestamp) {
    toastHistory.unshift({header, message, type, timestamp});
    if (toastHistory.length > MAX_TOAST_HISTORY) {
        toastHistory.pop();
    }
    updateToastHistoryBadge();
}

function updateToastHistoryBadge() {
    const badge = document.getElementById("toast-history-badge");
    if (badge) {
        const unreadCount = toastHistory.filter(t => !t.read).length;
        badge.textContent = unreadCount > 0 ? unreadCount : "";
        badge.style.display = unreadCount > 0 ? "inline" : "none";
    }
}

export function showToastHistoryModal() {
    const modal = document.getElementById("toast-history-modal");
    if (!modal) return;

    const body = document.getElementById("toast-history-body");
    if (!body) return;

    // Mark all as read
    toastHistory.forEach(t => t.read = true);
    updateToastHistoryBadge();

    if (toastHistory.length === 0) {
        body.innerHTML = '<p class="text-muted text-center">No notifications yet</p>';
    } else {
        body.innerHTML = toastHistory.map(t => {
            let iconClass, iconColor;
            if (t.type === "success") {
                iconClass = "bi-check-square-fill";
                iconColor = "green";
            } else if (t.type === "fail") {
                iconClass = "bi-exclamation-square-fill";
                iconColor = "red";
            } else {
                iconClass = "bi-caret-right-square-fill";
                iconColor = "orange";
            }
            const timeStr = new Date(t.timestamp).toLocaleTimeString();
            const isLongMessage = t.message.length > 200;
            const displayMessage = isLongMessage ? t.message.substring(0, 200) + "..." : t.message;

            return `
                <div class="toast-history-item border-bottom pb-2 mb-2">
                    <div class="d-flex align-items-center">
                        <i class="bi ${iconClass} me-2" style="color: ${iconColor}"></i>
                        <strong>${escapeHtml(t.header)}</strong>
                        <small class="text-muted ms-auto">${timeStr}</small>
                    </div>
                    <div class="mt-1 toast-history-message ${isLongMessage ? 'expandable' : ''}"
                         data-full-message="${escapeHtml(t.message)}"
                         data-truncated="${isLongMessage}">
                        ${escapeHtml(displayMessage)}
                        ${isLongMessage ? '<a href="#" class="expand-message-link ms-1">Show more</a>' : ''}
                    </div>
                </div>
            `;
        }).join("");

        // Add click handlers for expand links
        body.querySelectorAll(".expand-message-link").forEach(link => {
            link.addEventListener("click", (e) => {
                e.preventDefault();
                const container = e.target.closest(".toast-history-message");
                const fullMessage = container.dataset.fullMessage;
                container.innerHTML = fullMessage;
            });
        });
    }

    new bootstrap.Modal(modal).show();
}

function escapeHtml(text) {
    const div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
}

export function initToastHistoryListeners() {
    const historyBtn = document.getElementById("toast-history-btn");
    const clearBtn = document.getElementById("clear-toast-history");
    const copyBtn = document.getElementById("copy-error-btn");

    if (historyBtn) {
        historyBtn.addEventListener("click", () => {
            showToastHistoryModal();
        });
    }

    if (clearBtn) {
        clearBtn.addEventListener("click", () => {
            clearToastHistory();
            const body = document.getElementById("toast-history-body");
            if (body) {
                body.innerHTML = '<p class="text-muted text-center">No notifications yet</p>';
            }
        });
    }

    if (copyBtn) {
        copyBtn.addEventListener("click", () => {
            const errorBody = document.getElementById("error-detail-body");
            if (errorBody) {
                navigator.clipboard.writeText(errorBody.textContent).then(() => {
                    copyBtn.innerHTML = '<i class="bi bi-check me-1"></i>Copied!';
                    setTimeout(() => {
                        copyBtn.innerHTML = '<i class="bi bi-clipboard me-1"></i>Copy to Clipboard';
                    }, 2000);
                });
            }
        });
    }
}

function createButton(id, ariaLabel, classAttr, text) {
    let button = document.createElement("button");
    button.setAttribute("id", id);
    button.setAttribute("aria-label", ariaLabel);
    button.setAttribute("class", classAttr);
    button.setAttribute("type", "button");
    button.innerText = text;
    return button;
}

export function createToast(header, message, type, delay = 5000) {
    const timestamp = Date.now();
    addToHistory(header, message, type, timestamp);

    // Error messages show longer (15 seconds) unless explicitly set
    const effectiveDelay = type === "fail" && delay === 5000 ? 15000 : delay;

    let toast = document.createElement("div");
    toast.setAttribute("class", "toast");
    toast.setAttribute("role", "alert");
    toast.setAttribute("data-bs-delay", effectiveDelay);
    toast.setAttribute("aria-live", "assertive");
    toast.setAttribute("aria-atomic", "true");

    let toastHeader = document.createElement("div");
    toastHeader.setAttribute("class", "toast-header mr-2");
    let icon = document.createElement("i");
    if (type === "success") {
        icon.setAttribute("class", "bi bi-check-square-fill");
        icon.setAttribute("style", "color: green");
    } else if (type === "fail") {
        icon.setAttribute("class", "bi bi-exclamation-square-fill");
        icon.setAttribute("style", "color: red");
    } else {
        icon.setAttribute("class", "bi bi-caret-right-square-fill");
        icon.setAttribute("style", "color: orange");
    }
    let strong = document.createElement("strong");
    strong.setAttribute("class", "me-auto");
    strong.innerText = header;
    let small = document.createElement("small");
    small.innerText = "Now";
    let button = createButton("", "Close", "btn-close", "");
    button.setAttribute("data-bs-dismiss", "toast");

    let toastBody = document.createElement("div");
    toastBody.setAttribute("class", "toast-body");

    // For long error messages, show truncated with "Show more" button
    const maxLength = 150;
    if (type === "fail" && message.length > maxLength) {
        const truncatedText = message.substring(0, maxLength) + "...";

        let textSpan = document.createElement("span");
        textSpan.innerText = truncatedText;

        let showMoreBtn = document.createElement("button");
        showMoreBtn.setAttribute("class", "btn btn-link btn-sm p-0 ms-1 toast-show-more");
        showMoreBtn.innerText = "Show more";
        showMoreBtn.addEventListener("click", () => {
            showErrorDetailModal(header, message);
        });

        toastBody.append(textSpan, showMoreBtn);
    } else {
        toastBody.innerText = message;
    }

    toastHeader.append(icon, strong, small, button);
    toast.append(toastHeader, toastBody);
    document.getElementById("toast-container").append(toast);
    new bootstrap.Toast(toast).show();
}

function showErrorDetailModal(header, message) {
    const modal = document.getElementById("error-detail-modal");
    if (!modal) return;

    const titleEl = document.getElementById("error-detail-title");
    const bodyEl = document.getElementById("error-detail-body");

    if (titleEl) titleEl.textContent = header;
    if (bodyEl) {
        bodyEl.textContent = message;
        bodyEl.style.whiteSpace = "pre-wrap";
        bodyEl.style.wordBreak = "break-word";
    }

    new bootstrap.Modal(modal).show();
}
