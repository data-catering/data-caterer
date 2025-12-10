/**
 * Tests for toast history functionality.
 *
 * These tests verify the toast history management, including storing,
 * retrieving, and clearing toast notifications.
 */

describe('Toast History Functionality', () => {

    // Set up DOM elements needed for toast functionality
    beforeEach(() => {
        document.body.innerHTML = `
            <div id="toast-container" class="toast-container"></div>
            <button id="toast-history-btn">
                <span id="toast-history-badge" style="display: none;"></span>
            </button>
            <div id="toast-history-modal" class="modal">
                <div id="toast-history-body"></div>
            </div>
            <button id="clear-toast-history"></button>
            <div id="error-detail-modal" class="modal">
                <span id="error-detail-title"></span>
                <pre id="error-detail-body"></pre>
            </div>
            <button id="copy-error-btn"></button>
        `;
    });

    afterEach(() => {
        document.body.innerHTML = '';
        jest.clearAllMocks();
    });

    describe('Toast History Storage', () => {

        test('should store toast in history when createToast is called', async () => {
            // Dynamically import to get fresh module state
            jest.resetModules();
            const { createToast, getToastHistory } = await import('./toast-history-functions');

            createToast('Test Header', 'Test message', 'success');

            const history = getToastHistory();
            expect(history.length).toBe(1);
            expect(history[0].header).toBe('Test Header');
            expect(history[0].message).toBe('Test message');
            expect(history[0].type).toBe('success');
            expect(history[0].timestamp).toBeDefined();
        });

        test('should store multiple toasts in history', async () => {
            jest.resetModules();
            const { createToast, getToastHistory } = await import('./toast-history-functions');

            createToast('Header 1', 'Message 1', 'success');
            createToast('Header 2', 'Message 2', 'fail');
            createToast('Header 3', 'Message 3', 'start');

            const history = getToastHistory();
            expect(history.length).toBe(3);
            // Most recent should be first
            expect(history[0].header).toBe('Header 3');
            expect(history[1].header).toBe('Header 2');
            expect(history[2].header).toBe('Header 1');
        });

        test('should limit history to 50 items', async () => {
            jest.resetModules();
            const { createToast, getToastHistory } = await import('./toast-history-functions');

            // Create 55 toasts
            for (let i = 0; i < 55; i++) {
                createToast(`Header ${i}`, `Message ${i}`, 'success');
            }

            const history = getToastHistory();
            expect(history.length).toBe(50);
            // Most recent should be Header 54
            expect(history[0].header).toBe('Header 54');
        });

        test('should clear history when clearToastHistory is called', async () => {
            jest.resetModules();
            const { createToast, getToastHistory, clearToastHistory } = await import('./toast-history-functions');

            createToast('Header 1', 'Message 1', 'success');
            createToast('Header 2', 'Message 2', 'fail');

            expect(getToastHistory().length).toBe(2);

            clearToastHistory();

            expect(getToastHistory().length).toBe(0);
        });
    });

    describe('Toast Display Duration', () => {

        test('should use default delay for success toasts', async () => {
            jest.resetModules();
            const { createToast } = await import('./toast-history-functions');

            createToast('Success', 'Operation completed', 'success');

            const toast = document.querySelector('.toast');
            expect(toast.getAttribute('data-bs-delay')).toBe('5000');
        });

        test('should use longer delay (15s) for error toasts', async () => {
            jest.resetModules();
            const { createToast } = await import('./toast-history-functions');

            createToast('Error', 'Something went wrong', 'fail');

            const toast = document.querySelector('.toast');
            expect(toast.getAttribute('data-bs-delay')).toBe('15000');
        });

        test('should respect custom delay for error toasts', async () => {
            jest.resetModules();
            const { createToast } = await import('./toast-history-functions');

            createToast('Error', 'Something went wrong', 'fail', 20000);

            const toast = document.querySelector('.toast');
            expect(toast.getAttribute('data-bs-delay')).toBe('20000');
        });

        test('should use default delay for start/running toasts', async () => {
            jest.resetModules();
            const { createToast } = await import('./toast-history-functions');

            createToast('Running', 'Process started', 'start');

            const toast = document.querySelector('.toast');
            expect(toast.getAttribute('data-bs-delay')).toBe('5000');
        });
    });

    describe('Long Error Message Handling', () => {

        test('should truncate long error messages with show more button', async () => {
            jest.resetModules();
            const { createToast } = await import('./toast-history-functions');

            const longMessage = 'A'.repeat(200);
            createToast('Error', longMessage, 'fail');

            const toastBody = document.querySelector('.toast-body');
            const showMoreBtn = toastBody.querySelector('.toast-show-more');

            expect(showMoreBtn).not.toBeNull();
            expect(showMoreBtn.innerText).toBe('Show more');
        });

        test('should not truncate short error messages', async () => {
            jest.resetModules();
            const { createToast } = await import('./toast-history-functions');

            const shortMessage = 'Short error message';
            createToast('Error', shortMessage, 'fail');

            const toastBody = document.querySelector('.toast-body');
            const showMoreBtn = toastBody.querySelector('.toast-show-more');

            expect(showMoreBtn).toBeNull();
            expect(toastBody.innerText).toBe(shortMessage);
        });

        test('should not truncate success messages regardless of length', async () => {
            jest.resetModules();
            const { createToast } = await import('./toast-history-functions');

            const longMessage = 'A'.repeat(200);
            createToast('Success', longMessage, 'success');

            const toastBody = document.querySelector('.toast-body');
            const showMoreBtn = toastBody.querySelector('.toast-show-more');

            expect(showMoreBtn).toBeNull();
            expect(toastBody.innerText).toBe(longMessage);
        });
    });

    describe('Toast Icon Styling', () => {

        test('should show green checkmark icon for success toasts', async () => {
            jest.resetModules();
            const { createToast } = await import('./toast-history-functions');

            createToast('Success', 'Done', 'success');

            const icon = document.querySelector('.toast-header i');
            expect(icon.classList.contains('bi-check-square-fill')).toBe(true);
            expect(icon.style.color).toBe('green');
        });

        test('should show red exclamation icon for fail toasts', async () => {
            jest.resetModules();
            const { createToast } = await import('./toast-history-functions');

            createToast('Error', 'Failed', 'fail');

            const icon = document.querySelector('.toast-header i');
            expect(icon.classList.contains('bi-exclamation-square-fill')).toBe(true);
            expect(icon.style.color).toBe('red');
        });

        test('should show orange caret icon for start/running toasts', async () => {
            jest.resetModules();
            const { createToast } = await import('./toast-history-functions');

            createToast('Running', 'Started', 'start');

            const icon = document.querySelector('.toast-header i');
            expect(icon.classList.contains('bi-caret-right-square-fill')).toBe(true);
            expect(icon.style.color).toBe('orange');
        });
    });

    describe('Toast History Modal', () => {

        test('should display empty message when no history', async () => {
            jest.resetModules();
            const { showToastHistoryModal, clearToastHistory } = await import('./toast-history-functions');

            clearToastHistory();
            showToastHistoryModal();

            const body = document.getElementById('toast-history-body');
            expect(body.textContent).toContain('No notifications yet');
        });

        test('should display toasts in history modal', async () => {
            jest.resetModules();
            const { createToast, showToastHistoryModal } = await import('./toast-history-functions');

            createToast('Test Header', 'Test message', 'success');
            showToastHistoryModal();

            const body = document.getElementById('toast-history-body');
            expect(body.innerHTML).toContain('Test Header');
            expect(body.innerHTML).toContain('Test message');
        });

        test('should mark toasts as read when modal is shown', async () => {
            jest.resetModules();
            const { createToast, getToastHistory, showToastHistoryModal } = await import('./toast-history-functions');

            createToast('Test', 'Message', 'success');

            const historyBefore = getToastHistory();
            expect(historyBefore[0].read).toBeFalsy();

            showToastHistoryModal();

            const historyAfter = getToastHistory();
            expect(historyAfter[0].read).toBe(true);
        });
    });

    describe('Toast History Badge', () => {

        test('should update badge count for unread toasts', async () => {
            jest.resetModules();
            const { createToast } = await import('./toast-history-functions');

            createToast('Test 1', 'Message 1', 'success');
            createToast('Test 2', 'Message 2', 'fail');

            const badge = document.getElementById('toast-history-badge');
            expect(badge.textContent).toBe('2');
            expect(badge.style.display).toBe('inline');
        });

        test('should hide badge when all toasts are read', async () => {
            jest.resetModules();
            const { createToast, showToastHistoryModal } = await import('./toast-history-functions');

            createToast('Test', 'Message', 'success');

            // Opening modal marks as read
            showToastHistoryModal();

            const badge = document.getElementById('toast-history-badge');
            expect(badge.style.display).toBe('none');
        });

        test('should hide badge when history is cleared', async () => {
            jest.resetModules();
            const { createToast, clearToastHistory } = await import('./toast-history-functions');

            createToast('Test', 'Message', 'success');
            clearToastHistory();

            const badge = document.getElementById('toast-history-badge');
            expect(badge.textContent).toBe('');
            expect(badge.style.display).toBe('none');
        });
    });

    describe('Event Listeners', () => {

        test('should initialize toast history button listener', async () => {
            jest.resetModules();
            const { initToastHistoryListeners, createToast } = await import('./toast-history-functions');

            initToastHistoryListeners();
            createToast('Test', 'Message', 'success');

            const historyBtn = document.getElementById('toast-history-btn');
            historyBtn.click();

            const modal = document.getElementById('toast-history-modal');
            expect(modal.classList.contains('show')).toBe(true);
        });

        test('should initialize clear history button listener', async () => {
            jest.resetModules();
            const { initToastHistoryListeners, createToast, getToastHistory } = await import('./toast-history-functions');

            initToastHistoryListeners();
            createToast('Test', 'Message', 'success');
            expect(getToastHistory().length).toBe(1);

            const clearBtn = document.getElementById('clear-toast-history');
            clearBtn.click();

            expect(getToastHistory().length).toBe(0);
        });

        test('should initialize copy error button listener', async () => {
            jest.resetModules();
            const { initToastHistoryListeners } = await import('./toast-history-functions');

            initToastHistoryListeners();

            const errorBody = document.getElementById('error-detail-body');
            errorBody.textContent = 'Test error message';

            const copyBtn = document.getElementById('copy-error-btn');
            copyBtn.click();

            expect(navigator.clipboard.writeText).toHaveBeenCalledWith('Test error message');
        });
    });
});
