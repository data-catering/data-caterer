/**
 * Jest setup file for Data Caterer UI tests
 *
 * Sets up the DOM environment and global mocks needed for testing
 * the UI JavaScript code.
 */

// Set up jQuery globally
const $ = require('jquery');
global.$ = $;
global.jQuery = $;

// Mock bootstrap-select (selectpicker)
$.fn.selectpicker = function(action, value) {
    if (action === 'val') {
        if (value !== undefined) {
            this.val(value);
            return this;
        }
        return this.val();
    }
    if (action === 'destroy' || action === 'render') {
        return this;
    }
    return this;
};

// Mock Bootstrap
class MockToast {
    constructor(element) {
        this.element = element;
    }
    show() {
        if (this.element) {
            this.element.classList.add('show');
        }
    }
    hide() {
        if (this.element) {
            this.element.classList.remove('show');
        }
    }
}

class MockModal {
    constructor(element) {
        this.element = element;
    }
    show() {
        if (this.element) {
            this.element.classList.add('show');
            this.element.style.display = 'block';
        }
    }
    hide() {
        if (this.element) {
            this.element.classList.remove('show');
            this.element.style.display = 'none';
        }
    }
}

global.bootstrap = {
    Toast: MockToast,
    Modal: MockModal
};

// Mock for dispatchEvent helper
global.dispatchEvent = function(element, eventType) {
    if (element && element.length) {
        element = element[0];
    }
    if (element) {
        element.dispatchEvent(new Event(eventType, { bubbles: true }));
    }
};

// Mock crypto.randomUUID
global.crypto = {
    randomUUID: () => 'test-uuid-' + Math.random().toString(36).substr(2, 9)
};

// Mock fetch
global.fetch = jest.fn();

// Mock wait function
global.wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Mock navigator.clipboard
Object.defineProperty(navigator, 'clipboard', {
    value: {
        writeText: jest.fn().mockResolvedValue(undefined)
    },
    writable: true
});

