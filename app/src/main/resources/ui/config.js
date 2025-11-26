/**
 * API Configuration
 *
 * Dynamically determines the API base URL based on the current page location.
 * This ensures the frontend always connects to the correct backend, regardless of:
 * - Docker networking mode (host vs bridge)
 * - Access location (localhost vs remote IP)
 * - Deployment environment (dev, staging, prod)
 *
 * Override behavior:
 * - Set window.DATA_CATERER_API_URL to force a specific URL
 * - Useful for development or complex proxy setups
 */
const API_CONFIG = {
    /**
     * Base URL for all API calls
     * Defaults to window.location.origin (same host/port as UI)
     * Can be overridden via window.DATA_CATERER_API_URL
     */
    baseUrl: window.DATA_CATERER_API_URL || window.location.origin,

    /**
     * Timeout for API requests (milliseconds)
     */
    timeout: 30000,

    /**
     * Enable detailed logging for debugging
     */
    debug: window.DATA_CATERER_DEBUG === 'true' || false,
};

/**
 * Enhanced fetch wrapper with logging and error handling
 *
 * @param {string} endpoint - API endpoint (e.g., "/connections")
 * @param {Object} options - Fetch options (method, headers, body, etc.)
 * @returns {Promise<Response>} Fetch response
 */
export async function apiFetch(endpoint, options = {}) {
    const url = `${API_CONFIG.baseUrl}${endpoint}`;

    if (API_CONFIG.debug) {
        console.log(`[API] Request:`, {
            method: options.method || 'GET',
            url,
            headers: options.headers,
            body: options.body ? JSON.parse(options.body) : undefined,
        });
    }

    const startTime = Date.now();

    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), API_CONFIG.timeout);

        const response = await fetch(url, {
            ...options,
            signal: controller.signal,
        });

        clearTimeout(timeoutId);
        const duration = Date.now() - startTime;

        if (API_CONFIG.debug) {
            console.log(`[API] Response:`, {
                status: response.status,
                statusText: response.statusText,
                url,
                duration: `${duration}ms`,
            });
        }

        return response;
    } catch (error) {
        const duration = Date.now() - startTime;

        console.error(`[API] Error:`, {
            url,
            duration: `${duration}ms`,
            error: error.message,
            type: error.name,
        });

        // Provide helpful error messages
        if (error.name === 'TypeError' || error.message.includes('fetch')) {
            const helpfulError = new Error(
                `Failed to connect to Data Caterer API at ${url}\n\n` +
                `Possible causes:\n` +
                `1. Backend server is not running\n` +
                `2. Incorrect URL (currently: ${API_CONFIG.baseUrl})\n` +
                `3. Network connectivity issues\n` +
                `4. CORS restrictions\n\n` +
                `Troubleshooting:\n` +
                `- Check if backend is accessible: curl ${url}\n` +
                `- Check browser console for detailed errors\n` +
                `- If using Docker with network_mode: host, access UI from same machine\n` +
                `- Set DATA_CATERER_API_URL if backend is on different host\n\n` +
                `Original error: ${error.message}`
            );
            helpfulError.name = 'ConnectionError';
            helpfulError.originalError = error;
            throw helpfulError;
        }

        if (error.name === 'AbortError') {
            const timeoutError = new Error(
                `Request to ${url} timed out after ${API_CONFIG.timeout}ms\n\n` +
                `This usually indicates:\n` +
                `1. Backend is slow or unresponsive\n` +
                `2. Network latency issues\n` +
                `3. Large data transfer taking too long\n\n` +
                `Try increasing timeout via: window.DATA_CATERER_TIMEOUT = 60000 (60 seconds)`
            );
            timeoutError.name = 'TimeoutError';
            timeoutError.originalError = error;
            throw timeoutError;
        }

        throw error;
    }
}

export default API_CONFIG;
