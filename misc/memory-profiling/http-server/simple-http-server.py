#!/usr/bin/env python3
"""
Simple HTTP server for testing Data Caterer memory profiling.
Receives POST requests and logs statistics.
"""

import json
import logging
import argparse
import time
import signal
import sys
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Lock
import psutil
import os


class RequestStats:
    """Thread-safe request statistics tracker."""

    def __init__(self):
        self.lock = Lock()
        self.total_requests = 0
        self.total_bytes = 0
        self.start_time = time.time()
        self.first_request_time = None  # Track when first request actually arrives
        self.last_request_time = None
        self.request_times = []
        self.request_arrivals = []  # Track arrival timestamps for accurate throughput
        self.errors = 0

    def record_request(self, size_bytes, processing_time):
        current_time = time.time()
        with self.lock:
            self.total_requests += 1
            self.total_bytes += size_bytes
            self.request_times.append(processing_time)
            self.request_arrivals.append(current_time)

            # Track first and last request times
            if self.first_request_time is None:
                self.first_request_time = current_time
            self.last_request_time = current_time

            # Keep only last 1000 request times for average calculation
            if len(self.request_times) > 1000:
                self.request_times.pop(0)
            if len(self.request_arrivals) > 1000:
                self.request_arrivals.pop(0)

    def record_error(self):
        with self.lock:
            self.errors += 1

    def get_stats(self):
        with self.lock:
            # Calculate throughput from first to last request (not from server start)
            if self.first_request_time and self.last_request_time:
                active_duration = self.last_request_time - self.first_request_time
                throughput = self.total_requests / active_duration if active_duration > 0 else 0
            else:
                throughput = 0

            # Calculate recent throughput (last N requests)
            recent_throughput = 0
            if len(self.request_arrivals) >= 2:
                recent_duration = self.request_arrivals[-1] - self.request_arrivals[0]
                recent_throughput = len(self.request_arrivals) / recent_duration if recent_duration > 0 else 0

            elapsed_total = time.time() - self.start_time
            avg_latency = sum(self.request_times) / len(self.request_times) if self.request_times else 0

            return {
                'total_requests': self.total_requests,
                'total_bytes': self.total_bytes,
                'elapsed_seconds': elapsed_total,
                'active_duration': self.last_request_time - self.first_request_time if self.first_request_time and self.last_request_time else 0,
                'throughput_per_sec': throughput,
                'recent_throughput_per_sec': recent_throughput,
                'avg_latency_ms': avg_latency * 1000,
                'errors': self.errors,
                'memory_mb': psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
            }


stats = RequestStats()


class DataCatererHTTPHandler(BaseHTTPRequestHandler):
    """HTTP request handler for Data Caterer requests."""

    def log_message(self, format, *args):
        """Override to use custom logging."""
        if logging.getLogger().level <= logging.DEBUG:
            logging.debug(f"{self.address_string()} - {format % args}")

    def do_POST(self):
        """Handle POST requests from Data Caterer."""
        arrival_timestamp = time.time()
        arrival_datetime = datetime.fromtimestamp(arrival_timestamp)
        start_time = time.time()

        try:
            # Read request body
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)

            # Try to parse JSON for better logging
            data_preview = None
            try:
                data = json.loads(body.decode('utf-8'))
                data_preview = {k: v for i, (k, v) in enumerate(data.items()) if i < 3}  # First 3 fields
                if logging.getLogger().level <= logging.DEBUG:
                    logging.debug(f"Received data: {json.dumps(data, indent=2)}")
            except (json.JSONDecodeError, UnicodeDecodeError):
                # Not JSON or binary data, just log size
                logging.debug(f"Received {content_length} bytes of data")

            # Record statistics
            processing_time = time.time() - start_time
            stats.record_request(content_length, processing_time)

            # Send success response
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'status': 'success', 'message': 'Data received'}).encode('utf-8'))

            # Get current stats
            current_stats = stats.get_stats()

            # Log EVERY request with millisecond timestamp
            logging.info(
                f"[{arrival_datetime.strftime('%H:%M:%S.%f')[:-3]}] "
                f"Request #{current_stats['total_requests']} | "
                f"Size: {content_length}B | "
                f"Latency: {processing_time*1000:.2f}ms | "
                f"Throughput: {current_stats['throughput_per_sec']:.2f}/sec | "
                f"Recent: {current_stats['recent_throughput_per_sec']:.2f}/sec"
                + (f" | Data: {json.dumps(data_preview)}" if data_preview and logging.getLogger().level <= logging.DEBUG else "")
            )

            # Log summary every 25 requests
            if current_stats['total_requests'] % 25 == 0:
                logging.info(
                    f">>> SUMMARY @ {current_stats['total_requests']} requests: "
                    f"Overall Throughput: {current_stats['throughput_per_sec']:.2f}/sec | "
                    f"Recent Throughput: {current_stats['recent_throughput_per_sec']:.2f}/sec | "
                    f"Avg Latency: {current_stats['avg_latency_ms']:.2f}ms | "
                    f"Active Duration: {current_stats['active_duration']:.1f}s | "
                    f"Memory: {current_stats['memory_mb']:.1f}MB"
                )

        except Exception as e:
            logging.error(f"Error processing request: {e}")
            stats.record_error()
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'status': 'error', 'message': str(e)}).encode('utf-8'))

    def do_GET(self):
        """Handle GET requests for health checks and stats."""
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'status': 'healthy'}).encode('utf-8'))

        elif self.path == '/stats':
            current_stats = stats.get_stats()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(current_stats, indent=2).encode('utf-8'))

        else:
            self.send_response(404)
            self.end_headers()


def print_final_stats():
    """Print final statistics before shutdown."""
    final_stats = stats.get_stats()

    print("\n" + "="*70)
    print("HTTP Server Final Statistics")
    print("="*70)
    print(f"Total Requests:        {final_stats['total_requests']:,}")
    print(f"Total Data:            {final_stats['total_bytes'] / (1024*1024):.2f} MB")
    print(f"Total Uptime:          {final_stats['elapsed_seconds']:.2f} seconds")
    print(f"Active Duration:       {final_stats['active_duration']:.2f} seconds (first to last request)")
    print(f"Overall Throughput:    {final_stats['throughput_per_sec']:.2f} requests/sec")
    print(f"Recent Throughput:     {final_stats['recent_throughput_per_sec']:.2f} requests/sec (last 1000)")
    print(f"Avg Latency:           {final_stats['avg_latency_ms']:.3f} ms")
    print(f"Errors:                {final_stats['errors']}")
    print(f"Server Memory:         {final_stats['memory_mb']:.2f} MB")
    print("="*70)


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    print("\nShutting down HTTP server...")
    print_final_stats()
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description='Simple HTTP server for Data Caterer memory profiling tests'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=8080,
        help='Port to listen on (default: 8080)'
    )
    parser.add_argument(
        '--host',
        type=str,
        default='0.0.0.0',
        help='Host to bind to (default: 0.0.0.0)'
    )
    parser.add_argument(
        '--log-level',
        choices=['debug', 'info', 'warning', 'error'],
        default='info',
        help='Logging level (default: info)'
    )
    parser.add_argument(
        '--stats-interval',
        type=int,
        default=1000,
        help='Print stats every N requests (default: 1000)'
    )

    args = parser.parse_args()

    # Configure logging
    log_level = getattr(logging, args.log_level.upper())
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create and start server
    server_address = (args.host, args.port)
    httpd = HTTPServer(server_address, DataCatererHTTPHandler)

    logging.info(f"Starting HTTP server on {args.host}:{args.port}")
    logging.info(f"Ready to receive Data Caterer requests")
    logging.info(f"Health check: http://localhost:{args.port}/health")
    logging.info(f"Statistics: http://localhost:{args.port}/stats")
    logging.info("Press Ctrl+C to stop")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print_final_stats()
        httpd.server_close()


if __name__ == '__main__':
    main()
