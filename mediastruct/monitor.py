"""Terminal-based progress monitoring for mediastruct using curses."""
import curses
import threading
import time
import logging
from typing import Dict, Optional

log = logging.getLogger(__name__)

class ProgressMonitor:
    """Manages a curses-based terminal interface to display progress of mediastruct tasks."""

    def __init__(self):
        log.debug("Initializing ProgressMonitor")
        self.statuses: Dict[str, Dict[str, str]] = {
            "ingest": {"status": "Idle", "progress": "0%", "details": "", "last_updated": 0.0},
            "crawl": {"status": "Idle", "progress": "0%", "details": "", "last_updated": 0.0},
            "dedupe": {"status": "Idle", "progress": "0%", "details": "", "last_updated": 0.0},
            "archive": {"status": "Idle", "progress": "0%", "details": "", "last_updated": 0.0},
            "validate": {"status": "Idle", "progress": "0%", "details": "", "last_updated": 0.0}
        }
        self.running = False
        self.screen = None
        self.lock = threading.Lock()
        self.min_display_time = 1.0  # Minimum time (in seconds) to display a status
        log.debug("ProgressMonitor initialized")

    def start(self):
        """Start the curses interface in a separate thread."""
        log.debug("Starting ProgressMonitor thread")
        self.running = True
        self.thread = threading.Thread(target=self._curses_wrapper)
        self.thread.daemon = True
        self.thread.start()
        log.debug("ProgressMonitor thread started")

    def stop(self):
        """Stop the curses interface."""
        log.debug("Stopping ProgressMonitor")
        self.running = False
        self.thread.join(timeout=5.0)  # Wait up to 5 seconds for the thread to exit
        if self.screen:
            curses.endwin()
        log.debug("ProgressMonitor stopped")

    def update_status(self, task: str, status: str, progress: str, details: str):
        """Update the status of a task."""
        with self.lock:
            if task in self.statuses:
                self.statuses[task]["status"] = status
                self.statuses[task]["progress"] = progress
                self.statuses[task]["details"] = details
                self.statuses[task]["last_updated"] = time.time()
                log.debug(f"ProgressMonitor - Updated {task}: status={status}, progress={progress}, details={details}")

    def update_progress(self, task: str, status: str, processed: int, total: int, current: str):
        """Update the progress of a task, mapping to update_status."""
        with self.lock:
            if task in self.statuses:
                # Calculate progress percentage
                if total > 0:
                    progress = f"{(processed / total) * 100:.1f}%"
                else:
                    progress = "0%"
                # Use the current item as the details
                details = current if current else ""
                self.update_status(task, status, progress, details)

    def _curses_wrapper(self):
        """Wrapper to initialize curses and run the display loop."""
        log.debug("Entering _curses_wrapper")
        try:
            curses.wrapper(self._display)
        except Exception as e:
            log.error(f"ProgressMonitor - Failed to initialize curses: {e}")
            self.running = False
        log.debug("Exiting _curses_wrapper")

    def _display(self, stdscr):
        """Main display loop using curses."""
        log.debug("Entering _display loop")
        self.screen = stdscr
        curses.curs_set(0)  # Hide the cursor
        stdscr.timeout(10)  # Refresh every 10ms

        while self.running:
            try:
                stdscr.clear()
                height, width = stdscr.getmaxyx()

                # Title
                title = "MediaStruct Progress Monitor"
                stdscr.addstr(0, (width - len(title)) // 2, title, curses.A_BOLD)

                # Display each task's status
                row = 2
                current_time = time.time()
                for task, info in self.statuses.items():
                    if row >= height - 1:
                        break
                    # Ensure the status is displayed for at least min_display_time
                    if info["status"] in ["Completed", "Failed"] and (current_time - info["last_updated"]) < self.min_display_time:
                        time.sleep(self.min_display_time - (current_time - info["last_updated"]))
                    status_line = f"{task.capitalize():<10} | Status: {info['status']:<10} | Progress: {info['progress']:<6} | Details: {info['details']}"
                    # Truncate if too long for the terminal width
                    status_line = status_line[:width - 2]
                    stdscr.addstr(row, 1, status_line)
                    row += 1

                # Instructions
                if row < height - 1:
                    stdscr.addstr(height - 1, 1, "Press Ctrl+C to exit", curses.A_DIM)

                stdscr.refresh()
                time.sleep(0.01)  # Small delay to reduce CPU usage
            except curses.error:
                log.debug("ProgressMonitor - Curses error during display loop")
                continue
            except KeyboardInterrupt:
                log.debug("ProgressMonitor - KeyboardInterrupt in display loop")
                self.running = False
                break
        log.debug("Exiting _display loop")
