"""Watchdog-based folder watcher – queues new PDFs for processing.

Uses a work queue + worker thread for concurrency safety.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class _PDFHandler:
    """Enqueue newly created / moved PDF files.

    Implements the watchdog FileSystemEventHandler interface.
    """

    def __init__(self, work_queue: queue.Queue[Path]) -> None:
        from watchdog.events import FileSystemEventHandler  # type: ignore[import-untyped]
        self.__class__.__bases__ = (FileSystemEventHandler,)
        super().__init__()
        self._q = work_queue

    def on_created(self, event: Any) -> None:
        self._maybe_enqueue(event)

    def on_moved(self, event: Any) -> None:
        self._maybe_enqueue(event)

    def _maybe_enqueue(self, event: Any) -> None:
        if getattr(event, "is_directory", False):
            return
        path = Path(str(event.src_path))
        if path.suffix.lower() == ".pdf":
            logger.info("Detected new PDF: %s", path.name)
            # Small delay to allow the file write to complete
            time.sleep(0.5)
            self._q.put(path)


class Watcher:
    """Watch *input_dir* for new PDFs and process them via the pipeline."""

    def __init__(
        self,
        input_dir: Path,
        output_dir: Path,
        failed_dir: Path | None = None,
        num_workers: int = 2,
    ) -> None:
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._failed_dir = failed_dir
        self._num_workers = num_workers
        self._queue: queue.Queue[Path] = queue.Queue()
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start the observer + worker threads.  Blocks until interrupted."""
        from watchdog.observers import Observer  # type: ignore[import-untyped]
        from invoice_uom.pipeline import process_pdf  # noqa: F811

        self._process_pdf = process_pdf
        self._input_dir.mkdir(parents=True, exist_ok=True)
        self._output_dir.mkdir(parents=True, exist_ok=True)

        handler = _PDFHandler(self._queue)
        observer = Observer()
        observer.schedule(handler, str(self._input_dir), recursive=False)
        observer.start()
        logger.info("Watching %s for new PDFs …", self._input_dir)

        workers: list[threading.Thread] = []
        for i in range(self._num_workers):
            t = threading.Thread(target=self._worker, name=f"worker-{i}", daemon=True)
            t.start()
            workers.append(t)

        try:
            while not self._stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down watcher …")
        finally:
            self._stop_event.set()
            observer.stop()
            observer.join()

    def stop(self) -> None:
        self._stop_event.set()

    def _worker(self) -> None:
        from invoice_uom.pipeline import process_pdf

        while not self._stop_event.is_set():
            try:
                pdf_path = self._queue.get(timeout=2)
            except queue.Empty:
                continue
            try:
                process_pdf(pdf_path, self._output_dir, self._failed_dir)
            except Exception:
                logger.exception("Worker error processing %s", pdf_path)
            finally:
                self._queue.task_done()
