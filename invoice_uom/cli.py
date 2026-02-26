"""CLI entry points – ``run`` and ``watch`` subcommands.

Usage:
    python -m invoice_uom run   --input ./input_pdfs --output ./outputs
    python -m invoice_uom watch --input ./input_pdfs --output ./outputs
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from invoice_uom import config


def _setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    # Log to file ONLY to keep standard terminal output clean for Rich
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "invoice_uom.log", encoding="utf-8"),
        ],
    )


def _run(args: argparse.Namespace) -> None:
    from invoice_uom.pipeline import process_pdf

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    failed_dir = Path(args.failed) if args.failed else config.FAILED_DIR

    _setup_logging(Path(args.log_dir) if args.log_dir else config.LOG_DIR)

    from rich.console import Console
    from rich.progress import (
        Progress,
        SpinnerColumn,
        TextColumn,
        BarColumn,
        TimeElapsedColumn,
    )

    console = Console()
    pdfs = sorted(input_dir.glob("*.pdf"))
    if not pdfs:
        console.print(f"[yellow]Warning:[/yellow] No PDF files found in {input_dir}")
        return

    console.print(f"Found [bold]{len(pdfs)}[/bold] PDF(s) in {input_dir}")
    success = 0
    skipped = 0
    failed_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        overall_task = progress.add_task("[cyan]Processing PDFs...", total=len(pdfs))

        for pdf in pdfs:
            pdf_task = progress.add_task(f"[bold blue]{pdf.name}[/bold blue] Starting...", total=None)

            # Define localized callback for closures
            def create_cb(t_id, p_name):
                def cb(msg: str) -> None:
                    progress.update(t_id, description=f"[bold blue]{p_name}[/bold blue] {msg}")
                return cb

            status_cb = create_cb(pdf_task, pdf.name)

            result = process_pdf(
                pdf, output_dir, failed_dir, force=args.force, status_cb=status_cb
            )

            progress.update(overall_task, advance=1)
            progress.stop_task(pdf_task)

            if result is None:
                skipped += 1
                progress.console.print(f"[yellow]Skipped[/yellow] {pdf.name} (already processed)")
            elif result:
                success += 1
                progress.console.print(
                    f"[green]✓ Success[/green] {pdf.name} → [{result['stats']['num_items']} items, {result['stats']['num_escalations']} escalations]"
                )
            else:
                failed_count += 1
                progress.console.print(f"[red]✗ Failed[/red]  {pdf.name}")

            progress.remove_task(pdf_task)

    console.print(f"\n[bold green]Done:[/bold green] {success} processed, {skipped} skipped, {failed_count} failed")

def _watch(args: argparse.Namespace) -> None:
    from invoice_uom.watcher import Watcher

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    failed_dir = Path(args.failed) if args.failed else config.FAILED_DIR

    _setup_logging(Path(args.log_dir) if args.log_dir else config.LOG_DIR)

    watcher = Watcher(input_dir, output_dir, failed_dir)
    watcher.start()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="invoice_uom",
        description="Invoice ingestion & line-item extraction pipeline",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ── run ──────────────────────────────────────────────────────────────
    p_run = sub.add_parser("run", help="Batch-process all PDFs in a folder")
    p_run.add_argument("--input", "-i", default="./input_pdfs", help="Input PDF directory")
    p_run.add_argument("--output", "-o", default="./outputs", help="Output JSON directory")
    p_run.add_argument("--failed", "-f", default=None, help="Failed output directory")
    p_run.add_argument("--log-dir", default=None, help="Log directory")
    p_run.add_argument("--force", action="store_true", help="Reprocess even if already done")
    p_run.set_defaults(func=_run)

    # ── watch ────────────────────────────────────────────────────────────
    p_watch = sub.add_parser("watch", help="Watch a folder for new PDFs")
    p_watch.add_argument("--input", "-i", default="./input_pdfs", help="Input PDF directory")
    p_watch.add_argument("--output", "-o", default="./outputs", help="Output JSON directory")
    p_watch.add_argument("--failed", "-f", default=None, help="Failed output directory")
    p_watch.add_argument("--log-dir", default=None, help="Log directory")
    p_watch.set_defaults(func=_watch)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
