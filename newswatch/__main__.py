"""CLI entry point."""

import asyncio
import sys

import click
from rich.console import Console
from rich.table import Table

from newswatch import __version__
from newswatch.pipeline import NewsPipeline

console = Console()


@click.group()
@click.version_option(__version__, prog_name="newswatch")
def main():
    """newswatch — News monitoring pipeline powered by QuartzUnit tools."""


@main.command()
@click.option("--category", "-c", multiple=True, help="Feed categories to subscribe (e.g., technology, science)")
@click.option("--feed", "-f", multiple=True, help="Individual feed URLs to subscribe")
def setup(category, feed):
    """Subscribe to feeds from the built-in catalog or custom URLs."""
    pipeline = NewsPipeline()
    count = asyncio.run(pipeline.setup(
        categories=list(category) if category else None,
        feeds=list(feed) if feed else None,
    ))
    console.print(f"[green]Subscribed to {count} feeds[/green]")
    pipeline.close()


@main.command()
@click.option("--extract-limit", "-n", default=50, help="Max articles to extract")
@click.option("--track", "-t", multiple=True, help="URLs to monitor for changes")
def run(extract_limit, track):
    """Run the full pipeline: collect → extract → index → track."""
    pipeline = NewsPipeline()

    console.print("[bold]Running newswatch pipeline...[/bold]\n")

    result = asyncio.run(pipeline.run(
        extract_limit=extract_limit,
        track_urls=list(track) if track else None,
    ))

    table = Table(title="Pipeline Results")
    table.add_column("Step", style="cyan")
    table.add_column("Result", justify="right")

    table.add_row("Feeds collected", str(result.feeds_collected))
    table.add_row("New articles", str(result.articles_new))
    table.add_row("Articles extracted", str(result.articles_extracted))
    table.add_row("Articles indexed", str(result.articles_indexed))
    if result.changes_detected:
        table.add_row("Changes detected", str(result.changes_detected))

    console.print(table)

    if result.errors:
        console.print(f"\n[yellow]{len(result.errors)} errors:[/yellow]")
        for e in result.errors:
            console.print(f"  - {e}")

    pipeline.close()


@main.command()
@click.argument("query")
@click.option("--count", "-n", default=5, help="Number of results")
def search(query, count):
    """Semantic search across collected articles."""
    pipeline = NewsPipeline()
    results = pipeline.search(query, top_k=count)

    if not results:
        console.print("[dim]No results. Run `newswatch run` first to collect and index articles.[/dim]")
    else:
        table = Table(title=f"Search: '{query}'")
        table.add_column("Score", justify="right", style="green")
        table.add_column("Text", max_width=80)
        table.add_column("Source", style="dim")

        for r in results:
            table.add_row(str(r["score"]), r["text"][:80], r["file"])
        console.print(table)

    pipeline.close()


if __name__ == "__main__":
    main()
