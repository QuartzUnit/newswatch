"""Core pipeline — collect → extract → index → track."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path

from feedkit import FeedStore, search_catalog
from feedkit.core import collect

from markgrab import extract

from embgrep import EmbGrep

from diffgrab import DiffTracker

logger = logging.getLogger(__name__)

DEFAULT_DB_DIR = Path.home() / ".newswatch"


@dataclass
class PipelineResult:
    """Result of a full pipeline run."""

    feeds_collected: int = 0
    articles_new: int = 0
    articles_extracted: int = 0
    articles_indexed: int = 0
    changes_detected: int = 0
    errors: list[str] = field(default_factory=list)


class NewsPipeline:
    """End-to-end news monitoring pipeline.

    Chains four QuartzUnit libraries:
    1. feedkit  — RSS/Atom feed collection
    2. markgrab — Full article content extraction
    3. embgrep  — Semantic search indexing
    4. diffgrab — Change tracking for key pages
    """

    def __init__(self, db_dir: str | Path | None = None) -> None:
        self.db_dir = Path(db_dir) if db_dir else DEFAULT_DB_DIR
        self.db_dir.mkdir(parents=True, exist_ok=True)

        self._store = FeedStore(self.db_dir / "feeds.db")
        self._index = EmbGrep(db_path=self.db_dir / "index.db")
        self._tracker = DiffTracker(db_path=self.db_dir / "tracker.db")

    async def setup(self, categories: list[str] | None = None, feeds: list[str] | None = None) -> int:
        """Subscribe to feeds. Returns count of subscribed feeds.

        Args:
            categories: Catalog categories to subscribe (e.g., ["technology", "science"]).
            feeds: Individual feed URLs to subscribe.
        """
        count = 0
        if categories:
            for cat in categories:
                cat_feeds = search_catalog(category=cat, limit=1000)
                for f in cat_feeds:
                    self._store.subscribe(f.url, title=f.title, category=f.category, language=f.language)
                    count += len(cat_feeds)

        if feeds:
            for url in feeds:
                self._store.subscribe(url)
                count += 1

        return count

    async def collect(self, concurrency: int = 20) -> tuple[int, int]:
        """Collect new articles from all subscribed feeds.

        Returns:
            Tuple of (feeds_ok, new_articles).
        """
        result = await collect(self._store, concurrency=concurrency)
        return result.feeds_ok, result.new_articles

    async def extract_and_index(self, limit: int = 50) -> tuple[int, int]:
        """Extract full article content and index for semantic search.

        Args:
            limit: Max articles to process per run.

        Returns:
            Tuple of (extracted, indexed).
        """
        articles = self._store.get_latest(count=limit)
        extracted = 0
        indexed = 0
        texts = []

        for article in articles:
            if not article.url:
                continue
            try:
                result = await extract(article.url, max_chars=10_000, timeout=15.0)
                if result.markdown and result.word_count > 30:
                    texts.append({
                        "text": f"# {article.title}\n\n{result.markdown}",
                        "source": article.url,
                    })
                    extracted += 1
            except Exception as e:
                logger.debug(f"Extract failed for {article.url}: {e}")

        if texts:
            # Write extracted texts to temp files for indexing
            extract_dir = self.db_dir / "extracted"
            extract_dir.mkdir(exist_ok=True)
            for i, t in enumerate(texts):
                fpath = extract_dir / f"article_{i:04d}.md"
                fpath.write_text(t["text"], encoding="utf-8")

            self._index.index(str(extract_dir), patterns=["*.md"])
            indexed = len(texts)

        return extracted, indexed

    async def track_pages(self, urls: list[str]) -> int:
        """Track specific pages for changes.

        Args:
            urls: URLs to monitor for changes.

        Returns:
            Number of changes detected.
        """
        for url in urls:
            await self._tracker.track(url)

        changes = await self._tracker.check()
        return sum(1 for c in changes if c.changed)

    def search(self, query: str, top_k: int = 5) -> list[dict]:
        """Semantic search across collected and indexed articles.

        Args:
            query: Natural language search query.
            top_k: Number of results.

        Returns:
            List of search results with file_path, score, and chunk_text.
        """
        results = self._index.search(query, top_k=top_k)
        return [
            {
                "file": r.file_path,
                "score": round(r.score, 4),
                "text": r.chunk_text[:200],
                "lines": f"{r.line_start}-{r.line_end}",
            }
            for r in results
        ]

    async def run(
        self,
        extract_limit: int = 50,
        track_urls: list[str] | None = None,
    ) -> PipelineResult:
        """Run the full pipeline: collect → extract → index → track.

        Args:
            extract_limit: Max articles to extract per run.
            track_urls: Optional URLs to monitor for changes.
        """
        result = PipelineResult()

        # Step 1: Collect
        try:
            feeds_ok, new_articles = await self.collect()
            result.feeds_collected = feeds_ok
            result.articles_new = new_articles
        except Exception as e:
            result.errors.append(f"Collect: {e}")

        # Step 2: Extract + Index
        try:
            extracted, indexed = await self.extract_and_index(limit=extract_limit)
            result.articles_extracted = extracted
            result.articles_indexed = indexed
        except Exception as e:
            result.errors.append(f"Extract/Index: {e}")

        # Step 3: Track changes
        if track_urls:
            try:
                changes = await self.track_pages(track_urls)
                result.changes_detected = changes
            except Exception as e:
                result.errors.append(f"Track: {e}")

        return result

    def close(self) -> None:
        self._store.close()
        self._index.close()
