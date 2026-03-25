"""Tests for the newswatch pipeline."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from newswatch.pipeline import NewsPipeline, PipelineResult


@pytest.fixture
def tmp_pipeline(tmp_path):
    """Create a pipeline with temporary directory."""
    pipeline = NewsPipeline(db_dir=tmp_path / "newswatch")
    yield pipeline
    pipeline.close()


class TestPipelineInit:
    def test_creates_db_dir(self, tmp_path):
        db_dir = tmp_path / "test_newswatch"
        pipeline = NewsPipeline(db_dir=db_dir)
        assert db_dir.exists()
        pipeline.close()

    def test_default_db_dir(self):
        pipeline = NewsPipeline()
        assert pipeline.db_dir.name == ".newswatch"
        pipeline.close()


class TestPipelineSetup:
    @pytest.mark.asyncio
    async def test_setup_with_feeds(self, tmp_pipeline):
        count = await tmp_pipeline.setup(feeds=["https://example.com/rss"])
        assert count == 1

    @pytest.mark.asyncio
    async def test_setup_with_category(self, tmp_pipeline):
        count = await tmp_pipeline.setup(categories=["academia"])
        assert count > 0


class TestPipelineSearch:
    def test_search_empty_index(self, tmp_pipeline):
        results = tmp_pipeline.search("test query")
        assert results == []


class TestPipelineResult:
    def test_default_values(self):
        result = PipelineResult()
        assert result.feeds_collected == 0
        assert result.articles_new == 0
        assert result.articles_extracted == 0
        assert result.articles_indexed == 0
        assert result.changes_detected == 0
        assert result.errors == []

    def test_error_accumulation(self):
        result = PipelineResult()
        result.errors.append("test error")
        assert len(result.errors) == 1


class TestPipelineCollect:
    @pytest.mark.asyncio
    async def test_collect_no_subscriptions(self, tmp_pipeline):
        feeds_ok, new = await tmp_pipeline.collect()
        assert feeds_ok == 0
        assert new == 0


class TestPipelineRun:
    @pytest.mark.asyncio
    async def test_run_empty(self, tmp_pipeline):
        result = await tmp_pipeline.run()
        assert isinstance(result, PipelineResult)
        assert result.feeds_collected == 0
        assert result.articles_new == 0
