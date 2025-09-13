"""Tests for base command utilities and mixins."""
import asyncio
import time
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch

import pytest
import discord

from .base import (
    BaseCommandMixin,
    cache_result,
    get_utc_time,
    format_utc_time
)


class TestCacheResult:
    """Test the cache_result decorator."""

    @pytest.mark.asyncio
    async def test_cache_result_caches_function_result(self):
        """Test that cache_result properly caches function results."""
        call_count = 0

        @cache_result(ttl_seconds=10)
        async def test_function(value):
            nonlocal call_count
            call_count += 1
            return f"result_{value}"

        # First call should execute function
        result1 = await test_function("test")
        assert result1 == "result_test"
        assert call_count == 1

        # Second call should use cache
        result2 = await test_function("test")
        assert result2 == "result_test"
        assert call_count == 1

        # Different arguments should not use cache
        result3 = await test_function("different")
        assert result3 == "result_different"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_cache_result_expires_after_ttl(self):
        """Test that cached results expire after TTL."""
        call_count = 0

        @cache_result(ttl_seconds=0.1)  # Very short TTL
        async def test_function(value):
            nonlocal call_count
            call_count += 1
            return f"result_{value}"

        # First call
        result1 = await test_function("test")
        assert call_count == 1

        # Wait for cache to expire
        await asyncio.sleep(0.2)

        # Second call should execute function again
        result2 = await test_function("test")
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_cache_result_thread_safety(self):
        """Test that cache_result is thread-safe."""
        call_count = 0

        @cache_result(ttl_seconds=1)
        async def test_function(value):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)  # Simulate async work
            return f"result_{value}"

        # Run multiple concurrent calls
        tasks = [test_function("test") for _ in range(5)]
        results = await asyncio.gather(*tasks)

        # All should return same result
        assert all(r == "result_test" for r in results)

        # Function should only be called once due to cache
        assert call_count == 1


class TestUtilityFunctions:
    """Test utility functions."""

    def test_get_utc_time_returns_datetime(self):
        """Test that get_utc_time returns a datetime object."""
        result = get_utc_time()
        assert isinstance(result, datetime)

    def test_format_utc_time_with_default(self):
        """Test format_utc_time with default parameters."""
        result = format_utc_time()
        assert "UTC" in result
        assert len(result) > 10  # Should be a formatted datetime string

    def test_format_utc_time_with_custom_datetime(self):
        """Test format_utc_time with custom datetime."""
        test_dt = datetime(2023, 1, 1, 12, 0, 0)
        result = format_utc_time(test_dt)
        assert "2023-01-01 12:00:00 UTC" == result

    def test_format_utc_time_with_custom_format(self):
        """Test format_utc_time with custom format string."""
        test_dt = datetime(2023, 1, 1, 12, 0, 0)
        result = format_utc_time(test_dt, "%Y-%m-%d")
        assert "2023-01-01" == result


class TestBaseCommandMixin:
    """Test the BaseCommandMixin class."""

    def setup_method(self):
        """Set up test instance."""
        self.mixin = BaseCommandMixin()

    def test_init_creates_cache_structures(self):
        """Test that __init__ creates cache structures."""
        assert hasattr(self.mixin, '_node_cache')
        assert hasattr(self.mixin, '_cache_timestamps')
        assert hasattr(self.mixin, '_cache_ttl')
        assert isinstance(self.mixin._node_cache, dict)
        assert isinstance(self.mixin._cache_timestamps, dict)

    def test_get_cached_data_returns_fresh_data(self):
        """Test _get_cached_data fetches fresh data when cache is empty."""
        def mock_fetch():
            return {"test": "data"}

        result = self.mixin._get_cached_data("test_key", mock_fetch)
        assert result == {"test": "data"}
        assert "test_key" in self.mixin._node_cache

    def test_get_cached_data_returns_cached_data(self):
        """Test _get_cached_data returns cached data when available."""
        # Pre-populate cache
        self.mixin._node_cache["test_key"] = {"cached": "data"}
        self.mixin._cache_timestamps["test_key"] = time.time()

        def mock_fetch():
            return {"fresh": "data"}

        result = self.mixin._get_cached_data("test_key", mock_fetch)
        assert result == {"cached": "data"}  # Should return cached data

    def test_get_cached_data_handles_fetch_error(self):
        """Test _get_cached_data handles fetch function errors."""
        def failing_fetch():
            raise Exception("Fetch failed")

        result = self.mixin._get_cached_data("test_key", failing_fetch)
        assert result == []  # Should return empty list on error

    def test_clear_cache_empties_cache(self):
        """Test clear_cache empties all cache structures."""
        # Pre-populate cache
        self.mixin._node_cache["key1"] = "data1"
        self.mixin._cache_timestamps["key1"] = time.time()

        self.mixin.clear_cache()

        assert len(self.mixin._node_cache) == 0
        assert len(self.mixin._cache_timestamps) == 0

    @pytest.mark.asyncio
    async def test_send_long_message_short_message(self):
        """Test _send_long_message with short message."""
        mock_channel = Mock()
        mock_channel.send = AsyncMock()

        short_message = "Short message"
        await self.mixin._send_long_message(mock_channel, short_message)

        mock_channel.send.assert_called_once_with(short_message)

    @pytest.mark.asyncio
    async def test_send_long_message_long_message(self):
        """Test _send_long_message with long message that needs splitting."""
        mock_channel = Mock()
        mock_channel.send = AsyncMock()

        long_message = "A" * 2500  # Longer than 2000 chars
        await self.mixin._send_long_message(mock_channel, long_message)

        # Should be called multiple times for chunks
        assert mock_channel.send.call_count > 1

    @pytest.mark.asyncio
    async def test_send_long_message_handles_exception(self):
        """Test _send_long_message handles send exceptions."""
        mock_channel = Mock()

        # Make first call fail, second call succeed (error message)
        mock_channel.send = AsyncMock(side_effect=[Exception("Send failed"), None])

        # Should not raise exception
        try:
            await self.mixin._send_long_message(mock_channel, "test message")
        except Exception:
            pytest.fail("_send_long_message should handle exceptions gracefully")

        # Should have tried to send twice (original message + error message)
        assert mock_channel.send.call_count >= 1

    @pytest.mark.asyncio
    async def test_safe_send_success(self):
        """Test _safe_send with successful send."""
        mock_channel = Mock()
        mock_channel.send = AsyncMock()

        await self.mixin._safe_send(mock_channel, "test message")
        mock_channel.send.assert_called_once_with("test message")

    @pytest.mark.asyncio
    async def test_safe_send_handles_exception(self):
        """Test _safe_send handles send exceptions."""
        mock_channel = Mock()
        mock_channel.send = AsyncMock(side_effect=Exception("Send failed"))

        # Should not raise exception
        await self.mixin._safe_send(mock_channel, "test message")

    def test_format_node_info_complete_data(self, sample_node_data):
        """Test _format_node_info with complete node data."""
        result = self.mixin._format_node_info(sample_node_data)

        assert "Test Node" in result
        assert "!12345678" in result
        assert "123456789" in result
        assert "85%" in result
        assert "23.5°C" in result

    def test_format_node_info_minimal_data(self):
        """Test _format_node_info with minimal node data."""
        minimal_node = {"node_id": "!12345678"}
        result = self.mixin._format_node_info(minimal_node)

        assert "!12345678" in result
        assert "Unknown" in result

    def test_format_node_info_handles_exception(self):
        """Test _format_node_info handles malformed data."""
        # Create a malformed node that will cause an exception in formatting
        class BadDict(dict):
            def get(self, key, default=None):
                if key == 'node_id':
                    return '!12345678'
                elif key == 'temperature':
                    raise ValueError("Bad temperature data")
                return default

        malformed_node = BadDict()
        result = self.mixin._format_node_info(malformed_node)

        assert "Error formatting data" in result

    def test_calculate_distance_valid_coordinates(self):
        """Test calculate_distance with valid coordinates."""
        # Distance between NYC and LA (approximately 3944 km)
        nyc_lat, nyc_lon = 40.7128, -74.0060
        la_lat, la_lon = 34.0522, -118.2437

        distance = self.mixin.calculate_distance(nyc_lat, nyc_lon, la_lat, la_lon)

        # Should be approximately 3,944,000 meters (allow 10% variance)
        assert 3_500_000 < distance < 4_400_000

    def test_calculate_distance_same_coordinates(self):
        """Test calculate_distance with same coordinates."""
        lat, lon = 40.7128, -74.0060
        distance = self.mixin.calculate_distance(lat, lon, lat, lon)

        assert distance == 0.0

    def test_calculate_distance_handles_exception(self):
        """Test calculate_distance handles invalid coordinates."""
        # Test with invalid coordinates that might cause math errors
        distance = self.mixin.calculate_distance(None, None, 40.0, -74.0)

        assert distance == 0.0  # Should return 0.0 on error
