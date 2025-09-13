"""Base command classes and utilities for Meshbot commands."""
import asyncio
import functools
import logging
import math
import time
from datetime import datetime
from typing import Dict, Any

import discord

logger = logging.getLogger(__name__)


class FunctionCache: #pylint: disable=too-few-public-methods
    """Cache implementation for function results"""

    def __init__(self):
        self.cache = {}
        self.timestamps = {}
        self.lock = asyncio.Lock()

    async def get_or_set(self, cache_key, fetch_func, ttl_seconds, *args, **kwargs):
        """Get cached result or call function and cache result"""
        async with self.lock:
            current_time = time.time()

            # Check if cached result exists and is still valid
            if (cache_key in self.cache and
                cache_key in self.timestamps and
                current_time - self.timestamps[cache_key] < ttl_seconds):
                logger.debug("Cache hit for %s", fetch_func.__name__)
                return self.cache[cache_key]

            # Call the actual function
            logger.debug("Cache miss for %s", fetch_func.__name__)
            result = await fetch_func(*args, **kwargs)

            # Store result in cache
            self.cache[cache_key] = result
            self.timestamps[cache_key] = current_time

            # Clean old cache entries
            expired_keys = [
                key for key, timestamp in self.timestamps.items()
                if current_time - timestamp >= ttl_seconds
            ]
            for key in expired_keys:
                self.cache.pop(key, None)
                self.timestamps.pop(key, None)

            return result


def cache_result(ttl_seconds=300):
    """Cache function results for a specified time (thread-safe)"""
    def decorator(func):
        # Create cache instance for this function
        if not hasattr(func, 'cache_instance'):
            func.cache_instance = FunctionCache()

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Create a cache key from function arguments
            cache_key = str(args) + str(sorted(kwargs.items()))
            return await func.cache_instance.get_or_set(
                cache_key, func, ttl_seconds, *args, **kwargs
            )

        return wrapper
    return decorator


def get_utc_time():
    """Get current time in UTC"""
    return datetime.utcnow()


def format_utc_time(dt=None, format_str="%Y-%m-%d %H:%M:%S UTC"):
    """Format datetime in UTC"""
    if dt is None:
        dt = get_utc_time()
    return dt.strftime(format_str)


class BaseCommandMixin:
    """Base mixin for command functionality"""

    def __init__(self):
        # Cache for frequently accessed data
        self._node_cache = {}
        self._cache_timestamps = {}
        self._cache_ttl = 60  # 1 minute cache TTL

    def _get_cached_data(self, key: str, fetch_func, *args, **kwargs):
        """Get data from cache or fetch if not available"""
        now = time.time()

        if (key in self._node_cache and
            key in self._cache_timestamps and
            now - self._cache_timestamps[key] < self._cache_ttl):
            return self._node_cache[key]

        # Fetch fresh data
        try:
            data = fetch_func(*args, **kwargs)
            self._node_cache[key] = data
            self._cache_timestamps[key] = now
            return data
        except (OSError, ValueError, TypeError, KeyError) as e:
            logger.error("Error fetching data for cache key %s: %s", key, e)
            # Return cached data if available, even if stale
            return self._node_cache.get(key, [])

    def clear_cache(self):
        """Clear all cached data"""
        self._node_cache.clear()
        self._cache_timestamps.clear()
        logger.info("Command cache cleared")

    async def _send_long_message(self, channel, message: str):
        """Send long messages by splitting if needed"""
        try:
            if len(message) <= 2000:
                await channel.send(message)
            else:
                # Split into chunks
                chunks = [message[i:i+1900] for i in range(0, len(message), 1900)]
                for chunk in chunks:
                    await channel.send(chunk)
        except (discord.HTTPException, discord.Forbidden, discord.NotFound) as e:
            logger.error("Error sending long message: %s", e)
            # Try to send a simple error message
            try:
                await channel.send("❌ Error sending message to channel.")
            except discord.HTTPException:
                pass  # Already logged the main error

    async def _safe_send(self, channel, message: str):
        """Safely send a message to a channel with error handling"""
        try:
            await channel.send(message)
        except (discord.HTTPException, discord.Forbidden, discord.NotFound) as e:
            logger.error("Error sending message to channel: %s", e)

    def _get_node_basic_info(self, node: Dict[str, Any]) -> tuple:
        """Extract basic node information"""
        return (
            str(node.get('long_name', 'Unknown')),
            str(node.get('node_id', 'Unknown')),
            str(node.get('node_num', 'Unknown')),
            str(node.get('hops_away', '0')),
            str(node.get('snr', '?'))
        )

    def _get_node_telemetry(self, node: Dict[str, Any]) -> tuple:
        """Extract node telemetry information"""
        battery = (f"{node.get('battery_level', 'N/A')}%"
                   if node.get('battery_level') is not None else "N/A")
        temperature = (f"{node.get('temperature', 'N/A'):.1f}°C"
                       if node.get('temperature') is not None else "N/A")
        return battery, temperature

    def _get_node_last_heard(self, node: Dict[str, Any]) -> str:
        """Get formatted last heard time"""
        if node.get('last_heard'):
            try:
                last_heard = datetime.fromisoformat(node['last_heard'])
                return last_heard.strftime('%H:%M:%S')
            except (ValueError, TypeError, AttributeError):
                return "Unknown"
        return "Unknown"

    def _format_node_info(self, node: Dict[str, Any]) -> str:
        """Format node information for display"""
        try:
            long_name, node_id, node_num, hops_away, snr = self._get_node_basic_info(node)
            battery, temperature = self._get_node_telemetry(node)
            time_str = self._get_node_last_heard(node)

            return (
                f"**{long_name}** (ID: {node_id}, Num: {node_num}) - "
                f"Hops: {hops_away}, SNR: {snr}, Battery: {battery}, "
                f"Temp: {temperature}, Last: {time_str}"
            )

        except (ValueError, TypeError, KeyError, AttributeError) as e:
            logger.error("Error formatting node info: %s", e)
            return f"**Node {node.get('node_id', 'Unknown')}** - Error formatting data"

    def _convert_coords_to_radians(self, lat1: float, lon1: float,
                                   lat2: float, lon2: float) -> tuple:
        """Convert coordinates to radians"""
        return (
            math.radians(lat1),
            math.radians(lon1),
            math.radians(lat2),
            math.radians(lon2)
        )

    def _apply_haversine_formula(self, lat1_rad: float, lon1_rad: float,
                                lat2_rad: float, lon2_rad: float) -> float:
        """Apply Haversine formula to calculate angular distance"""
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        a = (math.sin(dlat/2)**2 +
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2)
        return 2 * math.asin(math.sqrt(a))

    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two coordinates in meters using Haversine formula"""
        try:
            lat1_rad, lon1_rad, lat2_rad, lon2_rad = self._convert_coords_to_radians(
                lat1, lon1, lat2, lon2
            )
            c = self._apply_haversine_formula(lat1_rad, lon1_rad, lat2_rad, lon2_rad)

            # Earth's radius in meters
            earth_radius = 6371000
            return earth_radius * c

        except (ValueError, TypeError, ZeroDivisionError, OverflowError) as e:
            logger.error("Error calculating distance: %s", e)
            return 0.0
