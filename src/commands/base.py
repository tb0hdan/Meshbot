"""Base command classes and utilities for Meshbot commands."""
import asyncio
import functools
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional

import discord

from src.database import MeshtasticDatabase

logger = logging.getLogger(__name__)


def cache_result(ttl_seconds=300):
    """Cache function results for a specified time (thread-safe)"""
    def decorator(func):
        # Use the function object as the cache key base
        if not hasattr(func, '_cache'):
            func._cache = {}
            func._cache_timestamps = {}
            func._cache_lock = asyncio.Lock()

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Create a cache key from function arguments
            cache_key = str(args) + str(sorted(kwargs.items()))

            async with func._cache_lock:  # Thread-safe access
                current_time = time.time()

                # Check if cached result exists and is still valid
                if (cache_key in func._cache and
                    cache_key in func._cache_timestamps and
                    current_time - func._cache_timestamps[cache_key] < ttl_seconds):
                    logger.debug("Cache hit for %s", func.__name__)
                    return func._cache[cache_key]

                # Call the actual function
                logger.debug("Cache miss for %s", func.__name__)
                result = await func(*args, **kwargs)

                # Store result in cache
                func._cache[cache_key] = result
                func._cache_timestamps[cache_key] = current_time

                # Clean old cache entries
                expired_keys = [
                    key for key, timestamp in func._cache_timestamps.items()
                    if current_time - timestamp >= ttl_seconds
                ]
                for key in expired_keys:
                    func._cache.pop(key, None)
                    func._cache_timestamps.pop(key, None)

                return result

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
        except Exception as e:
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
        except Exception as e:
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
        except Exception as e:
            logger.error("Error sending message to channel: %s", e)

    def _format_node_info(self, node: Dict[str, Any]) -> str:
        """Format node information for display"""
        try:
            long_name = str(node.get('long_name', 'Unknown'))
            node_id = str(node.get('node_id', 'Unknown'))
            node_num = str(node.get('node_num', 'Unknown'))
            hops_away = str(node.get('hops_away', '0'))
            snr = str(node.get('snr', '?'))
            battery = f"{node.get('battery_level', 'N/A')}%" if node.get('battery_level') is not None else "N/A"
            temperature = f"{node.get('temperature', 'N/A'):.1f}°C" if node.get('temperature') is not None else "N/A"

            if node.get('last_heard'):
                try:
                    last_heard = datetime.fromisoformat(node['last_heard'])
                    time_str = last_heard.strftime('%H:%M:%S')
                except (ValueError, TypeError, AttributeError):
                    time_str = "Unknown"
            else:
                time_str = "Unknown"

            return (
                f"**{long_name}** (ID: {node_id}, Num: {node_num}) - "
                f"Hops: {hops_away}, SNR: {snr}, Battery: {battery}, "
                f"Temp: {temperature}, Last: {time_str}"
            )

        except Exception as e:
            logger.error("Error formatting node info: %s", e)
            return f"**Node {node.get('node_id', 'Unknown')}** - Error formatting data"

    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two coordinates in meters using Haversine formula"""
        try:
            import math

            # Convert to radians
            lat1_rad = math.radians(lat1)
            lon1_rad = math.radians(lon1)
            lat2_rad = math.radians(lat2)
            lon2_rad = math.radians(lon2)

            # Haversine formula
            dlat = lat2_rad - lat1_rad
            dlon = lon2_rad - lon1_rad
            a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))

            # Earth's radius in meters
            earth_radius = 6371000
            distance = earth_radius * c

            return distance
        except Exception as e:
            logger.error("Error calculating distance: %s", e)
            return 0.0
