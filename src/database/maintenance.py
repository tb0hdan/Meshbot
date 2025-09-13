"""
Database maintenance operations module
Handles cleanup, optimization, and background maintenance tasks
"""

import sqlite3
import logging
import threading
import time
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class DatabaseMaintenance:
    """Handles database maintenance and optimization tasks"""

    def __init__(self, connection_manager):
        self.connection_manager = connection_manager
        self._shutdown = False
        self._maintenance_thread = None

    def start_maintenance_task(self):
        """Start background maintenance task"""
        def maintenance_worker():
            while not self._shutdown:
                try:
                    # Check shutdown flag more frequently
                    for _ in range(360):  # Check every 10 seconds for 1 hour
                        if self._shutdown:
                            break
                        time.sleep(10)
                    if not self._shutdown:
                        self.run_maintenance()
                except Exception as e:
                    logger.error("Error in maintenance task: %s", e)
                    time.sleep(300)  # Wait 5 minutes before retrying

        self._maintenance_thread = threading.Thread(target=maintenance_worker, daemon=True)
        self._maintenance_thread.start()
        logger.info("Database maintenance task started")

    def run_maintenance(self):
        """Run database maintenance tasks"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Analyze database for query optimization
                cursor.execute("ANALYZE")

                # Clean up old data (keep 30 days)
                self.cleanup_old_data(30)

                # Vacuum if needed (check database size)
                cursor.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]
                cursor.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]
                db_size_mb = (page_count * page_size) / (1024 * 1024)

                if db_size_mb > 100:  # If database is larger than 100MB
                    logger.info("Running VACUUM to optimize database")
                    cursor.execute("VACUUM")

                logger.info("Database maintenance completed")

        except Exception as e:
            logger.error("Error during database maintenance: %s", e)

    def cleanup_old_data(self, days: int = 30):
        """Clean up old telemetry and position data"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                cutoff_time = datetime.now() - timedelta(days=days)

                # Clean up old telemetry
                cursor.execute(
                    "DELETE FROM telemetry WHERE timestamp < ?",
                    (cutoff_time.isoformat(),)
                )
                telemetry_deleted = cursor.rowcount

                # Clean up old positions
                cursor.execute(
                    "DELETE FROM positions WHERE timestamp < ?",
                    (cutoff_time.isoformat(),)
                )
                positions_deleted = cursor.rowcount

                # Clean up old messages
                cursor.execute(
                    "DELETE FROM messages WHERE timestamp < ?",
                    (cutoff_time.isoformat(),)
                )
                messages_deleted = cursor.rowcount

                logger.info(
                    "Cleaned up %s telemetry, %s positions, %s messages",
                    telemetry_deleted, positions_deleted, messages_deleted
                )

        except sqlite3.OperationalError as e:
            logger.error("Database operational error cleaning up old data: %s", e)
        except sqlite3.Error as e:
            logger.error("Database error cleaning up old data: %s", e)
        except Exception as e:
            logger.error("Unexpected error cleaning up old data: %s", e)

    def stop_maintenance(self):
        """Stop the maintenance task"""
        self._shutdown = True
        if self._maintenance_thread and self._maintenance_thread.is_alive():
            self._maintenance_thread.join(timeout=5)
        logger.info("Database maintenance stopped")