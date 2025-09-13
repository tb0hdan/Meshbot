"""
Telemetry database operations module
Handles all telemetry-related database operations
"""

import sqlite3
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class TelemetryOperations:
    """Handles all telemetry-related database operations"""

    def __init__(self, connection_manager):
        self.connection_manager = connection_manager

    def add_telemetry(self, node_id: str, telemetry_data: Dict[str, Any]) -> bool:
        """Add telemetry data for a node"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT INTO telemetry (
                        node_id, battery_level, voltage, channel_utilization, air_util_tx, uptime_seconds,
                        temperature, humidity, pressure, gas_resistance, iaq,
                        pm10, pm25, pm100,
                        ch1_voltage, ch2_voltage, ch3_voltage, ch4_voltage, ch5_voltage, ch6_voltage, ch7_voltage, ch8_voltage,
                        ch1_current, ch2_current, ch3_current, ch4_current, ch5_current, ch6_current, ch7_current, ch8_current,
                        snr, rssi, frequency,
                        latitude, longitude, altitude, speed, heading, accuracy
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    node_id,
                    telemetry_data.get('battery_level'),
                    telemetry_data.get('voltage'),
                    telemetry_data.get('channel_utilization'),
                    telemetry_data.get('air_util_tx'),
                    telemetry_data.get('uptime_seconds'),
                    telemetry_data.get('temperature'),
                    telemetry_data.get('humidity'),
                    telemetry_data.get('pressure'),
                    telemetry_data.get('gas_resistance'),
                    telemetry_data.get('iaq'),
                    telemetry_data.get('pm10'),
                    telemetry_data.get('pm25'),
                    telemetry_data.get('pm100'),
                    telemetry_data.get('ch1_voltage'),
                    telemetry_data.get('ch2_voltage'),
                    telemetry_data.get('ch3_voltage'),
                    telemetry_data.get('ch4_voltage'),
                    telemetry_data.get('ch5_voltage'),
                    telemetry_data.get('ch6_voltage'),
                    telemetry_data.get('ch7_voltage'),
                    telemetry_data.get('ch8_voltage'),
                    telemetry_data.get('ch1_current'),
                    telemetry_data.get('ch2_current'),
                    telemetry_data.get('ch3_current'),
                    telemetry_data.get('ch4_current'),
                    telemetry_data.get('ch5_current'),
                    telemetry_data.get('ch6_current'),
                    telemetry_data.get('ch7_current'),
                    telemetry_data.get('ch8_current'),
                    telemetry_data.get('snr'),
                    telemetry_data.get('rssi'),
                    telemetry_data.get('frequency'),
                    telemetry_data.get('latitude'),
                    telemetry_data.get('longitude'),
                    telemetry_data.get('altitude'),
                    telemetry_data.get('speed'),
                    telemetry_data.get('heading'),
                    telemetry_data.get('accuracy')
                ))

                return True

        except sqlite3.OperationalError as e:
            logger.error("Database operational error adding telemetry: %s", e)
            return False
        except sqlite3.Error as e:
            logger.error("Database error adding telemetry: %s", e)
            return False
        except (KeyError, ValueError, TypeError) as e:
            logger.error("Unexpected error adding telemetry: %s", e)
            return False

    def get_telemetry_summary(self, minutes: int = 60) -> Dict[str, Any]:
        """Get telemetry summary for active nodes"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                cutoff_time = datetime.now() - timedelta(minutes=minutes)

                cursor.execute("""
                    SELECT
                        COUNT(DISTINCT n.node_id) as total_nodes,
                        COUNT(DISTINCT CASE WHEN n.last_heard > ? THEN n.node_id END) as active_nodes,
                        AVG(t.battery_level) as avg_battery,
                        AVG(t.temperature) as avg_temperature,
                        AVG(t.humidity) as avg_humidity,
                        AVG(t.snr) as avg_snr,
                        AVG(t.rssi) as avg_rssi
                    FROM nodes n
                    LEFT JOIN telemetry t ON n.node_id = t.node_id
                """, (cutoff_time.isoformat(),))

                result = cursor.fetchone()
                columns = [description[0] for description in cursor.description]

                return dict(zip(columns, result))

        except sqlite3.OperationalError as e:
            logger.error("Database operational error getting telemetry summary: %s", e)
            return {}
        except sqlite3.Error as e:
            logger.error("Database error getting telemetry summary: %s", e)
            return {}
        except (ValueError, TypeError) as e:
            logger.error("Unexpected error getting telemetry summary: %s", e)
            return {}

    def get_telemetry_history(
        self, node_id: str, hours: int = 24, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get telemetry history for a specific node"""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()

                cutoff_time = datetime.now() - timedelta(hours=hours)

                cursor.execute("""
                    SELECT timestamp, battery_level, voltage, temperature, humidity,
                           pressure, gas_resistance, iaq, snr, rssi, frequency,
                           latitude, longitude, altitude, speed, heading, accuracy
                    FROM telemetry
                    WHERE node_id = ? AND timestamp > ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                """, (node_id, cutoff_time.isoformat(), limit))

                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()

                return [dict(zip(columns, row)) for row in rows]

        except (sqlite3.Error, ValueError, TypeError) as e:
            logger.error("Error getting telemetry history: %s", e)
            return []
