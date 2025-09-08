"""
Database module for Meshtastic Discord Bridge Bot
Handles SQLite storage for nodes, telemetry, and position data
"""

import sqlite3
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

class MeshtasticDatabase:
    """SQLite database manager for Meshtastic node data"""
    
    def __init__(self, db_path: str = "meshtastic.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database tables"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Nodes table - stores basic node information
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS nodes (
                        node_id TEXT PRIMARY KEY,
                        node_num INTEGER,
                        long_name TEXT NOT NULL,
                        short_name TEXT,
                        macaddr TEXT,
                        hw_model TEXT,
                        firmware_version TEXT,
                        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_heard TIMESTAMP,
                        hops_away INTEGER DEFAULT 0,
                        is_router BOOLEAN DEFAULT FALSE,
                        is_client BOOLEAN DEFAULT TRUE
                    )
                """)
                
                # Telemetry table - stores telemetry data
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS telemetry (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        node_id TEXT NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        battery_level REAL,
                        voltage REAL,
                        temperature REAL,
                        humidity REAL,
                        pressure REAL,
                        gas_resistance REAL,
                        iaq REAL,
                        snr REAL,
                        rssi REAL,
                        frequency REAL,
                        latitude REAL,
                        longitude REAL,
                        altitude REAL,
                        speed REAL,
                        heading REAL,
                        accuracy REAL,
                        FOREIGN KEY (node_id) REFERENCES nodes (node_id)
                    )
                """)
                
                # Position table - stores position data
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS positions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        node_id TEXT NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        latitude REAL,
                        longitude REAL,
                        altitude REAL,
                        speed REAL,
                        heading REAL,
                        accuracy REAL,
                        source TEXT,
                        FOREIGN KEY (node_id) REFERENCES nodes (node_id)
                    )
                """)
                
                # Messages table - stores message history
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        from_node_id TEXT,
                        to_node_id TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        message_text TEXT,
                        port_num TEXT,
                        payload TEXT,
                        hops_away INTEGER,
                        snr REAL,
                        rssi REAL,
                        FOREIGN KEY (from_node_id) REFERENCES nodes (node_id),
                        FOREIGN KEY (to_node_id) REFERENCES nodes (node_id)
                    )
                """)
                
                # Create indexes for better performance
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_nodes_last_heard ON nodes (last_heard)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp ON telemetry (timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_timestamp ON positions (timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages (timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_nodes_long_name ON nodes (long_name)")
                
                conn.commit()
                logger.info("Database initialized successfully")
                
        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error: {e}")
            raise
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing database: {e}")
            raise
    
    def add_or_update_node(self, node_data: Dict[str, Any]) -> Tuple[bool, bool]:
        """Add new node or update existing node information"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Check if node exists
                cursor.execute("SELECT node_id FROM nodes WHERE node_id = ?", (node_data['node_id'],))
                exists = cursor.fetchone()
                
                if exists:
                    # Update existing node
                    cursor.execute("""
                        UPDATE nodes SET 
                            node_num = ?,
                            long_name = ?,
                            short_name = ?,
                            macaddr = ?,
                            hw_model = ?,
                            firmware_version = ?,
                            last_seen = CURRENT_TIMESTAMP,
                            last_heard = ?,
                            hops_away = ?,
                            is_router = ?,
                            is_client = ?
                        WHERE node_id = ?
                    """, (
                        node_data.get('node_num'),
                        node_data.get('long_name', 'Unknown'),
                        node_data.get('short_name'),
                        node_data.get('macaddr'),
                        node_data.get('hw_model'),
                        node_data.get('firmware_version'),
                        node_data.get('last_heard'),
                        node_data.get('hops_away', 0),
                        node_data.get('is_router', False),
                        node_data.get('is_client', True),
                        node_data['node_id']
                    ))
                    
                    # Check if this is a new node (first time seen)
                    cursor.execute("SELECT first_seen FROM nodes WHERE node_id = ?", (node_data['node_id'],))
                    first_seen = cursor.fetchone()[0]
                    
                    if first_seen == node_data.get('last_heard'):
                        # This is a new node
                        conn.commit()
                        return True, True  # (success, is_new_node)
                    
                else:
                    # Insert new node
                    cursor.execute("""
                        INSERT INTO nodes (
                            node_id, node_num, long_name, short_name, macaddr,
                            hw_model, firmware_version, last_heard, hops_away,
                            is_router, is_client
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        node_data['node_id'],
                        node_data.get('node_num'),
                        node_data.get('long_name', 'Unknown'),
                        node_data.get('short_name'),
                        node_data.get('macaddr'),
                        node_data.get('hw_model'),
                        node_data.get('firmware_version'),
                        node_data.get('last_heard'),
                        node_data.get('hops_away', 0),
                        node_data.get('is_router', False),
                        node_data.get('is_client', True)
                    ))
                    conn.commit()
                    return True, True  # (success, is_new_node)
                
                conn.commit()
                return True, False  # (success, not_new_node)
                
        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error adding/updating node: {e}")
            return False, False
        except sqlite3.Error as e:
            logger.error(f"Database error adding/updating node: {e}")
            return False, False
        except Exception as e:
            logger.error(f"Unexpected error adding/updating node: {e}")
            return False, False
    
    def add_telemetry(self, node_id: str, telemetry_data: Dict[str, Any]) -> bool:
        """Add telemetry data for a node"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    INSERT INTO telemetry (
                        node_id, battery_level, voltage, temperature, humidity,
                        pressure, gas_resistance, iaq, snr, rssi, frequency,
                        latitude, longitude, altitude, speed, heading, accuracy
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    node_id,
                    telemetry_data.get('battery_level'),
                    telemetry_data.get('voltage'),
                    telemetry_data.get('temperature'),
                    telemetry_data.get('humidity'),
                    telemetry_data.get('pressure'),
                    telemetry_data.get('gas_resistance'),
                    telemetry_data.get('iaq'),
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
                
                conn.commit()
                return True
                
        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error adding telemetry: {e}")
            return False
        except sqlite3.Error as e:
            logger.error(f"Database error adding telemetry: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error adding telemetry: {e}")
            return False
    
    def add_position(self, node_id: str, position_data: Dict[str, Any]) -> bool:
        """Add position data for a node"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    INSERT INTO positions (
                        node_id, latitude, longitude, altitude, speed, heading, accuracy, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    node_id,
                    position_data.get('latitude'),
                    position_data.get('longitude'),
                    position_data.get('altitude'),
                    position_data.get('speed'),
                    position_data.get('heading'),
                    position_data.get('accuracy'),
                    position_data.get('source', 'unknown')
                ))
                
                conn.commit()
                return True
                
        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error adding position: {e}")
            return False
        except sqlite3.Error as e:
            logger.error(f"Database error adding position: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error adding position: {e}")
            return False
    
    def add_message(self, message_data: Dict[str, Any]) -> bool:
        """Add message to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    INSERT INTO messages (
                        from_node_id, to_node_id, message_text, port_num, payload,
                        hops_away, snr, rssi
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    message_data.get('from_node_id'),
                    message_data.get('to_node_id'),
                    message_data.get('message_text'),
                    message_data.get('port_num'),
                    message_data.get('payload'),
                    message_data.get('hops_away'),
                    message_data.get('snr'),
                    message_data.get('rssi')
                ))
                
                conn.commit()
                return True
                
        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error adding message: {e}")
            return False
        except sqlite3.Error as e:
            logger.error(f"Database error adding message: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error adding message: {e}")
            return False
    
    def get_active_nodes(self, minutes: int = 60) -> List[Dict[str, Any]]:
        """Get nodes active in the last N minutes"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cutoff_time = datetime.now() - timedelta(minutes=minutes)
                
                cursor.execute("""
                    SELECT n.*, 
                           t.battery_level, t.voltage, t.temperature, t.humidity,
                           t.pressure, t.gas_resistance, t.iaq, t.snr, t.rssi,
                           p.latitude, p.longitude, p.altitude, p.speed, p.heading
                    FROM nodes n
                    LEFT JOIN (
                        SELECT node_id, battery_level, voltage, temperature, humidity,
                               pressure, gas_resistance, iaq, snr, rssi
                        FROM telemetry 
                        WHERE timestamp = (
                            SELECT MAX(timestamp) FROM telemetry t2 
                            WHERE t2.node_id = telemetry.node_id
                        )
                    ) t ON n.node_id = t.node_id
                    LEFT JOIN (
                        SELECT node_id, latitude, longitude, altitude, speed, heading
                        FROM positions 
                        WHERE timestamp = (
                            SELECT MAX(timestamp) FROM positions p2 
                            WHERE p2.node_id = positions.node_id
                        )
                    ) p ON n.node_id = p.node_id
                    WHERE n.last_heard > ?
                    ORDER BY n.last_heard DESC
                """, (cutoff_time.isoformat(),))
                
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                
                return [dict(zip(columns, row)) for row in rows]
                
        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error getting active nodes: {e}")
            return []
        except sqlite3.Error as e:
            logger.error(f"Database error getting active nodes: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error getting active nodes: {e}")
            return []
    
    def get_all_nodes(self) -> List[Dict[str, Any]]:
        """Get all known nodes"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT n.*, 
                           t.battery_level, t.voltage, t.temperature, t.humidity,
                           t.pressure, t.gas_resistance, t.iaq, t.snr, t.rssi,
                           p.latitude, p.longitude, p.altitude, p.speed, p.heading
                    FROM nodes n
                    LEFT JOIN (
                        SELECT node_id, battery_level, voltage, temperature, humidity,
                               pressure, gas_resistance, iaq, snr, rssi
                        FROM telemetry 
                        WHERE timestamp = (
                            SELECT MAX(timestamp) FROM telemetry t2 
                            WHERE t2.node_id = telemetry.node_id
                        )
                    ) t ON n.node_id = t.node_id
                    LEFT JOIN (
                        SELECT node_id, latitude, longitude, altitude, speed, heading
                        FROM positions 
                        WHERE timestamp = (
                            SELECT MAX(timestamp) FROM positions p2 
                            WHERE p2.node_id = positions.node_id
                        )
                    ) p ON n.node_id = p.node_id
                    ORDER BY n.last_heard DESC
                """)
                
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                
                return [dict(zip(columns, row)) for row in rows]
                
        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error getting all nodes: {e}")
            return []
        except sqlite3.Error as e:
            logger.error(f"Database error getting all nodes: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error getting all nodes: {e}")
            return []
    
    def find_node_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find node by fuzzy matching on long name"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Try exact match first
                cursor.execute("""
                    SELECT * FROM nodes WHERE long_name = ? OR short_name = ?
                """, (name, name))
                result = cursor.fetchone()
                
                if result:
                    columns = [description[0] for description in cursor.description]
                    return dict(zip(columns, result))
                
                # Try partial match
                cursor.execute("""
                    SELECT * FROM nodes WHERE long_name LIKE ? OR short_name LIKE ?
                    ORDER BY 
                        CASE 
                            WHEN long_name = ? THEN 1
                            WHEN long_name LIKE ? THEN 2
                            WHEN short_name = ? THEN 3
                            WHEN short_name LIKE ? THEN 4
                            ELSE 5
                        END,
                        last_heard DESC
                    LIMIT 1
                """, (f"%{name}%", f"%{name}%", name, f"{name}%", name, f"{name}%"))
                
                result = cursor.fetchone()
                if result:
                    columns = [description[0] for description in cursor.description]
                    return dict(zip(columns, result))
                
                return None
                
        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error finding node by name: {e}")
            return None
        except sqlite3.Error as e:
            logger.error(f"Database error finding node by name: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error finding node by name: {e}")
            return None
    
    def get_telemetry_summary(self, minutes: int = 60) -> Dict[str, Any]:
        """Get telemetry summary for active nodes"""
        try:
            with sqlite3.connect(self.db_path) as conn:
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
            logger.error(f"Database operational error getting telemetry summary: {e}")
            return {}
        except sqlite3.Error as e:
            logger.error(f"Database error getting telemetry summary: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error getting telemetry summary: {e}")
            return {}
    
    def cleanup_old_data(self, days: int = 30):
        """Clean up old telemetry and position data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cutoff_time = datetime.now() - timedelta(days=days)
                
                # Clean up old telemetry
                cursor.execute("DELETE FROM telemetry WHERE timestamp < ?", (cutoff_time.isoformat(),))
                telemetry_deleted = cursor.rowcount
                
                # Clean up old positions
                cursor.execute("DELETE FROM positions WHERE timestamp < ?", (cutoff_time.isoformat(),))
                positions_deleted = cursor.rowcount
                
                # Clean up old messages
                cursor.execute("DELETE FROM messages WHERE timestamp < ?", (cutoff_time.isoformat(),))
                messages_deleted = cursor.rowcount
                
                conn.commit()
                
                logger.info(f"Cleaned up {telemetry_deleted} telemetry, {positions_deleted} positions, {messages_deleted} messages")
                
        except sqlite3.OperationalError as e:
            logger.error(f"Database operational error cleaning up old data: {e}")
        except sqlite3.Error as e:
            logger.error(f"Database error cleaning up old data: {e}")
        except Exception as e:
            logger.error(f"Unexpected error cleaning up old data: {e}")

    def get_node_display_name(self, node_id: str) -> str:
        """Return the best human-friendly name for a node_id (long_name > short_name > node_id)."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT 
                        CASE 
                            WHEN long_name IS NOT NULL AND TRIM(long_name) <> '' THEN long_name
                            WHEN short_name IS NOT NULL AND TRIM(short_name) <> '' THEN short_name
                            ELSE node_id
                        END AS display_name
                    FROM nodes WHERE node_id = ?
                    """, 
                    (node_id,)
                )
                row = cursor.fetchone()
                if row and row[0]:
                    return str(row[0])
        except Exception as e:
            logger.warning(f"Failed to lookup display name for {node_id}: {e}")
        return str(node_id)

    def get_recent_messages(self, limit:int=20) -> List[Dict[str, Any]]:
        """Return the most recent text messages with names resolved if available."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT m.timestamp, m.from_node_id, m.to_node_id, m.message_text, m.hops_away, m.snr, m.rssi,
                           fn.long_name AS from_long, fn.short_name AS from_short,
                           tn.long_name AS to_long, tn.short_name AS to_short
                    FROM messages m
                    LEFT JOIN nodes fn ON fn.node_id = m.from_node_id
                    LEFT JOIN nodes tn ON tn.node_id = m.to_node_id
                    ORDER BY m.timestamp DESC
                    LIMIT ?
                    """,
                    (limit,)
                )
                cols = [d[0] for d in cursor.description]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error fetching recent messages: {e}")
            return []


    def search_messages(self, query:str, limit:int=20) -> List[Dict[str, Any]]:
        """Search message_text for a case-insensitive substring."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.create_function("LIKECI", 2, lambda a,b: 1 if (a or "").lower().find((b or "").lower())!=-1 else 0)
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT m.timestamp, m.from_node_id, m.to_node_id, m.message_text, m.hops_away, m.snr, m.rssi,
                           fn.long_name AS from_long, fn.short_name AS from_short,
                           tn.long_name AS to_long, tn.short_name AS to_short
                    FROM messages m
                    LEFT JOIN nodes fn ON fn.node_id = m.from_node_id
                    LEFT JOIN nodes tn ON tn.node_id = m.to_node_id
                    WHERE LIKECI(m.message_text, ?) = 1
                    ORDER BY m.timestamp DESC
                    LIMIT ?
                    """,
                    (query, limit)
                )
                cols = [d[0] for d in cursor.description]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error searching messages: {e}")
            return []


    def get_node_by_id(self, node_id:str) -> Optional[Dict[str, Any]]:
        """Get a single node row by ID."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM nodes WHERE node_id = ?", (node_id,))
                row = cursor.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cursor.description]
                return dict(zip(cols, row))
        except Exception as e:
            logger.error(f"Error getting node by id: {e}")
            return None


    def update_node_last_heard(self, node_id:str, when:Optional[datetime]=None) -> None:
        """Update last_heard for a node if it exists."""
        try:
            when = when or datetime.now()
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE nodes SET last_heard = ? WHERE node_id = ?", (when.isoformat(), node_id))
                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to update last_heard for {node_id}: {e}")

    def count_messages_since(self, since_iso: str) -> int:
        """Count messages since an ISO timestamp."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM messages WHERE timestamp > ?", (since_iso,))
                return int(c.fetchone()[0] or 0)
        except Exception as e:
            logger.error(f"count_messages_since error: {e}")
            return 0


    def top_talkers_since(self, since_iso: str, limit: int = 5):
        """Return top talkers by from_node_id since timestamp, with names if available."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                c = conn.cursor()
                c.execute("""
                    SELECT m.from_node_id, COUNT(*) as cnt,
                           COALESCE(n.long_name, n.short_name, m.from_node_id) as display_name
                    FROM messages m
                    LEFT JOIN nodes n ON n.node_id = m.from_node_id
                    WHERE m.timestamp > ?
                    GROUP BY m.from_node_id
                    ORDER BY cnt DESC
                    LIMIT ?
                """, (since_iso, limit))
                rows = c.fetchall()
                return [{"node_id": r[0], "count": r[1], "name": r[2]} for r in rows]
        except Exception as e:
            logger.error(f"top_talkers_since error: {e}")
            return []


    def new_nodes_since(self, since_iso: str) -> int:
        """Count nodes first_seen since timestamp."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM nodes WHERE first_seen > ?", (since_iso,))
                return int(c.fetchone()[0] or 0)
        except Exception as e:
            logger.error(f"new_nodes_since error: {e}")
            return 0


    def avg_link_quality_since(self, since_iso: str):
        """Average SNR/RSSI from messages since timestamp."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                c = conn.cursor()
                c.execute("SELECT AVG(snr), AVG(rssi) FROM messages WHERE timestamp > ?", (since_iso,))
                row = c.fetchone()
                return {"avg_snr": row[0], "avg_rssi": row[1]}
        except Exception as e:
            logger.error(f"avg_link_quality_since error: {e}")
            return {"avg_snr": None, "avg_rssi": None}

