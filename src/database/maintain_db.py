#!/usr/bin/env python3
"""
Database maintenance script for Meshtastic Discord Bridge Bot
Use this script to clean up old data and view database statistics
"""

import sqlite3
import argparse
from datetime import datetime, timedelta
import sys

def connect_db(db_path="meshtastic.db"):
    """Connect to the database"""
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except (sqlite3.Error, OSError) as e:
        print(f"Error connecting to database: {e}")
        return None

def show_stats(conn):
    """Show database statistics"""
    try:
        cursor = conn.cursor()

        print("📊 Database Statistics")
        print("=" * 50)

        # Node statistics
        cursor.execute("SELECT COUNT(*) FROM nodes")
        total_nodes = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM nodes WHERE last_heard > datetime('now', '-1 hour')")
        active_nodes = cursor.fetchone()[0]

        print(f"Total nodes: {total_nodes}")
        print(f"Active nodes (last hour): {active_nodes}")

        # Telemetry statistics
        cursor.execute("SELECT COUNT(*) FROM telemetry")
        total_telemetry = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM telemetry WHERE timestamp > datetime('now', '-1 day')")
        recent_telemetry = cursor.fetchone()[0]

        print(f"Total telemetry records: {total_telemetry}")
        print(f"Telemetry records (last 24h): {recent_telemetry}")

        # Position statistics
        cursor.execute("SELECT COUNT(*) FROM positions")
        total_positions = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM positions WHERE timestamp > datetime('now', '-1 day')")
        recent_positions = cursor.fetchone()[0]

        print(f"Total position records: {total_positions}")
        print(f"Position records (last 24h): {recent_positions}")

        # Message statistics
        cursor.execute("SELECT COUNT(*) FROM messages")
        total_messages = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM messages WHERE timestamp > datetime('now', '-1 day')")
        recent_messages = cursor.fetchone()[0]

        print(f"Total messages: {total_messages}")
        print(f"Messages (last 24h): {recent_messages}")

        # Database size
        cursor.execute(
            "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()"
        )
        db_size = cursor.fetchone()[0]
        print(f"Database size: {db_size / (1024*1024):.2f} MB")

    except (sqlite3.Error, ValueError) as e:
        print(f"Error getting statistics: {e}")

def show_recent_nodes(conn, limit=10):
    """Show recent nodes"""
    try:
        cursor = conn.cursor()

        print(f"\n📡 Recent Nodes (Last {limit})")
        print("=" * 50)

        cursor.execute("""
            SELECT long_name, node_id, last_heard, hops_away
            FROM nodes
            ORDER BY last_heard DESC
            LIMIT ?
        """, (limit,))

        nodes = cursor.fetchall()

        if not nodes:
            print("No nodes found")
            return

        for node in nodes:
            long_name, node_id, last_heard, hops_away = node
            print(f"{long_name:20} | ID: {node_id:10} | Last: {last_heard:19} | Hops: {hops_away}")

    except (sqlite3.Error, ValueError) as e:
        print(f"Error getting recent nodes: {e}")

def cleanup_old_data(conn, days=30):
    """Clean up old data"""
    try:
        cursor = conn.cursor()

        print(f"\n🧹 Cleaning up data older than {days} days...")

        cutoff_date = datetime.now() - timedelta(days=days)
        cutoff_str = cutoff_date.isoformat()

        # Clean up old telemetry
        cursor.execute("DELETE FROM telemetry WHERE timestamp < ?", (cutoff_str,))
        telemetry_deleted = cursor.rowcount

        # Clean up old positions
        cursor.execute("DELETE FROM positions WHERE timestamp < ?", (cutoff_str,))
        positions_deleted = cursor.rowcount

        # Clean up old messages
        cursor.execute("DELETE FROM messages WHERE timestamp < ?", (cutoff_str,))
        messages_deleted = cursor.rowcount

        conn.commit()

        print("Cleaned up:")
        print(f"  - {telemetry_deleted} telemetry records")
        print(f"  - {positions_deleted} position records")
        print(f"  - {messages_deleted} message records")

        # Vacuum database to reclaim space
        cursor.execute("VACUUM")
        print("Database vacuumed to reclaim space")

    except (sqlite3.Error, ValueError) as e:
        print(f"Error cleaning up data: {e}")

def show_node_details(conn, node_name):
    """Show detailed information about a specific node"""
    try:
        cursor = conn.cursor()

        print(f"\n🔍 Node Details: {node_name}")
        print("=" * 50)

        # Find node by name
        cursor.execute("""
            SELECT * FROM nodes
            WHERE long_name LIKE ? OR short_name LIKE ?
            ORDER BY last_heard DESC
            LIMIT 1
        """, (f"%{node_name}%", f"%{node_name}%"))

        node = cursor.fetchone()
        if not node:
            print(f"No node found with name containing '{node_name}'")
            return

        # Get column names
        cursor.execute("PRAGMA table_info(nodes)")
        columns = [col[1] for col in cursor.fetchall()]

        # Display node info
        node_data = dict(zip(columns, node))
        for key, value in node_data.items():
            if value is not None:
                print(f"{key:20}: {value}")

        # Get latest telemetry
        cursor.execute("""
            SELECT * FROM telemetry
            WHERE node_id = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """, (node_data['node_id'],))

        telemetry = cursor.fetchone()
        if telemetry:
            print("\n📊 Latest Telemetry:")
            cursor.execute("PRAGMA table_info(telemetry)")
            telemetry_columns = [col[1] for col in cursor.fetchall()]
            telemetry_data = dict(zip(telemetry_columns, telemetry))

            for key, value in telemetry_data.items():
                if value is not None and key not in ['id', 'node_id', 'timestamp']:
                    print(f"  {key:20}: {value}")

        # Get latest position
        cursor.execute("""
            SELECT * FROM positions
            WHERE node_id = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """, (node_data['node_id'],))

        position = cursor.fetchone()
        if position:
            print("\n📍 Latest Position:")
            cursor.execute("PRAGMA table_info(positions)")
            position_columns = [col[1] for col in cursor.fetchall()]
            position_data = dict(zip(position_columns, position))

            for key, value in position_data.items():
                if value is not None and key not in ['id', 'node_id', 'timestamp']:
                    print(f"  {key:20}: {value}")

    except (sqlite3.Error, ValueError) as e:
        print(f"Error getting node details: {e}")

def main():
    """Main function for database maintenance script"""
    parser = argparse.ArgumentParser(
        description="Database maintenance for Meshtastic Discord Bridge Bot"
    )
    parser.add_argument("--db", default="meshtastic.db", help="Database file path")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--nodes", type=int, metavar="N", help="Show N most recent nodes")
    parser.add_argument(
        "--cleanup", type=int, metavar="DAYS", help="Clean up data older than N days"
    )
    parser.add_argument(
        "--node-info", metavar="NAME", help="Show detailed info for node with given name"
    )

    args = parser.parse_args()

    if not any([args.stats, args.nodes, args.cleanup, args.node_info]):
        parser.print_help()
        return

    conn = connect_db(args.db)
    if not conn:
        sys.exit(1)

    try:
        if args.stats:
            show_stats(conn)

        if args.nodes:
            show_recent_nodes(conn, args.nodes)

        if args.cleanup:
            cleanup_old_data(conn, args.cleanup)

        if args.node_info:
            show_node_details(conn, args.node_info)

    finally:
        conn.close()
