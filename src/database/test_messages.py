"""Tests for message database operations."""
from datetime import datetime, timezone, timedelta

import pytest

from src.database.messages import MessageOperations


class TestMessageOperations:
    """Test cases for MessageOperations class."""

    def test_add_message_basic(self, db_connection, sample_message_data):
        """Test adding basic message data."""
        message_ops = MessageOperations(db_connection)

        success = message_ops.add_message(sample_message_data)
        assert success is True

        # Verify data was added
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM messages WHERE from_node_id = ?",
                          (sample_message_data['from_node_id'],))
            result = cursor.fetchone()

            assert result is not None
            assert result['from_node_id'] == sample_message_data['from_node_id']
            assert result['to_node_id'] == sample_message_data['to_node_id']
            assert result['message_text'] == sample_message_data['message_text']
            assert result['port_num'] == sample_message_data['port_num']

    def test_add_message_all_fields(self, db_connection, sample_message_data):
        """Test adding message with all fields populated."""
        message_ops = MessageOperations(db_connection)

        success = message_ops.add_message(sample_message_data)
        assert success is True

        # Verify all fields were stored
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM messages WHERE from_node_id = ?",
                          (sample_message_data['from_node_id'],))
            result = cursor.fetchone()

            assert result['from_node_id'] == sample_message_data['from_node_id']
            assert result['to_node_id'] == sample_message_data['to_node_id']
            assert result['message_text'] == sample_message_data['message_text']
            assert result['port_num'] == sample_message_data['port_num']
            assert result['payload'] == sample_message_data['payload']
            assert result['hops_away'] == sample_message_data['hops_away']
            assert result['snr'] == sample_message_data['snr']
            assert result['rssi'] == sample_message_data['rssi']

    def test_add_message_minimal_data(self, db_connection):
        """Test adding message with minimal data."""
        message_ops = MessageOperations(db_connection)

        minimal_data = {
            'from_node_id': '!sender1',
            'to_node_id': '!receiver1',
            'message_text': 'Test message'
        }

        success = message_ops.add_message(minimal_data)
        assert success is True

        # Verify message was stored
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM messages WHERE message_text = ?", ('Test message',))
            result = cursor.fetchone()

            assert result['from_node_id'] == '!sender1'
            assert result['to_node_id'] == '!receiver1'
            assert result['message_text'] == 'Test message'
            # Optional fields should be None
            assert result['payload'] is None
            assert result['hops_away'] is None

    def test_add_multiple_messages(self, db_connection, sample_message_data):
        """Test adding multiple messages."""
        message_ops = MessageOperations(db_connection)

        # Add first message
        success1 = message_ops.add_message(sample_message_data)
        assert success1 is True

        # Add second message
        second_data = sample_message_data.copy()
        second_data['message_text'] = 'Second test message'
        second_data['snr'] = 12.0

        success2 = message_ops.add_message(second_data)
        assert success2 is True

        # Verify both messages exist
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM messages WHERE from_node_id = ?",
                          (sample_message_data['from_node_id'],))
            count = cursor.fetchone()[0]
            assert count == 2

    def test_get_network_topology_basic(self, db_connection):
        """Test getting basic network topology."""
        message_ops = MessageOperations(db_connection)

        # Add some nodes first
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO nodes (node_id, long_name, last_heard, is_router)
                VALUES (?, ?, ?, ?)
            """, ('!node1', 'Node 1', datetime.now().isoformat(), False))
            cursor.execute("""
                INSERT INTO nodes (node_id, long_name, last_heard, is_router)
                VALUES (?, ?, ?, ?)
            """, ('!node2', 'Node 2', datetime.now().isoformat(), True))

        # Add some messages
        messages = [
            {'from_node_id': '!node1', 'to_node_id': '!node2', 'message_text': 'Hello', 'hops_away': 1, 'snr': 10.0},
            {'from_node_id': '!node2', 'to_node_id': '!node1', 'message_text': 'Hi back', 'hops_away': 1, 'snr': 11.0}
        ]

        for msg in messages:
            message_ops.add_message(msg)

        topology = message_ops.get_network_topology()

        assert 'connections' in topology
        assert 'total_nodes' in topology
        assert 'active_nodes' in topology
        assert 'router_nodes' in topology
        assert topology['total_nodes'] == 2
        assert topology['router_nodes'] == 1
        assert len(topology['connections']) >= 0

    def test_get_network_topology_with_connections(self, db_connection):
        """Test network topology with message connections."""
        message_ops = MessageOperations(db_connection)

        # Add messages to create connections
        messages = [
            {'from_node_id': '!A', 'to_node_id': '!B', 'hops_away': 1, 'snr': 10.0},
            {'from_node_id': '!A', 'to_node_id': '!B', 'hops_away': 1, 'snr': 12.0},
            {'from_node_id': '!B', 'to_node_id': '!C', 'hops_away': 2, 'snr': 8.0}
        ]

        for msg in messages:
            message_ops.add_message(msg)

        topology = message_ops.get_network_topology()

        assert len(topology['connections']) >= 2

        # Find the A->B connection
        ab_connection = next((c for c in topology['connections']
                             if c['from_node'] == '!A' and c['to_node'] == '!B'), None)
        assert ab_connection is not None
        assert ab_connection['message_count'] == 2
        assert ab_connection['avg_hops'] == 1
        assert 10.0 <= ab_connection['avg_snr'] <= 12.0

    def test_get_message_statistics_basic(self, db_connection, sample_message_data):
        """Test getting basic message statistics."""
        message_ops = MessageOperations(db_connection)

        # Add some messages
        for i in range(3):
            msg_data = sample_message_data.copy()
            msg_data['message_text'] = f'Message {i}'
            msg_data['snr'] = 10.0 + i
            msg_data['rssi'] = -80 + i*2
            message_ops.add_message(msg_data)

        stats = message_ops.get_message_statistics(hours=24)

        assert 'total_messages' in stats
        assert 'unique_senders' in stats
        assert 'unique_recipients' in stats
        assert 'avg_hops' in stats
        assert 'avg_snr' in stats
        assert 'avg_rssi' in stats
        assert 'hourly_distribution' in stats

        assert stats['total_messages'] == 3
        assert stats['unique_senders'] == 1
        assert stats['unique_recipients'] == 1

    def test_get_message_statistics_multiple_senders(self, db_connection):
        """Test message statistics with multiple senders."""
        message_ops = MessageOperations(db_connection)

        # Add messages from different senders
        messages = [
            {'from_node_id': '!sender1', 'to_node_id': '!receiver1', 'hops_away': 1, 'snr': 10.0, 'rssi': -75},
            {'from_node_id': '!sender2', 'to_node_id': '!receiver1', 'hops_away': 2, 'snr': 8.0, 'rssi': -80},
            {'from_node_id': '!sender1', 'to_node_id': '!receiver2', 'hops_away': 1, 'snr': 12.0, 'rssi': -70}
        ]

        for msg in messages:
            message_ops.add_message(msg)

        stats = message_ops.get_message_statistics(hours=24)

        assert stats['total_messages'] == 3
        assert stats['unique_senders'] == 2
        assert stats['unique_recipients'] == 2
        assert 1.0 <= stats['avg_hops'] <= 2.0
        assert 8.0 <= stats['avg_snr'] <= 12.0
        assert -80 <= stats['avg_rssi'] <= -70

    def test_get_message_statistics_time_filter(self, db_connection, sample_message_data):
        """Test message statistics time filtering."""
        message_ops = MessageOperations(db_connection)

        # Add a message
        message_ops.add_message(sample_message_data)

        # Get stats for a time window that includes recent messages
        stats_recent = message_ops.get_message_statistics(hours=24)
        assert stats_recent['total_messages'] == 1

        # Test that hours=0 means no time filter (all messages)
        stats_all = message_ops.get_message_statistics(hours=0)
        assert stats_all['total_messages'] == 1

    def test_get_message_statistics_hourly_distribution(self, db_connection):
        """Test hourly distribution in message statistics."""
        message_ops = MessageOperations(db_connection)

        # Add messages
        for i in range(5):
            msg_data = {
                'from_node_id': f'!sender{i}',
                'to_node_id': '!receiver',
                'message_text': f'Message {i}'
            }
            message_ops.add_message(msg_data)

        stats = message_ops.get_message_statistics(hours=24)

        # Should have hourly distribution data
        assert isinstance(stats['hourly_distribution'], dict)
        # Should have at least one hour with messages
        total_in_distribution = sum(stats['hourly_distribution'].values())
        assert total_in_distribution > 0

    def test_message_error_handling(self, db_connection):
        """Test error handling in message operations."""
        message_ops = MessageOperations(db_connection)

        # Test that statistics don't crash on empty database
        empty_stats = message_ops.get_message_statistics()
        assert isinstance(empty_stats, dict)
        assert empty_stats['total_messages'] == 0

        # Test with invalid/empty data
        success = message_ops.add_message({})
        assert success is True  # Should succeed with all None values

        # After adding a message, statistics should show 1 message
        stats_after = message_ops.get_message_statistics()
        assert stats_after['total_messages'] == 1

        empty_topology = message_ops.get_network_topology()
        assert isinstance(empty_topology, dict)
        assert empty_topology['total_nodes'] == 0

    def test_message_timestamp_handling(self, db_connection, sample_message_data):
        """Test that message timestamps are handled correctly."""
        message_ops = MessageOperations(db_connection)

        success = message_ops.add_message(sample_message_data)
        assert success is True

        # Verify timestamp was set
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT timestamp FROM messages WHERE from_node_id = ?",
                          (sample_message_data['from_node_id'],))
            result = cursor.fetchone()

            assert result['timestamp'] is not None
            # Should be a reasonable timestamp string
            assert len(result['timestamp']) > 10

    def test_message_port_types(self, db_connection):
        """Test different message port types."""
        message_ops = MessageOperations(db_connection)

        port_types = ['TEXT_MESSAGE_APP', 'POSITION_APP', 'NODEINFO_APP', 'ROUTING_APP']

        for i, port in enumerate(port_types):
            msg_data = {
                'from_node_id': f'!sender{i}',
                'to_node_id': '!receiver',
                'message_text': f'Message for {port}',
                'port_num': port
            }
            success = message_ops.add_message(msg_data)
            assert success is True

        # Verify all port types were stored
        with db_connection.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT port_num FROM messages WHERE port_num IS NOT NULL")
            stored_ports = {row[0] for row in cursor.fetchall()}

            for port in port_types:
                assert port in stored_ports
