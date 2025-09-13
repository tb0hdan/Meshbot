"""Meshtastic data processing utilities."""
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class MeshtasticDataProcessor:
    """Handles processing of telemetry and position data from Meshtastic packets"""

    @staticmethod
    def extract_telemetry_data(node_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract telemetry data from node data"""
        telemetry_data = {}

        # Define telemetry fields to extract
        telemetry_fields = [
            'snr', 'rssi', 'frequency', 'latitude', 'longitude',
            'altitude', 'speed', 'heading', 'accuracy'
        ]

        for field in telemetry_fields:
            value = node_data.get(field)
            if value is not None:
                telemetry_data[field] = value

        return telemetry_data

    @staticmethod
    def extract_position_data(node_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract position data from node data"""
        # Position data requires at least latitude and longitude
        if (node_data.get('latitude') is None or
            node_data.get('longitude') is None):
            return None

        return {
            'latitude': node_data.get('latitude'),
            'longitude': node_data.get('longitude'),
            'altitude': node_data.get('altitude'),
            'speed': node_data.get('speed'),
            'heading': node_data.get('heading'),
            'accuracy': node_data.get('accuracy'),
            'source': 'meshtastic'
        }

    @staticmethod
    def extract_environmental_data(telemetry_packet: Dict[str, Any]) -> Dict[str, Any]:
        """Extract environmental sensor data from telemetry packet"""
        environmental_data = {}

        # Environmental sensor fields
        env_fields = [
            'temperature', 'humidity', 'pressure', 'gas_resistance',
            'voltage', 'current', 'battery_level'
        ]

        for field in env_fields:
            value = telemetry_packet.get(field)
            if value is not None:
                environmental_data[field] = value

        return environmental_data

    @staticmethod
    def extract_device_metrics(telemetry_packet: Dict[str, Any]) -> Dict[str, Any]:
        """Extract device metrics from telemetry packet"""
        device_metrics = {}

        # Device metric fields
        device_fields = [
            'channel_utilization', 'air_util_tx', 'uptime_seconds'
        ]

        for field in device_fields:
            value = telemetry_packet.get(field)
            if value is not None:
                device_metrics[field] = value

        return device_metrics

    @staticmethod
    def normalize_node_data(raw_node_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize and clean node data from various sources"""
        normalized = {}

        # Handle user information
        user_info = raw_node_data.get('user', {})
        normalized['long_name'] = str(user_info.get('longName', 'Unknown'))
        normalized['short_name'] = str(user_info.get('shortName', ''))

        # Handle device information
        normalized['hw_model'] = raw_node_data.get('hwModel')
        normalized['firmware_version'] = raw_node_data.get('firmwareVersion')
        normalized['macaddr'] = raw_node_data.get('macaddr')

        # Handle network information
        normalized['hops_away'] = raw_node_data.get('hopsAway', 0)
        normalized['is_router'] = raw_node_data.get('isRouter', False)
        normalized['is_client'] = raw_node_data.get('isClient', True)

        return normalized

    @staticmethod
    def validate_telemetry_data(telemetry_data: Dict[str, Any]) -> bool:
        """Validate telemetry data for reasonable values"""
        if not telemetry_data:
            return False

        # Basic validation rules
        validation_rules = {
            'snr': lambda x: -20 <= x <= 20,  # SNR typically in dB
            'rssi': lambda x: -150 <= x <= -10,  # RSSI in dBm
            'temperature': lambda x: -50 <= x <= 80,  # Celsius
            'humidity': lambda x: 0 <= x <= 100,  # Percentage
            'pressure': lambda x: 300 <= x <= 1200,  # hPa
            'battery_level': lambda x: 0 <= x <= 100,  # Percentage
            'voltage': lambda x: 0 <= x <= 30,  # Volts
            'latitude': lambda x: -90 <= x <= 90,  # Degrees
            'longitude': lambda x: -180 <= x <= 180,  # Degrees
            'altitude': lambda x: -500 <= x <= 9000,  # Meters
            'speed': lambda x: 0 <= x <= 200,  # m/s
        }

        for field, value in telemetry_data.items():
            if field in validation_rules:
                try:
                    if not validation_rules[field](value):
                        logger.warning("Invalid %s value: %s", field, value)
                        return False
                except (TypeError, ValueError):
                    logger.warning("Invalid %s type: %s", field, type(value))
                    return False

        return True

    @staticmethod
    def format_packet_for_storage(packet_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format packet data for database storage"""
        formatted = {}

        # Extract common fields
        if 'from' in packet_data:
            formatted['from_node'] = str(packet_data['from'])
        if 'to' in packet_data:
            formatted['to_node'] = str(packet_data['to'])
        if 'timestamp' in packet_data:
            formatted['timestamp'] = packet_data['timestamp']
        if 'payload' in packet_data:
            formatted['payload'] = packet_data['payload']

        # Extract decoded payload if available
        if 'decoded' in packet_data:
            decoded = packet_data['decoded']
            if 'text' in decoded:
                formatted['message_text'] = decoded['text']
            if 'telemetry' in decoded:
                formatted['telemetry'] = decoded['telemetry']
            if 'position' in decoded:
                formatted['position'] = decoded['position']

        return formatted

