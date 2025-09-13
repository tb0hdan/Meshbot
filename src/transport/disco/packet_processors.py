"""Packet processing utilities for different Meshtastic packet types.

Handles processing of telemetry, position, routing, and other packet types.
"""
import logging
import math
import queue
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)


class PacketProcessor:
    """Processes different types of Meshtastic packets"""

    def __init__(self, database, mesh_to_discord_queue: queue.Queue,
                 meshtastic, command_handler=None):
        self.database = database
        self.mesh_to_discord_queue = mesh_to_discord_queue
        self.meshtastic = meshtastic
        self.command_handler = command_handler

    def process_text_packet(self, packet: Dict[str, Any]):
        """Process text message packet"""
        try:
            from_id = packet.get('fromId', 'Unknown')
            to_id = packet.get('toId', 'Primary')
            text = packet['decoded']['text']
            hops_away = packet.get('hopsAway', 0)

            from_name = self.database.get_node_display_name(from_id) if self.database else from_id
            to_name = self.database.get_node_display_name(to_id) if self.database else to_id

            # Check for ping messages from mesh
            if text.strip().lower() == "ping":
                logger.info("Ping received from mesh node %s", from_name)
                self._handle_mesh_ping(from_name)

            # Create message payload for Discord
            msg_payload = {
                'type': 'text',
                'from_id': from_id,
                'from_name': from_name,
                'to_id': to_id,
                'to_name': to_name,
                'text': text,
                'hops_away': hops_away,
                'snr': packet.get('snr'),
                'rssi': packet.get('rssi'),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
            self.mesh_to_discord_queue.put(msg_payload)
            logger.info(
                "💬 MESSAGE: Queued for Discord - '%s%s' from %s",
                text[:50], '...' if len(text) > 50 else '', from_name
            )

            # Add to live monitor buffer
            self._add_text_to_monitor(packet, from_name, text, hops_away)

            # Store in database
            self._store_text_message(packet, from_id, to_id, text)

        except Exception as e:
            logger.error("Error processing text packet: %s", e)

    def _handle_mesh_ping(self, from_name: str):
        """Handle ping message from mesh and send pong response"""
        try:
            # Access meshtastic interface through the processor
            if hasattr(self, 'meshtastic') and self.meshtastic:
                pong_message = f"Pong! - - > {from_name}"
                self.meshtastic.send_text(pong_message)
                logger.info("Pong sent to mesh network: %s", pong_message)
            else:
                logger.warning("Meshtastic interface not available for pong response")
        except Exception as pong_error:
            logger.error("Error sending pong to mesh: %s", pong_error)

    def _add_text_to_monitor(self, packet: Dict[str, Any], from_name: str,
                           text: str, hops_away: int):
        """Add text packet to live monitor buffer"""
        if self.command_handler:
            text_packet_info = {
                'type': 'text',
                'portnum': 'TEXT_MESSAGE_APP',
                'from_name': from_name,
                'from_id': packet.get('fromId', 'Unknown'),
                'to_id': packet.get('toId', 'Primary'),
                'text': text,
                'hops': hops_away,
                'snr': packet.get('snr', 'N/A'),
                'rssi': packet.get('rssi', 'N/A')
            }
            self.command_handler.add_packet_to_buffer(text_packet_info)

    def _store_text_message(self, packet: Dict[str, Any], from_id: str, to_id: str, text: str):
        """Store text message in database"""
        try:
            if self.database:
                message_data = {
                    'from_node_id': from_id,
                    'to_node_id': to_id,
                    'message_text': text,
                    'port_num': packet['decoded']['portnum'],
                    'payload': str(packet.get('payload', '')),
                    'hops_away': packet.get('hopsAway', 0),
                    'snr': packet.get('snr'),
                    'rssi': packet.get('rssi')
                }
                self.database.add_message(message_data)
        except Exception as msg_error:
            logger.error("Error storing message in database: %s", msg_error)

    def process_telemetry_packet(self, packet: Dict[str, Any]):
        """Process telemetry packet and extract sensor data"""
        try:
            from_id = packet.get('fromId', 'Unknown')

            # Skip if we don't have a valid node ID
            if not from_id or from_id == 'Unknown' or from_id is None:
                logger.warning("Skipping telemetry packet with invalid fromId: %s", from_id)
                return

            decoded = packet.get('decoded', {})
            telemetry_data = decoded.get('telemetry', {})

            if not telemetry_data:
                logger.debug("No telemetry data in packet from %s", from_id)
                return

            # Extract different types of telemetry data
            extracted_data = self._extract_telemetry_data(telemetry_data, packet)

            # Store telemetry data if we have any
            if extracted_data:
                self._store_telemetry_data(from_id, extracted_data)
                self._add_telemetry_to_monitor(from_id, extracted_data)

        except Exception as e:
            logger.error("Error processing telemetry packet: %s", e)

    def _extract_telemetry_data(self, telemetry_data: Dict[str, Any], packet: Dict[str, Any]) -> Dict[str, Any]:
        """Extract telemetry data from packet"""
        extracted_data = {}

        # Device metrics (battery, voltage, uptime, etc.)
        if 'deviceMetrics' in telemetry_data:
            device_metrics = telemetry_data['deviceMetrics']
            self._extract_device_metrics(device_metrics, extracted_data)

        # Environment metrics (temperature, humidity, pressure, etc.)
        if 'environmentMetrics' in telemetry_data:
            env_metrics = telemetry_data['environmentMetrics']
            self._extract_environment_metrics(env_metrics, extracted_data)

        # Air quality metrics
        if 'airQualityMetrics' in telemetry_data:
            air_metrics = telemetry_data['airQualityMetrics']
            self._extract_air_quality_metrics(air_metrics, extracted_data)

        # Power metrics
        if 'powerMetrics' in telemetry_data:
            power_metrics = telemetry_data['powerMetrics']
            self._extract_power_metrics(power_metrics, extracted_data)

        # Add radio metrics from packet
        self._extract_radio_metrics(packet, extracted_data)

        return extracted_data

    def _extract_device_metrics(self, device_metrics: Dict[str, Any],
                              extracted_data: Dict[str, Any]):
        """Extract device metrics from telemetry"""
        metrics_map = {
            'batteryLevel': 'battery_level',
            'voltage': 'voltage',
            'channelUtilization': 'channel_utilization',
            'airUtilTx': 'air_util_tx',
            'uptimeSeconds': 'uptime_seconds'
        }
        for key, db_key in metrics_map.items():
            if device_metrics.get(key) is not None:
                extracted_data[db_key] = device_metrics[key]

    def _extract_environment_metrics(self, env_metrics: Dict[str, Any],
                                    extracted_data: Dict[str, Any]):
        """Extract environment metrics from telemetry"""
        metrics_map = {
            'temperature': 'temperature',
            'relativeHumidity': 'humidity',
            'barometricPressure': 'pressure',
            'gasResistance': 'gas_resistance'
        }
        for key, db_key in metrics_map.items():
            if env_metrics.get(key) is not None:
                extracted_data[db_key] = env_metrics[key]

    def _extract_air_quality_metrics(self, air_metrics: Dict[str, Any],
                                    extracted_data: Dict[str, Any]):
        """Extract air quality metrics from telemetry"""
        metrics_map = {
            'pm10Environmental': 'pm10',
            'pm25Environmental': 'pm25',
            'pm100Environmental': 'pm100',
            'aqi': 'iaq'
        }
        for key, db_key in metrics_map.items():
            if air_metrics.get(key) is not None:
                extracted_data[db_key] = air_metrics[key]

    def _extract_power_metrics(self, power_metrics: Dict[str, Any],
                             extracted_data: Dict[str, Any]):
        """Extract power metrics from telemetry"""
        metrics_map = {
            'ch1Voltage': 'ch1_voltage',
            'ch2Voltage': 'ch2_voltage',
            'ch3Voltage': 'ch3_voltage'
        }
        for key, db_key in metrics_map.items():
            if power_metrics.get(key) is not None:
                extracted_data[db_key] = power_metrics[key]

    def _extract_radio_metrics(self, packet: Dict[str, Any],
                             extracted_data: Dict[str, Any]):
        """Extract radio metrics from packet"""
        radio_metrics_map = {
            'snr': 'snr',
            'rssi': 'rssi',
            'frequency': 'frequency'
        }
        for key, db_key in radio_metrics_map.items():
            if packet.get(key) is not None:
                extracted_data[db_key] = packet[key]

    def _store_telemetry_data(self, from_id: str, extracted_data: Dict[str, Any]):
        """Store telemetry data in database"""
        try:
            if self.database:
                success = self.database.add_telemetry(from_id, extracted_data)
                if success:
                    logger.info("Stored telemetry data for %s: %s", from_id, list(extracted_data.keys()))
                else:
                    logger.warning("Failed to store telemetry data for %s", from_id)
        except Exception as telemetry_error:
            logger.error("Error storing telemetry data for %s: %s", from_id, telemetry_error)

    def _add_telemetry_to_monitor(self, from_id: str, extracted_data: Dict[str, Any]):
        """Add telemetry to live monitor buffer"""
        if self.command_handler and from_id and from_id != 'Unknown':
            telemetry_packet_info = {
                'type': 'telemetry',
                'portnum': 'TELEMETRY_APP',
                'from_name': self.database.get_node_display_name(from_id) if self.database else from_id,
                'from_id': from_id,
                'sensor_data': list(extracted_data.keys()),
                'hops': 0,
                'snr': 'N/A',
                'rssi': 'N/A'
            }
            self.command_handler.add_packet_to_buffer(telemetry_packet_info)

    def process_position_packet(self, packet: Dict[str, Any]):
        """Process position packet and detect movement"""
        try:
            from_id = packet.get('fromId', 'Unknown')
            decoded = packet.get('decoded', {})
            position_data = decoded.get('position', {})

            if not position_data:
                logger.debug("No position data in packet from %s", from_id)
                return

            # Extract position coordinates
            new_lat = position_data.get('latitude_i', 0) / 1e7
            new_lon = position_data.get('longitude_i', 0) / 1e7
            new_alt = position_data.get('altitude', 0)

            # Skip if coordinates are invalid (0,0)
            if new_lat == 0 and new_lon == 0:
                logger.debug("Invalid position coordinates (0,0) from %s", from_id)
                return

            # Check for movement
            self._check_for_movement(from_id, new_lat, new_lon, new_alt)

            # Store new position
            self._store_position_data(from_id, position_data, new_lat, new_lon, new_alt)

        except Exception as e:
            logger.error("Error processing position packet: %s", e)

    def _check_for_movement(self, from_id: str, new_lat: float, new_lon: float, new_alt: float):
        """Check if node has moved significantly"""
        if not self.database:
            return

        last_position = self.database.get_last_position(from_id)
        if not last_position:
            return

        last_lat = last_position.get('latitude', 0)
        last_lon = last_position.get('longitude', 0)

        if last_lat == 0 and last_lon == 0:
            return

        # Calculate distance moved
        distance_moved = self.calculate_distance(last_lat, last_lon, new_lat, new_lon)

        # Movement threshold: 100 meters
        movement_threshold = 100.0

        if distance_moved > movement_threshold:
            self._create_movement_notification(from_id, distance_moved, last_lat, last_lon, new_lat, new_lon, new_alt)

    def _create_movement_notification(self, from_id: str, distance_moved: float,
                                    last_lat: float, last_lon: float,
                                    new_lat: float, new_lon: float, new_alt: float):
        """Create movement notification for Discord"""
        from_name = self.database.get_node_display_name(from_id) if self.database else from_id

        movement_payload = {
            'type': 'movement',
            'from_id': from_id,
            'from_name': from_name,
            'distance_moved': distance_moved,
            'old_lat': last_lat,
            'old_lon': last_lon,
            'new_lat': new_lat,
            'new_lon': new_lon,
            'new_alt': new_alt,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }

        self.mesh_to_discord_queue.put(movement_payload)
        logger.info("🚶 MOVEMENT: %s moved %.1fm from last position", from_name, distance_moved)

        # Add to live monitor buffer
        if self.command_handler:
            movement_packet_info = {
                'type': 'movement',
                'portnum': 'POSITION_APP',
                'from_name': from_name,
                'from_id': from_id,
                'distance_moved': distance_moved,
                'hops': 0,
                'snr': 'N/A',
                'rssi': 'N/A'
            }
            self.command_handler.add_packet_to_buffer(movement_packet_info)

    def _store_position_data(self, from_id: str, position_data: Dict[str, Any],
                           new_lat: float, new_lon: float, new_alt: float):
        """Store position data in database"""
        if not self.database:
            return

        try:
            position_data_to_store = {
                'latitude': new_lat,
                'longitude': new_lon,
                'altitude': new_alt,
                'speed': position_data.get('speed', 0),
                'heading': position_data.get('ground_track', 0),
                'accuracy': position_data.get('precision_bits', 0),
                'source': 'meshtastic'
            }
            self.database.add_position(from_id, position_data_to_store)
            logger.debug("Stored position for %s: %.6f, %.6f", from_id, new_lat, new_lon)
        except Exception as pos_error:
            logger.error("Error storing position for %s: %s", from_id, pos_error)

    def process_routing_packet(self, packet: Dict[str, Any]):
        """Process routing packet and display traceroute information"""
        try:
            from_id = packet.get('fromId', 'Unknown')
            to_id = packet.get('toId', 'Primary')
            decoded = packet.get('decoded', {})

            # Check if this is a RouteDiscovery packet
            if 'routing' in decoded and 'routeDiscovery' in decoded['routing']:
                route_data = decoded['routing']['routeDiscovery']
                self._process_route_discovery(from_id, to_id, route_data)
            else:
                logger.debug("Routing packet from %s does not contain RouteDiscovery data", from_id)

        except Exception as e:
            logger.error("Error processing routing packet: %s", e)

    def _process_route_discovery(self, from_id: str, to_id: str, route_data: Dict[str, Any]):
        """Process route discovery data and create traceroute display"""
        # Get node display names
        from_name = self.database.get_node_display_name(from_id) if self.database else from_id
        to_name = self.database.get_node_display_name(to_id) if self.database else to_id

        # Extract route information
        route = route_data.get('route', [])
        route_back = route_data.get('routeBack', [])
        snr_towards = route_data.get('snrTowards', [])
        snr_back = route_data.get('snrBack', [])

        # Build route string
        route_parts = self._build_route_string(from_name, to_name, route, route_back, snr_towards, snr_back)

        if route_parts:
            route_text = "\n".join(route_parts)
            hops_count = len(route) + len(route_back) if route_back else len(route)

            # Queue for Discord display
            traceroute_payload = {
                'type': 'traceroute',
                'from_id': from_id,
                'from_name': from_name,
                'to_id': to_id,
                'to_name': to_name,
                'route_text': route_text,
                'hops_count': hops_count,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
            self.mesh_to_discord_queue.put(traceroute_payload)
            logger.info("🛣️ TRACEROUTE: Queued route info - %s → %s (%s hops)", from_name, to_name, hops_count)

            # Add to live monitor buffer
            self._add_traceroute_to_monitor(from_name, from_id, to_name, to_id, hops_count)

    def _build_route_string(self, from_name: str, to_name: str, route: list,
                          route_back: list, snr_towards: list, snr_back: list) -> list:
        """Build route string for traceroute display"""
        route_parts = []

        # Route towards destination
        if route:
            route_parts.append(f"**Towards {to_name}:**")
            current_route = f"{from_name}"

            for i, node_num in enumerate(route):
                node_name = (
                    self.database.get_node_display_name(f"!{node_num:08x}")
                    if self.database else f"!{node_num:08x}"
                )
                snr = ""
                if i < len(snr_towards) and snr_towards[i] != -128:  # -128 is UNK_SNR
                    snr = f" ({snr_towards[i]/4:.1f}dB)"
                current_route += f" → {node_name}{snr}"

            # Add destination
            if snr_towards and len(snr_towards) > len(route):
                snr = f" ({snr_towards[-1]/4:.1f}dB)" if snr_towards[-1] != -128 else ""
            else:
                snr = ""
            current_route += f" → {to_name}{snr}"
            route_parts.append(current_route)

        # Route back from destination
        if route_back:
            route_parts.append(f"**Back from {to_name}:**")
            back_route = f"{to_name}"

            for i, node_num in enumerate(route_back):
                node_name = (
                    self.database.get_node_display_name(f"!{node_num:08x}")
                    if self.database else f"!{node_num:08x}"
                )
                snr = ""
                if i < len(snr_back) and snr_back[i] != -128:  # -128 is UNK_SNR
                    snr = f" ({snr_back[i]/4:.1f}dB)"
                back_route += f" → {node_name}{snr}"

            # Add origin
            if snr_back and len(snr_back) > len(route_back):
                snr = f" ({snr_back[-1]/4:.1f}dB)" if snr_back[-1] != -128 else ""
            else:
                snr = ""
            back_route += f" → {from_name}{snr}"
            route_parts.append(back_route)

        return route_parts

    def _add_traceroute_to_monitor(self, from_name: str, from_id: str,
                                 to_name: str, to_id: str, hops_count: int):
        """Add traceroute to live monitor buffer"""
        if self.command_handler:
            traceroute_packet_info = {
                'type': 'traceroute',
                'portnum': 'ROUTING_APP',
                'from_name': from_name,
                'from_id': from_id,
                'to_name': to_name,
                'to_id': to_id,
                'hops_count': hops_count,
                'hops': 0,
                'snr': 'N/A',
                'rssi': 'N/A'
            }
            self.command_handler.add_packet_to_buffer(traceroute_packet_info)

    @staticmethod
    def calculate_distance(lat1: float, lon1: float,
                         lat2: float, lon2: float) -> float:
        """Calculate distance between two coordinates in meters using Haversine formula"""
        try:
            # Convert to radians
            lat1_rad = math.radians(lat1)
            lon1_rad = math.radians(lon1)
            lat2_rad = math.radians(lat2)
            lon2_rad = math.radians(lon2)

            # Haversine formula
            dlat = lat2_rad - lat1_rad
            dlon = lon2_rad - lon1_rad
            a = (math.sin(dlat/2)**2 +
                 math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2)
            c = 2 * math.asin(math.sqrt(a))

            # Earth's radius in meters
            earth_radius = 6371000
            distance = earth_radius * c

            return distance
        except Exception as e:
            logger.error("Error calculating distance: %s", e)
            return 0.0

