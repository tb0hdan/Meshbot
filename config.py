"""
Configuration file for Meshtastic Discord Bridge Bot
Modify these settings to customize bot behavior
"""

# Bot Configuration
BOT_CONFIG = {
    # Message settings
    'message_max_length': 225,
    'command_prefix': '$',

    # Node management
    'node_refresh_interval': 60,  # seconds
    'active_node_threshold': 60,  # minutes - matches documentation

    # Discord settings
    'embed_color': 0x00ff00,  # Green color for embeds
    'message_timeout': 30,  # seconds for message deletion

    # Meshtastic settings
    'connection_timeout': 10,  # seconds
    'retry_attempts': 3,
    'retry_delay': 5,  # seconds

    # Queue management
    'max_queue_size': 1000,  # Maximum messages in queue
    'telemetry_update_interval': 3600,  # 1 hour in seconds
}

# Logging Configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': 'bot.log',
    'max_size': 10 * 1024 * 1024,  # 10MB
    'backup_count': 5,
}

# Command Aliases (for user convenience)
COMMAND_ALIASES = {
    '$telem': '$telemetry',
    '$nodes': '$activenodes',
    '$list': '$activenodes',
    '$info': '$status',
}

# Node Display Settings
NODE_DISPLAY = {
    'show_unknown_fields': False,
    'time_format': '%H:%M:%S',
    'date_format': '%Y-%m-%d',
    'max_nodes_per_message': 20,
}

# Message Templates
MESSAGE_TEMPLATES = {
    'mesh_message': "📡 **Mesh Message:** {message}",
    'message_sent': "📤 Message sent successfully",
    'error_generic': "❌ An error occurred: {error}",
    'no_nodes': "📡 No nodes available",
    'connection_status': (
        "🔧 **Connection Status:**\nDiscord: {discord_status}\n"
        "Meshtastic: {mesh_status}"
    ),
}
