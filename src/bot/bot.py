"""Main entry point for the Meshbot application."""
# Standard library imports
import logging
import os
import sqlite3
import sys

# Third party imports
from dotenv import load_dotenv

# Local imports
from src.config import BOT_CONFIG, Config
from src.database import MeshtasticDatabase
from src.transport.mesh import MeshtasticInterface
from src.transport.disco import DiscordBot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()



def main():
    """Main function to run the bot"""
    try:
        # Load configuration from environment and config.py
        try:
            channel_id_str = os.getenv("DISCORD_CHANNEL_ID", "0")
            channel_id = int(channel_id_str) if channel_id_str.isdigit() else 0
        except (ValueError, AttributeError):
            logger.error("Invalid DISCORD_CHANNEL_ID format")
            sys.exit(1)

        config = Config(
            discord_token=os.getenv("DISCORD_TOKEN"),
            channel_id=channel_id,
            meshtastic_hostname=os.getenv("MESHTASTIC_HOSTNAME"),
            message_max_length=BOT_CONFIG.get('message_max_length', 225),
            node_refresh_interval=BOT_CONFIG.get('node_refresh_interval', 60),
            active_node_threshold=BOT_CONFIG.get('active_node_threshold', 60),
            telemetry_update_interval=BOT_CONFIG.get('telemetry_update_interval', 3600),
            max_queue_size=BOT_CONFIG.get('max_queue_size', 1000)
        )

        # Validate configuration
        if not config.discord_token:
            logger.error("DISCORD_TOKEN not found in environment variables")
            sys.exit(1)

        if not config.channel_id:
            logger.error("DISCORD_CHANNEL_ID not found or invalid in environment variables")
            sys.exit(1)


        # Initialize database
        try:
            database = MeshtasticDatabase()
            logger.info("Database initialized successfully")
        except (ImportError, OSError, sqlite3.Error) as db_error:
            logger.error("Failed to initialize database: %s", db_error)
            sys.exit(1)

        # Create Meshtastic interface
        try:
            meshtastic_interface = MeshtasticInterface(config.meshtastic_hostname, database)
            logger.info("Meshtastic interface created successfully")
        except (ImportError, OSError, ConnectionError) as mesh_error:
            logger.error("Failed to create Meshtastic interface: %s", mesh_error)
            sys.exit(1)

        # Create and run bot
        try:
            bot = DiscordBot(config, meshtastic_interface, database)
            logger.info("Discord bot created successfully")
            bot.run(config.discord_token)
        except (ImportError, OSError, ConnectionError) as bot_error:
            logger.error("Failed to create or run Discord bot: %s", bot_error)
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        # Clean up database if it exists
        if 'database' in locals():
            try:
                database.close()
            except (OSError, sqlite3.Error):
                pass
    except (ImportError, OSError, RuntimeError) as e:
        logger.error("Fatal error: %s", e)
        # Clean up database if it exists
        if 'database' in locals():
            try:
                database.close()
            except (OSError, sqlite3.Error):
                pass
        sys.exit(1)
