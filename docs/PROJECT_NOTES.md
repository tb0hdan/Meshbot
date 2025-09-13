# Meshbot project notes

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Discord bot that bridges communication between Discord and Meshtastic mesh networks. The bot provides real-time monitoring, telemetry tracking, and network analysis features.

## Development Commands

### Running the Bot
```bash
# Install dependencies
pip install -r requirements.txt

# Run the bot
python bot.py

# Run database maintenance (optional)
python maintain_db.py
```

### Environment Setup
Create a `.env` file based on `sampledotenvfile`:
- `DISCORD_TOKEN` - Discord bot token
- `DISCORD_CHANNEL_ID` - Discord channel ID for messages
- `MESHTASTIC_HOSTNAME` - IP/hostname of Meshtastic device (optional, defaults to serial)

## Architecture

### Core Components

1. **bot.py** - Main application entry point containing:
   - `DiscordBot` class - Discord client implementation with command handling
   - `MeshtasticInterface` class - Handles Meshtastic radio communication
   - `CommandHandler` class - Processes Discord commands and generates responses
   - `Config` dataclass - Configuration management

2. **database.py** - SQLite database management:
   - `MeshtasticDatabase` class - Handles all database operations
   - Tables: nodes, telemetry, positions, messages
   - Connection pooling and WAL mode for performance
   - Automatic schema management

3. **config.py** - Configuration settings:
   - Bot behavior settings (message length, timeouts, intervals)
   - Command aliases and display templates
   - Logging configuration

4. **maintain_db.py** - Database maintenance utility:
   - Cleanup old records
   - Optimize database performance
   - Backup functionality

### Key Design Patterns

- **Event-driven architecture** using pypubsub for Meshtastic packet handling
- **Async/await** for Discord operations
- **Caching decorator** (`@cache_result`) for expensive operations
- **Database connection pooling** for concurrent access
- **Command cooldowns** to prevent spam

### Discord Command Structure

Commands are prefixed with `$` and handled in the `CommandHandler` class. Each command method follows the pattern:
- Validate input
- Query database or Meshtastic interface
- Format response as Discord embed
- Return embed to Discord channel

### Meshtastic Integration

The bot subscribes to Meshtastic packet events using pypubsub:
- Text messages
- Telemetry data (battery, temperature, humidity, etc.)
- Position updates (GPS coordinates)
- Node information
- Routing/traceroute packets

### Database Schema

- **nodes**: Node ID, name, hardware model, last seen
- **telemetry**: Sensor data from mesh nodes
- **positions**: GPS coordinates and movement tracking
- **messages**: Communication history between Discord and mesh

All timestamps are stored in UTC format.

## Recent Updates (2025-09-12)

### Thread Safety Improvements
- Added async locks to cache decorator for thread-safe operation
- Implemented packet buffer lock for concurrent access protection
- Added proper queue size limits with configurable maximum (default: 1000)

### Error Handling Enhancements
- Improved message validation to reject control characters
- Added queue overflow handling with user feedback
- Enhanced database shutdown procedures with proper cleanup
- Fixed exception handling to use specific exception types

### Configuration Management
- Integrated config.py with bot initialization
- Made queue sizes configurable via BOT_CONFIG
- Fixed active_node_threshold to match documentation (60 minutes)
- Added fallback for missing config imports

### Database Improvements
- Added context manager support (__enter__/__exit__)
- Implemented proper shutdown flag for maintenance thread
- Enhanced column validation in schema migration
- Added graceful shutdown with thread cleanup

## Important Considerations

- Active node threshold is 60 minutes (see `Config` class and config.py)
- Message max length is 225 characters for mesh network
- Command cooldown is 2 seconds between commands
- Database uses WAL mode for concurrent access
- All times are handled in UTC internally
- Movement detection triggers when nodes move >100 meters
- Queue size limit is 1000 messages (configurable)
- Thread-safe operations implemented for caching and packet buffers
