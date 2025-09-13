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
python meshbot.py

# Run database maintenance (optional, now integrated)
# Database maintenance is handled automatically via the modular database system
```

### Environment Setup
Create a `.env` file based on `sampledotenvfile`:
- `DISCORD_TOKEN` - Discord bot token
- `DISCORD_CHANNEL_ID` - Discord channel ID for messages
- `MESHTASTIC_HOSTNAME` - IP/hostname of Meshtastic device (optional, defaults to serial)

## Architecture

### Core Components

1. **meshbot.py** - Main application entry point (renamed from main.py)

2. **src/bot/bot.py** - Bot orchestration:
   - Main bot initialization and coordination
   - Integration between Discord and Meshtastic components

3. **src/transport/discord/** - Discord integration:
   - `discord.py` - Discord client implementation
   - `embed_utils.py` - Discord embed formatting utilities
   - `message_handlers.py` - Message processing and handling
   - `packet_processors.py` - Meshtastic packet processing for Discord
   - `task_managers.py` - Background task management

4. **src/transport/meshtastic/meshtastic.py** - Meshtastic interface:
   - Handles Meshtastic radio communication
   - Packet subscription and event handling

5. **src/commands/** - Command system:
   - `handler.py` - Main command handler (legacy, mostly moved to specialized modules)
   - `base.py` - Base command classes and utilities
   - `basic.py` - Basic commands (help, txt, send, nodes, etc.)
   - `debug.py` - Debug and administrative commands
   - `monitoring.py` - Live monitoring and telemetry commands
   - `network.py` - Network analysis commands (topo, trace, stats)

6. **src/database/** - Modular database system:
   - `connection.py` - Database connection management
   - `manager.py` - Main database manager
   - `schema.py` - Schema definitions and migrations
   - `nodes.py` - Node-related database operations
   - `telemetry.py` - Telemetry data management
   - `positions.py` - Position tracking
   - `messages.py` - Message history
   - `maintenance.py` - Database maintenance utilities

7. **src/config/config.py** - Configuration settings:
   - Bot behavior settings (message length, timeouts, intervals)
   - Command aliases and display templates
   - Logging configuration

### Testing Structure

Tests are co-located with the actual code modules rather than in a separate `tests/` directory:

- **src/commands/** - Command tests:
  - `test_base.py` - Base command functionality tests
  - `test_basic.py` - Basic command tests
  - `test_debug.py` - Debug command tests
  - `test_handler.py` - Command handler tests
  - `test_monitoring.py` - Monitoring command tests
  - `test_network.py` - Network analysis command tests
  - `conftest.py` - Shared test fixtures for commands

- **src/database/** - Database tests:
  - `test_connection.py` - Database connection tests
  - `test_manager.py` - Database manager tests
  - `test_messages.py` - Message operations tests
  - `test_nodes.py` - Node operations tests
  - `test_positions.py` - Position operations tests
  - `test_schema.py` - Schema and migration tests
  - `conftest.py` - Shared test fixtures for database operations

The test suite uses pytest with coverage reporting and provides comprehensive testing of all database operations, command functionality, and integration points.

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

### Major Code Restructure
- **Project restructure**: Moved from monolithic architecture to modular src/ structure
- **Database restructure**: Split monolithic database.py into specialized modules:
  - `src/database/connection.py` - Connection management
  - `src/database/manager.py` - Main database coordinator
  - `src/database/nodes.py` - Node operations
  - `src/database/telemetry.py` - Telemetry data handling
  - `src/database/positions.py` - Position tracking
  - `src/database/messages.py` - Message history
  - `src/database/schema.py` - Schema definitions
  - `src/database/maintenance.py` - Maintenance utilities
- **Command system restructure**: Split command handler into specialized modules:
  - `src/commands/basic.py` - Basic commands (help, txt, send, nodes)
  - `src/commands/debug.py` - Debug and admin commands
  - `src/commands/monitoring.py` - Live monitoring and telemetry
  - `src/commands/network.py` - Network analysis (topo, trace, stats)
- **Discord transport restructure**: Modularized Discord integration:
  - `src/transport/discord/embed_utils.py` - Embed formatting
  - `src/transport/discord/message_handlers.py` - Message processing
  - `src/transport/discord/packet_processors.py` - Packet processing
  - `src/transport/discord/task_managers.py` - Background tasks
- **Entry point**: Renamed main.py to meshbot.py for clarity

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
- Moved config.py to src/config/config.py
- Integrated config.py with bot initialization
- Made queue sizes configurable via BOT_CONFIG
- Fixed active_node_threshold to match documentation (60 minutes)
- Added fallback for missing config imports

### Database Improvements
- Added context manager support (__enter__/__exit__)
- Implemented proper shutdown flag for maintenance thread
- Enhanced column validation in schema migration
- Added graceful shutdown with thread cleanup
- Modularized database operations for better maintainability

### Test Suite Enhancements (2025-09-13)
- Fixed all failing tests related to database operations and timezone handling
- Improved timezone handling in node operations using UTC timestamps
- Fixed SQLite boolean comparison issues (using 0/1 instead of True/False)
- Enhanced NULL value handling with proper default values
- Fixed position retrieval ordering for deterministic results
- Corrected foreign key constraint validation logic
- All 159 tests now pass with 70% code coverage

## Important Considerations

- Active node threshold is 60 minutes (see `Config` class and config.py)
- Message max length is 225 characters for mesh network
- Command cooldown is 2 seconds between commands
- Database uses WAL mode for concurrent access
- All times are handled in UTC internally
- Movement detection triggers when nodes move >100 meters
- Queue size limit is 1000 messages (configurable)
- Thread-safe operations implemented for caching and packet buffers
