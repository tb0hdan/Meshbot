# 🤖 Meshtastic Discord Bridge Bot

A production-ready Discord bot that bridges communication between Discord and Meshtastic mesh networks, providing comprehensive real-time monitoring, telemetry tracking, and network analysis features. Built with a modular architecture, extensive test coverage, and enterprise-grade reliability.

## ✨ Features

### 🔗 **Core Functionality**
- **Bidirectional Communication**: Send messages from Discord to mesh network and vice versa
- **Real-time Monitoring**: Live packet monitoring with `$live` command
- **Node Management**: Track and display all mesh network nodes
- **Telemetry Tracking**: Monitor sensor data from mesh nodes
- **Movement Detection**: Alert when nodes move significant distances

### 📊 **Advanced Analytics**
- **Network Topology**: Visual network maps and connection analysis
- **Route Tracing**: Hop-by-hop path analysis with signal quality
- **Message Statistics**: Comprehensive network activity metrics
- **Performance Leaderboards**: Node performance rankings
- **Live Telemetry**: Real-time sensor data monitoring

### 🎯 **Discord Commands**

#### **Basic Commands**
- `$help` - Show all available commands
- `$txt <message>` - Send message to primary mesh channel
- `$send <node_name> <message>` - Send message to specific node
- `$nodes` - List all known mesh nodes
- `$activenodes` - Show nodes active in last 60 minutes
- `$telem` - Display telemetry information
- `$status` - Show bot and network status

#### **Advanced Commands**
- `$topo` - Visual network topology tree
- `$topology` - Detailed network connections analysis
- `$trace <node_name>` - Trace route to specific node
- `$stats` - Network message statistics
- `$live` - Real-time packet monitoring (1 minute)
- `$art` - ASCII network art visualization
- `$leaderboard` - Network performance rankings

#### **Admin Commands**
- `$debug` - Show debug information
- `$clear` - Clear database (admin only)

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Discord Bot Token
- Meshtastic device (USB, TCP/IP, or Bluetooth connection)

### Production Features
- **Zero-configuration startup** with automatic database setup
- **Comprehensive test suite** - 159 tests with 70% code coverage
- **Enterprise reliability** with connection pooling and error recovery
- **Thread-safe operations** for concurrent access
- **Modular architecture** for easy maintenance and extension

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/Meshbot.git
   cd Meshbot
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   # or
   source venv/bin/activate  # Linux/Mac
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure the bot**
   - Copy the sample environment file:
     ```bash
     cp sampledotenvfile .env
     ```
   - Edit `.env` with your settings:
     - `DISCORD_TOKEN` - Your Discord bot token
     - `DISCORD_CHANNEL_ID` - Discord channel ID for messages
     - `MESHTASTIC_HOSTNAME` - IP/hostname (optional, defaults to serial)

5. **Run the bot**
   ```bash
   python meshbot.py
   ```

## ⚙️ Configuration

### Environment Variables
Create a `.env` file based on `sampledotenvfile`:
```env
DISCORD_TOKEN=your_discord_bot_token
DISCORD_CHANNEL_ID=your_channel_id
MESHTASTIC_HOSTNAME=192.168.1.100  # Optional - IP/hostname for TCP connection
# Leave MESHTASTIC_HOSTNAME empty to use serial/USB connection
```

### Database
The bot uses SQLite with automatic schema management and migration:
- **Nodes**: Mesh network node information and status
- **Telemetry**: Sensor data (battery, temperature, humidity, pressure, air quality)
- **Positions**: GPS coordinates and movement tracking
- **Messages**: Bidirectional communication history
- **Automatic Maintenance**: Database cleanup runs periodically
- **WAL Mode**: Enabled for concurrent access and better performance

## 📡 Meshtastic Integration

### Supported Packet Types
- **Text Messages**: Bidirectional text communication
- **Telemetry**: Battery, temperature, humidity, pressure, air quality
- **Position**: GPS coordinates and movement tracking
- **Node Info**: Node identification and status
- **Routing**: Traceroute and path analysis
- **Admin**: Administrative commands

### Movement Detection
- Automatically detects when nodes move >100 meters
- Sends Discord notifications with movement details
- Tracks route quality and signal strength

## 🎮 Usage Examples

### Basic Communication
```
# Send message to mesh network
$txt Hello mesh network!

# Send message to specific node
$send WeatherStation Temperature check please

# Ping test
ping
```

### Network Analysis
```
# View network topology
$topo

# Trace route to a node
$trace WeatherStation

# Monitor live activity
$live

# Check network statistics
$stats
```

### Telemetry Monitoring
```
# View current telemetry
$telem

# Check specific node telemetry
$telem WeatherStation
```

## 🔧 Advanced Features

### Live Monitoring
- Real-time packet monitoring with `$live`
- Shows packet types, sources, and signal quality
- 1-minute monitoring sessions with manual stop
- Cooldown protection to prevent abuse

### Route Tracing
- Visual hop-by-hop path analysis
- Signal quality indicators for each hop
- Route quality assessment
- Connection statistics

### Movement Detection
- Automatic detection of node movement
- Rich Discord notifications with coordinates
- Distance and speed indicators
- Historical position tracking

## 📊 Database Schema

### Tables
- **nodes**: Node information and status
- **telemetry**: Sensor data and metrics
- **positions**: GPS coordinates and movement
- **messages**: Communication history

### Features
- Connection pooling for performance
- WAL mode for concurrency
- Automatic maintenance and cleanup
- Indexed queries for speed

## 🧪 Testing

### Running Tests
```bash
# Run all tests
make test

# Run with coverage
pytest --cov=src --cov-report=term-missing

# Run specific test module
pytest src/commands/test_basic.py
```

### Linting
```bash
# Run linter
make lint
```

## 🛠️ Development

### Project Structure
```
Meshbot/
├── meshbot.py                           # Main application entry point
├── src/
│   ├── bot/
│   │   └── bot.py                       # Bot orchestration
│   ├── commands/
│   │   ├── base.py                      # Base command classes
│   │   ├── basic.py                     # Basic commands (help, txt, send)
│   │   ├── debug.py                     # Debug and admin commands
│   │   ├── monitoring.py                # Live monitoring and telemetry
│   │   ├── network.py                   # Network analysis commands
│   │   └── handler.py                   # Legacy command handler
│   ├── config/
│   │   └── config.py                    # Configuration settings
│   ├── database/
│   │   ├── connection.py                # Database connections
│   │   ├── manager.py                   # Database coordinator
│   │   ├── schema.py                    # Schema definitions
│   │   ├── nodes.py                     # Node operations
│   │   ├── telemetry.py                 # Telemetry data
│   │   ├── positions.py                 # Position tracking
│   │   ├── messages.py                  # Message history
│   │   └── maintenance.py               # Database maintenance
│   └── transport/
│       ├── discord/
│       │   ├── discord.py               # Discord client
│       │   ├── embed_utils.py           # Embed formatting
│       │   ├── message_handlers.py      # Message processing
│       │   ├── packet_processors.py     # Packet processing
│       │   └── task_managers.py         # Background tasks
│       └── meshtastic/
│           └── meshtastic.py            # Meshtastic interface
├── requirements.txt                     # Python dependencies
└── venv/                               # Virtual environment
```

### Key Components
- **meshbot.py**: Main application entry point
- **src/bot/bot.py**: Bot orchestration and coordination
- **src/transport/discord/**: Discord integration modules
- **src/transport/meshtastic/**: Meshtastic communication
- **src/commands/**: Modular command system with specialized handlers
- **src/database/**: Modular database management with connection pooling
- **src/config/config.py**: Centralized configuration management

### Quality Assurance
- **159 tests** covering all major functionality with **70% code coverage**
- **Complete linting** with mypy type checking for code quality
- **Thread-safe operations** throughout the entire codebase
- **Error handling and recovery** mechanisms at all levels
- Tests co-located with modules for better maintainability
- Comprehensive fixtures for database and command testing
- **Production-ready** with professional logging and monitoring

## 🐛 Troubleshooting

### Common Issues
1. **Bot not responding**: Check Discord token and permissions
2. **No mesh data**: Verify Meshtastic device connection
3. **Database errors**: Check file permissions and disk space
4. **Command cooldowns**: Wait 2 seconds between commands

### Debug Commands
- `$debug` - Show system information
- `$status` - Check bot and network status
- Check console logs for detailed error information

## 📈 Performance & Reliability

### Production Optimizations
- **Database connection pooling** for concurrent access
- **Thread-safe caching** with efficient result storage
- **Batch message processing** for high-throughput scenarios
- **Memory-efficient packet buffering** with configurable limits
- **Automatic database maintenance** with scheduled cleanup
- **WAL mode SQLite** for enhanced concurrency

### Enterprise Monitoring
- **Real-time performance metrics** and health checks
- **Comprehensive error logging** with detailed diagnostics
- **Network activity tracking** with statistical analysis
- **Automatic recovery mechanisms** for transient failures
- **Resource usage monitoring** and optimization alerts

## 🤝 Contributing

We welcome contributions! This project maintains high quality standards with comprehensive testing and linting.

### Development Workflow
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes following the existing code style
4. **Run the full test suite** (`make test`) - all 159 tests must pass
5. **Run linting and type checking** (`make lint` and `make mypy`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Submit a pull request

### Quality Requirements
- **All tests must pass** (currently 159 tests with 70% coverage)
- **Code must pass linting** and mypy type checking
- **Follow existing patterns** for thread safety and error handling
- **Include tests** for new functionality
- **Update documentation** as needed

## 📄 License

This project is open source. Please check the license file for details.

## 🙏 Acknowledgments

- Meshtastic community for the amazing mesh networking platform
- Discord.py for the excellent Discord API wrapper
- All contributors and testers

## 📞 Support

For issues and questions:
1. Check the troubleshooting section above
2. Review console logs for detailed error information
3. Check `docs/PROJECT_NOTES.md` for technical details
4. Create an issue on GitHub with:
   - Error messages and logs
   - Steps to reproduce
   - System information
5. Join the community discussion

---

**Happy Meshing!** 🌐📡
