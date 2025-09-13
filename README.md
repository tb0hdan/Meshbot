# 🤖 Meshtastic Discord Bridge Bot

A powerful Discord bot that bridges communication between Discord and Meshtastic mesh networks, providing real-time monitoring, telemetry tracking, and network analysis features.

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
- Meshtastic device or connection

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/n30nex/Meshbot/
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
   - Update `src/config/config.py` with your settings:
     - Discord Bot Token
     - Meshtastic connection details
     - Database configuration

5. **Run the bot**
   ```bash
   python meshbot.py
   ```

## ⚙️ Configuration

### Environment Variables
Create a `.env` file with:
```env
DISCORD_TOKEN=your_discord_bot_token
MESHTASTIC_DEVICE=/dev/ttyUSB0  # or your device path
DATABASE_PATH=meshtastic.db
```

### Database
The bot uses SQLite with automatic schema management:
- **Nodes**: Mesh network node information
- **Telemetry**: Sensor data from nodes
- **Positions**: GPS coordinates and movement tracking
- **Messages**: Communication history

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
- **src/commands/**: Modular command system
- **src/database/**: Modular database management system

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

## 📈 Performance

### Optimizations
- Database connection pooling
- Command result caching
- Batch message processing
- Memory-efficient packet buffering

### Monitoring
- Real-time performance metrics
- Database health monitoring
- Network activity tracking
- Error logging and reporting

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is open source. Please check the license file for details.

## 🙏 Acknowledgments

- Meshtastic community for the amazing mesh networking platform
- Discord.py for the excellent Discord API wrapper
- All contributors and testers

## 📞 Support

For issues and questions:
1. Check the troubleshooting section
2. Review console logs
3. Create an issue on GitHub
4. Join the Canadaverse

---

**Happy Meshing!** 🌐📡
