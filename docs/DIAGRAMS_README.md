# D-LOCKSS PlantUML Diagrams

This directory contains comprehensive PlantUML diagrams documenting the D-LOCKSS system architecture, components, workflows, and data structures.

## Diagrams Overview

The diagrams are split into individual files for easier viewing and maintenance:

1. **`system_architecture.puml`** - High-level system architecture
2. **`component_diagram.puml`** - Core component relationships
3. **`data_structures.puml`** - Class diagram of data structures
4. **`file_ingestion_sequence.puml`** - File ingestion workflow
5. **`replication_check_sequence.puml`** - Replication checking process
6. **`delegation_sequence.puml`** - Custodial handoff process
7. **`shard_split_sequence.puml`** - Dynamic shard splitting
8. **`file_lifecycle_state.puml`** - File state transitions
9. **`message_flow.puml`** - Message propagation through network

Each diagram can be viewed independently. The original `diagrams.puml` file contains all diagrams in one file for batch processing.

### Individual Diagrams:

### 1. **System Architecture** (`system_architecture.puml`)
   - High-level view of the D-LOCKSS node architecture
   - Shows relationships between main components and external dependencies
   - Illustrates how components interact with libp2p stack

### 2. **Component Diagram** (`component_diagram.puml`)
   - Detailed view of core components and their interfaces
   - Shows public methods and key data structures
   - Illustrates component dependencies

### 3. **Data Structures** (`data_structures.puml`)
   - Class diagram showing all major data structures
   - Relationships between state management structures
   - Mutex-protected shared state

### 4. **File Ingestion Sequence** (`file_ingestion_sequence.puml`)
   - Step-by-step flow when a file is dropped into `./data`
   - Shows both responsible and custodial node paths
   - Illustrates delegation mechanism

### 5. **Replication Check Sequence** (`replication_check_sequence.puml`)
   - Complete flow of periodic replication checking
   - Shows backoff handling, DHT queries, and pin/unpin decisions
   - Illustrates how replication levels are maintained

### 6. **Delegation Sequence** (`delegation_sequence.puml`)
   - Detailed flow of custodial handoff process
   - Shows how non-responsible nodes delegate to responsible nodes
   - Illustrates completion of custodial handoff

### 7. **Shard Split Sequence** (`shard_split_sequence.puml`)
   - Flow when a shard becomes overloaded and splits
   - Shows how responsibility boundaries change
   - Illustrates dynamic sharding mechanism

### 8. **File Lifecycle State** (`file_lifecycle_state.puml`)
   - States a file can be in during its lifecycle
   - Transitions between states (Discovered, Pinned, Responsible, Custodial, etc.)
   - Shows conditions for state transitions

### 9. **Message Flow** (`message_flow.puml`)
   - Shows how different message types flow through GossipSub
   - Illustrates NEW, NEED, and DELEGATE message propagation
   - Shows rate limiting in action

## Viewing the Diagrams

### Option 1: View Individual Diagrams
Each diagram is now in its own file. Simply open any `.puml` file:

```bash
# View a specific diagram
plantuml -tpng docs/system_architecture.puml
plantuml -tpng docs/file_ingestion_sequence.puml
```

### Option 2: View All Diagrams at Once
```bash
# Generate all diagrams as PNG
plantuml -tpng docs/*.puml

# Generate all diagrams as SVG
plantuml -tsvg docs/*.puml
```

### Option 3: Online Viewer
1. Copy the contents of any `.puml` file
2. Paste into [PlantUML Online Server](http://www.plantuml.com/plantuml/uml/)
3. Or use [PlantText](https://www.planttext.com/)

### Option 4: VS Code Extension
1. Install the "PlantUML" extension in VS Code
2. Open any `.puml` file
3. Press `Alt+D` (or `Cmd+D` on Mac) to preview

### Option 5: Command Line
```bash
# Install PlantUML (requires Java)
# On Arch Linux:
sudo pacman -S plantuml

# Generate PNG images for all diagrams
plantuml -tpng docs/*.puml

# Generate SVG images for all diagrams
plantuml -tsvg docs/*.puml
```

### Option 6: Docker
```bash
# Generate all diagrams
docker run --rm -v $(pwd)/docs:/work plantuml/plantuml *.puml

# Generate specific diagram
docker run --rm -v $(pwd)/docs:/work plantuml/plantuml system_architecture.puml
```

## Diagram Conventions

- **Rectangles**: Components/modules
- **Arrows**: Dependencies/data flow
- **Participants**: Actors or components in sequence diagrams
- **States**: File lifecycle states
- **Notes**: Important clarifications

## Updating Diagrams

When modifying the codebase:
1. Update the relevant diagram section
2. Ensure sequence diagrams match actual code flow
3. Update component diagrams if interfaces change
4. Keep state diagrams synchronized with file lifecycle logic

## Related Documentation

- `../README.md` - Project overview and architecture
- `../scripts/CHART_GUIDE.md` - Metrics visualization guide
- Source code comments - Detailed implementation notes
