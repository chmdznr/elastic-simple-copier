# Elasticsearch Index Copier

A simple Python tool for copying Elasticsearch indices between clusters. This tool copies index settings, mappings, and documents from a source cluster to a destination cluster.

## Features

- Copy index settings and mappings
- Copy documents using scroll API and bulk operations
- Support for different target index names
- Configurable batch size for document copying
- Basic logging and error reporting
- Environment-based configuration

## Prerequisites

- Python 3.7+
- Access to source and destination Elasticsearch clusters
- Required Python packages (see requirements.txt)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/chmdznr/elastic-simple-copier.git
cd elastic-simple-copier
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the project root with your Elasticsearch credentials:
```env
# Required settings
SOURCE_HOST=http://localhost:9200
DEST_HOST=http://destination-host:9200
SOURCE_USERNAME=elastic
SOURCE_PASSWORD=your_source_password
DEST_USERNAME=elastic
DEST_PASSWORD=your_dest_password

# Optional settings
INDICES=source_index1:target_index1,source_index2
BATCH_SIZE=1000
TOTAL_FIELDS_LIMIT=2000  # Set to -1 to use source index setting
```

## Usage

### Basic Usage

Copy indices using settings from `.env` file:
```bash
python elastic_copier.py
```

### Command Line Options

```bash
python elastic_copier.py --help
```

Available options:
- `--indices`: List of indices to copy (format: source1:target1,source2:target2)
- `--batch-size`: Number of documents to process in each batch
- `--total-fields-limit`: Maximum number of fields in mappings (-1 to use source setting)
- `-v, --verbose`: Increase verbosity (use -vv for debug level)

Examples:
```bash
# Copy with different target names
python elastic_copier.py --indices "old_index:new_index,another_index"

# Copy with larger batch size
python elastic_copier.py --batch-size 2000

# Use source index's total fields limit
python elastic_copier.py --total-fields-limit -1

# Increase logging verbosity
python elastic_copier.py -v  # or -vv for debug level
```

### Configuration Priority

1. Command line arguments (highest priority)
2. Environment variables from `.env` file
3. Default values (lowest priority)

## Limitations

- This is a simple copying tool, not a full migration solution
- Does not handle index aliases
- Does not migrate cluster settings
- No support for pipeline processors
- Limited error recovery (basic retry logic only)
- Does not preserve document IDs in some cases

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Security

Please note that this tool handles sensitive information like Elasticsearch credentials. Always use environment variables or secure configuration management for credentials, and never commit sensitive information to version control.
