# Baseline
Music Maintenance Suite

A DJ library maintenance toolbox designed to keep your music collection
clean, organised, and performance ready.

Music Maintenance Suite provides a unified GUI and CLI interface for
analysing and maintaining DJ libraries across Rekordbox, Mixed In Key,
and local audio files. The suite focuses on real world reliability for
live DJ performance environments.

------------------------------------------------------------------------

## Features

### Rekordbox XML Analysis

-   Library statistics and overview reports
-   Duplicate track detection
-   Missing file detection
-   Metadata quality checks
-   Playlist structure analysis
-   Artwork detection
-   Excel, CSV, and playlist report generation

### Discogs Integration

-   Automatically fetch high quality artwork
-   Fill missing metadata including year and label
-   Resume support for large collections
-   Safe embedding using Mutagen
-   Optional artwork preview window

### Mixed In Key Database Tools

-   Remove missing tracks from MIK database
-   Sync file metadata back into MIK database
-   Sync embedded artwork into MIK database
-   Automatic database backup before modification
-   Dry run safety mode by default

### Filename Standardisation

-   Detect incorrect filename formatting
-   Generate rename suggestions using tag data
-   Supports artist, title, remix formatting rules
-   Optional rename application using generated CSV

### GUI Interface

-   Multi tab desktop interface
-   Built in run logging
-   Background script execution
-   Interactive prompts supported
-   Centralised settings management

------------------------------------------------------------------------

## Safety Philosophy

Music Maintenance Suite is designed with safety first principles.

-   Original files are never modified unless explicitly requested
-   Destructive actions use dry run mode by default
-   Automatic backups are created where applicable
-   Reports are generated before changes are applied

------------------------------------------------------------------------

## Requirements

-   Python 3.10 or newer (recommended)
-   Windows, Linux, or macOS (GUI tested primarily on Windows)

### Required Python Libraries

Install dependencies using:

    pip install -r requirements.txt

Typical dependencies include:

-   mutagen
-   pillow
-   requests
-   openpyxl

------------------------------------------------------------------------

## Installation

### 1. Clone the Repository

    git clone https://github.com/YOUR_USERNAME/MusicMaintenanceSuite.git
    cd MusicMaintenanceSuite

### 2. Create Virtual Environment (Recommended)

    python -m venv .venv
    .venv\Scripts\activate

### 3. Install Dependencies

    pip install -r requirements.txt

------------------------------------------------------------------------

## Running the Application

### Launch GUI

    python app.py

### Launch CLI

    python music_suite.py --help

------------------------------------------------------------------------

## Configuration

User settings are stored locally and are not included in the repository.

Create a settings file using the provided template:

    data/baseline_settings.example.json

Rename it to:

    baseline_settings.json

Then edit it to match your system paths.

------------------------------------------------------------------------

## Discogs API Setup

To enable Discogs integration, create a personal Discogs API key.

Set environment variables:

    DISCOGS_KEY=your_key_here
    DISCOGS_SECRET=your_secret_here

Alternatively configure keys inside the application settings panel.

------------------------------------------------------------------------

## Output Reports

Reports are typically generated in:

    Documents/Baseline/Logs
    baseline_work/

------------------------------------------------------------------------

## Packaging (Optional)

The project includes a PyInstaller specification file for building
standalone executables.

    pyinstaller MusicMaintenanceSuite.spec

------------------------------------------------------------------------

## Disclaimer

Music Maintenance Suite is not affiliated with Pioneer DJ, AlphaTheta,
Rekordbox, or Mixed In Key.

Use at your own risk. Always maintain backups of your music library.

------------------------------------------------------------------------

## Contributing

Contributions, bug reports, and feature suggestions are welcome. Please
open an issue or pull request.

------------------------------------------------------------------------

## Author

Alex Eneas\
DJ, developer, and creator of performance reliability tools for DJs.

------------------------------------------------------------------------

## License

This project is licensed under the MIT License. See LICENSE file for
details.
