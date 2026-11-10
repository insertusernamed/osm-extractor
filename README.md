# OSM Extractor

A high-performance OpenStreetMap data extraction tool written in Rust. Extracts points of interest and addresses from OSM PBF files into a SQLite database optimized for location-based applications.

## Background

This tool was developed for a carpooling application that needed fast, efficient access to location data for autocomplete functionality. Existing Java-based extraction methods were too slow for development workflows, taking 10-15 minutes to process Ontario's OSM data. This Rust implementation runs in well under a minute while producing a compact, indexed database ready for production use.

## Features

- Two-pass extraction that keeps coordinates only for the nodes POI ways reference, instead of every node in the extract
- Extracts categorized POIs (restaurants, schools, hospitals, etc.)
- Full address data with geocoding support
- Nearest-address inference for POIs tagged without one, capped at 150 m so remote POIs never inherit a distant town's address
- Deterministic output: the same PBF always produces the same database
- SQLite output with pre-built indexes for fast autocomplete queries
- Processes Ontario data (~1 GB PBF) in about 40 seconds with a ~2.2 GB peak footprint

## Usage

```sh
cargo build --release
./target/release/osm-extractor ontario-latest.osm.pbf         # writes osm_data.db
./target/release/osm-extractor ontario-latest.osm.pbf my.db   # custom output path
```

The database is written to a temporary file and renamed into place, so an
interrupted run leaves any previous database untouched and re-running over an
existing file is safe.

## Pre-built Releases
Weekly automated builds are available in the [Releases](../../releases) section. Each release includes:

- Pre-built OSM database (SQLite) with POIs and addresses
- GraphHopper routing graph cache for Ontario

## Acknowledgments
- OpenStreetMap contributors for the data
- Geofabrik for providing regional extracts
