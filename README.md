# OSM Extractor

A high-performance OpenStreetMap data extraction tool written in Rust. Extracts points of interest and addresses from OSM PBF files into a SQLite database optimized for location-based applications.

## Background

This tool was developed for a carpooling application that needed fast, efficient access to location data for autocomplete functionality. Existing Java-based extraction methods were too slow for development workflows, taking 10-15 minutes to process Ontario's OSM data. This Rust implementation reduces extraction time to 2-5 minutes while producing a compact, indexed database ready for production use.

## Features

- Fast two-pass extraction algorithm optimized for large OSM datasets
- Extracts categorized POIs (restaurants, schools, hospitals, etc.)
- Full address data with geocoding support
- Automatic city/street inference for incomplete address data
- SQLite output with pre-built indexes for fast querying
- Optional JSON output for debugging
- Processes Ontario data (~850 MB PBF) in under 5 minutes

## Pre-built Releases
Weekly automated builds are available in the [Releases](../../releases) section. Each release includes:

- Pre-built OSM database (SQLite) with POIs and addresses
- GraphHopper routing graph cache for Ontario

## Acknowledgments
- OpenStreetMap contributors for the data
- Geofabrik for providing regional extracts
