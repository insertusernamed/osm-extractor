use osmpbf::{Element, ElementReader};
use rstar::RTree;
use rusqlite::{params, Connection, Result as SqlResult};
use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// How often the streaming passes report progress.
const PROGRESS_INTERVAL: u64 = 10_000_000;

/// A POI only inherits a neighbouring address if one is this close.
/// Without a limit, remote POIs (parks, viewpoints) pick up a house number
/// from a town kilometres away and it is indistinguishable from real data.
const MAX_ENRICH_DISTANCE_M: f64 = 150.0;

const DEFAULT_DB_PATH: &str = "osm_data.db";

/// Coordinates of the nodes the POI ways reference, keyed by OSM node id.
type NodeCoords = HashMap<i64, (f64, f64)>;

// ---------------------------------------------------------------------------
// Category mapping
// ---------------------------------------------------------------------------

struct CategoryRule {
    key: &'static str,
    values: &'static [(&'static str, &'static str)],
}

/// Ordered by priority. When an element carries several POI tags the first
/// matching rule wins, so this order is part of the output contract: it must
/// never depend on hash iteration order. `building` is last because almost
/// every object carries one, which makes it the least descriptive tag.
static CATEGORY_RULES: &[CategoryRule] = &[
    CategoryRule {
        key: "amenity",
        values: &[
            // food and dining
            ("restaurant", "food"),
            ("cafe", "food"),
            ("fast_food", "food"),
            ("bar", "food"),
            ("pub", "food"),
            ("food_court", "food"),
            ("ice_cream", "food"),
            ("biergarten", "food"),
            // entertainment
            ("cinema", "entertainment"),
            ("theatre", "entertainment"),
            ("nightclub", "entertainment"),
            ("casino", "entertainment"),
            ("arts_centre", "entertainment"),
            ("community_centre", "entertainment"),
            // healthcare
            ("hospital", "healthcare"),
            ("clinic", "healthcare"),
            ("doctors", "healthcare"),
            ("dentist", "healthcare"),
            ("pharmacy", "healthcare"),
            ("veterinary", "healthcare"),
            // financial
            ("bank", "financial"),
            ("atm", "financial"),
            ("bureau_de_change", "financial"),
            // transportation
            ("fuel", "transportation"),
            ("parking", "transportation"),
            ("car_rental", "transportation"),
            ("bicycle_rental", "transportation"),
            ("bus_station", "transportation"),
            ("taxi", "transportation"),
            // education
            ("school", "education"),
            ("university", "education"),
            ("college", "education"),
            ("library", "education"),
            ("kindergarten", "education"),
        ],
    },
    CategoryRule {
        key: "shop",
        values: &[
            ("supermarket", "shopping"),
            ("convenience", "shopping"),
            ("clothes", "shopping"),
            ("mall", "shopping"),
            ("department_store", "shopping"),
            ("electronics", "shopping"),
            ("furniture", "shopping"),
            ("books", "shopping"),
            ("bakery", "shopping"),
            ("butcher", "shopping"),
            ("florist", "shopping"),
            ("hardware", "shopping"),
        ],
    },
    CategoryRule {
        key: "tourism",
        values: &[
            ("hotel", "accommodation"),
            ("motel", "accommodation"),
            ("hostel", "accommodation"),
            ("guest_house", "accommodation"),
            ("attraction", "entertainment"),
            ("museum", "entertainment"),
            ("gallery", "entertainment"),
            ("viewpoint", "entertainment"),
        ],
    },
    CategoryRule {
        key: "leisure",
        values: &[
            ("park", "entertainment"),
            ("sports_centre", "entertainment"),
            ("playground", "entertainment"),
            ("stadium", "entertainment"),
            ("swimming_pool", "entertainment"),
            ("fitness_centre", "entertainment"),
            ("golf_course", "entertainment"),
        ],
    },
    CategoryRule {
        key: "office",
        values: &[
            ("educational_institution", "education"),
            ("university", "education"),
        ],
    },
    CategoryRule {
        key: "education",
        values: &[
            ("school", "education"),
            ("university", "education"),
            ("college", "education"),
        ],
    },
    CategoryRule {
        key: "building",
        values: &[
            ("college", "education"),
            ("university", "education"),
            ("school", "education"),
        ],
    },
];

/// A borrowed OSM tag list. Callers pass the slice so this module never has to
/// allocate strings just to look a tag up.
type Tags<'a> = [(&'a str, &'a str)];

fn tag<'a>(tags: &Tags<'a>, key: &str) -> Option<&'a str> {
    tags.iter()
        .find(|(k, _)| *k == key)
        .map(|(_, value)| *value)
}

/// Resolve a category for an element, or `None` if no rule matches.
/// Deterministic: `CATEGORY_RULES` order decides, first match wins.
fn resolve_category(tags: &Tags<'_>) -> Option<(&'static str, &'static str)> {
    for rule in CATEGORY_RULES {
        let Some(value) = tag(tags, rule.key) else {
            continue;
        };
        if let Some((_, category)) = rule.values.iter().find(|(v, _)| *v == value) {
            return Some((category, value_static(rule, value)));
        }
    }
    None
}

/// The matching subcategory as a `&'static str` borrowed from the rule table.
fn value_static(rule: &'static CategoryRule, value: &str) -> &'static str {
    rule.values
        .iter()
        .map(|(v, _)| *v)
        .find(|v| *v == value)
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct PointOfInterest {
    id: i64,
    name: String,
    category: &'static str,
    subcategory: &'static str,
    latitude: f64,
    longitude: f64,
    housenumber: String,
    city: String,
    street: String,
    osm_type: &'static str,
}

#[derive(Debug, Clone)]
struct Address {
    id: i64,
    housenumber: String,
    street: String,
    city: String,
    postcode: String,
    suburb: String,
    place: String,
    latitude: f64,
    longitude: f64,
}

#[derive(Clone, Debug)]
struct AddressPoint {
    housenumber: String,
    street: String,
    city: String,
    latitude: f64,
    longitude: f64,
    /// Projected position the spatial index ranks on; see [`project`].
    point: [f64; 2],
}

/// Web-Mercator projection, in radians.
///
/// The index has to rank neighbours by real distance, but raw
/// `(longitude, latitude)` degrees are anisotropic: at Ontario's latitude a
/// degree of longitude is roughly 30% shorter than a degree of latitude, so the
/// nearest neighbour in degree space is not the nearest neighbour in metres.
/// Mercator is locally conformal, which makes plain Euclidean distance in this
/// space a faithful local ranking.
fn project(latitude: f64, longitude: f64) -> [f64; 2] {
    let phi = latitude.to_radians();
    [
        longitude.to_radians(),
        (std::f64::consts::FRAC_PI_4 + phi / 2.0).tan().ln(),
    ]
}

impl rstar::RTreeObject for AddressPoint {
    type Envelope = rstar::AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        rstar::AABB::from_point(self.point)
    }
}

impl rstar::PointDistance for AddressPoint {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.point[0] - point[0];
        let dy = self.point[1] - point[1];
        dx * dx + dy * dy
    }
}

/// A way that resolved to a POI category, held between the two streaming passes.
struct WayRecord {
    id: i64,
    name: String,
    category: &'static str,
    subcategory: &'static str,
    housenumber: String,
    street: String,
    city: String,
    refs: Vec<i64>,
}

// ---------------------------------------------------------------------------
// Address formatting
// ---------------------------------------------------------------------------

/// The single implementation of "full address" text, used for both the POI and
/// the address table so the two can never drift apart.
fn format_full_address(
    housenumber: &str,
    street: &str,
    place: &str,
    suburb: &str,
    city: &str,
    postcode: &str,
) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(4);

    let line_one = match (housenumber.is_empty(), street.is_empty()) {
        (false, false) => format!("{housenumber} {street}"),
        (false, true) => housenumber.to_string(),
        (true, false) => street.to_string(),
        (true, true) => String::new(),
    };
    if !line_one.is_empty() {
        parts.push(line_one);
    }
    for optional in [place, suburb] {
        if !optional.is_empty() {
            parts.push(optional.to_string());
        }
    }
    if !city.is_empty() {
        parts.push(if postcode.is_empty() {
            city.to_string()
        } else {
            format!("{city} {postcode}")
        });
    } else if !postcode.is_empty() {
        parts.push(postcode.to_string());
    }

    parts.join(", ")
}

/// Great-circle distance in metres.
fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;
    let (phi1, phi2) = (lat1.to_radians(), lat2.to_radians());
    let delta_phi = phi2 - phi1;
    let delta_lambda = (lon2 - lon1).to_radians();
    let a = (delta_phi / 2.0).sin().powi(2)
        + phi1.cos() * phi2.cos() * (delta_lambda / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * a.sqrt().asin()
}

/// Nearest indexed address, but only if it is within `max_m` metres.
fn nearest_address_within(
    index: &RTree<AddressPoint>,
    lat: f64,
    lon: f64,
    max_m: f64,
) -> Option<&AddressPoint> {
    let candidate = index.nearest_neighbor(&project(lat, lon))?;
    let distance = haversine_m(lat, lon, candidate.latitude, candidate.longitude);
    (distance <= max_m).then_some(candidate)
}

// ---------------------------------------------------------------------------
// Extraction state
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Extraction {
    pois: Vec<PointOfInterest>,
    addresses: Vec<Address>,
    address_points: Vec<AddressPoint>,
}

impl Extraction {
    fn handle_node(&mut self, id: i64, lat: f64, lon: f64, tags: &Tags<'_>) {
        if let Some((category, subcategory)) = resolve_category(tags) {
            self.pois.push(PointOfInterest {
                id,
                name: tag(tags, "name").unwrap_or("Unnamed").to_string(),
                category,
                subcategory,
                latitude: lat,
                longitude: lon,
                housenumber: tag(tags, "addr:housenumber")
                    .unwrap_or_default()
                    .to_string(),
                city: tag(tags, "addr:city").unwrap_or_default().to_string(),
                street: tag(tags, "addr:street").unwrap_or_default().to_string(),
                osm_type: "node",
            });
        }

        let housenumber = tag(tags, "addr:housenumber").unwrap_or_default();
        let street = tag(tags, "addr:street").unwrap_or_default();
        if housenumber.is_empty() && street.is_empty() {
            return;
        }
        let city = tag(tags, "addr:city").unwrap_or_default();

        self.addresses.push(Address {
            id,
            housenumber: housenumber.to_string(),
            street: street.to_string(),
            city: city.to_string(),
            postcode: tag(tags, "addr:postcode").unwrap_or_default().to_string(),
            suburb: tag(tags, "addr:suburb").unwrap_or_default().to_string(),
            place: tag(tags, "addr:place").unwrap_or_default().to_string(),
            latitude: lat,
            longitude: lon,
        });

        // The spatial index only holds addresses complete enough to be useful.
        if !street.is_empty() && !housenumber.is_empty() {
            self.address_points.push(AddressPoint {
                housenumber: housenumber.to_string(),
                street: street.to_string(),
                city: city.to_string(),
                latitude: lat,
                longitude: lon,
                point: project(lat, lon),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Pass 1: collect the ways we care about
// ---------------------------------------------------------------------------

/// Collects POI-bearing ways with their node refs, and the set of node ids those
/// ways need. Only referenced nodes are kept in pass 2, which is what keeps
/// memory proportional to the POI ways instead of to every node in the extract.
fn collect_ways(pbf_path: &str) -> Result<(Vec<WayRecord>, HashSet<i64>), Box<dyn Error>> {
    let mut ways: Vec<WayRecord> = Vec::new();
    let mut needed: HashSet<i64> = HashSet::new();
    let mut processed: u64 = 0;

    let reader = ElementReader::from_path(pbf_path)?;
    reader.for_each(|element| {
        processed += 1;
        if processed.is_multiple_of(PROGRESS_INTERVAL) {
            println!(
                "  Scanned {}M elements - {} candidate ways",
                processed / 1_000_000,
                ways.len()
            );
        }

        let Element::Way(way) = element else {
            return;
        };
        let tags: Vec<(&str, &str)> = way.tags().collect();
        let Some((category, subcategory)) = resolve_category(&tags) else {
            return;
        };
        let refs: Vec<i64> = way.refs().collect();
        if refs.is_empty() {
            return;
        }
        needed.extend(refs.iter().copied());
        ways.push(WayRecord {
            id: way.id(),
            name: tag(&tags, "name").unwrap_or("Unnamed").to_string(),
            category,
            subcategory,
            housenumber: tag(&tags, "addr:housenumber")
                .unwrap_or_default()
                .to_string(),
            street: tag(&tags, "addr:street").unwrap_or_default().to_string(),
            city: tag(&tags, "addr:city").unwrap_or_default().to_string(),
            refs,
        });
    })?;

    Ok((ways, needed))
}

// ---------------------------------------------------------------------------
// Pass 2: nodes -> POIs, addresses, and the coordinates the ways need
// ---------------------------------------------------------------------------

fn collect_nodes(
    pbf_path: &str,
    needed: &HashSet<i64>,
) -> Result<(Extraction, NodeCoords), Box<dyn Error>> {
    let mut extraction = Extraction::default();
    let mut coords: NodeCoords = HashMap::with_capacity(needed.len());
    let mut processed: u64 = 0;

    let reader = ElementReader::from_path(pbf_path)?;
    reader.for_each(|element| {
        processed += 1;
        if processed.is_multiple_of(PROGRESS_INTERVAL) {
            println!(
                "  Processed {}M elements - Found {} POIs, {} addresses",
                processed / 1_000_000,
                extraction.pois.len(),
                extraction.addresses.len()
            );
        }

        // Collecting an untagged element yields a zero-length Vec, so the
        // common case allocates nothing.
        let (id, lat, lon, tags) = match element {
            Element::Node(node) => {
                let tags: Vec<(&str, &str)> = node.tags().collect();
                (node.id(), node.lat(), node.lon(), tags)
            }
            Element::DenseNode(node) => {
                let tags: Vec<(&str, &str)> = node.tags().collect();
                (node.id(), node.lat(), node.lon(), tags)
            }
            _ => return,
        };

        if needed.contains(&id) {
            coords.insert(id, (lat, lon));
        }
        extraction.handle_node(id, lat, lon, &tags);
    })?;

    Ok((extraction, coords))
}

/// Turn the collected ways into POIs using their centroid.
fn build_way_pois(ways: Vec<WayRecord>, coords: &NodeCoords) -> Vec<PointOfInterest> {
    let mut pois = Vec::with_capacity(ways.len());
    for way in ways {
        let mut lat_sum = 0.0;
        let mut lon_sum = 0.0;
        let mut valid_nodes = 0u32;
        for node_id in &way.refs {
            if let Some((lat, lon)) = coords.get(node_id) {
                lat_sum += lat;
                lon_sum += lon;
                valid_nodes += 1;
            }
        }
        if valid_nodes == 0 {
            continue;
        }
        let divisor = f64::from(valid_nodes);
        pois.push(PointOfInterest {
            id: way.id,
            name: way.name,
            category: way.category,
            subcategory: way.subcategory,
            latitude: lat_sum / divisor,
            longitude: lon_sum / divisor,
            housenumber: way.housenumber,
            city: way.city,
            street: way.street,
            osm_type: "way",
        });
    }
    pois
}

// ---------------------------------------------------------------------------
// Enrichment
// ---------------------------------------------------------------------------

/// Fill in an address for POIs that carry none, taking street, house number and
/// city from the nearest indexed address.
///
/// The rule is deliberately all-or-nothing: a POI either keeps the address it
/// was tagged with, or adopts a complete neighbouring one. Mixing the two
/// produces impossible pairs such as one street's name with another's house
/// number.
fn enrich_pois_with_addresses(pois: &mut [PointOfInterest], index: &RTree<AddressPoint>) -> usize {
    let mut enriched = 0;
    for poi in pois.iter_mut() {
        let missing_address = poi.street.is_empty() && poi.housenumber.is_empty();
        if !missing_address && !poi.city.is_empty() {
            continue;
        }
        let Some(nearest) =
            nearest_address_within(index, poi.latitude, poi.longitude, MAX_ENRICH_DISTANCE_M)
        else {
            continue;
        };
        if missing_address {
            poi.housenumber = nearest.housenumber.clone();
            poi.street = nearest.street.clone();
            enriched += 1;
        }
        if poi.city.is_empty() {
            poi.city = nearest.city.clone();
        }
    }
    enriched
}

// ---------------------------------------------------------------------------
// SQLite export
// ---------------------------------------------------------------------------

/// Collation note: a column's default collation is what queries use, so the
/// `COLLATE NOCASE` belongs on the column, not on the index. Declaring it only
/// on the index leaves the column BINARY and the index unusable — every lookup
/// silently falls back to a table scan.
///
/// Autocomplete should query a prefix with a range rather than `LIKE`:
///
/// ```sql
/// WHERE name >= :prefix AND name < :prefix || '\u{10FFFF}'
/// ```
///
/// That seeks `idx_poi_name` and is case-insensitive thanks to NOCASE, whereas
/// `name LIKE :prefix || '%'` scans the whole index.
const SCHEMA: &str = "
CREATE TABLE pois (
    id            INTEGER NOT NULL,
    name          TEXT COLLATE NOCASE NOT NULL,
    category      TEXT NOT NULL,
    subcategory   TEXT,
    latitude      REAL NOT NULL,
    longitude     REAL NOT NULL,
    housenumber   TEXT,
    street        TEXT COLLATE NOCASE,
    city          TEXT COLLATE NOCASE,
    osm_type      TEXT NOT NULL,
    full_address  TEXT COLLATE NOCASE,
    PRIMARY KEY (osm_type, id)
);
CREATE INDEX idx_poi_name         ON pois(name);
CREATE INDEX idx_poi_full_address ON pois(full_address);
CREATE INDEX idx_poi_category     ON pois(category);
CREATE INDEX idx_poi_city         ON pois(city);

CREATE TABLE addresses (
    id            INTEGER PRIMARY KEY,
    housenumber   TEXT,
    street        TEXT COLLATE NOCASE,
    city          TEXT COLLATE NOCASE,
    postcode      TEXT,
    suburb        TEXT,
    place         TEXT,
    latitude      REAL NOT NULL,
    longitude     REAL NOT NULL,
    full_address  TEXT COLLATE NOCASE
);
CREATE INDEX idx_addr_full   ON addresses(full_address);
CREATE INDEX idx_addr_street ON addresses(street);
";

/// Write the database next to `db_path` and rename it into place, so an
/// interrupted run never leaves a half-populated database behind and re-running
/// over an existing file works.
fn export_to_sqlite(
    pois: &[PointOfInterest],
    addresses: &[Address],
    db_path: &Path,
) -> SqlResult<()> {
    let tmp_path = with_tmp_suffix(db_path);
    if tmp_path.exists() {
        fs::remove_file(&tmp_path).map_err(to_sql_error)?;
    }

    let conn = Connection::open(&tmp_path)?;
    // Bulk load: no rollback journal to maintain and no fsync per page.
    conn.execute_batch(
        "PRAGMA journal_mode = OFF;
         PRAGMA synchronous = OFF;
         PRAGMA temp_store = MEMORY;
         PRAGMA cache_size = -65536;",
    )?;
    conn.execute_batch(SCHEMA)?;

    {
        let tx = conn.unchecked_transaction()?;
        let mut stmt = tx.prepare(
            "INSERT INTO pois (id, name, category, subcategory, latitude, longitude,
                               housenumber, street, city, osm_type, full_address)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        )?;
        for poi in pois {
            let full_address =
                format_full_address(&poi.housenumber, &poi.street, "", "", &poi.city, "");
            stmt.execute(params![
                poi.id,
                poi.name,
                poi.category,
                poi.subcategory,
                poi.latitude,
                poi.longitude,
                poi.housenumber,
                poi.street,
                poi.city,
                poi.osm_type,
                full_address,
            ])?;
        }
        drop(stmt);
        tx.commit()?;
    }

    {
        let tx = conn.unchecked_transaction()?;
        let mut stmt = tx.prepare(
            "INSERT INTO addresses (id, housenumber, street, city, postcode, suburb, place,
                                    latitude, longitude, full_address)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )?;
        for address in addresses {
            let full_address = format_full_address(
                &address.housenumber,
                &address.street,
                &address.place,
                &address.suburb,
                &address.city,
                &address.postcode,
            );
            stmt.execute(params![
                address.id,
                address.housenumber,
                address.street,
                address.city,
                address.postcode,
                address.suburb,
                address.place,
                address.latitude,
                address.longitude,
                full_address,
            ])?;
        }
        drop(stmt);
        tx.commit()?;
    }

    conn.execute_batch("ANALYZE;")?;
    // VACUUM rewrites the file compactly; measured at ~1.8s for an 18MB saving
    // on Ontario, worth it for a downloadable release artifact.
    conn.execute_batch("VACUUM;")?;
    drop(conn);

    fs::rename(&tmp_path, db_path).map_err(to_sql_error)?;
    Ok(())
}

fn with_tmp_suffix(db_path: &Path) -> PathBuf {
    let mut name = db_path.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}

fn to_sql_error(error: std::io::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let program = args.first().map(String::as_str).unwrap_or("osm-extractor");
    let Some(pbf_path) = args.get(1) else {
        eprintln!("Usage: {program} <pbf_file> [output.db]");
        eprintln!("\nExample: {program} ontario-latest.osm.pbf");
        std::process::exit(1);
    };
    let db_path = PathBuf::from(args.get(2).map(String::as_str).unwrap_or(DEFAULT_DB_PATH));

    println!("{}", "=".repeat(80));
    println!("OSM PBF Fast Extractor (Rust)");
    println!("{}", "=".repeat(80));
    println!("Input file: {pbf_path}");
    println!("Output DB:  {}", db_path.display());
    println!();

    let start = Instant::now();

    println!("PASS 1: Collecting POI ways...");
    let pass1 = Instant::now();
    let (ways, needed) = collect_ways(pbf_path)?;
    println!(
        "✓ Pass 1 complete in {:.2?} - {} candidate ways, {} referenced nodes",
        pass1.elapsed(),
        ways.len(),
        needed.len()
    );
    println!();

    println!("PASS 2: Extracting POIs and addresses...");
    let pass2 = Instant::now();
    let (mut extraction, coords) = collect_nodes(pbf_path, &needed)?;
    let way_pois = build_way_pois(ways, &coords);
    let way_poi_count = way_pois.len();
    extraction.pois.extend(way_pois);
    drop(coords);
    println!("✓ Pass 2 complete in {:.2?}", pass2.elapsed());
    println!();

    let address_index = RTree::bulk_load(extraction.address_points);
    println!("Enriching POIs with nearest addresses...");
    let enrich_start = Instant::now();
    let enriched = enrich_pois_with_addresses(&mut extraction.pois, &address_index);
    println!(
        "  ✓ Enriched {} POIs within {:.0}m in {:.2?}",
        enriched,
        MAX_ENRICH_DISTANCE_M,
        enrich_start.elapsed()
    );
    println!();

    let poi_count = extraction.pois.len();
    println!("Final Results:");
    println!(
        "  POIs found: {} ({} from nodes, {} from ways)",
        poi_count,
        poi_count - way_poi_count,
        way_poi_count
    );
    println!("  Addresses found: {}", extraction.addresses.len());
    println!(
        "  POIs with a street and house number: {}",
        extraction
            .pois
            .iter()
            .filter(|poi| !poi.street.is_empty() && !poi.housenumber.is_empty())
            .count()
    );
    println!();

    println!("Creating SQLite database at {}...", db_path.display());
    export_to_sqlite(&extraction.pois, &extraction.addresses, &db_path)?;
    println!("  ✓ POIs and addresses written");
    println!();

    println!("{}", "=".repeat(80));
    println!("Complete! Total time: {:.2?}", start.elapsed());
    println!("{}", "=".repeat(80));

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn poi(street: &str, housenumber: &str, lat: f64, lon: f64) -> PointOfInterest {
        PointOfInterest {
            id: 1,
            name: "Test".to_string(),
            category: "food",
            subcategory: "restaurant",
            latitude: lat,
            longitude: lon,
            housenumber: housenumber.to_string(),
            city: String::new(),
            street: street.to_string(),
            osm_type: "node",
        }
    }

    fn index_of(points: Vec<AddressPoint>) -> RTree<AddressPoint> {
        RTree::bulk_load(points)
    }

    fn address_point(
        housenumber: &str,
        street: &str,
        city: &str,
        lat: f64,
        lon: f64,
    ) -> AddressPoint {
        AddressPoint {
            housenumber: housenumber.to_string(),
            street: street.to_string(),
            city: city.to_string(),
            latitude: lat,
            longitude: lon,
            point: project(lat, lon),
        }
    }

    #[test]
    fn category_is_prioritised_not_hash_ordered() {
        // A petrol station with a shop attached is both amenity=fuel and
        // shop=convenience; the earlier rule must win every time.
        let tags = [("amenity", "fuel"), ("shop", "convenience")];
        assert_eq!(resolve_category(&tags), Some(("transportation", "fuel")));

        // building=* must never outrank a descriptive tag.
        let tags = [("building", "school"), ("amenity", "parking")];
        assert_eq!(resolve_category(&tags), Some(("transportation", "parking")));
    }

    #[test]
    fn category_resolution_is_repeatable() {
        let tags = [("amenity", "restaurant"), ("tourism", "viewpoint")];
        let first = resolve_category(&tags);
        for _ in 0..1_000 {
            assert_eq!(resolve_category(&tags), first);
        }
    }

    #[test]
    fn category_ignores_unmapped_values() {
        assert_eq!(resolve_category(&[("amenity", "bench")]), None);
        assert_eq!(resolve_category(&[]), None);
    }

    #[test]
    fn tag_lookup_is_exact() {
        let tags = [("name", "Tim Hortons"), ("addr:city", "Toronto")];
        assert_eq!(tag(&tags, "name"), Some("Tim Hortons"));
        assert_eq!(tag(&tags, "addr:city"), Some("Toronto"));
        assert_eq!(tag(&tags, "addr:street"), None);
    }

    #[test]
    fn full_address_never_leaves_a_dangling_separator() {
        // The previous builder produced "9266 Guelph Line," here.
        assert_eq!(
            format_full_address("9266", "Guelph Line", "", "", "", ""),
            "9266 Guelph Line"
        );
        assert_eq!(
            format_full_address("", "Guelph Line", "", "", "", ""),
            "Guelph Line"
        );
        assert_eq!(format_full_address("", "", "", "", "", ""), "");
    }

    #[test]
    fn full_address_joins_components_consistently() {
        assert_eq!(
            format_full_address("736", "Bathurst Street", "", "", "Toronto", "M5S 2R4"),
            "736 Bathurst Street, Toronto M5S 2R4"
        );
        assert_eq!(
            format_full_address("44", "Gould Street", "", "", "Cobden", ""),
            "44 Gould Street, Cobden"
        );
        assert_eq!(
            format_full_address(
                "1",
                "Main St",
                "Springfield",
                "East Side",
                "Shelbyville",
                ""
            ),
            "1 Main St, Springfield, East Side, Shelbyville"
        );
        // A postcode without a city still has to appear.
        assert_eq!(
            format_full_address("1", "Main St", "", "", "", "M5S 2R4"),
            "1 Main St, M5S 2R4"
        );
    }

    #[test]
    fn enrichment_keeps_the_address_the_poi_was_tagged_with() {
        // POI knows its street but not its house number; the nearest address is
        // on a different street. The old code overwrote the street.
        let index = index_of(vec![address_point("999", "Back Alley", "", 43.0001, -79.0)]);
        let mut pois = vec![poi("Main Street", "", 43.0, -79.0)];
        enrich_pois_with_addresses(&mut pois, &index);
        assert_eq!(pois[0].street, "Main Street");
        assert_eq!(pois[0].housenumber, "");
    }

    #[test]
    fn enrichment_fills_a_missing_address() {
        let index = index_of(vec![address_point(
            "12",
            "Main Street",
            "Toronto",
            43.0001,
            -79.0,
        )]);
        let mut pois = vec![poi("", "", 43.0, -79.0)];
        let enriched = enrich_pois_with_addresses(&mut pois, &index);
        assert_eq!(enriched, 1);
        assert_eq!(pois[0].housenumber, "12");
        assert_eq!(pois[0].street, "Main Street");
        assert_eq!(pois[0].city, "Toronto");
    }

    /// Regression test: ranking neighbours by raw (lon, lat) degrees picks the
    /// wrong address, because a degree of longitude is only ~70% of a degree of
    /// latitude at this latitude.
    #[test]
    fn nearest_address_ranks_by_real_distance_not_degrees() {
        // 0.0008 degrees of latitude ~ 89 m but is the smallest degree offset.
        // 0.0009 degrees of longitude ~ 70 m, so it is nearer in metres while
        // being farther in raw degrees.
        let index = index_of(vec![
            address_point("1", "Latitude Street", "", 45.4008, -75.7),
            address_point("2", "Longitude Street", "", 45.4, -75.7009),
        ]);
        let mut pois = vec![poi("", "", 45.4, -75.7)];
        enrich_pois_with_addresses(&mut pois, &index);
        assert_eq!(
            pois[0].street, "Longitude Street",
            "expected the metre-nearest address, not the degree-nearest one"
        );
    }

    #[test]
    fn enrichment_ignores_addresses_beyond_the_distance_limit() {
        // ~1.1 km away: far outside MAX_ENRICH_DISTANCE_M.
        let index = index_of(vec![address_point(
            "12",
            "Main Street",
            "Toronto",
            43.01,
            -79.0,
        )]);
        let mut pois = vec![poi("", "", 43.0, -79.0)];
        assert_eq!(enrich_pois_with_addresses(&mut pois, &index), 0);
        assert_eq!(pois[0].street, "");
    }

    #[test]
    fn enrichment_leaves_complete_addresses_alone() {
        let index = index_of(vec![address_point(
            "12",
            "Other Street",
            "Elsewhere",
            43.0,
            -79.0,
        )]);
        let mut pois = vec![poi("Main Street", "5", 43.0, -79.0)];
        pois[0].city = "Toronto".to_string();
        assert_eq!(enrich_pois_with_addresses(&mut pois, &index), 0);
        assert_eq!(pois[0].street, "Main Street");
        assert_eq!(pois[0].housenumber, "5");
        assert_eq!(pois[0].city, "Toronto");
    }

    fn temp_db_path(name: &str) -> PathBuf {
        let mut path = env::temp_dir();
        path.push(format!(
            "osm-extractor-test-{}-{name}.db",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        path
    }

    fn sample_data() -> (Vec<PointOfInterest>, Vec<Address>) {
        let mut address_poi = poi("Main Street", "5", 43.0, -79.0);
        address_poi.city = "Toronto".to_string();
        let mut unaddressed_poi = poi("", "", 43.0, -79.0);
        unaddressed_poi.id = 2;
        let pois = vec![address_poi, unaddressed_poi];
        let addresses = vec![Address {
            id: 10,
            housenumber: "9266".to_string(),
            street: "Guelph Line".to_string(),
            city: String::new(),
            postcode: String::new(),
            suburb: String::new(),
            place: String::new(),
            latitude: 43.0,
            longitude: -79.0,
        }];
        (pois, addresses)
    }

    #[test]
    fn export_can_be_run_twice_over_the_same_path() {
        let db_path = temp_db_path("idempotent");
        let (pois, addresses) = sample_data();

        export_to_sqlite(&pois, &addresses, &db_path).expect("first export");
        export_to_sqlite(&pois, &addresses, &db_path).expect("second export over existing file");
        export_to_sqlite(&pois, &addresses, &db_path).expect("third export over existing file");

        let conn = Connection::open(&db_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT count(*) FROM pois", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
        let address_count: i64 = conn
            .query_row("SELECT count(*) FROM addresses", [], |row| row.get(0))
            .unwrap();
        assert_eq!(address_count, 1);

        drop(conn);
        let _ = fs::remove_file(&db_path);
    }

    #[test]
    fn export_writes_a_usable_full_address() {
        let db_path = temp_db_path("full-address");
        let (pois, addresses) = sample_data();
        export_to_sqlite(&pois, &addresses, &db_path).unwrap();

        let conn = Connection::open(&db_path).unwrap();
        let poi_address: String = conn
            .query_row(
                "SELECT full_address FROM pois WHERE housenumber = '5'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(poi_address, "5 Main Street, Toronto");

        let address_text: String = conn
            .query_row("SELECT full_address FROM addresses", [], |row| row.get(0))
            .unwrap();
        assert_eq!(address_text, "9266 Guelph Line");

        drop(conn);
        let _ = fs::remove_file(&db_path);
    }

    /// The original schema declared `TEXT` columns but built `... COLLATE NOCASE`
    /// indexes. A column's default collation cannot use a differently-collated
    /// index, so every autocomplete query fell back to a full table scan. This
    /// asserts the planner actually accepts the indexes now.
    #[test]
    fn indexes_are_usable_by_the_query_planner() {
        let db_path = temp_db_path("index-plan");
        let pois: Vec<PointOfInterest> = (0..5_000)
            .map(|i| {
                let mut poi = poi(&format!("Main Street {i}"), "5", 43.0, -79.0);
                poi.id = i;
                poi.name = format!("Business {i}");
                // One rare value in an otherwise common column, so ANALYZE has
                // statistics that make an indexed lookup the cheaper plan.
                poi.city = if i == 7 { "Cobden" } else { "Toronto" }.to_string();
                poi
            })
            .collect();
        export_to_sqlite(&pois, &[], &db_path).unwrap();

        let conn = Connection::open(&db_path).unwrap();
        let plan_for = |sql: &str| -> String {
            conn.query_row(sql, [], |row| row.get::<_, String>(3))
                .unwrap()
        };

        let equality =
            plan_for("EXPLAIN QUERY PLAN SELECT id FROM pois WHERE name = 'Business 42'");
        assert!(
            equality.contains("idx_poi_name"),
            "equality on name should use idx_poi_name, got: {equality}"
        );

        let address_lookup = plan_for(
            "EXPLAIN QUERY PLAN SELECT id FROM pois WHERE full_address = '5 Main Street 42, Toronto'",
        );
        assert!(
            address_lookup.contains("idx_poi_full_address"),
            "equality on full_address should use idx_poi_full_address, got: {address_lookup}"
        );

        let city_lookup = plan_for("EXPLAIN QUERY PLAN SELECT id FROM pois WHERE city = 'Cobden'");
        assert!(
            city_lookup.contains("idx_poi_city"),
            "equality on city should use idx_poi_city, got: {city_lookup}"
        );

        drop(conn);
        let _ = fs::remove_file(&db_path);
    }

    #[test]
    fn haversine_matches_known_distance() {
        // Toronto -> Montreal is roughly 505 km.
        let distance = haversine_m(43.6532, -79.3832, 45.5019, -73.5674);
        assert!((distance - 504_000.0).abs() < 5_000.0, "got {distance}");
    }
}
