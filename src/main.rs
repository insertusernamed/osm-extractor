use osmpbf::{Element, ElementReader};
use rusqlite::{params, Connection, Result as SqlResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufWriter;
use std::time::Instant;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct PointOfInterest {
    id: i64,
    name: String,
    category: String,
    subcategory: String,
    latitude: f64,
    longitude: f64,
    housenumber: String,
    city: String,
    street: String,
    osm_type: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
    full_address: String,
}

#[derive(Debug, Serialize)]
struct PoiData {
    pois: Vec<PointOfInterest>,
}

#[derive(Debug, Serialize)]
struct AddressData {
    addresses: Vec<Address>,
}

// category mapping
fn get_category_mapping() -> HashMap<String, HashMap<String, String>> {
    let mut category_map: HashMap<String, HashMap<String, String>> = HashMap::new();

    // amenity mappings
    let mut amenity_map = HashMap::new();

    // food and dining places
    amenity_map.insert("restaurant".to_string(), "food".to_string());
    amenity_map.insert("cafe".to_string(), "food".to_string());
    amenity_map.insert("fast_food".to_string(), "food".to_string());
    amenity_map.insert("bar".to_string(), "food".to_string());
    amenity_map.insert("pub".to_string(), "food".to_string());
    amenity_map.insert("food_court".to_string(), "food".to_string());
    amenity_map.insert("ice_cream".to_string(), "food".to_string());
    amenity_map.insert("biergarten".to_string(), "food".to_string());

    // entertainment spots
    amenity_map.insert("cinema".to_string(), "entertainment".to_string());
    amenity_map.insert("theatre".to_string(), "entertainment".to_string());
    amenity_map.insert("nightclub".to_string(), "entertainment".to_string());
    amenity_map.insert("casino".to_string(), "entertainment".to_string());
    amenity_map.insert("arts_centre".to_string(), "entertainment".to_string());
    amenity_map.insert("community_centre".to_string(), "entertainment".to_string());

    // healthcare facilities
    amenity_map.insert("hospital".to_string(), "healthcare".to_string());
    amenity_map.insert("clinic".to_string(), "healthcare".to_string());
    amenity_map.insert("doctors".to_string(), "healthcare".to_string());
    amenity_map.insert("dentist".to_string(), "healthcare".to_string());
    amenity_map.insert("pharmacy".to_string(), "healthcare".to_string());
    amenity_map.insert("veterinary".to_string(), "healthcare".to_string());

    // financial services
    amenity_map.insert("bank".to_string(), "financial".to_string());
    amenity_map.insert("atm".to_string(), "financial".to_string());
    amenity_map.insert("bureau_de_change".to_string(), "financial".to_string());

    // transportation stuff
    amenity_map.insert("fuel".to_string(), "transportation".to_string());
    amenity_map.insert("parking".to_string(), "transportation".to_string());
    amenity_map.insert("car_rental".to_string(), "transportation".to_string());
    amenity_map.insert("bicycle_rental".to_string(), "transportation".to_string());
    amenity_map.insert("bus_station".to_string(), "transportation".to_string());
    amenity_map.insert("taxi".to_string(), "transportation".to_string());

    // education places
    amenity_map.insert("school".to_string(), "education".to_string());
    amenity_map.insert("university".to_string(), "education".to_string());
    amenity_map.insert("college".to_string(), "education".to_string());
    amenity_map.insert("library".to_string(), "education".to_string());
    amenity_map.insert("kindergarten".to_string(), "education".to_string());

    category_map.insert("amenity".to_string(), amenity_map);

    // shop mappings
    let mut shop_map = HashMap::new();
    shop_map.insert("supermarket".to_string(), "shopping".to_string());
    shop_map.insert("convenience".to_string(), "shopping".to_string());
    shop_map.insert("clothes".to_string(), "shopping".to_string());
    shop_map.insert("mall".to_string(), "shopping".to_string());
    shop_map.insert("department_store".to_string(), "shopping".to_string());
    shop_map.insert("electronics".to_string(), "shopping".to_string());
    shop_map.insert("furniture".to_string(), "shopping".to_string());
    shop_map.insert("books".to_string(), "shopping".to_string());
    shop_map.insert("bakery".to_string(), "shopping".to_string());
    shop_map.insert("butcher".to_string(), "shopping".to_string());
    shop_map.insert("florist".to_string(), "shopping".to_string());
    shop_map.insert("hardware".to_string(), "shopping".to_string());

    category_map.insert("shop".to_string(), shop_map);

    // tourism mappings
    let mut tourism_map = HashMap::new();
    tourism_map.insert("hotel".to_string(), "accommodation".to_string());
    tourism_map.insert("motel".to_string(), "accommodation".to_string());
    tourism_map.insert("hostel".to_string(), "accommodation".to_string());
    tourism_map.insert("guest_house".to_string(), "accommodation".to_string());
    tourism_map.insert("attraction".to_string(), "entertainment".to_string());
    tourism_map.insert("museum".to_string(), "entertainment".to_string());
    tourism_map.insert("gallery".to_string(), "entertainment".to_string());
    tourism_map.insert("viewpoint".to_string(), "entertainment".to_string());

    category_map.insert("tourism".to_string(), tourism_map);

    // leisure mappings
    let mut leisure_map = HashMap::new();
    leisure_map.insert("park".to_string(), "entertainment".to_string());
    leisure_map.insert("sports_centre".to_string(), "entertainment".to_string());
    leisure_map.insert("playground".to_string(), "entertainment".to_string());
    leisure_map.insert("stadium".to_string(), "entertainment".to_string());
    leisure_map.insert("swimming_pool".to_string(), "entertainment".to_string());
    leisure_map.insert("fitness_centre".to_string(), "entertainment".to_string());
    leisure_map.insert("golf_course".to_string(), "entertainment".to_string());

    category_map.insert("leisure".to_string(), leisure_map);

    // office mappings
    let mut office_map = HashMap::new();
    office_map.insert(
        "educational_institution".to_string(),
        "education".to_string(),
    );
    office_map.insert("university".to_string(), "education".to_string());

    category_map.insert("office".to_string(), office_map);

    // education key mappings
    let mut education_map = HashMap::new();
    education_map.insert("school".to_string(), "education".to_string());
    education_map.insert("university".to_string(), "education".to_string());
    education_map.insert("college".to_string(), "education".to_string());

    category_map.insert("education".to_string(), education_map);

    // building mappings
    let mut building_map = HashMap::new();
    building_map.insert("college".to_string(), "education".to_string());
    building_map.insert("university".to_string(), "education".to_string());
    building_map.insert("school".to_string(), "education".to_string());

    category_map.insert("building".to_string(), building_map);

    category_map
}

fn process_node_tags(
    node_id: i64,
    lat: f64,
    lon: f64,
    tags: HashMap<String, String>,
    category_map: &HashMap<String, HashMap<String, String>>,
    pois: &mut Vec<PointOfInterest>,
    addresses: &mut Vec<Address>,
) {
    // checking for points of interest
    let mut category: Option<String> = None;
    let mut subcategory: Option<String> = None;

    for (tag_key, value_map) in category_map.iter() {
        if let Some(tag_value) = tags.get(tag_key) {
            if let Some(cat) = value_map.get(tag_value) {
                category = Some(cat.clone());
                subcategory = Some(tag_value.clone());
                break;
            }
        }
    }

    if let Some(cat) = category {
        pois.push(PointOfInterest {
            id: node_id,
            name: tags
                .get("name")
                .cloned()
                .unwrap_or_else(|| "Unnamed".to_string()),
            category: cat,
            subcategory: subcategory.unwrap_or_default(),
            latitude: lat,
            longitude: lon,
            housenumber: tags.get("addr:housenumber").cloned().unwrap_or_default(),
            city: tags.get("addr:city").cloned().unwrap_or_default(),
            street: tags.get("addr:street").cloned().unwrap_or_default(),
            osm_type: "node".to_string(),
        });
    }

    // checking for addresses
    if tags.contains_key("addr:housenumber") || tags.contains_key("addr:street") {
        let housenumber = tags.get("addr:housenumber").cloned().unwrap_or_default();
        let street = tags.get("addr:street").cloned().unwrap_or_default();
        let city = tags.get("addr:city").cloned().unwrap_or_default();
        let postcode = tags.get("addr:postcode").cloned().unwrap_or_default();
        let suburb = tags.get("addr:suburb").cloned().unwrap_or_default();
        let place = tags.get("addr:place").cloned().unwrap_or_default();

        let mut full_addr = String::new();
        if !housenumber.is_empty() {
            full_addr.push_str(&format!("{} ", housenumber));
        }
        if !street.is_empty() {
            full_addr.push_str(&format!("{}, ", street));
        }
        if !place.is_empty() {
            full_addr.push_str(&format!("{}, ", place));
        }
        if !suburb.is_empty() {
            full_addr.push_str(&format!("{}, ", suburb));
        }
        if !city.is_empty() {
            full_addr.push_str(&format!("{} ", city));
        }
        if !postcode.is_empty() {
            full_addr.push_str(&postcode);
        }

        addresses.push(Address {
            id: node_id,
            housenumber,
            street,
            city,
            postcode,
            suburb,
            place,
            latitude: lat,
            longitude: lon,
            full_address: full_addr.trim().to_string(),
        });
    }
}

fn export_to_sqlite(
    pois: &[PointOfInterest],
    addresses: &[Address],
    db_path: &str,
) -> SqlResult<()> {
    println!("💾 Creating SQLite database at {}...", db_path);

    // creating the database connection
    let conn = Connection::open(db_path)?;

    // creating the pois table with indexes
    conn.execute(
    "CREATE TABLE IF NOT EXISTS pois (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT NOT NULL,
        subcategory TEXT,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        housenumber TEXT,
        street TEXT,
        city TEXT,
        osm_type TEXT,
        full_address TEXT GENERATED ALWAYS AS (
            CASE 
                WHEN housenumber IS NOT NULL AND housenumber != '' AND street IS NOT NULL AND street != '' 
                    THEN housenumber || ' ' || street || CASE WHEN city != '' THEN ', ' || city ELSE '' END
                WHEN street IS NOT NULL AND street != '' 
                    THEN street || CASE WHEN city != '' THEN ', ' || city ELSE '' END
                WHEN city IS NOT NULL AND city != '' 
                    THEN city
                ELSE ''
            END
        ) STORED
    )",
    [],
    )?;

    // creating indexes for quick autocomplete searches
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_poi_name ON pois(name COLLATE NOCASE)",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_poi_full_address ON pois(full_address COLLATE NOCASE)",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_poi_category ON pois(category)",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_poi_city ON pois(city COLLATE NOCASE)",
        [],
    )?;

    // creating the addresses table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS addresses (
            id INTEGER PRIMARY KEY,
            housenumber TEXT,
            street TEXT,
            city TEXT,
            postcode TEXT,
            suburb TEXT,
            place TEXT,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            full_address TEXT
        )",
        [],
    )?;

    // index for address searches
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_addr_full ON addresses(full_address COLLATE NOCASE)",
        [],
    )?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_addr_street ON addresses(street COLLATE NOCASE)",
        [],
    )?;

    println!("  Inserting {} POIs...", pois.len());

    // starting a transaction for bulk insert to make it faster
    let tx = conn.unchecked_transaction()?;

    {
        let mut stmt = tx.prepare(
            "INSERT INTO pois (id, name, category, subcategory, latitude, longitude, housenumber, city, street, osm_type)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
        )?;

        for poi in pois {
            stmt.execute(params![
                poi.id,
                poi.name,
                poi.category,
                poi.subcategory,
                poi.latitude,
                poi.longitude,
                poi.housenumber,
                poi.city,
                poi.street,
                poi.osm_type,
            ])?;
        }
    }

    tx.commit()?;
    println!("  ✓ POIs inserted");

    println!("  Inserting {} addresses...", addresses.len());

    let tx = conn.unchecked_transaction()?;

    {
        let mut stmt = tx.prepare(
            "INSERT INTO addresses (id, housenumber, street, city, postcode, suburb, place, latitude, longitude, full_address)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
        )?;

        for addr in addresses {
            stmt.execute(params![
                addr.id,
                addr.housenumber,
                addr.street,
                addr.city,
                addr.postcode,
                addr.suburb,
                addr.place,
                addr.latitude,
                addr.longitude,
                addr.full_address,
            ])?;
        }
    }

    tx.commit()?;
    println!("  ✓ Addresses inserted");

    // optimizing the database
    conn.execute("ANALYZE", [])?;
    conn.execute("VACUUM", [])?;

    println!("✓ SQLite database created successfully");

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <pbf-file.osm.pbf>", args[0]);
        eprintln!("\nExample: {} ontario-latest.osm.pbf", args[0]);
        std::process::exit(1);
    }

    let pbf_path = &args[1];

    println!("{}", "=".repeat(80));
    println!("OSM PBF Fast Extractor (Rust) - Two-Pass Version");
    println!("{}", "=".repeat(80));
    println!("Input file: {}", pbf_path);
    println!();

    let start = Instant::now();
    let category_map = get_category_mapping();

    // pass 1: storing all the node coordinates
    println!("📖 PASS 1: Reading node coordinates...");
    let pass1_start = Instant::now();
    let mut node_coords: HashMap<i64, (f64, f64)> = HashMap::new();
    let reader = ElementReader::from_path(pbf_path)?;

    let mut count = 0;
    reader.for_each(|element| {
        match element {
            Element::Node(node) => {
                node_coords.insert(node.id(), (node.lat(), node.lon()));
            }
            Element::DenseNode(node) => {
                node_coords.insert(node.id(), (node.lat(), node.lon()));
            }
            _ => {}
        }
        count += 1;
        if count % 10_000_000 == 0 {
            println!("  Stored {}M node coordinates...", count / 1_000_000);
        }
    })?;

    println!(
        "✓ Pass 1 complete in {:.2?} - Stored {} node coordinates",
        pass1_start.elapsed(),
        node_coords.len()
    );
    println!();

    // pass 2: extracting pois and addresses
    println!("📖 PASS 2: Extracting POIs and addresses...");
    let pass2_start = Instant::now();
    let mut pois: Vec<PointOfInterest> = Vec::new();
    let mut addresses: Vec<Address> = Vec::new();

    let reader = ElementReader::from_path(pbf_path)?;
    let mut processed = 0;

    reader.for_each(|element| {
        match &element {
            Element::Node(node) => {
                let node_id = node.id();
                let lat = node.lat();
                let lon = node.lon();
                let tags: HashMap<String, String> = node
                    .tags()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();

                process_node_tags(
                    node_id,
                    lat,
                    lon,
                    tags,
                    &category_map,
                    &mut pois,
                    &mut addresses,
                );
            }
            Element::DenseNode(node) => {
                let node_id = node.id();
                let lat = node.lat();
                let lon = node.lon();
                let tags: HashMap<String, String> = node
                    .tags()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();

                process_node_tags(
                    node_id,
                    lat,
                    lon,
                    tags,
                    &category_map,
                    &mut pois,
                    &mut addresses,
                );
            }
            Element::Way(way) => {
                let tags: HashMap<String, String> = way
                    .tags()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();

                // checking for poi category
                let mut category: Option<String> = None;
                let mut subcategory: Option<String> = None;

                for (tag_key, value_map) in category_map.iter() {
                    if let Some(tag_value) = tags.get(tag_key) {
                        if let Some(cat) = value_map.get(tag_value) {
                            category = Some(cat.clone());
                            subcategory = Some(tag_value.clone());
                            break;
                        }
                    }
                }

                // extracting ways that have names and categories like georgian college
                if category.is_some() || tags.contains_key("name") {
                    let node_refs: Vec<i64> = way.refs().collect();

                    if !node_refs.is_empty() {
                        let mut lat_sum = 0.0;
                        let mut lon_sum = 0.0;
                        let mut valid_nodes = 0;

                        for node_id in &node_refs {
                            if let Some((lat, lon)) = node_coords.get(node_id) {
                                lat_sum += lat;
                                lon_sum += lon;
                                valid_nodes += 1;
                            }
                        }

                        if valid_nodes > 0 {
                            let centroid_lat = lat_sum / valid_nodes as f64;
                            let centroid_lon = lon_sum / valid_nodes as f64;

                            if let Some(cat) = category {
                                pois.push(PointOfInterest {
                                    id: way.id(),
                                    name: tags
                                        .get("name")
                                        .cloned()
                                        .unwrap_or_else(|| "Unnamed".to_string()),
                                    category: cat,
                                    subcategory: subcategory.unwrap_or_default(),
                                    latitude: centroid_lat,
                                    longitude: centroid_lon,
                                    housenumber: tags
                                        .get("addr:housenumber")
                                        .cloned()
                                        .unwrap_or_default(),
                                    city: tags.get("addr:city").cloned().unwrap_or_default(),
                                    street: tags.get("addr:street").cloned().unwrap_or_default(),
                                    osm_type: "way".to_string(),
                                });
                            }
                        }
                    }
                }
            }
            Element::Relation(_) => {
                //TODO not doing relations for now, thats a whole other can of worms for later
            }
        }

        processed += 1;
        if processed % 10_000_000 == 0 {
            println!(
                "  Processed {}M elements - Found {} POIs, {} addresses",
                processed / 1_000_000,
                pois.len(),
                addresses.len()
            );
        }
    })?;

    println!("✓ Pass 2 complete in {:.2?}", pass2_start.elapsed());
    println!();

    println!("📊 Final Results:");
    println!(
        "  POIs found: {} ({} from nodes, {} from ways)",
        pois.len(),
        pois.iter().filter(|p| p.osm_type == "node").count(),
        pois.iter().filter(|p| p.osm_type == "way").count()
    );
    println!("  Addresses found: {}", addresses.len());
    println!();

    // saving the files
    println!("💾 Saving results...");
    let poi_file = File::create("poi_data.json")?;
    let poi_output = PoiData {
        pois: pois.clone(), // cloning for json
    };
    serde_json::to_writer_pretty(BufWriter::new(poi_file), &poi_output)?;
    println!("✓ Saved {} POIs", pois.len());

    // saving addresses
    println!("💾 Saving addresses to address_data.json...");
    let addr_file = File::create("address_data.json")?;
    let addr_output = AddressData {
        addresses: addresses.clone(), // cloning for json
    };
    serde_json::to_writer_pretty(BufWriter::new(addr_file), &addr_output)?;
    println!("✓ Saved {} addresses", addresses.len());
    println!();

    // now we can use pois and addresses for the sqlite database
    export_to_sqlite(&pois, &addresses, "osm_data.db")
        .map_err(|e| format!("SQLite export failed: {}", e))?;

    let total_time = start.elapsed();
    println!("{}", "=".repeat(80));
    println!("✅ Complete! Total time: {:.2?}", total_time);
    println!("{}", "=".repeat(80));

    Ok(())
}
