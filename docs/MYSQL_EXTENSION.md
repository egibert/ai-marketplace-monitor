# Ollama + MySQL extension

When you use `provider = "ollama_mysql"`, the monitor will:

1. **Sales comps (Zillow)** – Query `sales` joined with `properties` using **beds, baths, and age (year_built)** from the listing. Location hierarchy: **zip first**, then **county**, then **region** if no results.
2. **FB comparison** – Query **fb_listings** for similar listings (title + asking_price).
3. **AI prompt** – Both datasets are injected into the Ollama prompt so it can judge if the listing is a good deal.
4. **Insert into fb_listings** – When a listing passes the rating threshold, it is inserted (or updated) in **fb_listings** (external_id, title, description, asking_price, url, city, state, zip, beds, baths, county_id, region_id when available).

---

## Quick setup

1. **Install a MySQL driver**

   ```bash
   pip install mysql-connector-python
   # or
   pip install PyMySQL
   ```

2. **Configure** in `~/.ai-marketplace-monitor/config.toml` (see full example below).

---

## Full example: sales comps + fb_listings + insert

Use your **Zillow** tables (`sales`, `properties`, `zip_county`, `counties`) for sold comps and **fb_listings** for similar FB listings. Accepted listings are written to **fb_listings**.

```toml
[marketplace.facebook]
search_city = "houston"

[ai.ollama_local]
provider = "ollama_mysql"
base_url = "http://localhost:11434/v1"
model = "llama3.2"

[ai.ollama_local.mysql]
host = "localhost"
user = "your_user"
password = "your_password"
database = "your_database"
enabled = true

# Sales comps: zip → county → region, filtered by beds/baths/year from listing
use_sales_comps = true
sales_max_rows = 10
sales_table = "sales"
properties_table = "properties"
zip_county_table = "zip_county"
counties_table = "counties"
year_tolerance = 5

# FB comparison: similar listings from fb_listings
comparison_table = "fb_listings"
title_column = "title"
price_column = "asking_price"
max_rows = 10

# Insert accepted listings into fb_listings
insert_into_fb = true
fb_listings_table = "fb_listings"

# When DB has no zip for city/state, call Nominatim to get zip (optional)
# geocode_fallback = true
# geocode_rate_limit_seconds = 1.0

output_format = "full"

[item.homes]
search_phrases = "3 bed 2 bath"
min_price = 200000
max_price = 500000
ai = ["ollama_local"]
# Only consider listings in Pennsylvania (ignore all others)
seller_locations = ["PA", "Pennsylvania"]

[user.me]
pushbullet_token = "your_token"
```

---

## How it works

### 1. Beds, baths, and age

Parsed from **title** and **description** (e.g. “3 bed”, “2 bath”, “built 1990” or “1995”). Used to filter sales comps and stored in **fb_listings** on insert when present.

### 2. Location: zip → county → region

- **listing.location** is parsed for a 5-digit **zip** (e.g. “Houston, TX 77001” or “77001”).
- **zip_county** gives **county_id**; **counties** gives **region_id**.
- Sales comps are queried in order: first by **zip**, then by **county**, then by **region** until results are found.
- **City/state → zip fallback:** When there’s no 5-digit zip in the listing text, the code looks up a zip from **city + state** using your **properties** table: it finds one row with matching `city` and `state` and uses that row’s `zip`. So if you have Zillow properties for "Freedom, PA", sales comps can still run. Ensure `properties` uses the same style as Facebook (e.g. state as "PA" not "Pennsylvania") for best matching.
- **Geocode API fallback:** If the DB still has no zip (e.g. no matching row in **properties**), you can enable **geocode_fallback** in `[ai.xxx.mysql]`. The code will call **OpenStreetMap Nominatim** with the listing’s city and state to get a US postcode. Results are cached and rate-limited (1 request per second by default). Set **geocode_rate_limit_seconds** (default 1.0) to comply with Nominatim’s usage policy.

### 3. Sales comps query

- `sales` JOIN `properties` on `property_id`.
- Filters: **beds**, **baths**, **year_built** (±`year_tolerance` years) when parsed from the listing.
- Location: `properties.zip`, then `properties.county_id`, then `properties.region_id` as above.
- Returns: sale_price, sale_date, beds, baths, square_feet, year_built, city, state, zip.

### 4. FB comparison

- Queries **fb_listings** by **title** (LIKE) and **asking_price** (≤ listing price × 1.5).
- Combined with sales comps in the same prompt block.

### 5. AI output: good deal vs Zillow and vs FB + listing URL

- The prompt asks the AI to state in its recommendation: **(1) Versus Zillow sold comps:** good deal / fair / overpriced (one sentence); **(2) Versus other Facebook listings:** good deal / fair / overpriced (one sentence).
- The **listing URL** (Facebook Marketplace link) is **always appended** to the AI comment so you can open it from the notification (e.g. PushBullet, email).

### 6. Insert into fb_listings

- **By default** inserts run only when a listing **passes the rating threshold** (and is about to be notified). If no listings pass (e.g. threshold 4 and all get 2–3), **fb_listings stays empty**.
- Set **insert_all_evaluated = true** in `[ai.xxx.mysql]` to insert **every evaluated listing** (regardless of score). Use this to populate fb_listings so you have data for comparison.
- The backend inserts (or updates by **external_id** = listing.id): title, description, asking_price, url, city, state, zip, beds, baths, county_id, region_id when available.
- Logs: `[MySQL] Inserting listing <id> into fb_listings...` and either `Inserted/updated fb_listing <id>` or `Insert fb_listing failed: <error>`. If you see no insert logs, either no listings passed the threshold (and insert_all_evaluated is false) or insert_into_fb is false.

---

## Schema reference (your tables)

| Table                | Purpose |
|----------------------|--------|
| **regions**          | Region names |
| **counties**         | county_name, region_id |
| **zip_county**       | zip → county_id |
| **properties**       | Zillow properties: beds, baths, year_built, zip, county_id, region_id |
| **sales**            | property_id, sale_price, sale_date |
| **fb_listings**      | external_id, title, description, asking_price, city, state, zip, beds, baths, county_id, region_id, url, posted_date |
| **fb_listing_history** | Price history per fb_listing |

---

## Config options summary

| Option                | Description |
|-----------------------|-------------|
| **use_sales_comps**   | Query sales + properties (zip→county→region) by beds/baths/year (default false). |
| **sales_max_rows**    | Max sold comps per scope (default 10). |
| **sales_table**, **properties_table**, **zip_county_table**, **counties_table** | Table names for sales comps. |
| **year_tolerance**    | ± years for year_built filter (default 5). |
| **comparison_table**  | Table for FB comparison (e.g. fb_listings). |
| **title_column**, **price_column** | Columns for FB comparison (e.g. title, asking_price). |
| **insert_into_fb**    | Insert listings into fb_listings (default false). |
| **insert_all_evaluated** | When true, insert every evaluated listing (not only accepted). Use to populate fb_listings (default false). |
| **fb_listings_table** | Table name for insert (default fb_listings). |
| **comparison_query**  | Custom SQL (placeholders {title}, {price}, {location}, {item_name}). Overrides built-in comparison when set. |
| **max_rows**          | Max rows for FB comparison (default 10). |
| **output_format**     | DB context in notifications: "full", "short", or "none". |
| **geocode_fallback**  | When true and DB has no zip for city/state, call Nominatim API to get zip (default false). |
| **geocode_rate_limit_seconds** | Seconds to wait after each Nominatim call (default 1.0). |

---

## Ignoring listings outside a state (e.g. PA only)

Use **seller_locations** on the item (or marketplace) so only listings whose location contains one of the strings are kept. To only consider Pennsylvania listings:

```toml
[item.homes]
seller_locations = ["PA", "Pennsylvania"]
```

Any listing whose `location` does not contain "PA" or "Pennsylvania" is skipped (excluded as "out of area"). You can set this at item level or in `[marketplace.facebook]` to apply to all items.

---

## Optional: custom query only

If you prefer a single custom query instead of built-in sales + fb comparison, set **comparison_query** and leave **use_sales_comps** false and **comparison_table** unset (or empty). Placeholders: `{title}`, `{price}`, `{location}`, `{item_name}`.
