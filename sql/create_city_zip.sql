-- Table for city/state -> zip lookup (used when listing has "City, ST" but no zip in text).
-- Populate via: python zip.py (reads uszips.csv and inserts into zip_county and city_zip).

CREATE TABLE IF NOT EXISTS city_zip (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(255) NOT NULL,
    state VARCHAR(10) NOT NULL,
    zip VARCHAR(5) NOT NULL,
    INDEX idx_city_state (city(100), state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Optional: if you don't have zip_county / counties yet, typical shapes are:
--
-- CREATE TABLE IF NOT EXISTS counties (
--     id INT AUTO_INCREMENT PRIMARY KEY,
--     county_name VARCHAR(255) NOT NULL,
--     region_id INT NULL,
--     UNIQUE KEY (county_name)
-- );
--
-- CREATE TABLE IF NOT EXISTS zip_county (
--     zip VARCHAR(5) NOT NULL,
--     county_id INT NOT NULL,
--     PRIMARY KEY (zip),
--     FOREIGN KEY (county_id) REFERENCES counties(id)
-- );
