CREATE DATABASE IF NOT EXISTS case_study;
USE case_study;

-- Drop existing tables if needed
DROP TABLE IF EXISTS parking_events;
DROP TABLE IF EXISTS parking_zones;
DROP TABLE IF EXISTS vehicles;

-- Create vehicles table
CREATE TABLE vehicles (
    vehicle_id VARCHAR(10) PRIMARY KEY,
    plate_number VARCHAR(20) NOT NULL,
    type VARCHAR(20),
    owner_name VARCHAR(100)
);

-- Create parking_zones table
CREATE TABLE parking_zones (
    zone_id VARCHAR(10) PRIMARY KEY,
    zone_name VARCHAR(50),
    rate_per_hour DECIMAL(5,2),
    is_valet BOOLEAN
);

-- Create parking_events table
CREATE TABLE parking_events (
    event_id VARCHAR(10) PRIMARY KEY,
    vehicle_id VARCHAR(10),
    zone_id VARCHAR(10),
    entry_time DATETIME,
    exit_time DATETIME,
    paid_amount DECIMAL(7,2),
    FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id),
    FOREIGN KEY (zone_id) REFERENCES parking_zones(zone_id)
);

-- Insert sample vehicles
INSERT INTO vehicles (vehicle_id, plate_number, type, owner_name) VALUES
('V001', 'MH12AB1234', 'sedan', 'Rahul Sharma'),
('V002', 'MH14XY9876', 'SUV', 'Neha Verma'),
('V003', 'DL01CD4567', 'hatchback', 'Aamir Sheikh'),
('V004', 'KA03ZX7788', 'SUV', 'Sneha Kulkarni'),
('V005', 'TN09QW1100', 'sedan', 'Arun Raj'),
('V006', 'MH12KL9988', 'EV', 'Manisha Pandey'),
('V007', 'GJ05HH2299', 'SUV', 'Rakesh Singh');

-- Insert sample zones
INSERT INTO parking_zones (zone_id, zone_name, rate_per_hour, is_valet) VALUES
('Z001', 'Short Term', 50, FALSE),
('Z002', 'Short Term', 40, FALSE),
('Z003', 'Long Term', 30, FALSE),
('Z004', 'Valet A', 70, TRUE),
('Z005', 'Economy lot B', 25, FALSE);

-- Insert sample parking events
INSERT INTO parking_events (event_id, vehicle_id, zone_id, entry_time, exit_time, paid_amount) VALUES
('E001', 'V001', 'Z001', '2024-07-18 08:00:00', '2024-07-18 10:30:00', 120),
('E002', 'V002', 'Z002', '2024-07-18 09:00:00', '2024-07-18 11:00:00', 80),
('E003', 'V003', 'Z004', '2024-07-18 12:00:00', '2024-07-18 12:45:00', 70),
('E004', 'V001', 'Z003', '2024-07-17 15:00:00', '2024-07-18 15:00:00', 300),
('E005', 'V004', 'Z005', '2024-07-16 07:00:00', '2024-07-16 10:00:00', 75),
('E006', 'V005', 'Z003', '2024-07-15 18:00:00', '2024-07-15 18:30:00', 15),
('E007', 'V002', 'Z001', '2024-07-14 08:00:00', '2024-07-14 09:00:00', 50),
('E008', 'V006', 'Z004', '2024-07-18 10:15:00', '2024-07-18 11:15:00', 70),
('E009', 'V007', 'Z001', '2024-07-18 07:30:00', '2024-07-18 08:00:00', 25),
('E010', 'V006', 'Z005', '2024-07-17 06:00:00', '2024-07-17 08:00:00', 50),
('E012', 'V005', 'Z001', '2024-07-18 01:00:00', '2024-07-18 12:00:00', 20),
('E013', 'V002', 'Z003', '2024-07-18 14:00:00', '2024-07-18 16:00:00', 60);