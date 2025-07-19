-- Join vehicles with events and zones
SELECT e.event_id, v.plate_number, z.zone_name, e.entry_time, e.exit_time, e.paid_amount
FROM parking_events e
JOIN vehicles v ON e.vehicle_id = v.vehicle_id
JOIN parking_zones z ON e.zone_id = z.zone_id;

-- Top 3 revenue-generating zones
SELECT z.zone_name, SUM(e.paid_amount) AS total_revenue
FROM parking_events e
JOIN parking_zones z ON e.zone_id = z.zone_id
GROUP BY z.zone_name
ORDER BY total_revenue DESC
LIMIT 3;

-- Frequent parkers (vehicles with most visits)
SELECT v.plate_number, COUNT(*) AS visit_count
FROM parking_events e
JOIN vehicles v ON e.vehicle_id = v.vehicle_id
GROUP BY v.plate_number
ORDER BY visit_count DESC;

-- Window function to rank visit durations
SELECT event_id, vehicle_id, entry_time, exit_time,
       TIMESTAMPDIFF(MINUTE, entry_time, exit_time) / 60 AS duration_hr,
       RANK() OVER (ORDER BY TIMESTAMPDIFF(MINUTE, entry_time, exit_time) DESC) AS duration_rank
FROM parking_events;

-- 
-- Trigger: Validate exit > entry
-- 
DELIMITER $$

CREATE TRIGGER validate_exit_after_entry
BEFORE INSERT ON parking_events
FOR EACH ROW
BEGIN
    IF NEW.exit_time < NEW.entry_time THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Exit time must be after entry time';
    END IF;
END$$

DELIMITER ;

-- 
-- Stored Procedure: Adjust paid_amount
-- 
DELIMITER $$

CREATE PROCEDURE adjust_paid_amount()
BEGIN
    UPDATE parking_events e
    JOIN parking_zones z ON e.zone_id = z.zone_id
    SET e.paid_amount = ROUND(TIMESTAMPDIFF(MINUTE, e.entry_time, e.exit_time) / 60 * z.rate_per_hour, 2)
    WHERE e.paid_amount < 0;
END$parking_events$

DELIMITER ;