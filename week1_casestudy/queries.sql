use casestudy;

select * from parking_events;

select * from parking_zones;

select * from vehicles;

-- Identify top-performing zones by revenue or usage.

SELECT 
    pe.zone_id,
    pz.zone_name,
    SUM(pe.paid_amount) AS revenue
FROM parking_events pe
JOIN parking_zones pz ON pe.zone_id = pz.zone_id
GROUP BY pe.zone_id, pz.zone_name
ORDER BY revenue DESC;




-- List vehicles with the highest number of visits or longest durations.

-- highest no of visits
SELECT 
    pe.vehicle_id,
    v.plate_number,
    v.owner_name,
    COUNT(*) AS total_visits
FROM parking_events pe
JOIN vehicles v ON pe.vehicle_id = v.vehicle_id
GROUP BY pe.vehicle_id, v.plate_number, v.owner_name
ORDER BY total_visits DESC;



-- longest duration
SELECT 
    vehicle_id,
    SUM(TIMESTAMPDIFF(SECOND, entry_time, exit_time) / 3600) AS total_hours
FROM parking_events
WHERE exit_time IS NOT NULL
GROUP BY vehicle_id
ORDER BY total_hours DESC;


-- Compare zones (e.g., short-term vs valet) by occupancy and revenue.

select pe.zone_id,pz.zone_name,count(*) as no_of_visits,sum(pe.paid_amount) as revenue   
from parking_events pe
inner join
parking_zones pz
on pe.zone_id=pz.zone_id
group by pe.zone_id;