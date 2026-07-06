import os
import json
import random
from datetime import datetime, timedelta

def generate_telemetry(base_path, num_devices=5, readings_per_device=50):
    os.makedirs(base_path, exist_ok=True)
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    telemetry_file = os.path.join(base_path, f"telemetry_{timestamp_str}.json")
    
    device_ids = [f"DEV-{1000 + i}" for i in range(num_devices)]
    readings = []
    
    start_time = datetime.now() - timedelta(hours=12)
    
    for device in device_ids:
        # Initial battery level
        battery = random.randint(10, 100)
        
        for r in range(readings_per_device):
            # Advance time by a few minutes per reading
            reading_time = start_time + timedelta(minutes=15 * r)
            # Simulated temperature in Fahrenheit (60F to 90F normal room temps)
            temp_f = round(random.uniform(60.0, 90.0), 1)
            # Humidity (30% to 70% normal range)
            humidity = round(random.uniform(30.0, 70.0), 1)
            
            # Slow battery drain
            battery = max(0, battery - random.choice([0, 0, 1]))
            
            record = {
                "device_id": device,
                "timestamp": reading_time.strftime("%Y-%m-%d %H:%M:%S"),
                "temperature_f": temp_f,
                "humidity": humidity,
                "battery_level": battery
            }
            readings.append(record)
            
    # Inject anomalous values (extreme outliers to test Silver cleaning logic)
    for _ in range(5):
        readings.append({
            "device_id": random.choice(device_ids),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature_f": 999.9, # Faulty sensor spike
            "humidity": 0.0,
            "battery_level": 50
        })
        readings.append({
            "device_id": random.choice(device_ids),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature_f": -500.0, # Faulty sensor drop
            "humidity": 100.0,
            "battery_level": 50
        })
        
    # Write to file (JSON lines format)
    with open(telemetry_file, "w") as f:
        for r in readings:
            f.write(json.dumps(r) + "\n")
            
    print(f"Generated IoT telemetry file: {telemetry_file}")

if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join("data", "project2", "raw")
    generate_telemetry(path)
