# ==========================================
# Fabric RTI Fleet Operations Project
# ==========================================

import json
import random
import time
import uuid
import os
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData

CITIES = [
    {"city": "Hyderabad",      "code": "HYD"},
    {"city": "Delhi",          "code": "DEL"},
    {"city": "Chandigarh",     "code": "CHD"},
    {"city": "Mumbai",         "code": "MUM"},
    {"city": "Bengaluru",      "code": "BLR"},
    {"city": "Chennai",        "code": "CHE"},
    {"city": "Kolkata",        "code": "KOL"},
    {"city": "Ahmedabad",      "code": "AMD"},
    {"city": "Pune",           "code": "PUN"},
    {"city": "Jaipur",         "code": "JAI"},
    {"city": "Surat",          "code": "SUR"},
    {"city": "Lucknow",        "code": "LKO"},
    {"city": "Kanpur",         "code": "KAN"},
    {"city": "Nagpur",         "code": "NAG"},
    {"city": "Indore",         "code": "IND"},
    {"city": "Bhopal",         "code": "BHO"},
    {"city": "Patna",          "code": "PAT"},
    {"city": "Vadodara",       "code": "VAD"},
    {"city": "Ludhiana",       "code": "LDH"},
    {"city": "Agra",           "code": "AGR"},
    {"city": "Nashik",         "code": "NAS"},
    {"city": "Meerut",         "code": "MEE"},
    {"city": "Rajkot",         "code": "RAJ"},
    {"city": "Varanasi",       "code": "VAR"},
    {"city": "Srinagar",       "code": "SRI"},
    {"city": "Amritsar",       "code": "AMR"},
    {"city": "Jodhpur",        "code": "JOD"},
    {"city": "Raipur",         "code": "RAI"},
    {"city": "Ranchi",         "code": "RAN"},
    {"city": "Guwahati",       "code": "GUW"},
    {"city": "Bhubaneswar",    "code": "BHU"},
    {"city": "Visakhapatnam",  "code": "VIZ"},
    {"city": "Vijayawada",     "code": "VIJ"},
    {"city": "Coimbatore",     "code": "COI"},
    {"city": "Madurai",        "code": "MAD"},
    {"city": "Kochi",          "code": "KOC"},
    {"city": "Mysuru",         "code": "MYS"},
    {"city": "Thiruvananthapuram", "code": "TVM"},
    {"city": "Jamshedpur",     "code": "JAM"},
    {"city": "Dhanbad",        "code": "DHA"},
    {"city": "Prayagraj",      "code": "PRY"},
    {"city": "Jabalpur",       "code": "JAB"},
    {"city": "Gwalior",        "code": "GWA"},
    {"city": "Kota",           "code": "KOT"},
    {"city": "Dehradun",       "code": "DED"},
    {"city": "Noida",          "code": "NOI"},
    {"city": "Gurugram",       "code": "GUR"},
    {"city": "Faridabad",      "code": "FAR"},
    {"city": "Aurangabad",     "code": "AUR"},
    {"city": "Tiruchirappalli","code": "TRI"},
]


VEHICLES = []

vehicle_number = 1

for city in CITIES:

    for truck_in_city in range(1, 21):

        route_number = ((truck_in_city - 1) % 5) + 1

        vehicle = {
            "vehicleId": f"TRUCK-{vehicle_number:04}",
            "driverId": f"DRV-{vehicle_number:04}",
            "city": city["city"],
            "cityCode": city["code"],
            "routeId": f"{city['code']}-R{route_number:03}",
            "deliveryId": f"DEL-{10000 + vehicle_number}"
        }

        VEHICLES.append(vehicle)

        vehicle_number += 1

def initialize_vehicle_state(vehicle):
    return {
        **vehicle,
        "speedKmph": round(random.uniform(0, 120), 1),
        "engineTemperatureC": round(random.uniform(75, 95), 1),
        "fuelLevelPct": round(random.uniform(40, 100), 1),
        "batteryVoltage": round(random.uniform(12.2, 14.4), 2),
        "vehicleStatus": random.choice(
            ["IN_TRANSIT", "IDLE", "AT_DELIVERY"]
            )
        }

FLEET_STATE = [
    initialize_vehicle_state(vehicle)
    for vehicle in VEHICLES
]

CONNECTION_STRING = os.getenv("FABRIC_EVENTSTREAM_CONNECTION_STRING")

def create_eventstream_producer():
    if not CONNECTION_STRING:
        raise RuntimeError(
            "FABRIC_EVENTSTREAM_CONNECTION_STRING environment variable is not configured."
        )

    return EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING
    )

def send_event_to_fabric(producer, event):
    event_batch = producer.create_batch()
    event_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_batch)

def update_vehicle_state(vehicle):
    status = vehicle["vehicleStatus"]

    # Occasionally change the operational status
    if random.random() < 0.05:
        status = random.choice(["IN_TRANSIT", "IDLE", "AT_DELIVERY"])
        vehicle["vehicleStatus"] = status

    # Speed should correspond to the operational status
    if status == "IN_TRANSIT":
        speed_change = random.uniform(-8, 8)
        vehicle["speedKmph"] = round(
            max(10, min(120, vehicle["speedKmph"] + speed_change)),
            1
        )

    elif status == "IDLE":
        vehicle["speedKmph"] = 0.0

    elif status == "AT_DELIVERY":
        vehicle["speedKmph"] = 0.0

    # Engine temperature gradually changes
    temperature_change = random.uniform(-1.5, 1.5)

    if status == "IN_TRANSIT":
        temperature_change += 0.3

    vehicle["engineTemperatureC"] = round(
        max(
            70,
            min(
                110,
                vehicle["engineTemperatureC"] + temperature_change
            )
        ),
        1
    )

    # Fuel decreases only while the truck is moving
    if status == "IN_TRANSIT":
        vehicle["fuelLevelPct"] = round(
            max(
                0,
                vehicle["fuelLevelPct"] - random.uniform(0.01, 0.08)
            ),
            2
        )

    # Battery voltage fluctuates slightly
    vehicle["batteryVoltage"] = round(
        max(
            11.5,
            min(
                14.5,
                vehicle["batteryVoltage"] + random.uniform(-0.05, 0.05)
            )
        ),
        2
    )

    return vehicle

def inject_abnormal_condition(vehicle):
    """
    Occasionally inject an abnormal telemetry condition.
    Detection and alerting will be handled later in Fabric RTI.
    """

    # Most telemetry remains normal
    if random.random() >= 0.05:
        return vehicle

    scenario = random.choice([
        "SPEEDING",
        "ENGINE_OVERHEATING",
        "LOW_FUEL",
        "LOW_BATTERY"
    ])

    if scenario == "SPEEDING":
        vehicle["vehicleStatus"] = "IN_TRANSIT"
        vehicle["speedKmph"] = round(random.uniform(105, 120), 1)

    elif scenario == "ENGINE_OVERHEATING":
        vehicle["engineTemperatureC"] = round(
            random.uniform(105, 120), 1
        )

    elif scenario == "LOW_FUEL":
        vehicle["fuelLevelPct"] = round(
            random.uniform(2, 10), 1
        )

    elif scenario == "LOW_BATTERY":
        vehicle["batteryVoltage"] = round(
            random.uniform(10.5, 11.5), 2
        )

    return vehicle

def create_telemetry_event(vehicle):
    event = {
        "eventId": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "vehicleId": vehicle["vehicleId"],
        "driverId": vehicle["driverId"],
        "city": vehicle["city"],
        "cityCode": vehicle["cityCode"],
        "routeId": vehicle["routeId"],
        "deliveryId": vehicle["deliveryId"],
        "speedKmph": vehicle["speedKmph"],
        "engineTemperatureC": vehicle["engineTemperatureC"],
        "fuelLevelPct": vehicle["fuelLevelPct"],
        "batteryVoltage": vehicle["batteryVoltage"],
        "vehicleStatus": vehicle["vehicleStatus"]
    }

    return event

def run_simulator(batch_size=20, interval_seconds=2):
    print("Fleet telemetry simulator starting...")
    print(f"Total fleet size: {len(FLEET_STATE)}")
    print(f"Events per cycle: {batch_size}")
    print(f"Cycle interval: {interval_seconds} seconds")
    print("Destination: Microsoft Fabric Eventstream")
    print("Press Ctrl+C to stop.\n")

    producer = create_eventstream_producer()

    try:
        while True:
            selected_vehicles = random.sample(FLEET_STATE, batch_size)

            for vehicle in selected_vehicles:
                update_vehicle_state(vehicle)
                inject_abnormal_condition(vehicle)

                event = create_telemetry_event(vehicle)

                send_event_to_fabric(producer, event)

                print(
                    f"SENT | {event['vehicleId']} | "
                    f"{event['city']} | "
                    f"Speed={event['speedKmph']} | "
                    f"Temp={event['engineTemperatureC']} | "
                    f"Fuel={event['fuelLevelPct']} | "
                    f"Status={event['vehicleStatus']}"
                )

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\nFleet telemetry simulator stopped.")

    finally:
        producer.close()

if __name__ == "__main__":
    print(f"Total Cities: {len(CITIES)}")
    print(f"Total Vehicles: {len(VEHICLES)}")
    print(f"Vehicles Initialized: {len(FLEET_STATE)}")

    run_simulator()