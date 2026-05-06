import os
import time
import threading
import logging
import math

from flask import Flask, jsonify, request
import mygeotab
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Cache ---
_cache = {
    "data": None,
    "timestamp": 0,
}
_cache_lock = threading.Lock()
CACHE_TTL = 60  # seconds


def get_geotab_client():
    username = os.environ["GEOTAB_USERNAME"]
    password = os.environ["GEOTAB_PASSWORD"]
    database = os.environ["GEOTAB_DATABASE"]
    server = os.environ.get("GEOTAB_SERVER", "my.geotab.com")

    api = mygeotab.API(
        username=username,
        password=password,
        database=database,
        server=server,
    )
    api.authenticate()
    return api


def reverse_geocode(lat, lng, geolocator):
    try:
        location = geolocator.reverse(
            (lat, lng),
            exactly_one=True,
            timeout=5,
            language="en",
        )
        if location is None:
            return None

        addr = location.raw.get("address", {})

        parts = []
        neighborhood = (
            addr.get("neighbourhood")
            or addr.get("suburb")
            or addr.get("quarter")
            or addr.get("hamlet")
            or addr.get("village")
        )
        city = (
            addr.get("city")
            or addr.get("town")
            or addr.get("county")
        )
        state = addr.get("state")

        if neighborhood:
            parts.append(neighborhood)
        if city:
            parts.append(city)
        elif state:
            parts.append(state)

        return ", ".join(parts) if parts else location.address

    except (GeocoderTimedOut, GeocoderServiceError) as e:
        logger.warning("Geocoding error for (%s, %s): %s", lat, lng, e)
        return None


def fetch_truck_locations():
    try:
        api = get_geotab_client()
    except KeyError as e:
        raise RuntimeError(f"Missing environment variable: {e}") from e
    except mygeotab.exceptions.AuthenticationException as e:
        raise RuntimeError(f"Geotab authentication failed: {e}") from e

    geolocator = Nominatim(user_agent="mistersoftee-truck-locator/1.0")

    statuses = api.get("DeviceStatusInfo")
    devices = api.get("Device", resultsLimit=500)
    device_names = {d["id"]: d.get("name", "Unknown") for d in devices}

    trucks = []
    for status in statuses:
        lat = status.get("latitude")
        lng = status.get("longitude")

        if not lat or not lng:
            continue
        if lat == 0.0 and lng == 0.0:
            continue

        device_id = status.get("device", {}).get("id") or status.get("id")
        name = device_names.get(device_id, "Unknown Truck")

        speed = status.get("speed", 0) or 0
        moving = speed > 0

        neighborhood = reverse_geocode(lat, lng, geolocator)

        trucks.append({
            "name": name,
            "neighborhood": neighborhood or "Location unavailable",
            "lat": round(lat, 6),
            "lng": round(lng, 6),
            "moving": moving,
            "speed_mph": round(speed, 1),
        })

    trucks.sort(key=lambda t: t["name"])
    return trucks


def get_cached_locations():
    now = time.time()
    with _cache_lock:
        if _cache["data"] is not None and (now - _cache["timestamp"]) < CACHE_TTL:
            logger.info("Returning cached truck locations")
            return _cache["data"], None

    try:
        trucks = fetch_truck_locations()
        result = {"trucks": trucks, "cached": False, "truck_count": len(trucks)}
    except RuntimeError as e:
        return None, str(e)
    except Exception as e:
        logger.exception("Unexpected error fetching truck locations")
        return None, f"Unexpected error: {e}"

    with _cache_lock:
        _cache["data"] = result
        _cache["timestamp"] = time.time()
        result = dict(result)
        result["cached"] = False

    return result, None


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def forward_geocode(location_text):
    geolocator = Nominatim(user_agent="mistersoftee-truck-locator/1.0")
    query = location_text.strip()
    if "CA" not in query.upper() and "CALIFORNIA" not in query.upper():
        query = f"{query}, CA"
    try:
        loc = geolocator.geocode(
            query,
            timeout=5,
            viewbox=[(38.5, -123.0), (36.9, -121.0)],
            bounded=False,
        )
        if loc is None:
            return None
        return (loc.latitude, loc.longitude)
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        logger.warning("Forward geocoding error for %r: %s", location_text, e)
        return None


@app.route("/nearest-truck", methods=["POST"])
def nearest_truck():
    body = request.get_json(force=True, silent=True) or {}

    location_text = None
    args = body.get("args")
    if isinstance(args, list) and len(args) > 0 and isinstance(args[0], dict):
        location_text = args[0].get("location")
    if not location_text:
        location_text = body.get("location")

    if not location_text:
        return jsonify({"error": "Please provide a location (neighborhood, city, or address)."}), 400

    coords = forward_geocode(location_text)
    if coords is None:
        return jsonify({"error": f"Sorry, I couldn't find a location matching '{location_text}'. Could you try a more specific address or neighborhood?"}), 400

    caller_lat, caller_lng = coords

    data, error = get_cached_locations()
    if error:
        return jsonify({"error": f"Could not fetch truck locations: {error}"}), 500

    trucks = data.get("trucks", [])
    if not trucks:
        return jsonify({"error": "No trucks are currently online."}), 500

    ranked = []
    for t in trucks:
        dist = haversine_distance(caller_lat, caller_lng, t["lat"], t["lng"])
        ranked.append({"truck": t, "distance": dist})
    ranked.sort(key=lambda x: x["distance"])

    def _fmt(entry):
        return {
            "name": entry["truck"]["name"],
            "neighborhood": entry["truck"]["neighborhood"],
            "distance_miles": round(entry["distance"], 1),
            "moving": entry["truck"]["moving"],
        }

    result = {
        "closest_truck": _fmt(ranked[0]),
        "alternatives": [_fmt(r) for r in ranked[1:3]],
        "caller_location_resolved": location_text,
    }
    return jsonify(result), 200


@app.route("/truck-locations", methods=["GET"])
def truck_locations():
    data, error = get_cached_locations()
    if error:
        logger.error("Error fetching truck locations: %s", error)
        return jsonify({"error": error}), 500
    return jsonify(data), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


def _cache_warmer():
    """Background thread that pre-warms the truck location cache."""
    # Delay first run to let Flask start cleanly
    time.sleep(10)
    while True:
        try:
            logger.info("Cache warmer: refreshing truck locations...")
            trucks = fetch_truck_locations()
            result = {"trucks": trucks, "cached": True, "truck_count": len(trucks)}
            with _cache_lock:
                _cache["data"] = result
                _cache["timestamp"] = time.time()
            logger.info("Cache warmer: refreshed %d trucks.", len(trucks))
        except BaseException as e:
            logger.warning("Cache warmer: failed to refresh — %s", e)
        time.sleep(120)


def start_cache_warmer():
    t = threading.Thread(target=_cache_warmer, daemon=True)
    t.start()
    logger.info("Cache warmer thread started.")


if __name__ == "__main__":
    start_cache_warmer()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
