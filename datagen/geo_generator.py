"""Geography reference data for realistic place names.

Provides countries, states/provinces, cities, and postal codes.
Real data for the top 10 cities per country; generated names thereafter.
"""

from .name_generator import ADJECTIVES, NOUNS

# Ordered by default frequency weighting (US heaviest)
COUNTRIES = [
    "United States", "Canada", "Mexico",
    "United Kingdom", "France", "Germany",
]

# States/provinces per country
STATES = {
    "United States": [
        "California", "Texas", "Florida", "New York", "Pennsylvania",
        "Illinois", "Ohio", "Georgia", "North Carolina", "Michigan",
        "New Jersey", "Virginia", "Washington", "Arizona", "Massachusetts",
        "Tennessee", "Indiana", "Missouri", "Maryland", "Wisconsin",
        "Colorado", "Minnesota", "South Carolina", "Alabama", "Louisiana",
        "Kentucky", "Oregon", "Oklahoma", "Connecticut", "Utah",
        "Iowa", "Nevada", "Arkansas", "Mississippi", "Kansas",
        "New Mexico", "Nebraska", "Idaho", "West Virginia", "Hawaii",
        "New Hampshire", "Maine", "Montana", "Rhode Island", "Delaware",
        "South Dakota", "North Dakota", "Alaska", "Vermont", "Wyoming",
    ],
    "Canada": [
        "Ontario", "Quebec", "British Columbia", "Alberta", "Manitoba",
        "Saskatchewan", "Nova Scotia", "New Brunswick",
        "Newfoundland and Labrador", "Prince Edward Island",
    ],
    "Mexico": [
        "Mexico City", "Jalisco", "Nuevo León", "Puebla", "Guanajuato",
        "Chihuahua", "Veracruz", "Michoacán", "Oaxaca", "Baja California",
    ],
    "United Kingdom": [
        "England", "Scotland", "Wales", "Northern Ireland",
    ],
    "France": [
        "Île-de-France", "Auvergne-Rhône-Alpes", "Nouvelle-Aquitaine",
        "Occitanie", "Hauts-de-France", "Provence-Alpes-Côte d'Azur",
        "Grand Est", "Pays de la Loire", "Bretagne", "Normandie",
    ],
    "Germany": [
        "Bavaria", "North Rhine-Westphalia", "Baden-Württemberg",
        "Lower Saxony", "Hesse", "Saxony", "Rhineland-Palatinate",
        "Berlin", "Schleswig-Holstein", "Brandenburg",
    ],
}

# Top 10 real cities per country with state and postal code
CITIES = {
    "United States": [
        ("New York", "New York", "10001"),
        ("Los Angeles", "California", "90001"),
        ("Chicago", "Illinois", "60601"),
        ("Houston", "Texas", "77001"),
        ("Phoenix", "Arizona", "85001"),
        ("Philadelphia", "Pennsylvania", "19101"),
        ("San Antonio", "Texas", "78201"),
        ("San Diego", "California", "92101"),
        ("Dallas", "Texas", "75201"),
        ("San Jose", "California", "95101"),
    ],
    "Canada": [
        ("Toronto", "Ontario", "M5A 1A1"),
        ("Montreal", "Quebec", "H2X 1Y4"),
        ("Vancouver", "British Columbia", "V5K 0A1"),
        ("Calgary", "Alberta", "T2P 1J9"),
        ("Edmonton", "Alberta", "T5J 0N3"),
        ("Ottawa", "Ontario", "K1A 0A6"),
        ("Winnipeg", "Manitoba", "R3C 4A5"),
        ("Quebec City", "Quebec", "G1R 4P5"),
        ("Hamilton", "Ontario", "L8P 4S6"),
        ("Kitchener", "Ontario", "N2G 4W1"),
    ],
    "Mexico": [
        ("Mexico City", "Mexico City", "06600"),
        ("Guadalajara", "Jalisco", "44100"),
        ("Monterrey", "Nuevo León", "64000"),
        ("Puebla", "Puebla", "72000"),
        ("Toluca", "Mexico City", "50000"),
        ("León", "Guanajuato", "37000"),
        ("Tijuana", "Baja California", "22000"),
        ("Ciudad Juárez", "Chihuahua", "32000"),
        ("Mérida", "Oaxaca", "97000"),
        ("Querétaro", "Guanajuato", "76000"),
    ],
    "United Kingdom": [
        ("London", "England", "EC1A 1BB"),
        ("Birmingham", "England", "B1 1BB"),
        ("Manchester", "England", "M1 1AE"),
        ("Leeds", "England", "LS1 1BA"),
        ("Glasgow", "Scotland", "G1 1AA"),
        ("Liverpool", "England", "L1 0AA"),
        ("Bristol", "England", "BS1 1AA"),
        ("Edinburgh", "Scotland", "EH1 1YZ"),
        ("Cardiff", "Wales", "CF10 1EP"),
        ("Belfast", "Northern Ireland", "BT1 5GS"),
    ],
    "France": [
        ("Paris", "Île-de-France", "75001"),
        ("Marseille", "Provence-Alpes-Côte d'Azur", "13001"),
        ("Lyon", "Auvergne-Rhône-Alpes", "69001"),
        ("Toulouse", "Occitanie", "31000"),
        ("Nice", "Provence-Alpes-Côte d'Azur", "06000"),
        ("Nantes", "Pays de la Loire", "44000"),
        ("Strasbourg", "Grand Est", "67000"),
        ("Montpellier", "Occitanie", "34000"),
        ("Bordeaux", "Nouvelle-Aquitaine", "33000"),
        ("Lille", "Hauts-de-France", "59000"),
    ],
    "Germany": [
        ("Berlin", "Berlin", "10115"),
        ("Hamburg", "Lower Saxony", "20095"),
        ("Munich", "Bavaria", "80331"),
        ("Cologne", "North Rhine-Westphalia", "50667"),
        ("Frankfurt", "Hesse", "60311"),
        ("Stuttgart", "Baden-Württemberg", "70173"),
        ("Düsseldorf", "North Rhine-Westphalia", "40213"),
        ("Leipzig", "Saxony", "04109"),
        ("Dortmund", "North Rhine-Westphalia", "44137"),
        ("Essen", "North Rhine-Westphalia", "45127"),
    ],
}


def _make_city_name(index, country_index):
    """Generate a plausible made-up city name from word lists."""
    adj = ADJECTIVES[index % len(ADJECTIVES)]
    noun = NOUNS[(index + country_index * 37) % len(NOUNS)]
    return f"{adj.capitalize()} {noun.capitalize()}"


def generate_country_pool(cardinality, seed=42):
    """Generate a pool of country names."""
    import numpy as np
    pool = list(COUNTRIES[:cardinality])
    if len(pool) < cardinality:
        # Repeat with numeric suffix for very high cardinality
        rng = np.random.default_rng(seed)
        while len(pool) < cardinality:
            pool.append(f"Country_{len(pool) + 1}")
    return pool


def generate_state_pool(cardinality, seed=42):
    """Generate a pool of state/province names across countries."""
    pool = []
    seen = set()
    # Interleave states from each country, US-heavy
    country_order = list(COUNTRIES)
    for country in country_order:
        for state in STATES.get(country, []):
            if state not in seen:
                pool.append(state)
                seen.add(state)
            if len(pool) >= cardinality:
                return pool[:cardinality]

    # Fill remainder with generated names
    idx = 0
    while len(pool) < cardinality:
        name = f"{ADJECTIVES[idx % len(ADJECTIVES)].capitalize()} Province"
        if name not in seen:
            pool.append(name)
            seen.add(name)
        idx += 1

    return pool[:cardinality]


def generate_city_pool(cardinality, seed=42):
    """Generate a pool of city names — real cities first, then generated."""
    pool = []
    seen = set()

    # Add real cities in country priority order
    for country in COUNTRIES:
        for city, state, postal in CITIES.get(country, []):
            if city not in seen:
                pool.append(city)
                seen.add(city)
            if len(pool) >= cardinality:
                return pool[:cardinality]

    # Generate additional city names
    idx = 0
    for ci, country in enumerate(COUNTRIES):
        while len(pool) < cardinality:
            name = _make_city_name(idx, ci)
            if name not in seen:
                pool.append(name)
                seen.add(name)
            idx += 1
            if idx > cardinality * 3:
                break
        if len(pool) >= cardinality:
            break

    # Last resort
    while len(pool) < cardinality:
        name = _make_city_name(len(pool), 0)
        if name not in seen:
            pool.append(name)
            seen.add(name)

    return pool[:cardinality]


def generate_postal_pool(cardinality, seed=42):
    """Generate a pool of postal codes — real ones first, then generated."""
    import numpy as np
    rng = np.random.default_rng(seed)

    pool = []
    seen = set()

    # Add real postal codes
    for country in COUNTRIES:
        for city, state, postal in CITIES.get(country, []):
            if postal not in seen:
                pool.append(postal)
                seen.add(postal)
            if len(pool) >= cardinality:
                return pool[:cardinality]

    # Generate US-style zip codes for the rest
    while len(pool) < cardinality:
        code = f"{rng.integers(10000, 99999)}"
        if code not in seen:
            pool.append(code)
            seen.add(code)

    return pool[:cardinality]


# Lookup tables for building consistent geo records
def get_city_record(city_name):
    """Look up (state, country, postal_code) for a real city, or return None."""
    for country, cities in CITIES.items():
        for city, state, postal in cities:
            if city == city_name:
                return {"state": state, "country": country, "postal_code": postal}
    return None
