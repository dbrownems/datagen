"""Generate pools of unique values for each column based on distribution config.

Pools are generated on the driver and broadcast to Spark executors.
For columns with cardinality > MAX_POOL_SIZE, values are generated on-the-fly.
"""

import numpy as np
from datetime import datetime, timedelta

MAX_POOL_SIZE = 5_000_000  # Max pool entries to generate on driver

# Optional scipy for better skewed distributions
try:
    from scipy.stats import skewnorm as _skewnorm

    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False


def _generate_skewed_values(mean, std_dev, skewness, n, rng, min_val=None, max_val=None):
    """Generate n values with a skewed normal distribution.

    Uses scipy.stats.skewnorm if available, otherwise falls back to
    a log-normal approximation for positive skewness.
    """
    if n <= 0:
        return np.array([])

    if HAS_SCIPY and abs(skewness) > 0.1:
        values = _skewnorm.rvs(
            skewness, loc=mean, scale=max(std_dev, 1e-10),
            size=n, random_state=rng,
        )
    else:
        if abs(skewness) > 0.1 and mean > 0 and std_dev > 0:
            # Log-normal approximation for positive skew
            if skewness > 0:
                sigma = min(abs(skewness) * 0.3, 2.0)
                mu = np.log(max(mean, 1e-10)) - sigma ** 2 / 2
                values = rng.lognormal(mu, sigma, n)
            else:
                sigma = min(abs(skewness) * 0.3, 2.0)
                mu = np.log(max(mean, 1e-10)) - sigma ** 2 / 2
                values = -(rng.lognormal(mu, sigma, n) - 2 * mean)
        else:
            values = rng.normal(mean, max(std_dev, 1e-10), n)

    if min_val is not None:
        values = np.clip(values, min_val, None)
    if max_val is not None:
        values = np.clip(values, None, max_val)

    return values


def generate_int64_pool(dist, cardinality, seed=42):
    """Generate a pool of unique integer values."""
    rng = np.random.default_rng(seed)

    mean = dist.mean if dist.mean is not None else 0
    std_dev = dist.std_dev if dist.std_dev is not None else max(cardinality / 3, 1)
    skewness = dist.skewness if dist.skewness is not None else 0.0
    min_val = dist.min
    max_val = dist.max

    # Generate more than needed to ensure uniqueness after rounding
    oversample = min(cardinality * 3, cardinality + 1_000_000)
    raw = _generate_skewed_values(mean, std_dev, skewness, oversample, rng, min_val, max_val)
    raw = np.round(raw).astype(np.int64)

    # Ensure uniqueness
    unique = np.unique(raw)

    if len(unique) >= cardinality:
        # Sub-sample to exact cardinality
        chosen = rng.choice(unique, size=cardinality, replace=False)
    else:
        # Not enough unique values from distribution; fill with sequential values
        chosen = list(unique)
        if min_val is not None and max_val is not None:
            fill_range = np.arange(int(min_val), int(max_val) + 1)
        else:
            center = int(mean)
            half = cardinality
            fill_range = np.arange(center - half, center + half + 1)

        existing = set(chosen)
        for v in fill_range:
            if len(chosen) >= cardinality:
                break
            if v not in existing:
                chosen.append(v)
                existing.add(v)

        # Last resort: extend beyond range
        counter = (int(max_val) + 1) if max_val is not None else (int(mean) + cardinality)
        while len(chosen) < cardinality:
            if counter not in existing:
                chosen.append(counter)
                existing.add(counter)
            counter += 1

        chosen = np.array(chosen[:cardinality], dtype=np.int64)

    chosen.sort()
    return chosen.tolist()


def generate_double_pool(dist, cardinality, seed=42):
    """Generate a pool of unique float values."""
    rng = np.random.default_rng(seed)

    mean = dist.mean if dist.mean is not None else 0.0
    std_dev = dist.std_dev if dist.std_dev is not None else 1.0
    skewness = dist.skewness if dist.skewness is not None else 0.0
    min_val = dist.min
    max_val = dist.max

    values = _generate_skewed_values(mean, std_dev, skewness, cardinality, rng, min_val, max_val)

    # Floats are almost always unique; round to 2 decimal places for realism
    values = np.round(values, 2)

    # Ensure uniqueness by adding tiny noise to duplicates
    seen = set()
    result = []
    for v in values:
        v = float(v)
        while v in seen:
            v += 0.01
        seen.add(v)
        result.append(v)

    result.sort()
    return result


def generate_datetime_pool(dist, cardinality, seed=42):
    """Generate a pool of unique datetime values."""
    rng = np.random.default_rng(seed)

    start_str = dist.start or "2020-01-01"
    end_str = dist.end or "2024-12-31"

    start_dt = datetime.strptime(start_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_str, "%Y-%m-%d")

    total_days = max(1, (end_dt - start_dt).days)

    if cardinality <= total_days:
        # Unique dates: sample from the range
        day_offsets = rng.choice(total_days + 1, size=cardinality, replace=False)
    else:
        # Need more granularity: add time component
        total_seconds = total_days * 86400
        second_offsets = rng.choice(
            min(total_seconds, cardinality * 2),
            size=cardinality,
            replace=False,
        )
        day_offsets = second_offsets / 86400.0

    day_offsets = np.sort(day_offsets)
    dates = []
    for offset in day_offsets:
        dt = start_dt + timedelta(days=float(offset))
        dates.append(dt.strftime("%Y-%m-%d %H:%M:%S"))

    return dates


def generate_boolean_pool(dist, cardinality, seed=42):
    """Generate a pool of boolean values."""
    if cardinality <= 1:
        true_ratio = dist.true_ratio if dist.true_ratio is not None else 0.5
        return [true_ratio >= 0.5]
    return [True, False]


# ---------------------------------------------------------------------------
# Key-specific pool generators
# ---------------------------------------------------------------------------

def generate_sequential_int_pool(cardinality, start=1):
    """Generate a pool of sequential integers: [start, start+1, ...]."""
    return list(range(start, start + cardinality))


def generate_prefixed_string_pool(cardinality, prefix="ID", seed=42):
    """Generate a pool of prefixed sequential strings: PREFIX-001, PREFIX-002, ..."""
    width = max(1, len(str(cardinality)))
    return [f"{prefix}-{str(i).zfill(width)}" for i in range(1, cardinality + 1)]


def generate_guid_pool(cardinality, seed=42):
    """Generate a pool of unique UUID v4 strings."""
    import uuid as _uuid

    rng = np.random.default_rng(seed)
    guids = []
    seen = set()
    while len(guids) < cardinality:
        random_bytes = bytes(rng.integers(0, 256, size=16, dtype=np.uint8))
        g = str(_uuid.UUID(bytes=random_bytes, version=4))
        if g not in seen:
            seen.add(g)
            guids.append(g)
    return guids


def generate_string_pool(dist, cardinality, seed=42):
    """Generate a pool of unique human-readable strings."""
    from .name_generator import generate_names

    avg_length = dist.avg_length or 12
    style = dist.style or "docker"

    if style == "docker":
        return generate_names(cardinality, avg_length=avg_length, seed=seed)
    elif style == "hex":
        rng = np.random.default_rng(seed)
        hex_len = max(4, avg_length)
        values = set()
        while len(values) < cardinality:
            batch = rng.integers(0, 16 ** hex_len, size=cardinality - len(values))
            for v in batch:
                values.add(format(int(v), f"0{hex_len}x"))
                if len(values) >= cardinality:
                    break
        return sorted(list(values))[:cardinality]
    elif style == "alpha":
        rng = np.random.default_rng(seed)
        chars = "abcdefghijklmnopqrstuvwxyz"
        str_len = max(3, avg_length)
        values = set()
        while len(values) < cardinality:
            word = "".join(rng.choice(list(chars), size=str_len))
            values.add(word)
        return sorted(list(values))[:cardinality]
    else:
        return generate_names(cardinality, avg_length=avg_length, seed=seed)


def generate_value_pool(col_config, global_seed=42):
    """Generate a pool of unique values for a column.

    Args:
        col_config: ColumnConfig instance (or dict with same fields).
        global_seed: Base seed for reproducibility.

    Returns:
        List of unique values for the column.
    """
    # Support both dataclass and dict access
    if hasattr(col_config, "name"):
        name = col_config.name
        data_type = col_config.data_type
        cardinality = col_config.cardinality
        dist = col_config.distribution
        is_key = col_config.is_key
        key_style = col_config.key_style
    else:
        name = col_config["name"]
        data_type = col_config["data_type"]
        cardinality = col_config["cardinality"]
        dist_data = col_config.get("distribution", {})
        from .config import DistributionConfig
        dist = DistributionConfig(**dist_data) if isinstance(dist_data, dict) else dist_data
        is_key = col_config.get("is_key", False)
        key_style = col_config.get("key_style")

    cardinality = max(1, cardinality)

    # Per-column seed for deterministic but independent generation
    col_seed = (global_seed + hash(name)) & 0x7FFFFFFF

    # Key columns get specialised pool generators
    if is_key and key_style:
        if key_style == "sequential":
            start = int(dist.min) if dist.min is not None else 1
            return generate_sequential_int_pool(cardinality, start=start)
        if key_style == "prefixed":
            prefix = dist.prefix or name[:4].upper()
            return generate_prefixed_string_pool(cardinality, prefix=prefix, seed=col_seed)
        if key_style == "guid":
            return generate_guid_pool(cardinality, seed=col_seed)

    # Regular columns
    if data_type == "int64":
        return generate_int64_pool(dist, cardinality, col_seed)
    elif data_type == "double":
        return generate_double_pool(dist, cardinality, col_seed)
    elif data_type == "datetime":
        return generate_datetime_pool(dist, cardinality, col_seed)
    elif data_type == "boolean":
        return generate_boolean_pool(dist, cardinality, col_seed)
    elif data_type == "string":
        return generate_string_pool(dist, cardinality, col_seed)
    else:
        # Default to string
        return generate_string_pool(dist, cardinality, col_seed)
