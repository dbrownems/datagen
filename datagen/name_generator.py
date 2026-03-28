"""Docker-style human-readable name generator for string columns.

Generates names like 'bold_raven', 'crimson_meadow_ancient', etc.
Supports controlling average string length and guaranteeing uniqueness.
"""

import numpy as np

ADJECTIVES = [
    # 3-4 chars
    "red", "big", "old", "new", "hot", "dry", "icy", "shy", "raw", "dim",
    "fit", "mad", "odd", "tan", "bold", "calm", "cold", "cool", "dark",
    "deep", "fair", "fast", "fine", "flat", "free", "full", "glad", "gold",
    "gray", "hard", "high", "keen", "kind", "late", "lean", "long", "lost",
    "loud", "mild", "neat", "next", "pale", "pink", "pure", "rare", "real",
    "rich", "ripe", "safe", "slim", "slow", "soft", "sure", "tall", "thin",
    "tiny", "true", "vast", "warm", "weak", "wide", "wild", "wise",
    # 5-6 chars
    "agile", "amber", "azure", "black", "blank", "brave", "brief", "broad",
    "brown", "clean", "clear", "coral", "crisp", "eager", "early", "empty",
    "exact", "fancy", "fresh", "giant", "grand", "green", "happy", "heavy",
    "ivory", "jolly", "jumbo", "large", "light", "lucky", "merry", "misty",
    "noble", "perky", "plain", "prime", "proud", "quick", "quiet", "rapid",
    "rocky", "rough", "round", "royal", "rusty", "sandy", "sharp", "shiny",
    "short", "silky", "sleek", "solid", "spare", "stark", "steep", "stiff",
    "stout", "sweet", "swift", "thick", "tight", "tough", "vivid", "white",
    "young", "zippy",
    # 7+ chars
    "ancient", "blazing", "ceramic", "coastal", "crystal", "curious",
    "durable", "dynamic", "earnest", "elastic", "elegant", "endless",
    "evident", "flowing", "fragile", "genuine", "glacial", "gleaming",
    "glowing", "gradual", "graphic", "healing", "helpful", "hopeful",
    "initial", "leading", "liberal", "logical", "magical", "massive",
    "melodic", "mineral", "missing", "morning", "musical", "natural",
    "neutral", "obvious", "organic", "pacific", "patient", "pending",
    "radiant", "refined", "regular", "relaxed", "restful", "roaming",
    "running", "settled", "shining", "silvery", "sincere", "skilled",
    "soaring", "spatial", "stellar", "storied", "supreme", "thermal",
    "vibrant", "vintage", "winding",
]

NOUNS = [
    # 3-4 chars
    "ant", "bay", "bee", "box", "cap", "car", "cat", "cub", "cup", "dam",
    "day", "den", "dew", "dog", "dot", "dusk", "elm", "elk", "eye", "fan",
    "fig", "fin", "fir", "fox", "gem", "hat", "hen", "hub", "hut", "ice",
    "ink", "inn", "ivy", "jam", "jar", "jay", "jet", "joy", "key", "kit",
    "lab", "log", "map", "mat", "net", "oak", "oat", "orb", "ore", "owl",
    "pad", "pan", "paw", "pea", "pen", "pie", "pin", "pod", "ram", "ray",
    "rib", "rim", "rod", "rug", "rye", "sea", "sky", "sun", "tap", "tea",
    "tin", "toy", "van", "vine", "wax", "web", "yak", "yew", "zen",
    # 5-6 chars
    "beach", "berry", "birch", "blaze", "bloom", "board", "brake", "brook",
    "brush", "cabin", "canoe", "cedar", "chalk", "cliff", "cloud", "crane",
    "creek", "crown", "daisy", "delta", "drift", "eagle", "earth", "ember",
    "fable", "ferry", "field", "flame", "flint", "forge", "frost", "globe",
    "grove", "haven", "heart", "heron", "house", "jewel", "knoll", "latch",
    "lemon", "light", "lotus", "maple", "marsh", "melon", "mirth", "moose",
    "night", "ocean", "olive", "orbit", "otter", "pearl", "petal", "pixel",
    "plaza", "plume", "prism", "quail", "raven", "ridge", "river",
    "robin", "shore", "spark", "spire", "spray", "squid", "steam", "stone",
    "storm", "sugar", "terra", "thorn", "tiger", "tower", "trail", "tulip",
    "vapor", "whale", "wheat", "wheel",
    # 7+ chars
    "anthill", "balcony", "balloon", "bamboo", "blossom", "boulder",
    "buffalo", "cascade", "cayenne", "chamber", "channel", "chimney",
    "circuit", "citadel", "compass", "conduit", "cottage", "crafter",
    "current", "cypress", "diamond", "dolphin", "doorway", "drizzle",
    "dynasty", "eclipse", "estuary", "evening", "factory", "feather",
    "firefly", "flannel", "foundry", "gallery", "gateway", "gazelle",
    "gondola", "gorilla", "granite", "habitat", "hammock", "harbour",
    "horizon", "jasmine", "juniper", "kingdom", "lantern", "lattice",
    "leopard", "library", "narwhal", "octagon", "panther", "passage",
    "peacock", "pelican", "pilgrim", "pumpkin", "pyramid", "rainbow",
    "redwood", "rooster", "sandbar", "sequoia", "shuttle", "spinner",
    "terrace", "thistle", "thunder", "tornado", "trellis", "unicorn",
    "volcano", "voyager", "whisper", "zephyr",
]

# Pre-compute lengths for efficient selection
_ADJ_LENS = np.array([len(w) for w in ADJECTIVES])
_NOUN_LENS = np.array([len(w) for w in NOUNS])
_N_ADJ = len(ADJECTIVES)
_N_NOUN = len(NOUNS)


def _avg_word_len(word_list):
    return sum(len(w) for w in word_list) / len(word_list)


# Average lengths for pattern estimation
_AVG_ADJ = _avg_word_len(ADJECTIVES)
_AVG_NOUN = _avg_word_len(NOUNS)
_AVG_SEP = 1  # underscore


def _estimate_pattern_length(n_adj, n_noun):
    """Estimate average string length for a pattern with n_adj adjectives and n_noun nouns."""
    total_words = n_adj + n_noun
    word_len = n_adj * _AVG_ADJ + n_noun * _AVG_NOUN
    sep_len = (total_words - 1) * _AVG_SEP if total_words > 1 else 0
    return word_len + sep_len


def _max_combos(n_adj, n_noun):
    """Maximum unique combinations for a pattern."""
    return (_N_ADJ ** n_adj) * (_N_NOUN ** n_noun)


# Available patterns: (n_adjectives, n_nouns)
_PATTERNS = [
    (0, 1),  # noun only (~5 chars)
    (1, 0),  # adjective only (~5 chars)
    (1, 1),  # adj_noun (~11 chars)
    (2, 1),  # adj_adj_noun (~17 chars)
    (1, 2),  # adj_noun_noun (~17 chars)
    (2, 2),  # adj_adj_noun_noun (~23 chars)
    (3, 1),  # adj_adj_adj_noun (~22 chars)
    (3, 2),  # adj_adj_adj_noun_noun (~28 chars)
]


def _select_pattern(avg_length, cardinality):
    """Select the best pattern(s) for the target average length and cardinality."""
    # Score each pattern by how close its avg length is to target
    scored = []
    for pat in _PATTERNS:
        est_len = _estimate_pattern_length(*pat)
        combos = _max_combos(*pat)
        if combos >= cardinality:
            scored.append((abs(est_len - avg_length), pat, est_len))

    if not scored:
        # No single pattern has enough capacity; use the largest
        return _PATTERNS[-1]

    scored.sort(key=lambda x: x[0])
    return scored[0][1]


def _index_to_name(index, n_adj, n_noun):
    """Convert a numeric index to a name using the specified pattern."""
    parts = []
    remaining = index

    # Generate noun parts (from the end)
    for _ in range(n_noun):
        noun_idx = remaining % _N_NOUN
        remaining //= _N_NOUN
        parts.append(NOUNS[noun_idx])

    # Generate adjective parts
    for _ in range(n_adj):
        adj_idx = remaining % _N_ADJ
        remaining //= _N_ADJ
        parts.append(ADJECTIVES[adj_idx])

    # Reverse so adjectives come first, then nouns
    parts.reverse()
    return "_".join(parts)


def generate_names(cardinality, avg_length=12, seed=42):
    """Generate a list of unique, human-readable names.

    Args:
        cardinality: Number of unique names to generate.
        avg_length: Target average string length.
        seed: Random seed for reproducibility.

    Returns:
        List of unique name strings.
    """
    if cardinality <= 0:
        return []

    rng = np.random.default_rng(seed)

    # Select the primary pattern
    n_adj, n_noun = _select_pattern(avg_length, cardinality)
    max_unique = _max_combos(n_adj, n_noun)

    if cardinality <= max_unique:
        # Enough combinations: sample without replacement
        if cardinality <= max_unique * 0.5:
            # Sparse sampling — pick random indices
            indices = rng.choice(max_unique, size=cardinality, replace=False)
        else:
            # Dense sampling — shuffle and take first N
            indices = rng.permutation(max_unique)[:cardinality]

        names = [_index_to_name(int(i), n_adj, n_noun) for i in indices]
    else:
        # Not enough combinations: generate all base names + add numeric suffixes
        base_names = [_index_to_name(i, n_adj, n_noun) for i in range(max_unique)]
        rng.shuffle(base_names)

        names = list(base_names)
        suffix = 2
        while len(names) < cardinality:
            batch_size = min(len(base_names), cardinality - len(names))
            for i in range(batch_size):
                names.append(f"{base_names[i]}_{suffix}")
            suffix += 1

        names = names[:cardinality]

    return names
