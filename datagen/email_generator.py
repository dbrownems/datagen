"""Email address generation for string columns.

Detects email columns by name pattern and generates realistic addresses
using superhero names:
  - Internal (employee) tables → Marvel heroes @contoso.com
  - External (customer/supplier) tables → DC heroes @adventureworks.com / @wideworldimporters.com
"""

# Marvel characters — internal/employee emails
_MARVEL_NAMES = [
    ("reed", "richards"), ("sue", "storm"), ("johnny", "storm"), ("ben", "grimm"),
    ("tony", "stark"), ("pepper", "potts"), ("steve", "rogers"), ("bucky", "barnes"),
    ("natasha", "romanoff"), ("clint", "barton"), ("bruce", "banner"), ("betty", "ross"),
    ("thor", "odinson"), ("loki", "laufeyson"), ("peter", "parker"), ("mary", "watson"),
    ("miles", "morales"), ("gwen", "stacy"), ("matt", "murdock"), ("jessica", "jones"),
    ("luke", "cage"), ("danny", "rand"), ("frank", "castle"), ("wade", "wilson"),
    ("scott", "lang"), ("hope", "pym"), ("hank", "pym"), ("janet", "vandyne"),
    ("wanda", "maximoff"), ("vision", "shade"), ("carol", "danvers"), ("monica", "rambeau"),
    ("kamala", "khan"), ("sam", "wilson"), ("james", "rhodes"), ("nick", "fury"),
    ("maria", "hill"), ("phil", "coulson"), ("shuri", "udaku"), ("okoye", "adichie"),
    ("stephen", "strange"), ("wong", "kamar"), ("charles", "xavier"), ("jean", "grey"),
    ("scott", "summers"), ("ororo", "munroe"), ("logan", "howlett"), ("anna", "marie"),
    ("kurt", "wagner"), ("bobby", "drake"), ("hank", "mccoy"), ("kitty", "pryde"),
    ("emma", "frost"), ("erik", "lehnsherr"), ("raven", "darkholme"), ("peter", "quill"),
    ("gamora", "zen"), ("drax", "destroyer"), ("rocket", "raccoon"), ("groot", "flora"),
]

# DC characters — external/customer/supplier emails
_DC_NAMES = [
    ("bruce", "wayne"), ("clark", "kent"), ("diana", "prince"), ("barry", "allen"),
    ("hal", "jordan"), ("arthur", "curry"), ("oliver", "queen"), ("dinah", "lance"),
    ("victor", "stone"), ("billy", "batson"), ("selina", "kyle"), ("harley", "quinn"),
    ("pamela", "isley"), ("kate", "kane"), ("barbara", "gordon"), ("dick", "grayson"),
    ("jason", "todd"), ("tim", "drake"), ("damian", "wayne"), ("alfred", "pennyworth"),
    ("lois", "lane"), ("jimmy", "olsen"), ("perry", "white"), ("lex", "luthor"),
    ("james", "gordon"), ("john", "constantine"), ("zatanna", "zatara"), ("kara", "kent"),
    ("wally", "west"), ("iris", "west"), ("jay", "garrick"), ("alan", "scott"),
    ("carter", "hall"), ("shiera", "sanders"), ("ray", "palmer"), ("ted", "kord"),
    ("jaime", "reyes"), ("renee", "montoya"), ("helena", "bertinelli"), ("ryan", "choi"),
    ("john", "stewart"), ("guy", "gardner"), ("kyle", "rayner"), ("jessica", "cruz"),
    ("mera", "xebel"), ("garth", "tempest"), ("donna", "troy"), ("cassie", "sandsmark"),
    ("connor", "hawke"), ("roy", "harper"), ("mia", "dearden"), ("courtney", "whitmore"),
    ("pat", "dugan"), ("michael", "holt"), ("kent", "nelson"), ("nabu", "order"),
]

_INTERNAL_DOMAIN = "contoso.com"
_EXTERNAL_DOMAINS = ["adventureworks.com", "wideworldimporters.com"]

# Column name patterns that suggest an email/username field
_EMAIL_PATTERNS = {
    "email", "emailaddress", "email_address", "useremail", "user_email",
    "username", "user_name", "userprincipalname", "user_principal_name",
    "upn", "loginname", "login_name", "login", "useraccount",
}

# Table name patterns that suggest external parties
_EXTERNAL_TABLE_PATTERNS = {
    "customer", "client", "supplier", "vendor", "partner", "contact",
    "account", "lead", "prospect", "buyer", "seller", "merchant",
}


def _normalize(name):
    return name.lower().replace("_", "").replace(" ", "").replace("-", "")


def is_email_column(col_name):
    """Check if a column name suggests it contains email addresses."""
    return _normalize(col_name) in _EMAIL_PATTERNS


def is_external_table(table_name):
    """Check if a table name suggests external parties (customers, suppliers, etc.)."""
    norm = _normalize(table_name)
    return any(p in norm for p in _EXTERNAL_TABLE_PATTERNS)


def generate_email_pool(cardinality, table_name="", seed=42):
    """Generate a pool of unique email addresses.

    Args:
        cardinality: Number of unique emails to generate.
        table_name: Table name (used to determine internal vs external).
        seed: Random seed.

    Returns:
        List of unique email strings.
    """
    import numpy as np
    rng = np.random.default_rng(seed)

    external = is_external_table(table_name)

    if external:
        names = list(_DC_NAMES)
        domains = _EXTERNAL_DOMAINS
    else:
        names = list(_MARVEL_NAMES)
        domains = [_INTERNAL_DOMAIN]

    rng.shuffle(names)

    pool = []
    seen = set()

    # Phase 1 — base names (first.last@domain)
    for first, last in names:
        if len(pool) >= cardinality:
            break
        domain = domains[len(pool) % len(domains)]
        email = f"{first}.{last}@{domain}"
        if email not in seen:
            pool.append(email)
            seen.add(email)

    # Phase 2 — numbered variants if we need more
    suffix = 2
    while len(pool) < cardinality:
        for first, last in names:
            if len(pool) >= cardinality:
                break
            domain = domains[len(pool) % len(domains)]
            email = f"{first}.{last}{suffix}@{domain}"
            if email not in seen:
                pool.append(email)
                seen.add(email)
        suffix += 1

    return pool[:cardinality]
