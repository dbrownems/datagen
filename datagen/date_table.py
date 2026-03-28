"""Date table detection and generation.

Detects date dimension tables by name and column patterns, then generates
all columns derived from a contiguous date spine instead of random values.
"""

from datetime import datetime, timedelta
import calendar

# ---------------------------------------------------------------------------
# Table-name patterns that suggest a date table
# ---------------------------------------------------------------------------

_DATE_TABLE_NAMES = {
    "date", "dates", "calendar", "calendardate", "dimdate", "dim_date",
    "datekey", "datetable", "date_table", "datedim", "date_dim",
    "dimcalendar", "dim_calendar", "calendarmonth", "time", "dimtime",
}

# ---------------------------------------------------------------------------
# Column-role detection — maps column names to derivation functions
# ---------------------------------------------------------------------------

# Each entry: set of lowercase name patterns → role tag
_COL_PATTERNS = {
    "date_key": {
        "datekey", "date_key", "dk", "dateint", "date_int",
        "smartdate", "smart_date", "surrogate_key",
    },
    "date": {
        "date", "fulldate", "full_date", "calendardate", "calendar_date",
        "thedate", "the_date", "datevalue",
    },
    "year": {
        "year", "calendaryear", "calendar_year", "yr",
    },
    "quarter": {
        "quarter", "quartername", "quarter_name", "quarterofyear",
        "quarter_of_year", "qtr", "calendarquarter", "calendar_quarter",
    },
    "quarter_number": {
        "quarternumber", "quarter_number", "quarternum", "quarter_num",
        "qtrnumber", "qtrnum", "quarterint",
    },
    "month_name": {
        "monthname", "month_name", "month", "calendarmonth",
        "calendar_month", "monthofyear", "month_of_year",
    },
    "month_number": {
        "monthnumber", "month_number", "monthnum", "month_num",
        "monthno", "month_no", "monthint", "monthorder", "month_order",
        "monthindex", "month_index", "monthsort",
    },
    "month_year": {
        "monthyear", "month_year", "yearmonth", "year_month",
    },
    "day": {
        "day", "dayofmonth", "day_of_month", "dom",
    },
    "day_of_week": {
        "dayofweek", "day_of_week", "weekday", "weekdayname",
        "weekday_name", "dayname", "day_name", "dow",
    },
    "day_of_week_number": {
        "dayofweeknumber", "day_of_week_number", "weekdaynumber",
        "weekday_number", "daysort", "dayorder", "day_order",
    },
    "day_of_year": {
        "dayofyear", "day_of_year", "doy",
    },
    "week_number": {
        "weeknumber", "week_number", "weeknum", "week_num",
        "weekofyear", "week_of_year", "isoweek", "iso_week",
    },
    "year_quarter": {
        "yearquarter", "year_quarter",
    },
    "is_weekend": {
        "isweekend", "is_weekend", "weekend",
    },
    "is_weekday": {
        "isweekday", "is_weekday", "workday", "isworkday", "is_workday",
        "isbusinessday", "is_business_day", "isworkingday", "is_working_day",
    },
    "fiscal_year": {
        "fiscalyear", "fiscal_year", "fy",
    },
    "fiscal_quarter": {
        "fiscalquarter", "fiscal_quarter", "fq",
    },
}


def _normalize(name):
    """Lowercase and strip separators for matching."""
    return name.lower().replace("_", "").replace(" ", "").replace("-", "")


def _detect_column_role(col_name):
    """Return the date-role tag for a column name, or None."""
    norm = _normalize(col_name)
    for role, patterns in _COL_PATTERNS.items():
        if norm in {_normalize(p) for p in patterns}:
            return role
    return None


def is_date_table(table_meta):
    """Heuristic: does this table look like a date dimension?

    Checks table name and column composition. Returns True if the table
    is likely a date/calendar dimension.
    """
    name = _normalize(table_meta.get("name", ""))
    if name in {_normalize(n) for n in _DATE_TABLE_NAMES}:
        return True

    # If table name doesn't match, check column composition:
    # a date table typically has a Date/DateTime column + several derived cols
    cols = table_meta.get("columns", [])
    roles_found = set()
    for col in cols:
        role = _detect_column_role(col.get("name", ""))
        if role:
            roles_found.add(role)

    # Must have a date or date_key column, plus at least 2 derived columns
    has_anchor = bool(roles_found & {"date", "date_key"})
    has_derived = len(roles_found & {
        "year", "quarter", "month_name", "month_number",
        "day_of_week", "week_number",
    }) >= 2

    return has_anchor and has_derived


def generate_date_table(table_meta, seed=42):
    """Generate a complete date table as a list of row dicts.

    Determines the date range from the table's row_count (number of days),
    anchored so the range ends at Jan 1 of the current year.
    All columns are derived from the date spine — no random generation.

    Args:
        table_meta: Parsed table metadata (from vpax_parser).
        seed: Not used (dates are deterministic), kept for API consistency.

    Returns:
        List of dicts, one per date, with keys matching column names.
    """
    row_count = max(1, table_meta.get("row_count", 365))
    cols = table_meta.get("columns", [])

    # Determine date range: end at Jan 1 of current year
    end_date = datetime(datetime.now().year, 1, 1)
    start_date = end_date - timedelta(days=row_count - 1)

    # Map columns to roles
    col_roles = {}
    for col in cols:
        role = _detect_column_role(col["name"])
        col_roles[col["name"]] = (role, col.get("data_type", "string"))

    # Generate rows
    rows = []
    for i in range(row_count):
        dt = start_date + timedelta(days=i)
        row = {}

        for col_name, (role, dtype) in col_roles.items():
            row[col_name] = _derive_value(dt, role, dtype)

        rows.append(row)

    return rows


def _derive_value(dt, role, dtype):
    """Derive a column value from a date based on its role."""
    if role == "date_key":
        return int(dt.strftime("%Y%m%d"))

    if role == "date":
        # Midnight, no time portion
        return dt.strftime("%Y-%m-%d 00:00:00")

    if role == "year":
        return dt.year

    if role == "quarter":
        q = (dt.month - 1) // 3 + 1
        return f"Q{q}"

    if role == "quarter_number":
        return (dt.month - 1) // 3 + 1

    if role == "month_name":
        return dt.strftime("%B")  # "January", "February", ...

    if role == "month_number":
        return dt.month

    if role == "month_year":
        return dt.strftime("%b %Y")  # "Jan 2024"

    if role == "day":
        return dt.day

    if role == "day_of_week":
        return dt.strftime("%A")  # "Monday", "Tuesday", ...

    if role == "day_of_week_number":
        return dt.isoweekday()  # 1=Monday, 7=Sunday

    if role == "day_of_year":
        return dt.timetuple().tm_yday

    if role == "week_number":
        return dt.isocalendar()[1]

    if role == "year_quarter":
        q = (dt.month - 1) // 3 + 1
        return f"{dt.year}-Q{q}"

    if role == "is_weekend":
        if dtype == "boolean":
            return dt.weekday() >= 5
        return dt.weekday() >= 5

    if role == "is_weekday":
        if dtype == "boolean":
            return dt.weekday() < 5
        return dt.weekday() < 5

    if role == "fiscal_year":
        # Common: fiscal year starts July 1
        return dt.year if dt.month >= 7 else dt.year - 1

    if role == "fiscal_quarter":
        # FY starts July 1: Jul-Sep=FQ1, Oct-Dec=FQ2, Jan-Mar=FQ3, Apr-Jun=FQ4
        fiscal_month = (dt.month - 7) % 12 + 1
        fq = (fiscal_month - 1) // 3 + 1
        return f"FQ{fq}"

    # Unknown role — return the date as a string fallback
    if dtype == "int64":
        return int(dt.strftime("%Y%m%d"))
    if dtype == "datetime":
        return dt.strftime("%Y-%m-%d 00:00:00")
    return dt.strftime("%Y-%m-%d")
