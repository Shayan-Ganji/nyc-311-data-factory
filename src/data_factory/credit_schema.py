"""
Schema definitions, drift checks, and validation rules for Give Me Some Credit dataset.
Enforces data contract and quarantine isolation before relational loading.
"""

from typing import Any
from .exceptions import SchemaDriftError

CREDIT_RAW_COLUMNS = {
    "SeriousDlqin2yrs",
    "RevolvingUtilizationOfUnsecuredLines",
    "age",
    "NumberOfTime30-59DaysPastDueNotWorse",
    "DebtRatio",
    "MonthlyIncome",
    "NumberOfOpenCreditLinesAndLoans",
    "NumberOfTimes90DaysLate",
    "NumberRealEstateLoansOrLines",
    "NumberOfTime60-89DaysPastDueNotWorse",
    "NumberOfDependents",
}

CREDIT_COLUMN_MAP = {
    "SeriousDlqin2yrs": "serious_dlq_in_2yrs",
    "RevolvingUtilizationOfUnsecuredLines": "revolving_utilization",
    "age": "age",
    "NumberOfTime30-59DaysPastDueNotWorse": "num_30_59_days_past_due",
    "DebtRatio": "debt_ratio",
    "MonthlyIncome": "monthly_income",
    "NumberOfOpenCreditLinesAndLoans": "num_open_credit_lines",
    "NumberOfTimes90DaysLate": "num_90_days_late",
    "NumberRealEstateLoansOrLines": "num_real_estate_loans",
    "NumberOfTime60-89DaysPastDueNotWorse": "num_60_89_days_past_due",
    "NumberOfDependents": "num_dependents",
}

# Error codes for Credit Quality Quarantine
ERR_INVALID_AGE = "ERR_INVALID_AGE"
ERR_INVALID_TARGET = "ERR_INVALID_TARGET"
ERR_NEGATIVE_VALUE = "ERR_NEGATIVE_VALUE"
ERR_MISSING_MANDATORY_FIELD = "ERR_MISSING_MANDATORY_FIELD"


def check_credit_schema_drift(records: list[dict[str, Any]]) -> None:
    """
    Verify incoming CSV batch contains all required credit scoring features.
    Raises SchemaDriftError if any mandatory column is missing.
    """
    if not records:
        return
    present_keys = set().union(*(r.keys() for r in records))
    missing = CREDIT_RAW_COLUMNS - present_keys
    if missing:
        raise SchemaDriftError(
            f"Credit dataset schema drift detected! Missing columns: {sorted(missing)}"
        )


def validate_and_transform_credit_record(
    raw: dict[str, Any]
) -> tuple[bool, str | None, str | None, dict[str, Any] | None]:
    """
    Validate a single credit applicant record.
    Returns:
        (True, None, None, canonical_record) if valid.
        (False, error_code, error_message, None) if quarantined.
    """
    # 1. Target validation
    target = raw.get("SeriousDlqin2yrs")
    if target is None:
        return False, ERR_MISSING_MANDATORY_FIELD, "Missing target SeriousDlqin2yrs", None
    try:
        target_int = int(target)
        if target_int not in (0, 1):
            return False, ERR_INVALID_TARGET, f"Target value must be 0 or 1, got {target}", None
    except (ValueError, TypeError):
        return False, ERR_INVALID_TARGET, f"Cannot cast target to int: {target}", None

    # 2. Age validation (Physical / Legal credit boundary: 18 to 120 years)
    age = raw.get("age")
    if age is None:
        return False, ERR_MISSING_MANDATORY_FIELD, "Missing age field", None
    try:
        age_int = int(age)
        if age_int < 18 or age_int > 120:
            return (
                False,
                ERR_INVALID_AGE,
                f"Age {age_int} is outside valid legal/physical borrowing limits [18, 120]",
                None,
            )
    except (ValueError, TypeError):
        return False, ERR_INVALID_AGE, f"Cannot parse age: {age}", None

    # 3. Non-negativity checks
    revolving = raw.get("RevolvingUtilizationOfUnsecuredLines")
    if revolving is not None and float(revolving) < 0:
        return False, ERR_NEGATIVE_VALUE, f"Negative revolving utilization: {revolving}", None

    lines = raw.get("NumberOfOpenCreditLinesAndLoans")
    if lines is not None and int(lines) < 0:
        return False, ERR_NEGATIVE_VALUE, f"Negative open credit lines: {lines}", None

    real_estate = raw.get("NumberRealEstateLoansOrLines")
    if real_estate is not None and int(real_estate) < 0:
        return False, ERR_NEGATIVE_VALUE, f"Negative real estate loans: {real_estate}", None

    dependents = raw.get("NumberOfDependents")
    if dependents is not None and float(dependents) < 0:
        return False, ERR_NEGATIVE_VALUE, f"Negative dependents count: {dependents}", None

    # Construct canonical record with clean snake_case names
    canonical: dict[str, Any] = {
        "serious_dlq_in_2yrs": target_int,
        "revolving_utilization": float(revolving) if revolving is not None else None,
        "age": age_int,
        "num_30_59_days_past_due": int(raw.get("NumberOfTime30-59DaysPastDueNotWorse", 0)),
        "debt_ratio": float(raw.get("DebtRatio", 0.0)),
        "monthly_income": float(raw.get("MonthlyIncome")) if raw.get("MonthlyIncome") is not None else None,
        "num_open_credit_lines": int(lines) if lines is not None else 0,
        "num_90_days_late": int(raw.get("NumberOfTimes90DaysLate", 0)),
        "num_real_estate_loans": int(real_estate) if real_estate is not None else 0,
        "num_60_89_days_past_due": int(raw.get("NumberOfTime60-89DaysPastDueNotWorse", 0)),
        "num_dependents": float(dependents) if dependents is not None else None,
    }

    return True, None, None, canonical
