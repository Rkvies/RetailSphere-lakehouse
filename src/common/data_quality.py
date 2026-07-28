"""
src/common/data_quality.py
Config-driven data quality rule engine with quarantine-pattern validate().
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.logger import get_logger, log_pipeline_event

logger = get_logger(__name__)


@dataclass
class RuleViolation:
    rule_name: str
    column: str
    reason: str


@dataclass
class ValidationResult:
    valid_df: DataFrame
    invalid_df: DataFrame
    valid_count: int
    invalid_count: int
    violations: list[RuleViolation] = field(default_factory=list)

    @property
    def pass_rate(self) -> float:
        total = self.valid_count + self.invalid_count
        return (self.valid_count / total * 100.0) if total > 0 else 100.0


def rule_not_null(column: str):
    def _check(df: DataFrame):
        return F.col(column).isNotNull()
    return _check


def rule_non_negative(column: str):
    def _check(df: DataFrame):
        return F.col(column).isNull() | (F.col(column) >= 0)
    return _check


def rule_allowed_values(column: str, allowed: list[Any]):
    def _check(df: DataFrame):
        return F.col(column).isNull() | F.col(column).isin(allowed)
    return _check


def rule_no_future_date(column: str):
    def _check(df: DataFrame):
        return F.col(column).isNull() | (F.col(column) <= F.current_date())
    return _check


RULE_REGISTRY: dict[str, Callable[..., Callable[[DataFrame], Any]]] = {
    "not_null": rule_not_null,
    "non_negative": rule_non_negative,
    "allowed_values": rule_allowed_values,
    "no_future_date": rule_no_future_date,
}


def _build_rule_check(rule_config: dict[str, Any]):
    rule_type = rule_config["type"]
    column = rule_config["column"]
    if rule_type not in RULE_REGISTRY:
        raise ValueError(f"Unknown data quality rule type '{rule_type}'. Available: {list(RULE_REGISTRY.keys())}")
    extra_params = {k: v for k, v in rule_config.items() if k not in ("type", "column")}
    check_fn = RULE_REGISTRY[rule_type](column, **extra_params)
    return rule_type, column, check_fn


def validate(df: DataFrame, dq_rules: list[dict[str, Any]], table_name: str) -> ValidationResult:
    if not dq_rules:
        log_pipeline_event(logger, "no_dq_rules_configured", level="WARNING", table=table_name)
        return ValidationResult(valid_df=df, invalid_df=df.limit(0), valid_count=df.count(), invalid_count=0)

    combined_valid_expr = F.lit(True)
    per_rule_exprs = []

    for rule_config in dq_rules:
        rule_name, column, check_fn = _build_rule_check(rule_config)
        rule_expr = check_fn(df)
        per_rule_exprs.append((rule_name, column, rule_expr))
        combined_valid_expr = combined_valid_expr & rule_expr

    valid_df = df.filter(combined_valid_expr)
    invalid_df = df.filter(~combined_valid_expr)
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    violations = []
    for rule_name, column, rule_expr in per_rule_exprs:
        failed_count = df.filter(~rule_expr).count()
        if failed_count > 0:
            violations.append(RuleViolation(
                rule_name=rule_name, column=column,
                reason=f"{failed_count} row(s) failed rule '{rule_name}' on column '{column}'",
            ))

    result = ValidationResult(
        valid_df=valid_df, invalid_df=invalid_df,
        valid_count=valid_count, invalid_count=invalid_count, violations=violations,
    )

    log_pipeline_event(
        logger, "data_quality_validation_complete",
        level="INFO" if result.pass_rate >= 95.0 else "WARNING",
        table=table_name, valid_count=valid_count, invalid_count=invalid_count,
        pass_rate=round(result.pass_rate, 2), violation_count=len(violations),
    )
    return result
