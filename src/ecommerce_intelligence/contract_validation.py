from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


CONTRACT_DIRECTORY = Path(__file__).resolve().parents[2] / "contracts"

EVENT_CONTRACT = "ecommerce_event_v1.schema.json"
PRODUCT_CONTRACT = "product_v1.schema.json"


@lru_cache(maxsize=None)
def load_validator(filename: str) -> Draft202012Validator:
    contract_path = CONTRACT_DIRECTORY / filename

    with contract_path.open(encoding="utf-8") as contract_file:
        schema: dict[str, Any] = json.load(contract_file)

    Draft202012Validator.check_schema(schema)

    return Draft202012Validator(
        schema,
        format_checker=FormatChecker(),
    )


def validate_event(record: dict[str, Any]) -> None:
    load_validator(EVENT_CONTRACT).validate(record)


def validate_product(record: dict[str, Any]) -> None:
    load_validator(PRODUCT_CONTRACT).validate(record)