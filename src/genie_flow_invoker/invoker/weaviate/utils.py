from typing import Optional, Any

from loguru import logger
from weaviate.collections.classes.filters import Filter, _Filters


def _create_attribute_filter(key: str, value: Any) -> _Filters | None:
    if " " in key:
        key_name, indicator = key.rsplit(" ", 1)
    else:
        key_name = key
        indicator = "=="
    logger.debug(
        "found indicator {indicator} for property {property} for value {value}",
        indicator=indicator,
        property=key_name,
        value=value,
    )
    filter_by_property = Filter.by_property(key_name)
    match indicator:
        case "==":
            return filter_by_property.equal(value)
        case "!=":
            return filter_by_property.not_equal(value)
        case "<":
            return filter_by_property.less_than(value)
        case "<=":
            return filter_by_property.less_or_equal(value)
        case ">":
            return filter_by_property.greater_than(value)
        case ">=":
            return filter_by_property.greater_or_equal(value)
        case "@":
            return filter_by_property.contains_any(value)
        case _:
            logger.error(
                "Invalid indicator '{indicator}' for property {property}",
                indicator=indicator,
                property=key_name,
            )
            raise ValueError(
                f"Got filter indicator '{indicator}' that is not supported"
            )


def compile_filter(query_params: dict) -> Optional[Filter]:
    query_filter = None

    if query_params["having_all"] is not None:
        logger.debug("building an `all_of' filter")
        query_filter = Filter.all_of(
            [
                _create_attribute_filter(key, value)
                for key, value in query_params["having_all"].items()
            ]
        )
    if query_params["having_any"] is not None:
        logger.debug("building an `any_of' filter")
        any_filter = Filter.any_of(
            [
                _create_attribute_filter(key, value)
                for key, value in query_params["having_any"].items()
            ]
        )
        if query_filter is not None:
            query_filter = query_filter & any_filter
        else:
            query_filter = any_filter

    return query_filter
