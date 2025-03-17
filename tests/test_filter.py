from genie_flow_invoker.invoker.weaviate.utils import compile_filter
from weaviate.collections.classes.filters import Filter, _FilterAnd, _Operator, _FilterOr, \
    _FilterValue


def test_filter_none():
    weaviate_fiter = compile_filter({})
    assert weaviate_fiter is None


def test_filter_all():
    filter_definition = {
        "having_all": {
            "equal_attr": 12,
            "equal_attr2 ==": 12,
            "not_equal_attr !=": 0,
            "less_attr <": "zeus",
            "less_equal_attr <=": "zeus",
            "greater_attr >": "apollo",
            "greater_equal_attr >=": "apollo",
            "in_attr @": "Dionysus",
        }
    }
    weaviate_filter = compile_filter(filter_definition)

    assert weaviate_filter is not None
    assert isinstance(weaviate_filter, _FilterAnd)
    assert len(weaviate_filter.filters) == 8
    filters = [
        [getattr(f, p) for p in ["operator", "target", "value"]]
        for f in weaviate_filter.filters
    ]
    assert filters == [
        [_Operator.EQUAL, "equal_attr", 12],
        [_Operator.EQUAL, "equal_attr2", 12],
        [_Operator.NOT_EQUAL, "not_equal_attr", 0],
        [_Operator.LESS_THAN, "less_attr", "zeus"],
        [_Operator.LESS_THAN_EQUAL, "less_equal_attr", "zeus"],
        [_Operator.GREATER_THAN, "greater_attr", "apollo"],
        [_Operator.GREATER_THAN_EQUAL, "greater_equal_attr", "apollo"],
        [_Operator.CONTAINS_ANY, "in_attr", "Dionysus"],
    ]


def test_filter_any():
    filter_definition = {
        "having_any": {
            "equal_attr": 12,
            "equal_attr2 ==": 12,
            "not_equal_attr !=": 0,
            "less_attr <": "zeus",
            "less_equal_attr <=": "zeus",
            "greater_attr >": "apollo",
            "greater_equal_attr >=": "apollo",
            "in_attr @": "Dionysus",
        }
    }
    weaviate_filter = compile_filter(filter_definition)

    assert weaviate_filter is not None
    assert isinstance(weaviate_filter, _FilterOr)
    assert len(weaviate_filter.filters) == 8
    filters = [
        [getattr(f, p) for p in ["operator", "target", "value"]]
        for f in weaviate_filter.filters
    ]
    assert filters == [
        [_Operator.EQUAL, "equal_attr", 12],
        [_Operator.EQUAL, "equal_attr2", 12],
        [_Operator.NOT_EQUAL, "not_equal_attr", 0],
        [_Operator.LESS_THAN, "less_attr", "zeus"],
        [_Operator.LESS_THAN_EQUAL, "less_equal_attr", "zeus"],
        [_Operator.GREATER_THAN, "greater_attr", "apollo"],
        [_Operator.GREATER_THAN_EQUAL, "greater_equal_attr", "apollo"],
        [_Operator.CONTAINS_ANY, "in_attr", "Dionysus"],
    ]


def test_filter_all_any():
    filter_definition = {
        "having_all": {
            "first_all_attr": 12,
            "second_all_attr": 24,
            "third_all_attr": 48,
        },
        "having_any": {
            "first_any_attr": 12,
            "second_any_attr": 24,
        },
    }
    weaviate_filter = compile_filter(filter_definition)

    assert weaviate_filter is not None
    assert isinstance(weaviate_filter, _FilterAnd)
    assert len(weaviate_filter.filters) == 2
    assert isinstance(weaviate_filter.filters[0], _FilterAnd)
    assert isinstance(weaviate_filter.filters[1], _FilterOr)
    assert len(weaviate_filter.filters[0].filters) == 3
    assert len(weaviate_filter.filters[1].filters) == 2
    for leaf_filter in weaviate_filter.filters[0].filters:
        assert "all" in leaf_filter.target
    for leaf_filter in weaviate_filter.filters[1].filters:
        assert "any" in leaf_filter.target


def test_filter_space_separator():
    filter_definition = {
        "having_all": {
            "a space separated   attribute   !=": 0
        }
    }
    weaviate_filter = compile_filter(filter_definition)

    assert weaviate_filter is not None
    assert isinstance(weaviate_filter, _FilterValue)
    assert weaviate_filter.target == "a space separated   attribute"
    assert weaviate_filter.operator == _Operator.NOT_EQUAL
    assert weaviate_filter.value == 0
