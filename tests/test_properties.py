from hashlib import md5

from genie_flow_invoker.invoker.weaviate.properties import flatten_properties, FlattenedProperty, \
    unflatten_properties, unmap_properties


def test_property_flattening():
    p = {
        "aap": "noot",
        "mies": {
            "wim": 1,
            "zus": 2,
        }
    }

    flat_p = flatten_properties(p)

    for pp in flat_p:
        h = md5(pp.path.encode("utf-8")).hexdigest()
        assert pp.flat_name == f"property_{h}"

        pv = p
        path_parts = pp.path.split(".")
        if len(path_parts) > 1:
            pv = pv[path_parts[0]]
            path_parts = path_parts[1:]
        assert pv[path_parts[0]] == pp.value


def test_property_unflattening():
    flat_p = [
        FlattenedProperty("property_1", "aap", "noot"),
        FlattenedProperty("property_2", "mies.wim", 1),
        FlattenedProperty("property_3", "mies.zus", 2),
    ]

    p = unflatten_properties(flat_p)
    assert p == {
        "aap": "noot",
        "mies": {
            "wim": 1,
            "zus": 2,
        }
    }


def test_property_unflattening_with_map():
    flat_p = {
        "property_1": "noot",
        "property_2": 1,
        "property_3": 2,
    }
    p_map = {
        "property_1": "aap",
        "property_2": "mies.wim",
        "property_3": "mies.zus",
    }

    p = unmap_properties(flat_p, p_map)
    assert p == {
        "aap": "noot",
        "mies": {
            "wim": 1,
            "zus": 2,
        }
    }
