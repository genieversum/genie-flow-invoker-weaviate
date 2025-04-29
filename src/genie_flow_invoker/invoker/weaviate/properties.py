from dataclasses import dataclass
from hashlib import md5
from typing import Any


def create_flat_name(path: str | list[str]) -> str:
    """
    Create a flat name from a path. The path is expected to be a string or a list of strings.
    Parts of a path are joined with a dot. The hash of the path is used as a suffix to the
    flat name.
    :param path: a dot-separated path to a property or a list of strings representing the path.
    :return: a flat name for the property.
    """
    if isinstance(path, list):
        path = ".".join(path)
    path_hash = md5(path.encode("utf-8")).hexdigest()
    return f"property_{path_hash}"


@dataclass
class FlattenedProperty:
    """
    Represents a flattened property with a name, path, and value.

    This class is used to hold information about a property that has been
    flattened, including its flat name, the original path, and its value.
    It is particularly useful in scenarios where hierarchical data structures
    are transformed into a flat representation.

    :ivar flat_name: The flat name of the property, derived from its path.
    :type flat_name: str
    :ivar path: The original path of the property.
    :type path: str
    :ivar value: The value associated with the property.
    :type value: Any
    """
    flat_name: str
    path: str
    value: Any

    @classmethod
    def from_path(cls, path: str, value: Any) -> "FlattenedProperty":
        return cls(create_flat_name(path), path, value)


def flatten_properties(properties: dict[str, Any], path: str = "") -> list[FlattenedProperty]:
    """
    Create a list of flattened properties from a nested dictionary.

    :param properties: the potentially nested dictionary to flatten
    :param path: the path to the current dictionary, used for recursion
    :return: the list of flattened properties
    """
    flattened_properties = []
    for item, value in properties.items():
        sub_path = path+"."+item if path else item
        if isinstance(value, dict):
            flattened_properties.extend(flatten_properties(value, sub_path))
        else:
            flattened_properties.append(FlattenedProperty.from_path(sub_path, value))
    return flattened_properties


def deep_update(properties: dict, path_parts: list[str], value: Any):
    """
    Update a nested dictionary from a list of path parts and a value.

    :param properties: the dictionary to create the nested dictionary in
    :param path_parts: the list of path parts to create the nested dictionary from
    :param value: the value to set at the end of the path parts list
    """
    if len(path_parts) == 1:
        properties[path_parts[0]] = value
    else:
        if path_parts[0] not in properties:
            properties[path_parts[0]] = {}
        deep_update(properties[path_parts[0]], path_parts[1:], value)


def unflatten_properties(flattened_properties: list[FlattenedProperty]) -> dict[str, Any]:
    """
    Unflatten a list of flattened properties into a nested dictionary.

    :param flattened_properties: a list of flattened properties
    :return: a nested dictionary with the flattened properties as keys and their values as values
    """
    unflattened_properties = {}
    for p in flattened_properties:
        path_parts = p.path.split(".")
        deep_update(unflattened_properties, path_parts, p.value)

    return unflattened_properties


def unmap_properties(
        flat_properties: dict[str, Any],
        property_map: dict[str, str]
) -> dict[str, Any]:
    """
    Unmap a dictionary of flattened properties to a nested dictionary of unflattened properties.
    :param flat_properties: a dictionary of flattened properties
    :param property_map: a map of flattened property names to unflattened property paths
    :return: a nested dictionary of unflattened properties with the flattened property
             names as keys and their values as values
    """
    flattened_properties = [
        FlattenedProperty(
            flat_name,
            property_map[flat_name],
            value,
        )
        for flat_name, value in flat_properties.items()
        if flat_name in property_map
    ]
    return unflatten_properties(flattened_properties)
