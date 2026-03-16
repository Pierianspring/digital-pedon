"""
Digital Pedon — A digital twin framework for soil profile monitoring.

Authors : Badreldin, N. & Youssef, A. (2026)
License : CC BY 4.0
Repo    : https://github.com/digitalpedon/framework
"""

from digital_pedon.pedon_api import (
    build_pedon,
    register_method,
    unregister_method,
    list_methods,
    register_soil_type,
    list_soil_types,
    get_soil_type,
    make_reading,
    print_snapshot,
    DigitalPedon,
    SOIL_TYPE_REGISTRY,
    METHOD_REGISTRY,
)

__version__ = "2.0.0"
__authors__  = ["Nasem Badreldin", "Ali Youssef"]
__license__  = "CC BY 4.0"

__all__ = [
    "build_pedon",
    "register_method",
    "unregister_method",
    "list_methods",
    "register_soil_type",
    "list_soil_types",
    "get_soil_type",
    "make_reading",
    "print_snapshot",
    "DigitalPedon",
    "SOIL_TYPE_REGISTRY",
    "METHOD_REGISTRY",
]
