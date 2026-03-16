"""Sensor layer — BYOD manifest, unit conversion, remote sensing support."""
from digital_pedon.sensor.sensor_layer import (
    SensorField, SensorManifest, SensorLayer, ManifestBuilder,
    CANONICAL_PROPERTIES,
    example_manifest_insitu, example_manifest_with_remote_sensing,
)
__all__ = ["SensorField","SensorManifest","SensorLayer","ManifestBuilder",
           "CANONICAL_PROPERTIES",
           "example_manifest_insitu","example_manifest_with_remote_sensing"]
