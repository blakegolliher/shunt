// Package config is the schema and validator for shunt's YAML configuration
// (docs/DESIGN.md §2.3). It is loaded at startup and by `shunt check-config`.
//
// Rules: unknown keys are errors; every cluster states type, scheme, and region;
// storage_class and access must pair legally; every reference (tenant → cluster,
// placement → cluster) must resolve. Validation errors name the offending key
// path, e.g. `clusters.vast-a.scheme`.
//
// Hot reload is deferred (POC.md); this package only parses and validates.
package config
