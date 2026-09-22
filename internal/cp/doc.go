// Package cp is shunt's control plane on embedded etcd (ADR-0015): the directory as records in
// etcd with every change one compare-and-swap transaction and a watch that keeps each node's copy
// current (store.go); the fleet of member proxies on leased keys (fleet.go); the etcd member's
// lifecycle wrapped so an operator never sees an etcd flag (etcd.go, api.go); and secrets at rest
// sealed under a data-encryption key only control nodes hold (crypt.go). shunt-control wires it
// up; bin/shunt never links it.
package cp
