package config

// Features holds feature flags: behavior that is off until an operator turns it on. Every flag
// defaults off and carries a removal criterion (CLAUDE.md). None exist yet; the empty struct keeps
// `features:` a known key, so an unknown flag name is still an error.
type Features struct{}

// KillSwitches turn off behavior that is normally on, for an operator who hits a bug in it while
// shunt is serving. A kill switch is not a feature flag: it defaults to the normal behavior
// (false), it has no removal criterion, and it documents what breaks when it is flipped
// (CLAUDE.md). It is removed when the behavior it guards no longer needs a switch.
type KillSwitches struct {
	// XMLRewriteDisable stops shunt rewriting backend bucket names, cluster endpoint hosts, and
	// uploadIds in resign-mode responses (ADR-0006).
	//
	// Flipping it hands clients the backend's names: a bucket name the client cannot address, the
	// cluster's own address in CompleteMultipartUpload's <Location>, and backend uploadIds that
	// stop working as soon as a bucket's placement moves. `shunt serve` warns at startup and names
	// the clusters whose <Location> will leak. Turn it on only to get past a bug in the rewriter,
	// and only in front of clients that may see backend names.
	XMLRewriteDisable bool `yaml:"xml_rewrite_disable"`
}
