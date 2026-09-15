package config

// Features holds every feature flag (CLAUDE.md: flags live in one file, each with a removal
// criterion). Flags default off, with one exception approved with POC-3 on 2026-09-15 and noted on
// the flag.
type Features struct {
	// XMLRewrite rewrites backend bucket names, cluster endpoint hosts, and uploadIds in resign-mode
	// response bodies (ADR-0006). Default TRUE, the one exception to default-off: turning it off
	// exposes backend names and endpoint addresses to clients, and `shunt serve` warns at startup
	// naming the clusters whose <Location> will leak. It exists to take the rewriter out of the
	// path if it misbehaves in production.
	// Removal: one full phase after P3b with no rewriter overflow or leak report.
	XMLRewrite *bool `yaml:"xml_rewrite"`
}

// XMLRewriteOn reports the xml_rewrite flag with its default (true) applied.
func (f Features) XMLRewriteOn() bool { return f.XMLRewrite == nil || *f.XMLRewrite }
