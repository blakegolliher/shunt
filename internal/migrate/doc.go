// Package migrate holds the stateless-ID codec (docs/DESIGN.md §2.4), the migration routing rules
// (§2.5), and the operator's briefing on the lost-write window that shunt migrate start and the
// mover both refuse without (ADR-0004 race 2).
package migrate
