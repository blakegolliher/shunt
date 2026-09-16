package migrate

import (
	"errors"
	"fmt"
)

// AcceptLostWriteWindowFlag is the flag with which an operator starts a migration, or runs the
// mover, into a cluster that ignores If-None-Match: * on PUT. shunt migrate start and test/mover
// both take it, and both refuse without it.
const AcceptLostWriteWindowFlag = "accept-lost-write-window"

// LostWriteWindow says what the operator accepts by migrating into cluster: the mover's
// HEAD-then-commit guard can overwrite a client write (ADR-0004 race 2, docs/migrating.md).
func LostWriteWindow(key, cluster string) string {
	return fmt.Sprintf("%s: target cluster %s has capabilities.conditional_write: false (it ignores If-None-Match: * on PUT), "+
		"so the mover falls back to HEAD-then-commit. A client write that lands between the mover's HEAD and its PUT "+
		"is overwritten with the source's older bytes, after the client was told 200, and nothing reports it "+
		"(one round trip per copied object; ADR-0004 race 2). "+
		"Quiesce writers for the mover run, or ramp to 1 and run the mover at low write volume; see docs/migrating.md", key, cluster)
}

// RefuseLostWriteWindow is the refusal both commands give without the flag.
func RefuseLostWriteWindow(key, cluster string) error {
	return errors.New(LostWriteWindow(key, cluster) + ". To start anyway, re-run with --" + AcceptLostWriteWindowFlag)
}
