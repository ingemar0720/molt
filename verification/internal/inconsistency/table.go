package inconsistency

import (
	"github.com/cockroachdb/molt/dbconn"
	"github.com/cockroachdb/molt/verification/verifybase"
)

// MismatchingTableDefinition represents a missing table definition.
type MismatchingTableDefinition struct {
	ConnID dbconn.ID
	verifybase.DBTable
	Info string
}