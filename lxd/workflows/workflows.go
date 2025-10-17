package workflows

import (
	"github.com/canonical/lxd/lxd/state"
)

var StateFunc func() *state.State
