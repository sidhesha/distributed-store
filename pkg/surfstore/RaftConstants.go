package surfstore

import (
	"fmt"
)

var ErrServerCrashedUnreachable = fmt.Errorf("server is crashed or unreachable")
var ErrServerCrashed = fmt.Errorf("server is crashed")
var ErrNotLeader = fmt.Errorf("server is not the leader")
var ErrPlzSendOlder = fmt.Errorf("plz send older entry")
