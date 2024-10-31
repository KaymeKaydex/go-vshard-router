package vshard_router //nolint:revive

import (
	"context"

	"github.com/tarantool/go-tarantool/v2"
)

// go-tarantool writes logs by default to stderr. Stderr might be not available, or user might use syslog or logging into file.
// So we should implement logging interface and redirect go-tarantool logs to the user's logger.
type tarantoolOptsLogger struct {
	loggerf LogfProvider
	ctx     context.Context
}

// Does almost the same thing as defaultLogger in go-tarantool, but uses user provided logger instead of stdout logger.
// https://github.com/tarantool/go-tarantool/blob/592db69eed8649b82ce432b930c27daeee98c52f/connection.go#L90
func (l tarantoolOptsLogger) Report(event tarantool.ConnLogKind, conn *tarantool.Connection, v ...interface{}) {
	// We use safe type assertion (with ok check), because we don't rely on go-tarantools internal contract about "v...".
	// Otherwise, we could encounter an unexpected panic due to logging, if go-tarantool maintainers change contract about "v...".
	switch event {
	case tarantool.LogReconnectFailed:
		var reconnects uint
		var err error
		var reconnectsOk, errOk bool

		if len(v) >= 2 {
			reconnects, reconnectsOk = v[0].(uint)
			err, errOk = v[1].(error)
		}

		if reconnectsOk && errOk {
			l.loggerf.Errorf(l.ctx, "tarantool: reconnect (%d) to %s failed: %s", reconnects, conn.Addr(), err)
		} else {
			l.loggerf.Errorf(l.ctx, "tarantool: reconnect to %s failed (unexpected v... format): %+v", conn.Addr(), v)
		}
	case tarantool.LogLastReconnectFailed:
		var err error
		var errOk bool

		if len(v) >= 1 {
			err, errOk = v[0].(error)
		}

		if errOk {
			l.loggerf.Errorf(l.ctx, "tarantool: last reconnect to %s failed: %s, giving it up", conn.Addr(), err)
		} else {
			l.loggerf.Errorf(l.ctx, "tarantool: last reconnect to %s failed (unexpected v... format): %v+", conn.Addr(), v)
		}
	case tarantool.LogUnexpectedResultId:
		var header tarantool.Header
		var headerOk bool

		if len(v) >= 1 {
			header, headerOk = v[0].(tarantool.Header)
		}

		if headerOk {
			l.loggerf.Errorf(l.ctx, "tarantool: connection %s got unexpected resultId (%d) in response"+
				"(probably cancelled request)",
				conn.Addr(), header.RequestId)
		} else {
			l.loggerf.Errorf(l.ctx, "tarantool: connection %s got unexpected resultId in response"+
				"(probably cancelled request) (unexpected v... format): %+v",
				conn.Addr(), v)
		}
	case tarantool.LogWatchEventReadFailed:
		var err error
		var errOk bool

		if len(v) >= 1 {
			err, errOk = v[0].(error)
		}

		if errOk {
			l.loggerf.Errorf(l.ctx, "tarantool: unable to parse watch event: %s", err)
		} else {
			l.loggerf.Errorf(l.ctx, "tarantool: unable to parse watch event (unexpected v... format): %+v", v)
		}
	case tarantool.LogAppendPushFailed:
		var err error
		var errOk bool

		if len(v) >= 1 {
			err, errOk = v[0].(error)
		}

		if errOk {
			l.loggerf.Errorf(l.ctx, "tarantool: unable to append a push response: %s", err)
		} else {
			l.loggerf.Errorf(l.ctx, "tarantool: unable to append a push response (unexpected v... format): %+v", v)
		}
	default:
		l.loggerf.Errorf(l.ctx, "tarantool: unexpected event %d on conn %s, v...: %+v", event, conn, v)
	}
}
