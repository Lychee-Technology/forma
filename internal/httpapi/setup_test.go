package httpapi

import (
	"io"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// discardLogger is the global logger for a test that does not read the log:
// enabled at every level, so it behaves like a deployed logger, with its
// output thrown away. zap.NewNop would not do: errorid.Log issues an error_id
// only for a line its logger will write, so under a no-op logger every body
// comes back without one, which is not what a deployment answers.
func discardLogger() *zap.Logger {
	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	return zap.New(zapcore.NewCore(encoder, zapcore.AddSync(io.Discard), zap.DebugLevel))
}
