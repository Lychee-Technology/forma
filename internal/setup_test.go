package internal

import (
	"os"
	"testing"

	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.DebugLevel), // DEBUG 级别
		Development:      true,                                 // 更易读的 console 输出
		Encoding:         "console",                            // 用 console 更易读
		EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
		OutputPaths:      []string{"stdout"}, // 输出到标准输出
		ErrorOutputPaths: []string{"stderr"},
	}

	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	// 2️⃣ 替换全局 logger
	zap.ReplaceGlobals(logger)

	// 3️⃣ 运行测试
	exitCode := m.Run()

	// 4️⃣ 退出（Important: 返回正确的 exit code）
	os.Exit(exitCode)
}
