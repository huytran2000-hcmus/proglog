package server

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	Developemnt mode = iota
	Production
)

const (
	Debug level = iota
	Info
	Warn
	Error
	Default = Info
)

type (
	mode  int
	level int
)

func newLogger(mode mode, name string, minLevel level, filePaths ...string) (*zap.Logger, error) {
	switch mode {
	case Production:
		return newLoggerWithConfig(name, minLevel, productionEncoderConfig(), filePaths...)
	default:
		return newLoggerWithConfig(name, minLevel, developmentEncoderConfig(), filePaths...)
	}
}

func newLoggerWithConfig(name string, minLevel level, cfg zapcore.EncoderConfig, paths ...string) (*zap.Logger, error) {
	consoleCore := consoleCore(minLevel, cfg)
	fileCore, err := pathCore(minLevel, cfg, paths...)
	if err != nil {
		return nil, err
	}

	logger := zap.New(zapcore.NewTee(consoleCore, fileCore))

	logger = logger.Named(name)

	return logger, nil
}

func pathCore(level level, encCfg zapcore.EncoderConfig, paths ...string) (zapcore.Core, error) {
	encoder := zapcore.NewJSONEncoder(encCfg)

	writer, _, err := zap.Open(paths...)
	if err != nil {
		return nil, fmt.Errorf("open logger file: %w", err)
	}

	return zapcore.NewCore(encoder, writer, zapLevel(level)), nil
}

func consoleCore(minLevel level, encCfg zapcore.EncoderConfig) zapcore.Core {
	encoder := zapcore.NewConsoleEncoder(encCfg)
	outputCore := zapcore.NewCore(encoder, os.Stdout, infoPriority(minLevel))
	errCore := zapcore.NewCore(encoder, os.Stderr, errorPriority(minLevel))

	return zapcore.NewTee(outputCore, errCore)
}

func developmentEncoderConfig() zapcore.EncoderConfig {
	cfg := zap.NewDevelopmentEncoderConfig()

	cfg.NameKey = "logger"
	cfg.EncodeName = zapcore.FullNameEncoder

	cfg.MessageKey = "message"

	cfg.TimeKey = "timestamp"
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder

	cfg.LevelKey = "level"
	cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder

	cfg.CallerKey = "caller"
	cfg.EncodeCaller = zapcore.ShortCallerEncoder

	cfg.StacktraceKey = "stacktrace"

	cfg.FunctionKey = zapcore.OmitKey

	return cfg
}

func productionEncoderConfig() zapcore.EncoderConfig {
	cfg := zap.NewProductionEncoderConfig()

	cfg.NameKey = "logger"
	cfg.EncodeName = zapcore.FullNameEncoder

	cfg.MessageKey = "msg"

	cfg.TimeKey = "ts"
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder

	cfg.LevelKey = "lv"
	cfg.EncodeLevel = zapcore.CapitalLevelEncoder

	cfg.CallerKey = "caller"
	cfg.EncodeCaller = zapcore.ShortCallerEncoder

	cfg.StacktraceKey = "stacktrace"

	cfg.FunctionKey = zapcore.OmitKey

	return cfg
}

func infoPriority(minLevel level) zap.LevelEnablerFunc {
	minLV := zapLevel(minLevel)
	return func(lv zapcore.Level) bool {
		return lv >= minLV && lv < zap.ErrorLevel
	}
}

func errorPriority(minLevel level) zap.LevelEnablerFunc {
	minLV := zapLevel(minLevel)
	return func(lv zapcore.Level) bool {
		return lv >= minLV && lv >= zap.ErrorLevel
	}
}

func zapLevel(level level) zapcore.Level {
	switch level {
	case Debug:
		return zap.DebugLevel
	case Error:
		return zap.ErrorLevel
	default:
		return zap.InfoLevel
	}
}
