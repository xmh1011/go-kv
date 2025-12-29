package log

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/natefinch/lumberjack"
	"github.com/sirupsen/logrus"
)

const (
	errorLogFileSubfix = "wf"
)

var logger *logrus.Logger

func init() {
	// 默认初始化为输出到控制台
	logger = logrus.New()
	logger.SetOutput(os.Stdout)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:          true,
		TimestampFormat:        time.DateTime,
		DisableLevelTruncation: true, // 防止日志级别被截断
	})
	logger.SetLevel(logrus.InfoLevel)
}

// Config 日志配置
type Config struct {
	Filename   string `mapstructure:"filename"`    // 日志文件路径
	MaxSize    int    `mapstructure:"max_size"`    // 单个日志文件最大大小（MB）
	MaxBackups int    `mapstructure:"max_backups"` // 保留的旧日志文件最大数量
	MaxAge     int    `mapstructure:"max_age"`     // 保留的旧日志文件最大天数
	Compress   bool   `mapstructure:"compress"`    // 是否压缩旧日志文件
	Level      string `mapstructure:"level"`       // 日志级别 (debug, info, warn, error, fatal, panic)
	Console    bool   `mapstructure:"console"`     // 是否同时输出到控制台
}

// Init 初始化日志
func Init(cfg Config) {
	// 设置日志级别
	level, err := logrus.ParseLevel(cfg.Level)
	if err != nil {
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)

	// 设置格式
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:          true,
		TimestampFormat:        time.DateTime,
		DisableLevelTruncation: true,
	})

	// 1. 配置常规日志输出 (Info, Warn, Debug)
	// 如果没有配置 ErrorFilename，则所有日志都输出到 Filename
	// 如果配置了 ErrorFilename，则 Filename 只包含非错误日志（或者包含所有日志，取决于需求，通常包含所有日志比较方便排查上下文，这里我们让它包含所有日志）
	var writers []io.Writer

	if cfg.Console {
		writers = append(writers, os.Stdout)
	}

	if cfg.Filename != "" {
		fileWriter := &lumberjack.Logger{
			Filename:   cfg.Filename,
			MaxSize:    cfg.MaxSize,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAge,
			Compress:   cfg.Compress,
		}
		writers = append(writers, fileWriter)
	}

	// 设置 logger 的主输出
	// 注意：如果配置了 ErrorFilename，我们通过 Hook 来处理错误日志的独立写入
	// 主输出依然会包含所有级别的日志（除非我们在 Hook 中阻止，但 logrus 的 Hook 不会阻止主输出）
	// 这样设计的好处是 app.log 是全量的，error.log 是纯错误的，方便查看。
	if len(writers) > 0 {
		logger.SetOutput(io.MultiWriter(writers...))
	} else {
		logger.SetOutput(os.Stdout)
	}

	// 2. 配置错误日志独立输出 Hook
	errorWriter := &lumberjack.Logger{
		Filename:   fmt.Sprintf("%s.%s", cfg.Filename, errorLogFileSubfix),
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxAge,
		Compress:   cfg.Compress,
	}

	// 添加 Hook
	logger.AddHook(&ErrorHook{
		Writer:    errorWriter,
		LogLevels: []logrus.Level{logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel},
		Formatter: logger.Formatter,
	})
}

// ErrorHook 是一个自定义的 logrus Hook，用于将错误日志写入单独的文件
type ErrorHook struct {
	Writer    io.Writer
	LogLevels []logrus.Level
	Formatter logrus.Formatter
}

func (h *ErrorHook) Levels() []logrus.Level {
	return h.LogLevels
}

func (h *ErrorHook) Fire(entry *logrus.Entry) error {
	// 使用相同的格式化器格式化日志条目
	line, err := h.Formatter.Format(entry)
	if err != nil {
		return err
	}
	// 写入错误日志文件
	_, err = h.Writer.Write(line)
	return err
}

// Debug 输出 Debug 级别日志
func Debug(args ...interface{}) {
	logger.Debug(args...)
}

// Debugf 输出 Debug 级别格式化日志
func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

// Info 输出 Info 级别日志
func Info(args ...interface{}) {
	logger.Info(args...)
}

// Infof 输出 Info 级别格式化日志
func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

// Warn 输出 Warn 级别日志
func Warn(args ...interface{}) {
	logger.Warn(args...)
}

// Warnf 输出 Warn 级别格式化日志
func Warnf(format string, args ...interface{}) {
	logger.Warnf(format, args...)
}

// Error 输出 Error 级别日志
func Error(args ...interface{}) {
	logger.Error(args...)
}

// Errorf 输出 Error 级别格式化日志
func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

// Fatal 输出 Fatal 级别日志，并退出程序
func Fatal(args ...interface{}) {
	logger.Fatal(args...)
}

// Fatalf 输出 Fatal 级别格式化日志，并退出程序
func Fatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)
}

// Panic 输出 Panic 级别日志，并触发 panic
func Panic(args ...interface{}) {
	logger.Panic(args...)
}

// Panicf 输出 Panic 级别格式化日志，并触发 panic
func Panicf(format string, args ...interface{}) {
	logger.Panicf(format, args...)
}

// WithFields 添加字段
func WithFields(fields map[string]interface{}) *logrus.Entry {
	return logger.WithFields(fields)
}

// GetLogger 获取原始 logger 实例
func GetLogger() *logrus.Logger {
	return logger
}
