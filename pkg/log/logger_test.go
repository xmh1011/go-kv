package log

import (
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestInitDefaultsToWarnLevel(t *testing.T) {
	t.Setenv(envLogLevel, "")

	Init(Config{})

	assert.Equal(t, logrus.WarnLevel, GetLogger().GetLevel())
}

func TestInitUsesConfiguredLevel(t *testing.T) {
	t.Setenv(envLogLevel, "")

	Init(Config{Level: "info"})

	assert.Equal(t, logrus.InfoLevel, GetLogger().GetLevel())
}

func TestInitEnvironmentLevelOverridesConfig(t *testing.T) {
	t.Setenv(envLogLevel, "debug")

	Init(Config{Level: "warn"})

	assert.Equal(t, logrus.DebugLevel, GetLogger().GetLevel())
}

func TestInitReplacesErrorHooks(t *testing.T) {
	t.Setenv(envLogLevel, "")
	cfg := Config{
		Filename:   filepath.Join(t.TempDir(), "go-kv.log"),
		MaxSize:    1,
		MaxBackups: 1,
		MaxAge:     1,
	}

	Init(cfg)
	Init(cfg)

	assert.Len(t, GetLogger().Hooks[logrus.ErrorLevel], 1)
}
