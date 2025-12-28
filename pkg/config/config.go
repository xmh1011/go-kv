package config

import (
	"fmt"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"

	"github.com/xmh1011/go-kv/pkg/log"
)

// Conf 是全局配置实例
var Conf AppConfig

// --- 配置项键名常量 ---
const (
	// Log
	KeyLogFilename   = "log.filename"
	KeyLogLevel      = "log.level"
	KeyLogMaxSize    = "log.max_size"
	KeyLogMaxBackups = "log.max_backups"
	KeyLogMaxAge     = "log.max_age"
	KeyLogCompress   = "log.compress"
	KeyLogConsole    = "log.console"

	// Raft
	KeyRaftID                = "raft.id"
	KeyRaftTransport         = "raft.transport"
	KeyRaftEngine            = "raft.engine"
	KeyRaftDataDir           = "raft.data_dir"
	KeyRaftHeartbeatTimeout  = "raft.heartbeat_timeout"
	KeyRaftElectionTimeout   = "raft.election_timeout"
	KeyRaftSnapshotThreshold = "raft.snapshot_threshold"
	KeyRaftPeers             = "raft.peers"

	// LSM
	KeyLSMMaxMemTableSize   = "lsm.max_mem_table_size"
	KeyLSMMaxSSTableSize    = "lsm.max_sstable_size"
	KeyLSMMaxIMemTableCount = "lsm.max_imem_table_count"
	KeyLSMMinSSTableLevel   = "lsm.min_sstable_level"
	KeyLSMMaxSSTableLevel   = "lsm.max_sstable_level"
	KeyLSMLevelSizeBase     = "lsm.level_size_base"
)

// --- 默认值常量 ---
const (
	DefaultRaftID            = 1
	DefaultDataDir           = "./data"
	DefaultLogFilename       = "go-kv.log"
	DefaultLogLevel          = "info"
	DefaultLogMaxSize        = 100 // MB
	DefaultLogMaxBackups     = 5
	DefaultLogMaxAge         = 30 // days
	DefaultRaftTransport     = "grpc"
	DefaultRaftEngine        = "lsm"
	DefaultHeartbeatTimeout  = 50 * time.Millisecond
	DefaultElectionTimeout   = 200 * time.Millisecond
	DefaultSnapshotThreshold = 8192
	DefaultMaxMemTableSize   = 2 * 1024 * 1024 // 2MB
	DefaultMaxSSTableSize    = 2 * 1024 * 1024 // 2MB
	DefaultMaxIMemTableCount = 10
	DefaultMinSSTableLevel   = 0
	DefaultMaxSSTableLevel   = 6
	DefaultLevelSizeBase     = 2
)

// AppConfig 是总配置结构体
type AppConfig struct {
	Log  log.Config `mapstructure:"log"`
	Raft RaftConfig `mapstructure:"raft"`
	LSM  LSMConfig  `mapstructure:"lsm"`
}

// RaftConfig 包含了 Raft 模块的配置
type RaftConfig struct {
	ID                int           `mapstructure:"id"`
	Transport         string        `mapstructure:"transport"`
	Engine            string        `mapstructure:"engine"`
	DataDir           string        `mapstructure:"data_dir"`
	HeartbeatTimeout  time.Duration `mapstructure:"heartbeat_timeout"`
	ElectionTimeout   time.Duration `mapstructure:"election_timeout"`
	SnapshotThreshold int           `mapstructure:"snapshot_threshold"`
	Peers             []PeerInfo    `mapstructure:"peers"`
}

// PeerInfo 描述了一个 Raft 集群中的对等节点
type PeerInfo struct {
	ID      int    `mapstructure:"id"`
	Address string `mapstructure:"address"`
}

// LSMConfig 包含了 LSM 引擎的配置
type LSMConfig struct {
	MaxMemTableSize   int `mapstructure:"max_mem_table_size"`
	MaxSSTableSize    int `mapstructure:"max_sstable_size"`
	MaxIMemTableCount int `mapstructure:"max_imem_table_count"`
	MinSSTableLevel   int `mapstructure:"min_sstable_level"`
	MaxSSTableLevel   int `mapstructure:"max_sstable_level"`
	LevelSizeBase     int `mapstructure:"level_size_base"`
}

func init() {
	setDefaults()
	// 初始化时加载默认配置到 Conf，确保在不调用 Init 的情况下也有默认值
	if err := viper.Unmarshal(&Conf); err != nil {
		fmt.Printf("Failed to unmarshal default config: %v\n", err)
	}
}

// Init 初始化配置
func Init(configPath string) error {
	// 1. 设置默认值 (虽然 init 已经调用过，但为了确保逻辑完整性，这里保留或移除均可，保留无害)
	setDefaults()

	// 2. 读取配置文件
	if configPath != "" {
		viper.SetConfigFile(configPath)
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("failed to read config file: %w", err)
		}
	} else {
		// 如果没有提供配置文件，Viper 会仅使用默认值
		log.Info("No config file provided, using default values.")
	}

	// 3. 解析配置到结构体
	if err := viper.Unmarshal(&Conf); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// 4. 初始化日志
	log.Init(Conf.Log)
	log.Info("Config loaded successfully")

	// 5. 监听配置文件变化（热更新）
	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		log.Infof("Config file changed: %s", e.Name)
		if err := viper.Unmarshal(&Conf); err != nil {
			log.Errorf("Failed to re-unmarshal config: %v", err)
			return
		}
		log.Init(Conf.Log)
		log.Info("Config reloaded and applied")
	})

	return nil
}

func setDefaults() {
	// Log
	viper.SetDefault(KeyLogFilename, DefaultLogFilename)
	viper.SetDefault(KeyLogLevel, DefaultLogLevel)
	viper.SetDefault(KeyLogMaxSize, DefaultLogMaxSize)
	viper.SetDefault(KeyLogMaxBackups, DefaultLogMaxBackups)
	viper.SetDefault(KeyLogMaxAge, DefaultLogMaxAge)
	viper.SetDefault(KeyLogCompress, true)
	viper.SetDefault(KeyLogConsole, true)

	// Raft
	viper.SetDefault(KeyRaftID, DefaultRaftID)
	viper.SetDefault(KeyRaftTransport, DefaultRaftTransport)
	viper.SetDefault(KeyRaftEngine, DefaultRaftEngine)
	viper.SetDefault(KeyRaftDataDir, DefaultDataDir)
	viper.SetDefault(KeyRaftHeartbeatTimeout, DefaultHeartbeatTimeout)
	viper.SetDefault(KeyRaftElectionTimeout, DefaultElectionTimeout)
	viper.SetDefault(KeyRaftSnapshotThreshold, DefaultSnapshotThreshold)
	viper.SetDefault(KeyRaftPeers, []PeerInfo{})

	// LSM
	viper.SetDefault(KeyLSMMaxMemTableSize, DefaultMaxMemTableSize)
	viper.SetDefault(KeyLSMMaxSSTableSize, DefaultMaxSSTableSize)
	viper.SetDefault(KeyLSMMaxIMemTableCount, DefaultMaxIMemTableCount)
	viper.SetDefault(KeyLSMMinSSTableLevel, DefaultMinSSTableLevel)
	viper.SetDefault(KeyLSMMaxSSTableLevel, DefaultMaxSSTableLevel)
	viper.SetDefault(KeyLSMLevelSizeBase, DefaultLevelSizeBase)
}

// GetConfig 获取配置副本
func GetConfig() AppConfig {
	return Conf
}
