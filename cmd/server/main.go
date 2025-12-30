package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/xmh1011/go-kv/pkg/config"
	"github.com/xmh1011/go-kv/pkg/log"
	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/storage"
	"github.com/xmh1011/go-kv/pkg/transport"
	"github.com/xmh1011/go-kv/raft"
)

var (
	configPath string
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "kv-server",
		Short: "A simple Raft KV server implementation",
		RunE:  runServer,
	}

	rootCmd.Flags().StringVarP(&configPath, "config", "c", "./conf/config.yaml", "Path to configuration file")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runServer(_ *cobra.Command, _ []string) error {
	// 1. 初始化配置
	if err := config.Init(configPath); err != nil {
		return fmt.Errorf("failed to initialize config: %w", err)
	}
	cfg := config.GetConfig()

	// 2. 初始化logger
	log.Init(cfg.Log)

	// 3. 创建并启动服务器
	srv, err := NewServer(cfg)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
		return fmt.Errorf("failed to create server: %w", err)
	}

	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
		return fmt.Errorf("failed to start server: %w", err)
	}

	waitForSignal(srv)

	return nil
}

// Server represents the Raft server instance
type Server struct {
	config config.AppConfig
	raft   *raft.Raft
}

// NewServer creates a new Server instance
func NewServer(cfg config.AppConfig) (*Server, error) {
	// 1. Parse peers
	peerMap := make(map[int]string)
	peerIDs := make([]int, 0)
	for _, p := range cfg.Raft.Peers {
		peerMap[p.ID] = p.Address
		peerIDs = append(peerIDs, p.ID)
	}

	myAddr, ok := peerMap[cfg.Raft.ID]
	if !ok {
		return nil, fmt.Errorf("my ID %d not found in peers list", cfg.Raft.ID)
	}

	// 2. Initialize storage
	store, stateMachine, err := storage.NewStorage(cfg.Raft.Engine, cfg.Raft.DataDir, cfg.Raft.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	// 3. Initialize transport
	trans, err := transport.NewTransport(cfg.Raft.Transport, myAddr)
	if err != nil {
		if closeErr := store.Close(); closeErr != nil {
			log.Errorf("Failed to close store: %v", closeErr.Error())
		}
		return nil, fmt.Errorf("failed to initialize transport: %w", err)
	}
	trans.SetPeers(peerMap)

	// 4. Create Raft node
	commitChan := make(chan param.CommitEntry, 100)
	rf := raft.NewRaft(cfg.Raft.ID, peerIDs, store, stateMachine, trans, commitChan)

	return &Server{
		config: cfg,
		raft:   rf,
	}, nil
}

// Start starts the Raft server components
func (s *Server) Start() error {
	// Register Raft to transport
	s.raft.Transport().RegisterRaft(s.raft)

	// Start transport service
	go func() {
		log.Infof("Starting %s transport service on %s", s.config.Raft.Transport, s.raft.Transport().Addr())
		if err := s.raft.Transport().Start(); err != nil {
			log.Fatalf("Failed to start transport service: %v", err)
		}
	}()

	// Start Raft node
	go s.raft.Run()

	// Handle committed entries
	go s.handleCommits()

	log.Infof("Raft node %d started", s.config.Raft.ID)
	return nil
}

// Stop stops the Raft server
func (s *Server) Stop() {
	log.Info("Shutting down...")
	s.raft.Stop()
	if err := s.raft.Transport().Close(); err != nil {
		log.Errorf("Failed to close transport: %s", err.Error())
	}
	if s.raft.Storage() != nil {
		if err := s.raft.Storage().Close(); err != nil {
			log.Errorf("Failed to close store: %s", err.Error())
		}
	}
	log.Infof("Node stopped")
}

func (s *Server) handleCommits() {
	for entry := range s.raft.CommitChan() {
		log.Debugf("Node %d committed entry: index=%d term=%d command=%v", s.config.Raft.ID, entry.Index, entry.Term, entry.Command)
	}
}

func waitForSignal(srv *Server) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	srv.Stop()
}
