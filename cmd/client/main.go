package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"

	"github.com/xmh1011/go-kv/pkg/client"
	"github.com/xmh1011/go-kv/pkg/config"
	"github.com/xmh1011/go-kv/pkg/param"
	"github.com/xmh1011/go-kv/pkg/transport"
)

var (
	configPath string
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "kv-client",
		Short: "A client for the go-kv cluster",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if err := config.Init(configPath); err != nil {
				log.Fatalf("Failed to initialize config: %v", err)
			}
		},
	}

	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "./conf/config.yaml", "Path to configuration file")

	var getCmd = &cobra.Command{
		Use:   "get <key>",
		Short: "Get a value by key",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			key := args[0]
			runCommand("get", key, "")
		},
	}

	var setCmd = &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a key-value pair",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			key := args[0]
			value := args[1]
			runCommand("set", key, value)
		},
	}

	var deleteCmd = &cobra.Command{
		Use:   "delete <key>",
		Short: "Delete a value by key",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			key := args[0]
			runCommand("delete", key, "")
		},
	}

	rootCmd.AddCommand(getCmd, setCmd, deleteCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runCommand(op, key, value string) {
	cfg := config.GetConfig()

	// 1. Parse peers from config
	peerMap := make(map[int]string)
	for _, p := range cfg.Raft.Peers {
		peerMap[p.ID] = p.Address
	}

	if len(peerMap) == 0 {
		log.Fatalf("No peers found in configuration")
	}

	// 2. Initialize network transport
	// Use a random port for the client
	clientAddr := "127.0.0.1:0"
	trans, err := transport.NewClientTransport(clientAddr, cfg.Raft.Transport)
	if err != nil {
		log.Fatalf("Failed to initialize transport: %v", err)
	}
	trans.SetPeers(peerMap)
	defer trans.Close()

	// 3. Create client instance
	c := client.NewClient(peerMap, trans)

	// 4. Construct command
	kvCmd := param.KVCommand{
		Op:    param.StringToOpType(op),
		Key:   key,
		Value: value,
	}
	cmdBytes, err := json.Marshal(kvCmd)
	if err != nil {
		log.Fatalf("Failed to marshal command: %v", err)
	}

	// 5. Send command
	log.Printf("Sending command: %s key=%s val=%s (via %s)", op, key, value, cfg.Raft.Transport)
	result, success := c.SendCommand(cmdBytes)

	if success {
		if op == "get" {
			if resultStr, ok := result.(string); ok {
				fmt.Printf("✅ Success! Value: %s\n", resultStr)
			} else {
				fmt.Printf("✅ Success! Result: %v\n", result)
			}
		} else {
			fmt.Printf("✅ Success! Result: %v\n", result)
		}
	} else {
		fmt.Printf("❌ Failed to execute command.\n")
	}
}
