package main

import (
	"fmt"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/huytran2000-hcmus/proglog/internal/agent"
	"github.com/huytran2000-hcmus/proglog/internal/config"
)

var version string

type cli struct {
	cfg cfg
}

type cfg struct {
	agent.Config
	ServerTLSConfig config.TLSConfig
	PeerTLSConfig   config.TLSConfig
	Version         bool
}

func (c *cli) run(cmd *cobra.Command, args []string) error {
	if c.cfg.Version {
		fmt.Println(version)
		return nil
	}

	agent, err := agent.New(c.cfg.Config)
	if err != nil {
		return fmt.Errorf("create new agent: %w", err)
	}

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	<-sigC
	return agent.Shutdown()
}

func (c *cli) setupConfig(cmd *cobra.Command, args []string) error {
	var err error
	configFile, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return fmt.Errorf("get config file path from flag: %w", err)
	}

	viper.SetConfigFile(configFile)
	if err := viper.ReadInConfig(); err != nil {
		_, ok := err.(viper.ConfigFileNotFoundError)
		if !ok {
			return fmt.Errorf("read config file: %w", err)
		}
	}

	c.cfg.Version = viper.GetBool("version")

	c.cfg.DataDir = viper.GetString("data-dir")
	c.cfg.NodeName = viper.GetString("node-name")
	c.cfg.BindAddr = viper.GetString("bind-addr")
	c.cfg.RPCPort = viper.GetInt("rpc-port")
	c.cfg.StartPointAddrs = viper.GetStringSlice("start-join-addrs")
	c.cfg.Bootstrap = viper.GetBool("bootstrap")
	c.cfg.ACLModelFile = viper.GetString("acl-mode-file")
	c.cfg.ACLPolicyFile = viper.GetString("acl-policy-file")
	c.cfg.ServerTLSConfig.CertFile = viper.GetString("server-tls-cert-file")
	c.cfg.ServerTLSConfig.KeyFile = viper.GetString("server-tls-key-file")
	c.cfg.ServerTLSConfig.CAFile = viper.GetString("server-tls-ca-file")
	c.cfg.PeerTLSConfig.CertFile = viper.GetString("peer-tls-cert-file")
	c.cfg.PeerTLSConfig.KeyFile = viper.GetString("peer-tls-key-file")
	c.cfg.PeerTLSConfig.CAFile = viper.GetString("peer-tls-ca-file")

	if c.cfg.ServerTLSConfig.CertFile != "" && c.cfg.ServerTLSConfig.KeyFile != "" {
		c.cfg.ServerTLSConfig.IsServer = true
		c.cfg.Config.ServerTLSConfig, err = config.SetupTLSConfig(c.cfg.ServerTLSConfig)
	}
	if err != nil {
		return fmt.Errorf("set up server tls config: %w", err)
	}
	if c.cfg.PeerTLSConfig.CertFile != "" && c.cfg.ServerTLSConfig.KeyFile != "" {
		c.cfg.Config.PeerTLSConfig, err = config.SetupTLSConfig(c.cfg.ServerTLSConfig)
	}
	if err != nil {
		return fmt.Errorf("set up peer tls config: %w", err)
	}

	return nil
}

func setupFlags(cmd *cobra.Command) error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("get hostname: %w", err)
	}

	cmd.Flags().String("config-file", "", "Path to config file")

	dataDir := path.Join(os.TempDir(), "proglog")
	cmd.Flags().String("data-dir", dataDir, "Directory to store log and other data")
	cmd.Flags().String("node-name", hostname, "Unique server ID.")
	cmd.Flags().String("bind-addr", "127.0.0.1:8401", "Address to bind Serf on.")
	cmd.Flags().Int("rpc-port", 8400, "Port for RPC clients (and Raft) connections.")
	cmd.Flags().StringSlice("start-join-addrs", nil, "Serf addresses to join.")
	cmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster.")
	cmd.Flags().String("acl-model-file", "", "Path to ACL model.")
	cmd.Flags().String("acl-policy-file", "", "Path to ACL policy.")
	cmd.Flags().String("server-tls-cert-file", "", "Path to server tls cert.")
	cmd.Flags().String("server-tls-key-file", "", "Path to server tls key.")
	cmd.Flags().String("server-tls-ca-file", "", "Path to server certificate authority.")
	cmd.Flags().String("peer-tls-cert-file", "", "Path to peer tls cert.")
	cmd.Flags().String("peer-tls-key-file", "", "Path to peer tls key.")
	cmd.Flags().String("peer-tls-ca-file", "", "Path to peer certificate authority.")

	cmd.Flags().Bool("version", false, "Print the version")

	return viper.BindPFlags(cmd.Flags())
}
