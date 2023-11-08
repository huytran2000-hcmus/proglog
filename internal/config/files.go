package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile               = ConfigFile("ca.pem")
	ServerCertFile       = ConfigFile("server.pem")
	ServerKeyFile        = ConfigFile("server-key.pem")
	RootClientCertFile   = ConfigFile("root-client.pem")
	RootClientKeyFile    = ConfigFile("root-client-key.pem")
	NobodyClientCertFile = ConfigFile("nobody-client.pem")
	NobodyClientKeyFile  = ConfigFile("nobody-client-key.pem")
	ACLModelFile         = ConfigFile("model.conf")
	ACLPolicyFile        = ConfigFile("policy.csv")
)

func ConfigFile(filename string) string {
	dir := os.Getenv("CONFIG_DIR")
	if dir != "" {
		return filepath.Join(dir, filename)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}

	return filepath.Join(homeDir, ".proglog", filename)
}
