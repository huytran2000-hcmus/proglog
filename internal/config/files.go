package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile         = ConfigFile("ca.pem")
	ServerCertFile = ConfigFile("server.pem")
	ServerKeyFile  = ConfigFile("server-key.pem")
	ClientCertFile = ConfigFile("client.pem")
	ClientKeyFile  = ConfigFile("client-key.pem")
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
