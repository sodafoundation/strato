package config

type CredentialStore struct {
	Credential string `conf:"credential,username:password@tcp(ip:port)/dbname"`
	Driver     string `conf:"driver,keystone"`
	Host   string `conf:"host,localhost:8089"`
}
