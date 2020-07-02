package rabbitmq

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"github.com/streadway/amqp"
	"io/ioutil"
	"time"
)

type Exchange struct {
	Name string
	Kind string
}

type Config struct {
	HostPort            string
	Username            string
	Password            string
	Prefetch            int
	Exchanges           []Exchange
	QueuePrefix         string
	QueueName           string
	QueueAutoDelete     bool
	QueueEnable         bool
	UseTls              bool
	ReplyConfirmTimeout time.Duration
	Amqp                amqp.Config
}

func NewConfig() *Config {
	return new(Config)
}

func NewTlsConfig(caFile, certFile, keyFile, keyFilePassword string) *tls.Config {
	cfg := new(tls.Config)
	cfg.RootCAs = x509.NewCertPool()
	if ca, err := ioutil.ReadFile(caFile); err == nil {
		cfg.RootCAs.AppendCertsFromPEM(ca)
	}
	if keyFilePassword != "" {
		keyIn, err := ioutil.ReadFile(keyFile)
		if err == nil {
			// Decode and decrypt our PEM block
			decodedPEM, _ := pem.Decode([]byte(keyIn))
			decrypedPemBlock, err := x509.DecryptPEMBlock(decodedPEM, []byte(keyFilePassword))
			if err == nil {
				if cert, err := tls.LoadX509KeyPair(certFile, string(decrypedPemBlock)); err == nil {
					cfg.Certificates = append(cfg.Certificates, cert)
				}
			}
		}
	} else {
		if cert, err := tls.LoadX509KeyPair(certFile, keyFile); err == nil {
			cfg.Certificates = append(cfg.Certificates, cert)
		}
	}
	return cfg
}
