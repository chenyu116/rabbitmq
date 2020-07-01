package rabbitmq

import (
	"crypto/tls"
	"crypto/x509"
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
	UseTls              bool
	ReplyConfirmTimeout time.Duration
	Amqp                amqp.Config
}

func NewConfig() *Config {
	return new(Config)
}

func NewTlsConfig(caFile, certFile, keyFile string) *tls.Config {
	cfg := new(tls.Config)
	cfg.RootCAs = x509.NewCertPool()
	if ca, err := ioutil.ReadFile(caFile); err == nil {
		cfg.RootCAs.AppendCertsFromPEM(ca)
	}

	// Move the client cert and key to a location specific to your application
	// and load them here.

	if cert, err := tls.LoadX509KeyPair(certFile, keyFile); err == nil {
		cfg.Certificates = append(cfg.Certificates, cert)
	}
	return cfg
}
