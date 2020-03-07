package config

import (
	"io"
	"log/syslog"
	"os"

	"github.com/sirupsen/logrus"
)

func GetLogger(verbose bool) (*logrus.Logger, error) {
	var log = logrus.New()
	sysWriter, err := syslog.New(syslog.LOG_INFO, "sentinel")
	if verbose {
		log.Level = logrus.DebugLevel
		multiWriter := io.MultiWriter(os.Stderr, sysWriter)
		log.Out = multiWriter
	} else {
		log.Level = logrus.InfoLevel
		log.Out = sysWriter
	}
	return log, err
}
