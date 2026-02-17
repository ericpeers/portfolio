package services

import (
	"time"

	log "github.com/sirupsen/logrus"
)

func TrackTime(funcName string, start time.Time) {
	elapsed := time.Since(start)
	log.Debugf("%s took %d ms", funcName, elapsed.Milliseconds())
}
