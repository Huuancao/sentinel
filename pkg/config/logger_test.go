package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_GetLogger(t *testing.T) {
	verbose := true
	logger, err := GetLogger(verbose)
	require.Nil(t, err)
	require.NotNil(t, logger)
}
