package viper_test

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	vprovider "github.com/KaymeKaydex/go-vshard-router/providers/viper"
)

func TestProvider_Close(t *testing.T) {
	require.NotPanics(t, (&vprovider.Provider{}).Close)
}

func TestNewProviderNilPanic(t *testing.T) {
	require.Panics(t, func() {
		vprovider.NewProvider(nil)
	})
}

func TestNewProviderDirect(t *testing.T) {
	v := viper.New()

	v.AddConfigPath("test/")
	v.SetConfigName("config-direct")
	v.SetConfigType("yaml")

	err := v.ReadInConfig()
	require.NoError(t, err)

	provider := vprovider.NewProvider(v)

	require.NotNil(t, provider)
}

func TestNewProviderSub(t *testing.T) {
	v := viper.New()

	v.AddConfigPath("test/")
	v.SetConfigName("config-sub")
	v.SetConfigType("yaml")

	err := v.ReadInConfig()
	require.NoError(t, err)

	v = v.Sub("supbpath")

	provider := vprovider.NewProvider(v)

	require.NotNil(t, provider)
}
