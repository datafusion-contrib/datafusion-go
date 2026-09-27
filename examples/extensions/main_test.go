package main

import (
	datafusion "github.com/datafusion-contrib/datafusion-go"
	"github.com/datafusion-contrib/datafusion-go/providertest"
	"testing"
)

func TestEventsProvider(t *testing.T) {
	providertest.Run(t, func(t *testing.T) datafusion.TableProvider { return events{} })
}
