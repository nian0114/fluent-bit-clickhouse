package clickhouse_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestClickhouse(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Clickhouse Suite")
}
