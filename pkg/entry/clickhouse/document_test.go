package clickhouse_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/ukrocks007/fluent-bit-clickhouse/pkg/entry/clickhouse"
	"github.com/ukrocks007/fluent-bit-clickhouse/pkg/log"
)

func stringEntry(value string) []uint8 {
	return []uint8(value)
}

func timeEntry(value time.Time) []uint8 {
	v, err := value.MarshalText()
	Expect(err).ToNot(HaveOccurred())

	return []uint8(v)
}

var _ = Describe("Convert document", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.TODO()

		logger, err := log.New(log.OutputPlugin, "test")
		Expect(err).ToNot(HaveOccurred())

		ctx = log.WithLogger(ctx, logger)
	})

	Context("With all fields", func() {
		var entry map[interface{}]interface{}

		BeforeEach(func() {
			entry = map[interface{}]interface{}{
				clickhouse.LogKey:            stringEntry("log"),
				clickhouse.StreamKey:         stringEntry("stream"),
				clickhouse.TimeKey:           timeEntry(time.Now()),
				clickhouse.JobExecutionIDKey: stringEntry("jobExecutionID"),
				clickhouse.ProjectIDKey:      stringEntry("projectID"),
				clickhouse.CustomerKey:       stringEntry("customer"),
				clickhouse.PlatformIDKey:     stringEntry("platformID"),
			}
		})

		It("Should work", func() {
			d, err := clickhouse.Convert(ctx, time.Now(), entry)
			Expect(err).ToNot(HaveOccurred())
			Expect(d).ToNot(BeNil())

			Expect(d.Customer).To(BeEquivalentTo(entry[clickhouse.CustomerKey]))
		})
	})

	Context("With missing field", func() {
		var entry map[interface{}]interface{}

		BeforeEach(func() {
			entry = map[interface{}]interface{}{
				clickhouse.LogKey:            stringEntry("log"),
				clickhouse.StreamKey:         stringEntry("stream"),
				clickhouse.TimeKey:           timeEntry(time.Now()),
				clickhouse.JobExecutionIDKey: stringEntry("jobExecutionID"),
				clickhouse.ProjectIDKey:      stringEntry("projectID"),
				clickhouse.CustomerKey:       stringEntry("customer"),
				clickhouse.PlatformIDKey:     stringEntry("platformID"),
			}
		})

		DescribeTable("Field", func(field string, ok bool) {
			delete(entry, field)

			d, err := clickhouse.Convert(ctx, time.Now(), entry)

			if ok {
				Expect(err).ToNot(HaveOccurred())
				Expect(d).ToNot(BeNil())
			} else {
				Expect(err).To(HaveOccurred())
			}
		},
			Entry("log message", clickhouse.LogKey, true),
			Entry("stream", clickhouse.StreamKey, true),
			Entry("time", clickhouse.TimeKey, true),
			Entry("job ID", clickhouse.JobExecutionIDKey, false),
			Entry("project ID", clickhouse.ProjectIDKey, false),
			Entry("customer", clickhouse.CustomerKey, false),
			Entry("platform ID", clickhouse.PlatformIDKey, false),
		)
	})
})
