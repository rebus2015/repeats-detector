package worker

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRetryCounters(t *testing.T) {
	for _, h := range []amqp.Table{{"x-delivery-count": int64(5)}, {"x-acquired-count": int64(5), "x-delivery-count": int64(0)}} {
		if deliveryAttempts(h) != 5 {
			t.Fatal("retry limit ignored", h)
		}
	}
}
