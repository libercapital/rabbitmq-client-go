package rabbitmq

import (
	"context"
	"strconv"

	"github.com/libercapital/liber-logger-go/tracing"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const traceIDHeader = "x-datadog-trace-id"

// amqpHeadersCarrier adapts amqp.Table to the Datadog TextMapWriter/TextMapReader interfaces.
type amqpHeadersCarrier amqp.Table

var (
	_ tracer.TextMapWriter = amqpHeadersCarrier(nil)
	_ tracer.TextMapReader = amqpHeadersCarrier(nil)
)

func (c amqpHeadersCarrier) Set(key, val string) {
	c[key] = val
}

// ForeachKey iterates over string and []byte header values; other types cannot carry trace context and are skipped.
func (c amqpHeadersCarrier) ForeachKey(handler func(key, val string) error) error {
	for key, raw := range c {
		var val string

		switch v := raw.(type) {
		case string:
			val = v
		case []byte:
			val = string(v)
		default:
			continue
		}

		if err := handler(key, val); err != nil {
			return err
		}
	}

	return nil
}

// injectTraceHeaders returns headers with the trace context of the span in ctx.
// The caller's table is copied, never overwritten; without a span in ctx headers is returned untouched.
func injectTraceHeaders(ctx context.Context, headers amqp.Table) amqp.Table {
	span, ok := tracing.SpanFromContext(ctx)
	if !ok {
		return headers
	}

	out := make(amqp.Table, len(headers))
	for key, val := range headers {
		out[key] = val
	}

	_ = tracer.Inject(span.Context(), amqpHeadersCarrier(out))

	// consumers older than full propagation only read this header
	if _, ok := out[traceIDHeader]; !ok {
		out[traceIDHeader] = strconv.FormatUint(span.Context().TraceID(), 10)
	}

	return out
}
