package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	liberlogger "github.com/libercapital/liber-logger-go"
	"github.com/libercapital/liber-logger-go/tracing"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/mocktracer"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

var consumerSpanConfig = tracing.SpanConfig{
	OperationName: "amqp.consume",
	SpanType:      "queue",
	ResourceName:  "my-queue",
	Tags:          tracing.SpanTags{"queue": "my-queue"},
}

type discardLogger struct{}

func (discardLogger) Log(string) {}

// startRealTracer starts the real Datadog tracer (128-bit trace ids, x-datadog-tags) pointed at an unreachable agent.
func startRealTracer(t *testing.T) {
	t.Setenv("DD_INSTRUMENTATION_TELEMETRY_ENABLED", "false")
	t.Setenv("DD_TRACE_STARTUP_LOGS", "false")
	t.Setenv("DD_REMOTE_CONFIGURATION_ENABLED", "false")
	t.Setenv("DD_TRACE_128_BIT_TRACEID_GENERATION_ENABLED", "true")

	ddtracer.Start(
		ddtracer.WithService("rabbitmq-client-test"),
		ddtracer.WithAgentAddr("127.0.0.1:1"),
		ddtracer.WithLogger(discardLogger{}),
		ddtracer.WithLogStartup(false),
	)
	t.Cleanup(ddtracer.Stop)
}

// consumeWithHandler runs processMessage with a hand-built delivery and returns the ctx the handler received.
func consumeWithHandler(t *testing.T, cfg tracing.SpanConfig, headers amqp.Table) context.Context {
	t.Helper()

	var received context.Context

	processMessage(cfg, func(ctx context.Context, _ IncomingEventMessage) bool {
		received = ctx
		return true
	}, ConsumerArgs{}, amqp.Delivery{Headers: headers, Body: []byte(`{}`)})

	require.NotNil(t, received, "handler was not called")

	return received
}

func logFields(t *testing.T, ctx context.Context) map[string]interface{} {
	t.Helper()

	fields, ok := ctx.Value(liberlogger.LogFieldsKey{}).(map[string]interface{})
	require.True(t, ok, "log fields missing from ctx")

	return fields
}

func finishedSpan(t *testing.T, mt mocktracer.Tracer, spanID uint64) mocktracer.Span {
	t.Helper()

	for _, s := range mt.FinishedSpans() {
		if s.SpanID() == spanID {
			return s
		}
	}

	require.FailNow(t, "span not finished", "span id %d", spanID)

	return nil
}

func finishedSpanByName(t *testing.T, mt mocktracer.Tracer, name string) mocktracer.Span {
	t.Helper()

	for _, s := range mt.FinishedSpans() {
		if s.OperationName() == name {
			return s
		}
	}

	require.FailNow(t, "span not finished", "operation %s", name)

	return nil
}

// fakeChannel records publishings; with replies set it answers each publish like a direct reply-to consumer.
type fakeChannel struct {
	published []amqp.Publishing
	replies   chan amqp.Delivery
}

func (f *fakeChannel) PublishWithContext(_ context.Context, _, _ string, _, _ bool, msg amqp.Publishing) error {
	f.published = append(f.published, msg)

	if f.replies != nil {
		f.replies <- amqp.Delivery{CorrelationId: msg.CorrelationId, Body: []byte(`{}`)}
	}

	return nil
}

func (f *fakeChannel) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	return f.replies, nil
}

func (f *fakeChannel) Cancel(string, bool) error { return nil }

func (f *fakeChannel) Close() error { return nil }

// foreignSpan carries a span context the active tracer cannot inject, forcing tracer.Inject to fail.
type foreignSpan struct{ traceID, spanID uint64 }

func (s foreignSpan) SetTag(string, interface{})                   {}
func (s foreignSpan) SetOperationName(string)                      {}
func (s foreignSpan) BaggageItem(string) string                    { return "" }
func (s foreignSpan) SetBaggageItem(string, string)                {}
func (s foreignSpan) Finish(...ddtrace.FinishOption)               {}
func (s foreignSpan) Context() ddtrace.SpanContext                 { return s }
func (s foreignSpan) SpanID() uint64                               { return s.spanID }
func (s foreignSpan) TraceID() uint64                              { return s.traceID }
func (s foreignSpan) ForeachBaggageItem(func(string, string) bool) {}

func TestAmqpHeadersCarrier(t *testing.T) {
	carrier := amqpHeadersCarrier(amqp.Table{
		"str":   "a",
		"bytes": []byte("b"),
		"int":   int32(7),
		"nil":   nil,
	})
	carrier.Set("set", "c")

	got := map[string]string{}
	require.NoError(t, carrier.ForeachKey(func(key, val string) error {
		got[key] = val
		return nil
	}))

	assert.Equal(t, map[string]string{"str": "a", "bytes": "b", "set": "c"}, got)

	stop := errors.New("stop")
	assert.ErrorIs(t, carrier.ForeachKey(func(string, string) error { return stop }), stop)
}

// AC9: headers written by tracer.Inject; caller headers preserved.
func TestInjectTraceHeaders(t *testing.T) {
	t.Run("without span headers are untouched", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		assert.Nil(t, injectTraceHeaders(context.Background(), nil))

		headers := amqp.Table{"x-custom": "v"}
		assert.Equal(t, amqp.Table{"x-custom": "v"}, injectTraceHeaders(context.Background(), headers))
	})

	t.Run("keeps caller headers and does not mutate them", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		span, ctx := ddtracer.StartSpanFromContext(context.Background(), "amqp.publish")
		defer span.Finish()

		caller := amqp.Table{"x-custom": "v", "x-retry": int32(2)}
		headers := injectTraceHeaders(ctx, caller)

		assert.Equal(t, "v", headers["x-custom"])
		assert.Equal(t, int32(2), headers["x-retry"])
		assert.Equal(t, strconv.FormatUint(span.Context().TraceID(), 10), headers["x-datadog-trace-id"])
		assert.Equal(t, strconv.FormatUint(span.Context().SpanID(), 10), headers["x-datadog-parent-id"])
		assert.Equal(t, amqp.Table{"x-custom": "v", "x-retry": int32(2)}, caller)
	})

	t.Run("drops stale caller trace headers when inject fails", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		ctx := ddtracer.ContextWithSpan(context.Background(), foreignSpan{traceID: 42, spanID: 7})
		headers := injectTraceHeaders(ctx, amqp.Table{
			"x-custom":                    "v",
			"x-datadog-trace-id":          "1",
			"x-datadog-parent-id":         "2",
			"x-datadog-sampling-priority": "1",
			"x-datadog-tags":              "_dd.p.tid=abc",
			"traceparent":                 "00-0000000000000000000000000000abcd-00000000000000ef-01",
			"tracestate":                  "dd=s:1",
		})

		assert.Equal(t, amqp.Table{"x-custom": "v", "x-datadog-trace-id": "42"}, headers)
	})

	t.Run("real tracer writes full datadog context with 128-bit trace id", func(t *testing.T) {
		startRealTracer(t)

		span, ctx := ddtracer.StartSpanFromContext(context.Background(), "amqp.publish")
		defer span.Finish()

		headers := injectTraceHeaders(ctx, amqp.Table{"x-custom": "v"})

		sc := span.Context()
		w3c, ok := sc.(ddtrace.SpanContextW3C)
		require.True(t, ok)
		traceID128 := w3c.TraceID128()

		assert.Equal(t, "v", headers["x-custom"])
		assert.Equal(t, strconv.FormatUint(sc.TraceID(), 10), headers["x-datadog-trace-id"])
		assert.Equal(t, strconv.FormatUint(sc.SpanID(), 10), headers["x-datadog-parent-id"])
		assert.Contains(t, headers, "x-datadog-sampling-priority")
		require.NotEqual(t, strings.Repeat("0", 16), traceID128[:16], "expected a 128-bit trace id")
		assert.Contains(t, headers["x-datadog-tags"], "_dd.p.tid="+traceID128[:16])
	})
}

// AC10: consumer span is a child of the publisher span.
func TestProcessMessageContinuesPublisherTrace(t *testing.T) {
	t.Run("mocktracer parent/child", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		// non-root publisher span, so ParentID differs from the trace id
		root, rootCtx := ddtracer.StartSpanFromContext(context.Background(), "http.request")
		pub, pubCtx := ddtracer.StartSpanFromContext(rootCtx, "amqp.publish")
		require.NotEqual(t, pub.Context().TraceID(), pub.Context().SpanID())
		headers := injectTraceHeaders(pubCtx, nil)
		pub.Finish()
		root.Finish()

		ctx := consumeWithHandler(t, consumerSpanConfig, headers)

		span, ok := ddtracer.SpanFromContext(ctx)
		require.True(t, ok)
		consumer := finishedSpan(t, mt, span.Context().SpanID())

		assert.Equal(t, pub.Context().SpanID(), consumer.ParentID())
		assert.Equal(t, pub.Context().TraceID(), consumer.TraceID())
		assert.Equal(t, "amqp.consume", consumer.OperationName())
		assert.Equal(t, "my-queue", consumer.Tag(ext.ResourceName))
		assert.Equal(t, "queue", consumer.Tag(ext.SpanType))
		assert.Equal(t, "my-queue", consumer.Tag("queue"))

		fields := logFields(t, ctx)
		assert.Equal(t, consumer.SpanID(), fields["dd.span_id"])
		assert.Equal(t, consumer.TraceID(), fields["dd.trace_id"])
		assert.NotEmpty(t, fields["log_id"])
	})

	t.Run("real tracer keeps 128-bit trace", func(t *testing.T) {
		startRealTracer(t)

		// non-root publisher span, so its span id differs from the lower 64 bits of the trace id
		root, rootCtx := ddtracer.StartSpanFromContext(context.Background(), "http.request")
		defer root.Finish()
		pub, pubCtx := ddtracer.StartSpanFromContext(rootCtx, "amqp.publish")
		defer pub.Finish()
		require.NotEqual(t, pub.Context().TraceID(), pub.Context().SpanID())
		headers := injectTraceHeaders(pubCtx, nil)

		ctx := consumeWithHandler(t, consumerSpanConfig, headers)

		consumer, ok := ddtracer.SpanFromContext(ctx)
		require.True(t, ok)

		assert.Equal(t,
			pub.Context().(ddtrace.SpanContextW3C).TraceID128(),
			consumer.Context().(ddtrace.SpanContextW3C).TraceID128(),
		)
		stringer, ok := consumer.(fmt.Stringer)
		require.True(t, ok)
		assert.Contains(t, stringer.String(), fmt.Sprintf("ParentID: %d\n", pub.Context().SpanID()))

		fields := logFields(t, ctx)
		assert.Equal(t, consumer.Context().SpanID(), fields["dd.span_id"])
		assert.Equal(t, consumer.Context().TraceID(), fields["dd.trace_id"])
	})
}

// AC11: only the legacy header keeps the TraceID behavior.
func TestProcessMessageLegacyTraceIDHeader(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	ctx := consumeWithHandler(t, consumerSpanConfig, amqp.Table{"x-datadog-trace-id": "12345"})

	span, ok := ddtracer.SpanFromContext(ctx)
	require.True(t, ok)

	consumer := finishedSpan(t, mt, span.Context().SpanID())

	assert.Equal(t, uint64(12345), consumer.ParentID())
	assert.Equal(t, uint64(12345), consumer.TraceID())

	fields := logFields(t, ctx)
	assert.Equal(t, consumer.SpanID(), fields["dd.span_id"])
	assert.Equal(t, consumer.TraceID(), fields["dd.trace_id"])
}

// AC12: no trace header gives a root span; no OperationName gives no span.
func TestProcessMessageWithoutTrace(t *testing.T) {
	t.Run("no header starts a root span", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		ctx := consumeWithHandler(t, consumerSpanConfig, nil)

		_, ok := ddtracer.SpanFromContext(ctx)
		require.True(t, ok)

		finished := mt.FinishedSpans()
		require.Len(t, finished, 1)
		assert.Equal(t, uint64(0), finished[0].ParentID())
		assert.Equal(t, finished[0].SpanID(), logFields(t, ctx)["dd.span_id"])
	})

	t.Run("no operation name starts no span", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		pub, pubCtx := ddtracer.StartSpanFromContext(context.Background(), "amqp.publish")
		headers := injectTraceHeaders(pubCtx, nil)
		pub.Finish()
		mt.Reset()

		ctx := consumeWithHandler(t, tracing.SpanConfig{}, headers)

		_, ok := ddtracer.SpanFromContext(ctx)
		assert.False(t, ok)
		assert.Nil(t, ctx.Value(liberlogger.LogFieldsKey{}))
		assert.Empty(t, mt.FinishedSpans())
	})
}

// AC9 through the public API: SendMessage keeps caller headers and adds the trace context.
func TestSendMessageInjectsTraceHeaders(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	channel := &fakeChannel{}
	publisher := &publisherImpl{client: &clientImpl{connection: &amqp.Connection{}}, channel: channel}

	root, ctx := ddtracer.StartSpanFromContext(context.Background(), "http.request")
	defer root.Finish()

	err := publisher.SendMessage(ctx, "exchange", "key", false, false,
		PublishingMessage{Headers: amqp.Table{"x-custom": "v"}},
		tracing.SpanConfig{OperationName: "amqp.publish"},
	)
	require.NoError(t, err)
	require.Len(t, channel.published, 1)

	pub := finishedSpanByName(t, mt, "amqp.publish")
	headers := channel.published[0].Headers

	assert.Equal(t, "v", headers["x-custom"])
	assert.Equal(t, strconv.FormatUint(root.Context().TraceID(), 10), headers["x-datadog-trace-id"])
	assert.Equal(t, strconv.FormatUint(pub.SpanID(), 10), headers["x-datadog-parent-id"])
}

// AC9 through the public API: DirectReplyTo publishes the trace context.
func TestDirectReplyToInjectsTraceHeaders(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	channel := &fakeChannel{replies: make(chan amqp.Delivery, 1)}
	client := &clientImpl{openReplyChannel: func() (replyToChannel, error) { return channel, nil }}

	root, ctx := ddtracer.StartSpanFromContext(context.Background(), "http.request")
	defer root.Finish()

	_, err := client.DirectReplyTo(ctx, "exchange", "key", 0,
		IncomingEventMessage{CorrelationID: "corr-1"},
		tracing.SpanConfig{OperationName: "amqp.rpc"},
	)
	require.NoError(t, err)
	require.Len(t, channel.published, 1)

	rpc := finishedSpanByName(t, mt, "amqp.rpc")
	headers := channel.published[0].Headers

	assert.Equal(t, "corr-1", channel.published[0].CorrelationId)
	assert.Equal(t, strconv.FormatUint(root.Context().TraceID(), 10), headers["x-datadog-trace-id"])
	assert.Equal(t, strconv.FormatUint(rpc.SpanID(), 10), headers["x-datadog-parent-id"])
}

// Legacy header edge cases: never panic, invalid values give a root span, full context wins.
func TestProcessMessageLegacyHeaderEdgeCases(t *testing.T) {
	for name, value := range map[string]interface{}{
		"non-string value": int64(12345),
		"malformed value":  "not-a-number",
	} {
		t.Run(name, func(t *testing.T) {
			mt := mocktracer.Start()
			defer mt.Stop()

			var ctx context.Context
			require.NotPanics(t, func() {
				ctx = consumeWithHandler(t, consumerSpanConfig, amqp.Table{"x-datadog-trace-id": value})
			})

			span, ok := ddtracer.SpanFromContext(ctx)
			require.True(t, ok)
			assert.Equal(t, uint64(0), finishedSpan(t, mt, span.Context().SpanID()).ParentID())
		})
	}

	t.Run("[]byte legacy value is accepted", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		ctx := consumeWithHandler(t, consumerSpanConfig, amqp.Table{"x-datadog-trace-id": []byte("12345")})

		span, ok := ddtracer.SpanFromContext(ctx)
		require.True(t, ok)
		assert.Equal(t, uint64(12345), finishedSpan(t, mt, span.Context().SpanID()).ParentID())
	})

	t.Run("full context wins over conflicting legacy header", func(t *testing.T) {
		startRealTracer(t)

		// datadog extraction fails without parent id, W3C traceparent carries the full context
		ctx := consumeWithHandler(t, consumerSpanConfig, amqp.Table{
			"x-datadog-trace-id": "999",
			"traceparent":        "00-0000000000000001000000000000abcd-00000000000000ef-01",
		})

		span, ok := ddtracer.SpanFromContext(ctx)
		require.True(t, ok)

		assert.Equal(t, "0000000000000001000000000000abcd", span.Context().(ddtrace.SpanContextW3C).TraceID128())
		stringer, ok := span.(fmt.Stringer)
		require.True(t, ok)
		assert.Contains(t, stringer.String(), fmt.Sprintf("ParentID: %d\n", 0xef))
	})
}
