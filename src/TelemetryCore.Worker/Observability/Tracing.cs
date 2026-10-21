using System.Diagnostics;

namespace TelemetryCore.Worker.Observability;

/// <summary>
/// Worker spans (SPEC §10, D14): <c>batch.process</c> per batch, linked to the producer contexts found in the
/// <c>traceparent</c> headers (a batch has many parents, so links, not a parent), and a child <c>db.write</c> per
/// transaction attempt. Exported over OTLP when <c>OTEL_EXPORTER_OTLP_ENDPOINT</c> is set.
/// </summary>
public static class Tracing
{
    public const string SourceName = "TelemetryCore.Worker";

    /// <summary>Upper bound of producer links per batch span.</summary>
    public const int MaxLinks = 32;

    public static readonly ActivitySource Source = new(SourceName);

    /// <summary>
    /// Collects up to <see cref="MaxLinks"/> distinct producer contexts of one batch. Reused by its partition processor
    /// (single-threaded); does nothing when no listener samples the worker source.
    /// </summary>
    public sealed class LinkCollector
    {
        private readonly List<ActivityContext> _contexts = new(MaxLinks);

        public void Add(in ActivityContext context)
        {
            if (context == default || _contexts.Count >= MaxLinks || !Source.HasListeners())
            {
                return;
            }

            foreach (var existing in _contexts)
            {
                if (existing.SpanId == context.SpanId && existing.TraceId == context.TraceId)
                {
                    return;
                }
            }

            _contexts.Add(context);
        }

        public void Clear() => _contexts.Clear();

        /// <summary>Starts the <c>batch.process</c> span with the collected links (null when nobody listens).</summary>
        public Activity? StartBatch(int partition, int batchSize)
        {
            if (!Source.HasListeners())
            {
                return null;
            }

            List<ActivityLink>? links = null;
            if (_contexts.Count > 0)
            {
                links = new List<ActivityLink>(_contexts.Count);
                foreach (var ctx in _contexts)
                {
                    links.Add(new ActivityLink(ctx));
                }
            }

            var activity = Source.StartActivity("batch.process", ActivityKind.Consumer, parentContext: default, links: links);
            activity?.SetTag("partition", partition);
            activity?.SetTag("batch.size", batchSize);
            return activity;
        }
    }
}
