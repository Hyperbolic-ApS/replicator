using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kurrent.Replicator.Shared.Contracts;
using Kurrent.Replicator.Shared.Logging;

namespace Kurrent.Replicator.Http;

public class HttpTransform {
    static readonly ILog Log = LogProvider.GetCurrentClassLogger();

    readonly HttpClient _client;
    readonly TimeSpan   _timeout;
    readonly TimeSpan   _retryMinDelay;
    readonly TimeSpan   _retryMaxDelay;

    public HttpTransform(string? url, int timeoutSeconds = 100, int retryDelaySeconds = 1, int retryMaxDelaySeconds = 30) {
        ArgumentException.ThrowIfNullOrEmpty(url);

        _timeout       = timeoutSeconds <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(timeoutSeconds);
        _retryMinDelay = TimeSpan.FromSeconds(Math.Max(0, retryDelaySeconds));
        _retryMaxDelay = TimeSpan.FromSeconds(Math.Max(0, retryMaxDelaySeconds));

        // We manage per-request timeouts via CancellationToken to better differentiate between
        // host shutdown/cycle cancellation and HTTP timeouts.
        _client = new() {
            BaseAddress = new(url),
            Timeout     = Timeout.InfiniteTimeSpan
        };
    }

    public async ValueTask<BaseProposedEvent> Transform(OriginalEvent originalEvent, CancellationToken cancellationToken) {
        var httpEvent = new HttpEvent(
            originalEvent.EventDetails.EventType,
            originalEvent.EventDetails.Stream,
            Encoding.UTF8.GetString(originalEvent.Data),
            originalEvent.Metadata == null ? null : Encoding.UTF8.GetString(originalEvent.Metadata)
        );

        var requestBytes = JsonSerializer.SerializeToUtf8Bytes(httpEvent);

        var attempt = 0;
        var delay   = _retryMinDelay;

        while (true) {
            cancellationToken.ThrowIfCancellationRequested();
            attempt++;

            try {
                using var requestCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

                if (_timeout != Timeout.InfiniteTimeSpan) {
                    requestCts.CancelAfter(_timeout);
                }

                using var content = new ByteArrayContent(requestBytes);
                content.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                using var response = await _client.PostAsync("", content, requestCts.Token).ConfigureAwait(false);

                if (response.StatusCode == HttpStatusCode.NoContent) {
                    return new IgnoredEvent(originalEvent.EventDetails, originalEvent.LogPosition, originalEvent.SequenceNumber);
                }

                if (!response.IsSuccessStatusCode) {
                    if (IsRetryable(response.StatusCode)) {
                        await Retry($"HTTP {(int)response.StatusCode} {response.ReasonPhrase}").ConfigureAwait(false);
                        continue;
                    }

                    throw new HttpRequestException($"Transformation request failed: {(int)response.StatusCode} {response.ReasonPhrase}");
                }

                var httpResponse = (await JsonSerializer.DeserializeAsync<HttpEvent>(
                        await response.Content.ReadAsStreamAsync(requestCts.Token).ConfigureAwait(false),
                        cancellationToken: requestCts.Token
                    )
                    .ConfigureAwait(false))!;

                return new ProposedEvent(
                    originalEvent.EventDetails with {
                        EventType = httpResponse.EventType, Stream = httpResponse.StreamName
                    },
                    Encoding.UTF8.GetBytes(httpResponse.Payload),
                    httpResponse.Metadata == null ? originalEvent.Metadata : Encoding.UTF8.GetBytes(httpResponse.Metadata),
                    originalEvent.LogPosition,
                    originalEvent.SequenceNumber
                );
            } catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested) {
                // Timeout (not host shutdown/cycle cancellation). Never drop the event.
                await Retry($"timeout after {_timeout.TotalSeconds:0}s").ConfigureAwait(false);
            } catch (HttpRequestException e) when (!cancellationToken.IsCancellationRequested) {
                // Connection errors, DNS, etc. Never drop the event.
                await Retry(e.Message).ConfigureAwait(false);
            }

            continue;

            async Task Retry(string reason) {
                // No delay configured: hot-looping is dangerous. Clamp to 1s.
                var effectiveDelay = delay == TimeSpan.Zero ? TimeSpan.FromSeconds(1) : delay;

                // Avoid log spam while still being visible during outages.
                if (attempt == 1 || attempt % 10 == 0) {
                    Log.Warn(
                        "HTTP transform failed ({Reason}). Will retry in {DelayMs}ms (attempt {Attempt}) for event {EventId} from {Stream}",
                        reason,
                        (long)effectiveDelay.TotalMilliseconds,
                        attempt,
                        originalEvent.EventDetails.EventId,
                        originalEvent.EventDetails.Stream
                    );
                }

                await Task.Delay(effectiveDelay, cancellationToken).ConfigureAwait(false);

                delay = NextDelay(delay);
            }

            TimeSpan NextDelay(TimeSpan current) {
                if (_retryMaxDelay == TimeSpan.Zero) return TimeSpan.Zero;

                var next = current == TimeSpan.Zero ? _retryMinDelay : TimeSpan.FromMilliseconds(current.TotalMilliseconds * 2);

                if (next < _retryMinDelay) next = _retryMinDelay;
                if (next > _retryMaxDelay) next = _retryMaxDelay;

                return next;
            }

            static bool IsRetryable(HttpStatusCode code)
                => code == HttpStatusCode.RequestTimeout ||
                   code == HttpStatusCode.TooManyRequests ||
                   (int)code >= 500;
        }
    }

    record HttpEvent(string EventType, string StreamName, string Payload, string? Metadata);
}
