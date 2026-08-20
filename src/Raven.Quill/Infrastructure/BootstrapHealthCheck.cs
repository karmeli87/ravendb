using Microsoft.Extensions.Diagnostics.HealthChecks;
using Raven.Quill.Hosting;

namespace Raven.Quill.Infrastructure;

/// The activating phase's answer for /healthz. There is no store to probe - RavenDB does not start
/// until activation unpacks the setup package - so the phase itself is the whole report. Always
/// unhealthy: the container is not serving anything yet, and Docker's HEALTHCHECK should say so.
internal sealed class BootstrapHealthCheck(IBootstrapState bootstrap) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var phase = bootstrap.Phase.ToWire();
        var description = bootstrap.Reason is { Length: > 0 } reason
            ? $"appliance not activated ({phase}): {reason}"
            : $"appliance not activated: {phase}";

        return Task.FromResult(HealthCheckResult.Unhealthy(description));
    }
}
