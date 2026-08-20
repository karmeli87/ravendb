using Microsoft.Extensions.Diagnostics.HealthChecks;
using Raven.Quill.AiHelper;
using Raven.Quill.Infrastructure;

namespace Raven.Quill.Hosting.Composition;

/// The pre-activation graph: fetch the setup package, report progress, and get out of the way. No
/// store, no auth, no API - nothing here can reach RavenDB, because RavenDB is not running yet.
/// Maps no endpoints of its own; <see cref="ApplianceCommon.MapEndpoints"/> covers what the FE polls.
public static class ActivatingPhase
{
    public static void AddServices(WebApplicationBuilder builder)
    {
        builder.Services.AddSingleton<IBootstrapState>(_ => new BootstrapStateFlag(BootstrapPhase.NeedsActivation));
        builder.Services.AddSingleton<ILicenseClient, LicenseHttpClient>();

        // The only writer of bootstrap state in this phase, and it runs exactly once per process:
        // when it succeeds it stops the host, and the supervisor's next process is Serving.
        builder.Services.AddHostedService<ApplianceActivationService>();

        builder.Services.AddHealthChecks()
            .AddCheck<BootstrapHealthCheck>("bootstrap", failureStatus: HealthStatus.Unhealthy);
    }
}
