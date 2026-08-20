using Microsoft.Extensions.Diagnostics.HealthChecks;
using Raven.Quill.AiHelper;
using Raven.Quill.Infrastructure;

namespace Raven.Quill.Hosting.Composition;

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
