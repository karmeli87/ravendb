using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Raven.Client.Documents;
using Raven.Quill.Auth;
using Raven.Quill.Embed;
using Raven.Quill.Hosting;

namespace QuillTests.E2E.Fixtures;

/// Hosts the appliance Program for tests: seeds a known operator API key, swaps in the test's in-process
/// IDocumentStore, and flips IServerReady ready. Every CreateClient() carries the TestApiKey by default;
/// unauthenticated-path tests remove it.
public sealed class ApplianceWebApplicationFactory : WebApplicationFactory<Program>
{
    public const string TestApiKey = "test-api-key";

    /// The shape `vite build` emits for the widget package. Tests run against the project's wwwroot, which
    /// carries no build output, so the bundle is stubbed here to keep the embed page's happy path testable.
    public const string StubWidgetManifestJson = """
                                                 {
                                                   "index.html": {
                                                     "file": "assets/widget-test123.js",
                                                     "name": "index",
                                                     "src": "index.html",
                                                     "isEntry": true,
                                                     "css": ["assets/widget-test123.css"],
                                                     "imports": ["_vendor-test456.js"]
                                                   },
                                                   "_vendor-test456.js": {
                                                     "file": "assets/vendor-test456.js",
                                                     "name": "vendor"
                                                   }
                                                 }
                                                 """;

    public static WidgetAssets StubWidgetAssets { get; } =
        WidgetAssets.FromManifestJson(StubWidgetManifestJson, NullLogger.Instance);

    private readonly string _setupPackagePath;
    private readonly bool _activated;
    private readonly IDocumentStore _applianceStore;
    private readonly Action<ApplianceOptions>? _configureOptions;
    private readonly Action<IServiceCollection>? _configureServices;

    /// <param name="activated">
    /// Whether the host should compose the serving phase. Default: a stub setup package is written to
    /// <paramref name="setupPackagePath"/> so Program derives Serving and maps the API. Pass false to get
    /// the activating phase - only the activation service and the endpoints the boot screen polls.
    /// </param>
    public ApplianceWebApplicationFactory(
        string setupPackagePath,
        IDocumentStore applianceStore,
        Action<ApplianceOptions>? configureOptions = null,
        Action<IServiceCollection>? configureServices = null,
        bool activated = true)
    {
        _setupPackagePath = string.IsNullOrEmpty(setupPackagePath)
            ? Path.Combine(Path.GetTempPath(), "quill-tests", Guid.NewGuid().ToString("N"))
            : setupPackagePath;
        _activated = activated;
        _applianceStore = applianceStore;
        _configureOptions = configureOptions;
        _configureServices = configureServices;
    }

    protected override IHost CreateHost(IHostBuilder builder)
    {
        // Before the host is built: Program derives the phase from the package on disk.
        if (_activated)
            SetupPackageStub.Write(_setupPackagePath);

        // Configuration rather than the environment, so parallel hosts do not fight over a process-global
        builder.ConfigureHostConfiguration(config => config.AddInMemoryCollection(
            new Dictionary<string, string?> { ["RAVEN_QUILL_SETUP_PACKAGE_PATH"] = _setupPackagePath }));

        builder.ConfigureServices(services =>
        {
            services.PostConfigure<ApplianceOptions>(opts =>
            {
                opts.SetupPackagePath = _setupPackagePath;
                opts.ApiKey = TestApiKey;
                _configureOptions?.Invoke(opts);
            });

            services.RemoveAll<IDocumentStore>();
            services.AddSingleton(_applianceStore);

            services.RemoveAll<WidgetAssets>();
            services.AddSingleton(StubWidgetAssets);

            var toRemove = services
                .Where(d => d.ImplementationType == typeof(RavenReadinessService))
                .ToList();
            foreach (var d in toRemove)
                services.Remove(d);

            // last, so a test's swap wins over everything the appliance registered
            _configureServices?.Invoke(services);
        });

        var host = base.CreateHost(builder);

        // Both are absent in the activating phase, where there is nothing to be ready for. In the serving
        // phase the appliance reaches these through RavenReadinessService, which tests drop, so stand in
        // for it: an activated test host behaves like one whose probe has already succeeded.
        if (_activated)
        {
            host.Services.GetService<IServerReady>()?.MarkReady();
            host.Services.GetService<IBootstrapState>()?.MarkReady();
        }

        return host;
    }

    protected override void ConfigureClient(HttpClient client)
    {
        base.ConfigureClient(client);
        client.DefaultRequestHeaders.Add(ApiKeyAuthenticationHandler.HeaderName, TestApiKey);
    }
}
