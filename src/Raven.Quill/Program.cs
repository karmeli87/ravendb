using System.Reflection;
using Raven.Quill.Endpoints;
using Raven.Quill.Hosting;
using Raven.Quill.Hosting.Composition;

var builder = WebApplication.CreateBuilder(args);
var isOpenApiDocumentGeneration = Assembly.GetEntryAssembly()?.GetName().Name == "GetDocument.Insider";

// loopback by default; the container sets the bind via RAVEN_QUILL_WEB_LISTEN_URL
var listenUrl = Environment.GetEnvironmentVariable("RAVEN_QUILL_WEB_LISTEN_URL") ?? "http://127.0.0.1:5000";
builder.WebHost.UseUrls(listenUrl);

// The appliance has two lifecycle phases and they do not share a service graph. Before activation there
// is no RavenDB - docker/quill/s6-rc.d/01-ravendb/run waits for the setup package - so the serving graph
// (store, auth, API) is not composed at all: a component that needs a store cannot be constructed in a
// phase that has none. Activation ends by stopping this host, and the supervisor starts a new process
// which derives Serving. The phase is derived here, once, and never toggled.
//
// Read through Configuration, not the raw environment, so a test host can point at its own package
// directory without touching process-global state; env vars are already part of Configuration.
// Doc generation needs the full endpoint surface whether a package exists or not.
var setupRoot = builder.Configuration["RAVEN_QUILL_SETUP_PACKAGE_PATH"] ?? "/setup";
var phase = isOpenApiDocumentGeneration || SetupPackage.IsPresent(setupRoot)
    ? AppliancePhase.Serving
    : AppliancePhase.Activating;

ApplianceCommon.AddServices(builder);

switch (phase)
{
    case AppliancePhase.Activating:
        ActivatingPhase.AddServices(builder);
        break;
    case AppliancePhase.Serving:
        ServingPhase.AddServices(builder, isOpenApiDocumentGeneration);
        break;
}

var app = builder.Build();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.UseForwardedHeaders();

if (phase == AppliancePhase.Serving)
{
    app.UseWebSockets();
    app.UseReadinessGate();
    app.UseRateLimiter();
    app.UseAuthentication();
    app.UseAuthorization();
}

StaticAssetEndpoints.Map(app);
HealthEndpoints.Map(app);
BootstrapEndpoints.Map(app);

if (phase == AppliancePhase.Serving)
{
    AuthEndpoints.Map(app);
    AppsEndpoints.Map(app);
    ChannelsEndpoints.Map(app);
    IFrameCustomizationEndpoints.Map(app);
    EmbedLinksEndpoints.Map(app);
    AiConnectionStringsEndpoints.Map(app);
    AiModelsEndpoints.Map(app);
    AgentsEndpoints.Map(app);
    StatsEndpoints.Map(app);
    SettingsEndpoints.Map(app);
    WizardEndpoints.Map(app);
    ChatEndpoints.Map(app);
    AssistantEndpoints.Map(app);
    EmbedEndpoints.Map(app);
}

// last, or /apps/{slug}/embed/* is swallowed as index.html
StaticAssetEndpoints.MapSpaFallback(app);

app.Run();

public partial class Program;
