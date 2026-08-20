using System.Threading.RateLimiting;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Polly;
using Raven.Client.Documents;
using Raven.Quill.Agents;
using Raven.Quill.AiHelper;
using Raven.Quill.Auth;
using Raven.Quill.Endpoints;
using Raven.Quill.Feedback;
using Raven.Quill.Infrastructure;
using Raven.Quill.Licensing;
using Raven.Quill.Telegram;

namespace Raven.Quill.Hosting.Composition;

/// The post-activation graph. The setup package is on disk, so RavenDB is running or booting and every
/// service here can take IDocumentStore directly - there is no phase in which this graph exists without
/// one. Composed only from Program's phase branch.
public static class ServingPhase
{
    public static void AddServices(WebApplicationBuilder builder, bool isOpenApiDocumentGeneration)
    {
        // A DI factory rather than a pre-built instance, so tests can swap the store before anything
        // resolves it. In the container the first resolution happens at host start (RavenReadinessService
        // takes it by constructor injection), so a malformed package still fails fast.
        builder.Services.AddSingleton<IDocumentStore>(sp =>
            RavenStoreFactory.Create(sp.GetRequiredService<IOptions<ApplianceOptions>>().Value));

        builder.Services.AddSingleton<IBootstrapState>(_ => new BootstrapStateFlag(BootstrapPhase.Restarting));
        builder.Services.AddSingleton<IServerReady, ServerReadyFlag>();
        builder.Services.AddSingleton<IAgentRouter, AgentRouter>();
        builder.Services.AddSingleton<WebhookActionExecutor>();
        builder.Services.AddSingleton<IApiKeyStore, ApiKeyStore>();
        builder.Services.AddTransient<IFeedbackSender, FeedbackSender>();
        builder.Services.AddTransient<ILicenseStatsProvider, LicenseStatsProvider>();
        builder.Services.AddSingleton<ITelegramBotClientFactory, TelegramBotClientFactory>();
        builder.Services.AddSingleton<TelegramChannelManager>();
        builder.Services.AddSingleton<ITelegramChannelManager>(sp => sp.GetRequiredService<TelegramChannelManager>());

        if (isOpenApiDocumentGeneration == false)
        {
            builder.Services.AddHostedService<RavenReadinessService>();
            builder.Services.AddHostedService(sp => sp.GetRequiredService<TelegramChannelManager>());
        }

        AddHttpClients(builder);

        builder.Services.AddResiliencePipeline(RavenReadinessService.PipelineName, (pipelineBuilder, ctx) =>
        {
            var opts = ctx.ServiceProvider.GetRequiredService<IOptions<ApplianceOptions>>().Value;
            RavenReadinessService.ConfigureProbePipeline(pipelineBuilder, opts);
        });

        builder.Services.AddHealthChecks()
            .AddCheck<RavenHealthCheck>("ravendb", failureStatus: HealthStatus.Unhealthy);

        AddRateLimiter(builder);
        AddAuth(builder);
    }

    public static void Map(WebApplication app)
    {
        app.UseWebSockets();

        app.UseReadinessGate();
        app.UseRateLimiter();
        app.UseAuthentication();
        app.UseAuthorization();

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
        // map before MapSpaFallback or /apps/{slug}/embed/* is swallowed as index.html
        EmbedEndpoints.Map(app);
    }

    private static void AddHttpClients(WebApplicationBuilder builder)
    {
        builder.Services.ConfigureHttpClientDefaults(httpBuilder =>
        {
            httpBuilder.ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler
            {
                AllowAutoRedirect = false
            });
        });

        builder.Services.AddHttpClient(WebhookActionExecutor.ClientName,
            static http => http.Timeout = TimeSpan.FromSeconds(30));

        builder.Services.AddHttpClient<IAiHelperClient, AiHelperInternalClient>(static (sp, http) =>
            {
                var opts = sp.GetRequiredService<IOptions<ApplianceOptions>>().Value;
                var store = sp.GetRequiredService<IDocumentStore>();
                http.BaseAddress = new Uri(string.IsNullOrEmpty(opts.AiApiUrl) ? store.Urls[0] : opts.AiApiUrl);
                http.Timeout = opts.AiAssistTimeout;
            })
            .ConfigurePrimaryHttpMessageHandler(static sp =>
            {
                var store = sp.GetRequiredService<IDocumentStore>();
                var handler = new HttpClientHandler
                {
                    AllowAutoRedirect = false
                };
                if (store.Certificate is not null)
                    handler.ClientCertificates.Add(store.Certificate);
                return handler;
            });
    }

    private static void AddRateLimiter(WebApplicationBuilder builder)
    {
        builder.Services.AddRateLimiter(options =>
        {
            options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
            // coarse per-IP backstop; the link's invocation cap + TTL is the primary control
            options.AddPolicy(EmbedEndpoints.ChatRateLimitPolicy, httpContext =>
                RateLimitPartition.GetFixedWindowLimiter(
                    partitionKey: httpContext.Connection.RemoteIpAddress?.ToString() ?? httpContext.Connection.Id,
                    _ => new FixedWindowRateLimiterOptions
                    {
                        PermitLimit = 60,
                        Window = TimeSpan.FromMinutes(1),
                        QueueLimit = 0,
                    }));

            options.AddPolicy(AuthEndpoints.LoginRateLimitPolicy, httpContext =>
                RateLimitPartition.GetFixedWindowLimiter(
                    partitionKey: httpContext.Connection.RemoteIpAddress?.ToString() ?? httpContext.Connection.Id,
                    _ => new FixedWindowRateLimiterOptions
                    {
                        PermitLimit = 10,
                        Window = TimeSpan.FromMinutes(1),
                        QueueLimit = 0,
                    }));
        });
    }

    private static void AddAuth(WebApplicationBuilder builder)
    {
        builder.Services
            .AddAuthentication(options =>
            {
                options.DefaultScheme = ApiKeyAuthenticationHandler.SchemeName;
                options.DefaultSignInScheme = CookieAuthenticationDefaults.AuthenticationScheme;
            })
            .AddScheme<AuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(ApiKeyAuthenticationHandler.SchemeName, null)
            .AddCookie(CookieAuthenticationDefaults.AuthenticationScheme, options =>
            {
                options.Cookie.Name = "quill.session";
                options.Cookie.HttpOnly = true;
                options.Cookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;
                options.Cookie.SameSite = SameSiteMode.Strict;
                options.SlidingExpiration = true;
                options.ExpireTimeSpan = TimeSpan.FromHours(8);
                options.Events.OnRedirectToLogin = ctx =>
                {
                    ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    return Task.CompletedTask;
                };
                options.Events.OnRedirectToAccessDenied = ctx =>
                {
                    ctx.Response.StatusCode = StatusCodes.Status403Forbidden;
                    return Task.CompletedTask;
                };
            });

        builder.Services.AddAuthorization(options =>
        {
            options.DefaultPolicy = new AuthorizationPolicyBuilder(
                    ApiKeyAuthenticationHandler.SchemeName, CookieAuthenticationDefaults.AuthenticationScheme)
                .RequireAuthenticatedUser()
                .Build();
        });
    }
}
