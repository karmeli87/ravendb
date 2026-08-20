using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.HttpOverrides;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Embed;
using Raven.Quill.Endpoints;
using Raven.Quill.Telegram;

namespace Raven.Quill.Hosting.Composition;

/// Services and endpoints both phases need. Deliberately takes no <see cref="AppliancePhase"/>: the
/// phase branch lives in Program and nowhere else, so nothing here can grow a hidden dependency on it.
public static class ApplianceCommon
{
    public static void AddServices(WebApplicationBuilder builder)
    {
        // enums as string names so operators can paste Studio JSON
        builder.Services.ConfigureHttpJsonOptions(static options =>
        {
            // message role lowercased ("assistant"/"user") for the FE + embed-widget contract; other enums stay PascalCase
            options.SerializerOptions.Converters.Add(
                new JsonStringEnumConverter<AiMessageRole>(JsonNamingPolicy.CamelCase));
            options.SerializerOptions.Converters.Add(new JsonStringEnumConverter());
        });

        builder.Services.AddOpenApi(options =>
        {
            options.AddSchemaTransformer((schema, context, _) =>
            {
                if (schema.Properties is null || schema.Properties.Count == 0)
                    return Task.CompletedTask;

                var writableProperties = context.JsonTypeInfo.Type
                    .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                    .Where(static property => property.GetMethod is { IsPublic: true } && property.SetMethod is { IsPublic: true })
                    .Select(GetJsonPropertyName)
                    .ToHashSet(StringComparer.Ordinal);

                var getterOnlyProperties = schema.Properties.Keys
                    .Where(propertyName => writableProperties.Contains(propertyName) == false)
                    .ToArray();

                foreach (var propertyName in getterOnlyProperties)
                {
                    schema.Properties.Remove(propertyName);
                    schema.Required?.Remove(propertyName);
                }

                return Task.CompletedTask;
            });
        });

        builder.Logging.AddFilter("Polly", LogLevel.None);

        AddApplianceOptions(builder);

        // Read once at startup: the widget manifest only changes when the image is rebuilt, and a missing one is
        // worth logging loudly the moment the process starts rather than on the first visitor's request.
        builder.Services.AddSingleton(sp => WidgetAssets.Load(
            sp.GetRequiredService<IWebHostEnvironment>(),
            sp.GetRequiredService<ILoggerFactory>().CreateLogger<WidgetAssets>()));

        // trust only the nginx loopback proxy so forwarded scheme/host/IP are honored
        builder.Services.Configure<ForwardedHeadersOptions>(options =>
        {
            options.ForwardedHeaders =
                ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto | ForwardedHeaders.XForwardedHost;
            options.KnownProxies.Clear();
            options.KnownIPNetworks.Clear();
            options.KnownProxies.Add(System.Net.IPAddress.Loopback);
            options.KnownProxies.Add(System.Net.IPAddress.IPv6Loopback);
        });
    }

    /// The FE's boot screen needs exactly these: the bundle, the phase to poll, and a liveness answer.
    public static void MapEndpoints(WebApplication app)
    {
        StaticAssetEndpoints.Map(app);
        HealthEndpoints.Map(app);
        BootstrapEndpoints.Map(app);
    }

    private static void AddApplianceOptions(WebApplicationBuilder builder)
    {
        builder.Services.AddOptions<ApplianceOptions>()
            .Configure(options =>
            {
                ReadEnv("RAVEN_QUILL_WEB_LISTEN_URL", v => options.WebListenUrl = v);
                ReadEnv("RAVEN_QUILL_CONFIG_DB", v => options.ConfigDatabase = v);
                ReadEnv("RAVEN_QUILL_SETUP_PACKAGE_PATH", v => options.SetupPackagePath = v);
                ReadEnv("RAVEN_QUILL_API_URL", v => options.AiApiUrl = v);
                ReadEnv("RAVEN_QUILL_TELEGRAM_API_URL", v => options.Telegram.ApiUrl = v);
                ReadEnv("QUILL_LICENSE_KEY", v => options.LicenseKey = v);
                ReadEnv("QUILL_API_KEY", v => options.ApiKey = v);
                ReadEnv("RAVEN_QUILL_RAVENDB_INTERNAL_PORT", v =>
                {
                    if (int.TryParse(v, out var p)) options.RavenInternalPort = p;
                });
                ReadEnv("RAVEN_QUILL_AI_ASSIST_TIMEOUT_SECONDS", v =>
                {
                    if (int.TryParse(v, out var s) && s > 0) options.AiAssistTimeout = TimeSpan.FromSeconds(s);
                });
                ReadEnv("RAVEN_QUILL_READINESS_INITIAL_DELAY_SECONDS", v =>
                    options.ReadinessInitialDelay = ParsePositiveSeconds("RAVEN_QUILL_READINESS_INITIAL_DELAY_SECONDS", v));
                ReadEnv("RAVEN_QUILL_READINESS_ATTEMPT_TIMEOUT_SECONDS", v =>
                    options.ReadinessAttemptTimeout = ParsePositiveSeconds("RAVEN_QUILL_READINESS_ATTEMPT_TIMEOUT_SECONDS", v));
                ReadEnv("RAVEN_QUILL_READINESS_OVERALL_TIMEOUT_SECONDS", v =>
                    options.ReadinessOverallTimeout = ParsePositiveSeconds("RAVEN_QUILL_READINESS_OVERALL_TIMEOUT_SECONDS", v));
            })
            .ValidateDataAnnotations()
            .Validate(o => string.IsNullOrEmpty(o.Telegram.ApiUrl) ||
                           Uri.TryCreate(o.Telegram.ApiUrl, UriKind.Absolute, out var u) &&
                           (u.Scheme == Uri.UriSchemeHttp || u.Scheme == Uri.UriSchemeHttps),
                "Telegram ApiUrl must be an absolute http(s) URL")
            .Validate(o => o.Telegram.MessageLimit is > 0 and <= TelegramMessageSplitter.TelegramApiMessageLimit,
                $"Telegram MessageLimit must be between 1 and {TelegramMessageSplitter.TelegramApiMessageLimit}")
            .Validate(o => o.Telegram.ChatQueueCapacity > 0, "Telegram ChatQueueCapacity must be positive")
            .Validate(o => o.Telegram.EditDebounce > TimeSpan.Zero, "Telegram EditDebounce must be positive")
            .Validate(o => o.Telegram.ApplyChangesInterval > TimeSpan.Zero, "Telegram ApplyChangesInterval must be positive")
            .Validate(o => o.Telegram.ChatIdleTimeout > TimeSpan.Zero, "Telegram ChatIdleTimeout must be positive")
            .Validate(o => o.Telegram.PollBackoffMax > TimeSpan.Zero, "Telegram PollBackoffMax must be positive")
            .ValidateOnStart();
    }

    private static void ReadEnv(string name, Action<string> apply)
    {
        var v = Environment.GetEnvironmentVariable(name);
        if (!string.IsNullOrEmpty(v)) apply(v);
    }

    private static TimeSpan ParsePositiveSeconds(string name, string value)
    {
        if (int.TryParse(value, out var seconds) == false || seconds <= 0)
            throw new InvalidOperationException($"{name} must be a positive number of seconds, got '{value}'");
        return TimeSpan.FromSeconds(seconds);
    }

    private static string GetJsonPropertyName(PropertyInfo property)
    {
        var attribute = property.GetCustomAttribute<JsonPropertyNameAttribute>();
        if (attribute is not null)
            return attribute.Name;

        return JsonNamingPolicy.CamelCase.ConvertName(property.Name);
    }
}
