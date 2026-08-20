using System.IO.Compression;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Options;
using Raven.Quill.AiHelper;

namespace Raven.Quill.Hosting;

public sealed class ApplianceActivationService(
    IBootstrapState bootstrap,
    IOptions<ApplianceOptions> options,
    ILicenseClient licenseClient,
    IHostApplicationLifetime lifetime,
    ILogger<ApplianceActivationService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = options.Value;

        if (string.IsNullOrWhiteSpace(opts.LicenseKey))
        {
            logger.LogInformation(
                "No QUILL_LICENSE_KEY; skipping startup activation (appliance stays in NeedsActivation).");
            return;
        }

        if (bootstrap.TryMarkRedeeming() == false)
        {
            logger.LogInformation(
                "Setup package already applied (bootstrap phase {Phase}); skipping startup activation.",
                bootstrap.Phase);
            return;
        }

        var tempZipPath = Path.Combine(Path.GetTempPath(), $"setup-package-{Guid.NewGuid():N}.zip");
        try
        {
            try
            {
                await using (var tempFile = File.Create(tempZipPath))
                    await licenseClient.DownloadSetupPackageToAsync(opts.LicenseKey ?? string.Empty, tempFile, stoppingToken);

                Directory.CreateDirectory(opts.SetupPackagePath);
                // zip-slip guard: ExtractToDirectory rejects ../ and absolute entries
                ZipFile.ExtractToDirectory(tempZipPath, opts.SetupPackagePath, overwriteFiles: true);
            }
            finally
            {
                try { File.Delete(tempZipPath); }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    logger.LogDebug(ex, "Failed to delete temp zip {Path}", tempZipPath);
                }
            }

            logger.LogInformation("Setup package activated and unpacked to {Path}.", opts.SetupPackagePath);

            WriteAdminThumbprint(opts);

            // A RavenDbS6Service value means we run under s6, where stopping this host restarts
            // it. Only its presence matters now - 01-ravendb/run picks the package up itself.
            if (string.IsNullOrEmpty(opts.RavenDbS6Service) == false)
            {
                RestartIntoSecureMode();
                return;
            }

            bootstrap.MarkReady();
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            bootstrap.MarkFailed("activation cancelled");
        }
        catch (InvalidDataException ex)
        {
            logger.LogWarning(ex, "Activation: setup package was not a valid zip.");
            bootstrap.MarkFailed("activation failed: the setup package was invalid");
        }
        catch (LicenseRetrievalException ex)
        {
            logger.LogError(ex, "Activation: failed to retrieve the setup package.");
            bootstrap.MarkFailed("activation failed: could not retrieve the setup package");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Activation failed.");
            bootstrap.MarkFailed("activation failed; see server logs for details");
        }
    }

    private void WriteAdminThumbprint(ApplianceOptions opts)
    {
        var adminPfx = Directory
            .GetFiles(opts.SetupPackagePath, "admin.client.certificate.*.pfx")
            .OrderBy(p => p, StringComparer.Ordinal)
            .FirstOrDefault();
        if (adminPfx is null)
        {
            logger.LogWarning(
                "No admin client certificate (admin.client.certificate.*.pfx) found under {Path}; " +
                "skipping the admin-thumbprint marker — RavenDB will not trust the admin cert and the " +
                "secure store may fail to authenticate (appliance can hang in Restarting).",
                opts.SetupPackagePath);
            return;
        }

        using var cert = X509CertificateLoader.LoadPkcs12FromFile(adminPfx, password: "");
        File.WriteAllText(Path.Combine(opts.SetupPackagePath, "admin-thumbprint"), cert.Thumbprint);
    }

    private void RestartIntoSecureMode()
    {
        bootstrap.TryMarkRestarting();

        // No "s6-svc -r 01-ravendb" here: that service waits for the package and starts RavenDB on
        // its own, so signalling a restart would only churn its pid - which 05-console reports as
        // a crash. Stopping this host is enough; s6 respawns it against the now-secure store.
        logger.LogInformation("Activation complete; restarting .NET host to bind the secure IDocumentStore.");
        lifetime.StopApplication();
    }
}
