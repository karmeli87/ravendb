using System.Text.Json;
using Raven.Quill.Hosting;

namespace QuillTests.E2E.Fixtures;

/// Writes the marker files that make Program derive <see cref="AppliancePhase.Serving"/>, so a test host
/// composes the store, auth and the API. The store itself is swapped in DI, so this never has to be a
/// real package - it only has to look activated. Per-host directory, so it stays parallel-safe.
internal static class SetupPackageStub
{
    public static void Write(string setupPackagePath)
    {
        var settings = SetupPackage.SettingsPath(setupPackagePath);
        Directory.CreateDirectory(Path.GetDirectoryName(settings)!);

        if (File.Exists(settings) == false)
        {
            File.WriteAllText(settings, JsonSerializer.Serialize(new
            {
                PublicServerUrl = "https://a.quill-tests.invalid",
            }));
        }

        // written last by real activation, and the only marker the appliance treats as "package complete"
        var sentinel = SetupPackage.SentinelPath(setupPackagePath);
        if (File.Exists(sentinel) == false)
            File.WriteAllText(sentinel, "0000000000000000000000000000000000000000");
    }
}
