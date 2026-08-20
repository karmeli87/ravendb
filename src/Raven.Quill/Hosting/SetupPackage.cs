namespace Raven.Quill.Hosting;

/// The setup package is the appliance's activation record: node settings, certificates and the license.
/// Activation writes <c>admin-thumbprint</c> last, after the zip is fully extracted, so that file - not
/// <c>A/settings.json</c>, which can appear mid-unzip - is what "activated" means. The s6 service
/// docker/quill/s6-rc.d/01-ravendb/run waits on the same marker before starting RavenDB, so both sides
/// of the container agree on one definition.
public static class SetupPackage
{
    public static string SentinelPath(string root) => Path.Combine(root, "admin-thumbprint");

    public static string SettingsPath(string root) => Path.Combine(root, "A", "settings.json");

    public static bool IsPresent(string root) => File.Exists(SentinelPath(root));
}
