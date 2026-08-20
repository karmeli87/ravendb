using FastTests;
using Raven.Quill.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class BootstrapStateFlagTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Setup_package_is_absent_until_the_sentinel_exists()
    {
        var dir = NewDataPath(forceCreateDir: true);

        Assert.False(SetupPackage.IsPresent(dir));

        // A/settings.json can appear mid-unzip, so it is deliberately not what "activated" means
        var settings = SetupPackage.SettingsPath(dir);
        Directory.CreateDirectory(Path.GetDirectoryName(settings)!);
        File.WriteAllText(settings, "{}");

        Assert.False(SetupPackage.IsPresent(dir));

        File.WriteAllText(SetupPackage.SentinelPath(dir), "thumbprint");

        Assert.True(SetupPackage.IsPresent(dir));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Starts_in_the_phase_the_composition_root_gives_it()
    {
        Assert.Equal(BootstrapPhase.NeedsActivation,
            new BootstrapStateFlag(BootstrapPhase.NeedsActivation).Phase);

        Assert.Equal(BootstrapPhase.Restarting,
            new BootstrapStateFlag(BootstrapPhase.Restarting).Phase);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void MarkFailed_keeps_the_reason_and_falls_back_to_NeedsActivation()
    {
        IBootstrapState state = new BootstrapStateFlag(BootstrapPhase.Redeeming);

        state.MarkFailed("activation failed: could not retrieve the setup package");

        Assert.Equal(BootstrapPhase.NeedsActivation, state.Phase);
        Assert.Equal("activation failed: could not retrieve the setup package", state.Reason);

        state.MarkReady();

        Assert.Equal(BootstrapPhase.Ready, state.Phase);
        Assert.Null(state.Reason);
    }
}
