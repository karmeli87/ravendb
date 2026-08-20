namespace Raven.Quill.Hosting;

public enum BootstrapPhase
{
    NeedsActivation,

    Redeeming,

    Restarting,

    Ready,
}

public interface IBootstrapState
{
    BootstrapPhase Phase { get; }
    string? Reason { get; }

    void MarkRedeeming();
    void MarkRestarting(string? reason = null);
    void MarkReady();
    void MarkFailed(string reason);
}

public static class BootstrapPhaseExtensions
{
    public static string ToWire(this BootstrapPhase phase) => phase switch
    {
        BootstrapPhase.NeedsActivation => "needs-activation",
        BootstrapPhase.Redeeming => "redeeming",
        BootstrapPhase.Restarting => "restarting",
        BootstrapPhase.Ready => "ready",
        _ => phase.ToString().ToLowerInvariant(),
    };
}

/// The phase the FE's boot screen polls. Its starting value is decided by the composition root - the
/// activating graph starts at NeedsActivation, the serving graph at Restarting until RavenDB answers -
/// so nothing here has to guess which lifecycle phase it is in, and activation cannot run twice: it is
/// only composed in one phase, and that phase ends by stopping the host.
public sealed class BootstrapStateFlag : IBootstrapState
{
    private int _phase;
    private string? _reason;

    public BootstrapStateFlag(BootstrapPhase initial)
    {
        _phase = (int)initial;
    }

    public BootstrapPhase Phase => (BootstrapPhase)Volatile.Read(ref _phase);
    public string? Reason => Volatile.Read(ref _reason);

    public void MarkRedeeming()
    {
        Volatile.Write(ref _reason, null);
        Volatile.Write(ref _phase, (int)BootstrapPhase.Redeeming);
    }

    public void MarkRestarting(string? reason = null)
    {
        Volatile.Write(ref _reason, reason);
        Volatile.Write(ref _phase, (int)BootstrapPhase.Restarting);
    }

    public void MarkReady()
    {
        Volatile.Write(ref _reason, null);
        Volatile.Write(ref _phase, (int)BootstrapPhase.Ready);
    }

    public void MarkFailed(string reason)
    {
        Volatile.Write(ref _reason, reason);
        Volatile.Write(ref _phase, (int)BootstrapPhase.NeedsActivation);
    }
}
