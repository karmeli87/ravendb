namespace Raven.Quill.Hosting;

/// Which lifecycle phase this process is serving. Derived once in Program from whether the setup
/// package is on disk, and never toggled: activation restarts the host, and the new process derives
/// it again. The two phases do not share a service graph.
public enum AppliancePhase
{
    /// No setup package yet. There is no RavenDB to talk to, so the store, auth and the API are not
    /// composed at all - only the activation service and the endpoints the FE boot screen polls.
    Activating,

    /// The setup package is present, so RavenDB is running (or booting) and the full graph is composed.
    Serving,
}
