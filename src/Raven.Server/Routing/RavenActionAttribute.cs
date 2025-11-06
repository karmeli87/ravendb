using System;

namespace Raven.Server.Routing
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public sealed class RavenActionAttribute : Attribute
    {
        public bool IsDebugInformationEndpoint { get; set; }

        public bool DisableOnCpuCreditsExhaustion { get; set; }

        public bool CheckForChanges { get; set; }

        public CorsMode CorsMode { get; set; }

        public string Path { get; }

        public string Method { get; }

        public string Description { get; set; }

        public AuthorizationStatus RequiredAuthorization { get; set; }

        public bool SkipUsagesCount { get; set; }

        public bool SkipLastRequestTimeUpdate { get; set; }

        public bool IsPosixSpecificEndpoint { get; set; }

        public EndpointType? EndpointType { get; }

        public RavenActionAttribute(
            string path,
            string method,
            AuthorizationStatus requireAuth,
            bool isDebugInformationEndpoint = false,
            bool isPosixSpecificEndpoint = false,
            bool checkForChanges = true,
            CorsMode corsMode = CorsMode.None)
        {
            if (requireAuth == AuthorizationStatus.ValidUser)
                throw new InvalidOperationException($"Please use the other constructor with endpoint type parameter. Route: '{method} {path}'");

            Path = path;
            Method = method;
            IsDebugInformationEndpoint = isDebugInformationEndpoint;
            RequiredAuthorization = requireAuth;
            IsPosixSpecificEndpoint = isPosixSpecificEndpoint;
            CorsMode = corsMode;
            CheckForChanges = checkForChanges;
        }

        public RavenActionAttribute(
            string path,
            string method,
            AuthorizationStatus requireAuth,
            EndpointType endpointType,
            bool isDebugInformationEndpoint = false,
            bool isPosixSpecificEndpoint = false,
            bool checkForChanges = true,
            CorsMode corsMode = CorsMode.None)
        {
            if (requireAuth != AuthorizationStatus.ValidUser)
                throw new InvalidOperationException($"Please use the other constructor without endpoint type parameter. Route: '{method} {path}'");

            Path = path;
            Method = method;
            EndpointType = endpointType;
            IsDebugInformationEndpoint = isDebugInformationEndpoint;
            RequiredAuthorization = requireAuth;
            IsPosixSpecificEndpoint = isPosixSpecificEndpoint;
            CorsMode = corsMode;
            CheckForChanges = checkForChanges;
        }
    }

    public enum EndpointType
    {
        None,
        Read,
        Write
    }

    public sealed class RavenShardedActionAttribute : Attribute
    {
        public string Path { get; }

        public string Method { get; }

        public RavenShardedActionAttribute(string path, string method)
        {
            Path = path;
            Method = method;
        }
    }

    public enum AuthorizationStatus
    {
        ClusterAdmin = 0,
        Operator = 1,
        DatabaseAdmin = 2,
        ValidUser = 3,
        UnauthenticatedClients = 4,
        RestrictedAccess = 5
    }

    /// <summary>
    /// Describes a query parameter for a RavenAction endpoint.
    /// Multiple instances can be applied to a single method to document all query parameters.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
    public sealed class RavenActionQueryParameterAttribute : Attribute
    {
        public string Name { get; }
        
        public bool Required { get; }
        
        public string Description { get; }
        
        public string Type { get; set; }
        
        public string DefaultValue { get; set; }

        public RavenActionQueryParameterAttribute(string name, bool required, string description)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Required = required;
            Description = description ?? throw new ArgumentNullException(nameof(description));
        }
    }
}
