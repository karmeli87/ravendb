using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Queries.TimeSeries;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesCollectionConfiguration : IDynamicJson
    {
        public bool Disabled;

        public List<RollupPolicy> RollupPolicies;

        public TimeSpan? RawDataRetentionTime;

        public void Validate()
        {
            if (RollupPolicies.Count == 0)
                return;

            RollupPolicies.Sort(TimeSeriesDownSamplePolicyComparer.Instance);
        }

        internal RollupPolicy GetPolicy(string name)
        {
            if (name.Contains(TimeSeriesConfiguration.TimeSeriesRollupSeparator) == false)
                return RollupPolicy.RawPolicy;

            return RollupPolicies.SingleOrDefault(p => name.Contains(p.Name));
        }

        internal RollupPolicy GetNextPolicy(RollupPolicy policy)
        {
            if (policy == RollupPolicy.RawPolicy)
                return RollupPolicies[0];

            var current = RollupPolicies.FindIndex(p => p == policy);
            if (current < 0)
                return null;

            if (current == RollupPolicies.Count)
                return RollupPolicy.AfterAllPolices;

            return RollupPolicies[current + 1];
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(RollupPolicies)] = new DynamicJsonArray(RollupPolicies.Select(p=>p.ToJson())),
                [nameof(RawDataRetentionTime)] = RawDataRetentionTime,
                [nameof(Disabled)] = Disabled
            };
        }
    }

    public class RollupPolicy : IDynamicJson, IComparable<RollupPolicy>
    {
        /// <summary>
        /// Name of the time series policy, defined by the convention "KeepFor{RetentionTime}AggregatedBy{TimeSpan} (e.g. keep for 12 hours and aggregate by 1 minute = KeepFor12:00:00AggregatedBy00:01:00)"
        /// </summary>
        public string Name; // defined by convention

        /// <summary>
        /// How long the data of this policy will be retained
        /// </summary>
        public TimeSpan RetentionTime;

        /// <summary>
        /// Define the aggregation of this policy
        /// </summary>
        public TimeSpan AggregateBy;

        public AggregationType Type;

        // The timestamp location of the rollup policy is defined by the type
        // start-point when First
        // mid-point   when Avg, Mean
        // end-point   when Sum, Count, Min, Max, Last
        // TODO: consider Continuous Query approach

        internal static RollupPolicy AfterAllPolices = new RollupPolicy(TimeSpan.MinValue, TimeSpan.MinValue);
        internal static RollupPolicy RawPolicy = new RollupPolicy(TimeSpan.MinValue, TimeSpan.MinValue);

        private RollupPolicy()
        {
            
        }

        public RollupPolicy(TimeSpan retentionTime, TimeSpan aggregateBy, AggregationType type = AggregationType.Avg)
        {
            RetentionTime = retentionTime;
            AggregateBy = aggregateBy;
            Type = type;

            Name = $"KeepFor{RetentionTime}AggregatedEvery{AggregateBy}To{Type}";
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(RetentionTime)] = RetentionTime,
                [nameof(AggregateBy)] = AggregateBy,
            };
        }

        protected bool Equals(RollupPolicy other)
        {
            Debug.Assert(Name == other.Name);
            return RetentionTime == other.RetentionTime &&
                   AggregateBy == other.AggregateBy;
        }

        public int CompareTo(RollupPolicy other)
        {
            return TimeSeriesDownSamplePolicyComparer.Instance.Compare(this, other);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((RollupPolicy)obj);
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }
    }

    internal class TimeSeriesDownSamplePolicyComparer : IComparer<RollupPolicy>
    {
        public static TimeSeriesDownSamplePolicyComparer Instance = new TimeSeriesDownSamplePolicyComparer();

        public int Compare(RollupPolicy x, RollupPolicy y)
        {
            if (x == null && y == null)
                return 0;
            if (x == null)
                return 1;
            if (y == null)
                return -1;

            var diff = x.AggregateBy.Ticks - y.AggregateBy.Ticks; // we can't cast to int, since it might overflow

            if (diff > 0)
                return 1;
            if (diff < 0)
                return -1;
            return 0;
        }
    }
}
