//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Documents.TimeSeries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class AsyncDocumentSession
    {
        public IAsyncSessionDocumentTimeSeries TimeSeriesFor(string documentId, string name)
        {
            return new AsyncSessionDocumentTimeSeries<TimeSeriesEntry>(this, documentId, name);
        }

        public IAsyncSessionDocumentTimeSeries TimeSeriesFor(object entity, string name)
        {
            return new AsyncSessionDocumentTimeSeries<TimeSeriesEntry>(this, entity, name);
        }

        public IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(string documentId, string name = null) where TValues : new()
        {
            var tsName = name ?? _documentStore.TimeSeries.GetTimeSeriesName<TValues>();
            return new AsyncSessionDocumentTimeSeries<TValues>(this, documentId, tsName);
        }

        public IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(object entity, string name = null) where TValues : new()
        {
            var tsName = name ?? _documentStore.TimeSeries.GetTimeSeriesName<TValues>();
            return new AsyncSessionDocumentTimeSeries<TValues>(this, entity, tsName);
        }

        public IAsyncSessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(object entity, string policy, string raw = null) where TValues : new()
        {
            var tsName = raw ?? _documentStore.TimeSeries.GetTimeSeriesName<TValues>();
            return new AsyncSessionDocumentTimeSeries<TValues>(this, entity, $"{tsName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{policy}");
        }

        public IAsyncSessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(string documentId, string policy, string raw = null) where TValues : new()
        {
            var tsName = raw ?? _documentStore.TimeSeries.GetTimeSeriesName<TValues>();
            return new AsyncSessionDocumentTimeSeries<TValues>(this, documentId, $"{tsName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{policy}");
        }
    }
}
