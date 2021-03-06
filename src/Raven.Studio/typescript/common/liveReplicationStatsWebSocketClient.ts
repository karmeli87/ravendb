/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import d3 = require("d3");
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import endpoints = require("endpoints");

class liveReplicationStatsWebSocketClient extends abstractWebSocketClient<resultsDto<Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceStatsBase<Raven.Client.Documents.Replication.ReplicationPerformanceBase>>> {

    private static readonly isoParser = d3.time.format.iso;
    private readonly onData: (data: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceStatsBase<Raven.Client.Documents.Replication.ReplicationPerformanceBase>[]) => void;

    private mergedData: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceStatsBase<Raven.Client.Documents.Replication.ReplicationPerformanceBase>[] = [];
    private pendingDataToApply: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceStatsBase<Raven.Client.Documents.Replication.ReplicationPerformanceBase>[] = [];

    private updatesPaused = false;
    loading = ko.observable<boolean>(true);

    constructor(db: database, onData: (data: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceStatsBase<Raven.Client.Documents.Replication.ReplicationPerformanceBase>[]) => void) {
        super(db);
        this.onData = onData;
    }

    get connectionDescription() {
        return "Live Replication Stats";
    }

    protected webSocketUrlFactory() {
        return endpoints.databases.replication.replicationPerformanceLive;
    }

    get autoReconnect() {
        return false;
    }

    pauseUpdates() {
        this.updatesPaused = true;
    }

    resumeUpdates() {
        this.updatesPaused = false;

        if (this.pendingDataToApply.length) {
            this.mergeIncomingData(this.pendingDataToApply);
        }
        this.pendingDataToApply = [];
        this.onData(this.mergedData);
    }

    protected onHeartBeat() {
        this.loading(false);
    }

    protected onMessage(e: resultsDto<Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceStatsBase<Raven.Client.Documents.Replication.ReplicationPerformanceBase>>) {
        this.loading(false);

        if (this.updatesPaused) {
            this.pendingDataToApply.push(...e.Results);
        } else {
            this.mergeIncomingData(e.Results);
            this.onData(this.mergedData);
        }
    }

    private mergeIncomingData(e: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceStatsBase<Raven.Client.Documents.Replication.ReplicationPerformanceBase>[]) {
        e.forEach(replicationStatsFromEndpoing => {
            const replicationDesc = replicationStatsFromEndpoing.Description;
            const replicationType = replicationStatsFromEndpoing.Type;

            let existingReplicationStats = this.mergedData.find(x => x.Type === replicationType && x.Description === replicationDesc);

            if (!existingReplicationStats) {
                existingReplicationStats = {
                    Description: replicationDesc,
                    Id: replicationStatsFromEndpoing.Id,
                    Type: replicationStatsFromEndpoing.Type,
                    Performance: []
                };
                this.mergedData.push(existingReplicationStats);
            }

            const idToIndexCache = new Map<number, number>();
            existingReplicationStats.Performance.forEach((v, idx) => {
                idToIndexCache.set(v.Id, idx);
            });

            replicationStatsFromEndpoing.Performance.forEach(perf => {  // each obj in Performance can be either outgoing or incoming..
                liveReplicationStatsWebSocketClient.fillCache(perf, replicationStatsFromEndpoing.Type, replicationStatsFromEndpoing.Description);

                if (idToIndexCache.has(perf.Id)) { 
                    // update 
                    const indexToUpdate = idToIndexCache.get(perf.Id);
                    existingReplicationStats.Performance[indexToUpdate] = perf;
                } else {
                    // this shouldn't invalidate idToIndexCache as we always append only
                    existingReplicationStats.Performance.push(perf);
                }
            });
        });
    }

    static fillCache(perf: Raven.Client.Documents.Replication.ReplicationPerformanceBase,
                     type: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceType,
                     description: string) {

        const withCache = perf as ReplicationPerformanceBaseWithCache;
        withCache.CompletedAsDate = perf.Completed ? liveReplicationStatsWebSocketClient.isoParser.parse(perf.Completed) : undefined;
        withCache.StartedAsDate = liveReplicationStatsWebSocketClient.isoParser.parse(perf.Started);
        withCache.Type = type;
        withCache.Description = description;
    }
}

export = liveReplicationStatsWebSocketClient;

