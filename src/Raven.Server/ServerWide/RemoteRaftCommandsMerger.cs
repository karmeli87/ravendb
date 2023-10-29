using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.ServerWide
{
    public class RemoteRaftCommandsMerger
    {
        private readonly RachisConsensus _engine;

        public class RemoteCommand
        {
            public readonly CommandBase Command;
            public readonly TaskCompletionSource<(long Index, object Result)> Promise;

            public RemoteCommand(CommandBase command)
            {
                Command = command;
                Promise = new TaskCompletionSource<(long Index, object Result)>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            public void SetContinuationToLeaderCommand(Task<(long Index, object Result)> t)
            {
                if (t.IsCompletedSuccessfully)
                {
                    Promise.SetResult(t.Result);
                    return;
                }

                if (t.IsFaulted)
                {
                    try
                    {
                        t.GetAwaiter().GetResult();
                    }
                    catch (Exception e)
                    {
                        Promise.SetException(e);
                    }

                    return;
                }

                if (t.IsCanceled)
                {
                    Promise.SetCanceled();
                }
            }
        }

        private BlockingCollection<RemoteCommand> _remoteCommands = new BlockingCollection<RemoteCommand>();
        private ClusterRequestExecutor _clusterRequestExecutor;

        public RemoteRaftCommandsMerger(RachisConsensus engine)
        {
            _engine = engine;
        }

        public Task<(long Index, object Result)> EnqueueAsync(CommandBase raftCommand)
        {
            var remote = new RemoteCommand(raftCommand);
            _remoteCommands.TryAdd(remote);
            return remote.Promise.Task;
        }

        public void Run()
        {
            _engine.TopologyChanged += (sender, clusterTopology) =>
            {
                if (_clusterRequestExecutor == null)
                {
                    _clusterRequestExecutor = ClusterRequestExecutor.Create(
                        clusterTopology.AllNodes.Select(n => n.Value).ToArray(),
                        _engine.ServerStore.Server.Certificate.Certificate, 
                        DocumentConventions.DefaultForServer);
                }
            };

            var batch = new Queue<RemoteCommand>(capacity: 128);
            var shutdown = _engine.ServerStore.ServerShutdown;
            try
            {
                while (_remoteCommands.TryTake(out var cmd, Timeout.Infinite, shutdown))
                {
                    batch.Clear();
                    batch.Enqueue(cmd);
                    while (_remoteCommands.TryTake(out cmd, TimeSpan.Zero))
                    {
                        batch.Enqueue(cmd);
                        if (batch.Count > 128)
                            break;
                    }

                    var timeoutTask = TimeoutManager.WaitFor(_engine.OperationTimeout, shutdown);
                    while (true)
                    {
                        shutdown.ThrowIfCancellationRequested();

                        if (_engine.CurrentState == RachisState.Leader && _engine.CurrentLeader?.Running == true)
                        {
                            while (batch.TryDequeue(out var remoteCommand))
                            {
                                try
                                {
                                    _engine.PutAsync(remoteCommand.Command).ContinueWith(remoteCommand.SetContinuationToLeaderCommand);
                                }
                                catch (Exception e)
                                {
                                    remoteCommand.Promise.SetException(e);
                                }
                            }

                            break;
                        }

                        if (_engine.CurrentState == RachisState.Passive)
                        {
                            foreach (RemoteCommand remoteCommand in batch)
                            {
                                remoteCommand.Promise.SetException(GetInvalidEngineState(remoteCommand.Command));
                            }

                            break;
                        }

                        var logChange = _engine.WaitForHeartbeat();
                        var reachedLeader = new Reference<bool>();
                        
                        try
                        {
                            using (_engine.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                            {
                                SendBatchToNode(context, batch.ToList(), reachedLeader);
                            }
                        }
                        catch (Exception e)
                        {
                            if (_engine.Log.IsInfoEnabled)
                                _engine.Log.Info($"Tried to send message to leader (reached: {reachedLeader.Value}), retrying", e);

                            if (reachedLeader.Value)
                                break;

                            Task.WaitAny(logChange, timeoutTask);
                            if (timeoutTask.IsCompleted)
                            {
                                foreach (RemoteCommand remoteCommand in batch)
                                {
                                    remoteCommand.Promise.SetException(GetTimeoutException(remoteCommand.Command, e));
                                }

                                break;
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                if (shutdown.IsCancellationRequested == false)
                    throw;

                _remoteCommands.CompleteAdding();

                foreach (RemoteCommand remoteCommand in batch)
                {
                    remoteCommand.Promise.TrySetCanceled();
                }

                while (_remoteCommands.TryTake(out var cmd, TimeSpan.Zero))
                {
                    cmd.Promise.TrySetCanceled();
                }
            }
        }

        private void SendBatchToNode(JsonOperationContext context, List<RemoteCommand> batch, Reference<bool> reachedLeader)
        {
            var command = new PutBatchRaftCommands(batch, _engine.Url, _engine.LeaderTag);
            try
            {
                _clusterRequestExecutor.Execute(command, context);

                for (int i = 0; i < batch.Count; i++)
                {
                    var remoteCommand = batch[i];
                    var r = command.Result.Results[i];
                    var data = remoteCommand.Command.FromRemote(r.Data);
                    remoteCommand.Promise.SetResult((r.RaftCommandIndex, data));
                }
            }
            catch (Exception e)
            {
                reachedLeader.Value = command.HasReachLeader();

                if (reachedLeader.Value)
                {
                    foreach (RemoteCommand remoteCommand in batch)
                    {
                        remoteCommand.Promise.SetException(e);
                    }
                }

                throw;
            }
        }

        private Exception GetInvalidEngineState(CommandBase cmd)
        {
            return new NotSupportedException("Cannot send command " + cmd.GetType().FullName + " to the cluster because this node is passive." + Environment.NewLine +
                                             "Passive nodes aren't members of a cluster and require admin action (such as creating a db) " +
                                             "to indicate that this node should create its own cluster");
        }

        private Exception GetTimeoutException(CommandBase cmd, Exception requestException)
        {
            return new TimeoutException($"Could not send command {cmd.GetType().FullName} from {_engine.Tag} to leader because there is no leader, " +
                                       $"and we timed out waiting for one after {_engine.OperationTimeout}", requestException);
        }

        public class PutBatchRaftCommands : RavenCommand<PutBatchRaftCommandResult>
        {
            public readonly List<RemoteCommand> Commands;
            private bool _reachedLeader;
            public override bool IsReadRequest => false;

            public bool HasReachLeader() => _reachedLeader;

            private readonly string _source;

            public PutBatchRaftCommands(List<RemoteCommand> commands, string source, string leader)
            {
                Commands = commands;
                _source = source;
                SelectedNodeTag = leader;
            }

            public override void OnResponseFailure(HttpResponseMessage response)
            {
                if (response.Headers.Contains("Reached-Leader") == false)
                    return;

                _reachedLeader = response.Headers.GetValues("Reached-Leader").Contains("true");
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/rachis/batch?source={_source}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                        {
                            writer.WriteStartObject();
                            writer.WriteArray(nameof(Commands), Commands.Select(c => ctx.ReadObject(c.Command.ToJson(ctx), "cmd")));
                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = PutBatchRaftCommandResultFunc(response);
            }
        }

        public class PutBatchRaftCommandResult
        {
            public List<ServerStore.PutRaftCommandResult> Results;
        }

        public static readonly Func<BlittableJsonReaderObject, PutBatchRaftCommandResult> PutBatchRaftCommandResultFunc = JsonDeserializationBase.GenerateJsonDeserializationRoutine<PutBatchRaftCommandResult>();
    }
}



