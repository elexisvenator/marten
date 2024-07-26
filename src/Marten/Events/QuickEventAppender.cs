using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Marten.Events.Projections;
using Marten.Internal.Sessions;

namespace Marten.Events;

internal class QuickEventAppender: IEventAppender
{
    public void ProcessEvents(EventGraph eventGraph, DocumentSessionBase session, IProjection[] inlineProjections)
    {
        registerOperationsForStreams(eventGraph, session);

        foreach (var projection in inlineProjections)
        {
            projection.Apply(session, session.WorkTracker.Streams.ToList());
        }
    }

    private static void registerOperationsForStreams(EventGraph eventGraph, DocumentSessionBase session)
    {
        var storage = session.EventStorage();

        foreach (var stream in session.WorkTracker.Streams.Where(x => x.Events.Any()))
        {
            stream.TenantId ??= session.TenantId;

            // Not really using it, just need a stand in
            var sequences = new Queue<long>();
            if (stream.ActionType == StreamActionType.Start)
            {
                stream.PrepareEvents(0, eventGraph, sequences, session);
            }
            else
            {
                if (stream.ExpectedVersionOnServer.HasValue)
                {
                    // We can supply the version to the events going in
                    stream.PrepareEvents(stream.ExpectedVersionOnServer.Value, eventGraph, sequences, session);
                }
                else
                {
                    stream.PrepareEvents(0, eventGraph, sequences, session);
                }
            }
        }

        foreach (var streamsByTenant in session.WorkTracker.Streams.Where(x => x.Events.Any()).GroupBy(x => x.TenantId))
        {
            session.QueueOperation(storage.BulkQuickAppendEvents(streamsByTenant.ToList()));
        }
    }

    public async Task ProcessEventsAsync(EventGraph eventGraph, DocumentSessionBase session, IProjection[] inlineProjections,
        CancellationToken token)
    {
        registerOperationsForStreams(eventGraph, session);

        foreach (var projection in inlineProjections)
        {
            await projection.ApplyAsync(session, session.WorkTracker.Streams.ToList(), token).ConfigureAwait(false);
        }
    }
}
