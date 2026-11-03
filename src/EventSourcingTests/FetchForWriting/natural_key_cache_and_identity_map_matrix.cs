using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventSourcingTests.Aggregation;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Marten;
using Marten.Testing.Documents;
using Marten.Testing.Harness;
using Shouldly;
using Xunit;

namespace EventSourcingTests.FetchForWriting;

public enum ByWhat
{
    StreamId,
    NaturalKey,
    BatchedStreamId,
    BatchedNaturalKey
}

/// <summary>
/// The write cache and the identity map answer a fetch whichever way the aggregate was first fetched, by stream
/// id or by natural key, and whichever way it is fetched again. Batches behave as they do by stream id: a
/// batched fetch for writing does not take from the write cache, a batched fetch for writing by stream id does
/// not write back to it, and a batched FetchLatest of an Async or Live aggregate does not fold the session's
/// pending events.
/// </summary>
public class natural_key_cache_and_identity_map_matrix: OneOffConfigurationsContext
{
    private readonly MartenTestAggregateWriteCache theCache = new();

    public static IEnumerable<object[]> WriteCacheCases() =>
        from lifecycle in new[] { SnapshotLifecycle.Inline, SnapshotLifecycle.Async }
        from seed in new[] { ByWhat.StreamId, ByWhat.NaturalKey, ByWhat.BatchedStreamId, ByWhat.BatchedNaturalKey }
        from take in new[] { ByWhat.StreamId, ByWhat.NaturalKey, ByWhat.BatchedStreamId, ByWhat.BatchedNaturalKey }
        select new object[] { lifecycle, seed, take };

    public static IEnumerable<object[]> IdentityMapCases() =>
        from lifecycle in new[] { MatrixLifecycle.Inline, MatrixLifecycle.Async, MatrixLifecycle.Live }
        from first in new[] { ByWhat.StreamId, ByWhat.NaturalKey }
        from then in new[] { ByWhat.StreamId, ByWhat.NaturalKey, ByWhat.BatchedStreamId, ByWhat.BatchedNaturalKey }
        select new object[] { lifecycle, first, then };

    private async Task<Guid> startAsync()
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream<KeyedSimpleAggregate>(streamId, new KeyedAEvent(RaceKey.For(streamId)),
            new BEvent(), new CEvent());
        await theSession.SaveChangesAsync();
        return streamId;
    }

    private static async Task<IEventStream<KeyedSimpleAggregate>> fetchForWritingAsync(IDocumentSession session,
        ByWhat by, Guid streamId)
    {
        var key = RaceKey.For(streamId);
        switch (by)
        {
            case ByWhat.StreamId:
                return await session.Events.FetchForWriting<KeyedSimpleAggregate>(streamId);
            case ByWhat.NaturalKey:
                return await session.Events.FetchForWriting<KeyedSimpleAggregate, RaceKey>(key);
        }

        var batch = session.CreateBatchQuery();
        var fetched = by == ByWhat.BatchedStreamId
            ? batch.Events.FetchForWriting<KeyedSimpleAggregate>(streamId)
            : batch.Events.FetchForWriting<KeyedSimpleAggregate, RaceKey>(key);
        await batch.Execute();
        return await fetched;
    }

    private static async Task<KeyedSimpleAggregate?> fetchLatestAsync(IDocumentSession session, ByWhat by,
        Guid streamId)
    {
        var key = RaceKey.For(streamId);
        switch (by)
        {
            case ByWhat.StreamId:
                return await session.Events.FetchLatest<KeyedSimpleAggregate>(streamId);
            case ByWhat.NaturalKey:
                return await session.Events.FetchLatest<KeyedSimpleAggregate, RaceKey>(key);
        }

        var batch = session.CreateBatchQuery();
        var fetched = by == ByWhat.BatchedStreamId
            ? batch.Events.FetchLatest<KeyedSimpleAggregate>(streamId)
            : batch.Events.FetchLatest<KeyedSimpleAggregate, RaceKey>(key);
        await batch.Execute();
        return await fetched;
    }

    [Theory]
    [MemberData(nameof(WriteCacheCases))]
    public async Task the_write_cache_is_taken_however_it_was_seeded(SnapshotLifecycle lifecycle, ByWhat seed,
        ByWhat take)
    {
        StoreOptions(opts =>
        {
            opts.Projections.Snapshot<KeyedSimpleAggregate>(lifecycle);
            opts.Events.AggregateWriteCaching.Cache = theCache;
            opts.Events.CacheAggregatesForWriting<KeyedSimpleAggregate>();
        });

        var streamId = await startAsync();

        // The commit writes the aggregate back to the cache, and points its key at it
        await using (var seeding = theStore.LightweightSession())
        {
            var stream = await fetchForWritingAsync(seeding, seed, streamId);
            stream.AppendOne(new BEvent());
            await seeding.SaveChangesAsync();
        }

        var hits = theCache.Hits;
        var hit = take is ByWhat.StreamId or ByWhat.NaturalKey && seed != ByWhat.BatchedStreamId;

        await using (var taking = theStore.LightweightSession())
        {
            var stream = await fetchForWritingAsync(taking, take, streamId);

            theCache.Hits.ShouldBe(hit ? hits + 1 : hits);
            stream.Aggregate!.BCount.ShouldBe(2);
            stream.CurrentVersion.ShouldBe(4);

            stream.AppendOne(new CEvent());
            await taking.SaveChangesAsync();
        }

        await using var query = theStore.LightweightSession();
        var latest = await query.Events.FetchLatest<KeyedSimpleAggregate, RaceKey>(RaceKey.For(streamId));
        latest!.CCount.ShouldBe(2);
        latest.BCount.ShouldBe(2);
    }

    [Theory]
    [MemberData(nameof(IdentityMapCases))]
    public async Task the_identity_map_answers_however_the_aggregate_was_fetched(MatrixLifecycle lifecycle,
        ByWhat first, ByWhat then)
    {
        StoreOptions(opts =>
        {
            if (lifecycle == MatrixLifecycle.Live) opts.Projections.LiveStreamAggregation<KeyedSimpleAggregate>();
            else opts.Projections.Snapshot<KeyedSimpleAggregate>(
                lifecycle == MatrixLifecycle.Inline ? SnapshotLifecycle.Inline : SnapshotLifecycle.Async);
        });

        var streamId = await startAsync();

        await using var session = lifecycle == MatrixLifecycle.Inline
            ? theStore.IdentitySession()
            : theStore.LightweightSession();

        var stream = await fetchForWritingAsync(session, first, streamId);

        // Appended in this session, so a read that went to the database would not see it
        stream.AppendOne(new DEvent());

        var before = session.RequestCount;
        var latest = await fetchLatestAsync(session, then, streamId);

        latest.ShouldNotBeNull();
        latest.Id.ShouldBe(streamId);

        if (then is ByWhat.StreamId or ByWhat.NaturalKey)
        {
            session.RequestCount.ShouldBe(before);
        }

        if (lifecycle == MatrixLifecycle.Inline)
        {
            // The same instance the fetch for writing loaded; its pending events are applied at commit
            latest.ShouldBeSameAs(stream.Aggregate);
        }
        else if (then is ByWhat.StreamId or ByWhat.NaturalKey)
        {
            // Built from the fetched stream, with this session's pending events folded in
            latest.DCount.ShouldBe(1);
            latest.BCount.ShouldBe(1);
        }
        else
        {
            // Read in the batch, which leaves the session's pending events out
            latest.DCount.ShouldBe(0);
            latest.BCount.ShouldBe(1);
        }
    }
}
