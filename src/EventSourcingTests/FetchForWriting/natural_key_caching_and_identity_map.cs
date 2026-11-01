using System;
using System.Linq;
using System.Threading.Tasks;
using JasperFx.Events.Projections;
using Marten.Testing.Harness;
using Shouldly;
using Xunit;

namespace EventSourcingTests.FetchForWriting;

/// <summary>
/// Natural key fetches use the aggregate write cache and the identity map, as fetches by stream id do. The
/// cache is found through a pointer from the key to the stream id, which the database confirms in the same
/// round trip; the identity map is searched by the aggregates' current key values.
/// </summary>
public class natural_key_caching_and_identity_map: OneOffConfigurationsContext
{
    private readonly MartenTestAggregateWriteCache theCache = new();

    // UseIdentityMapForAggregates is left at its default, true. With it off the Inline write-back caches an
    // aggregate the commit never updated (see the notes), which is not what these tests are about.
    private void CacheOrders()
    {
        StoreOptions(opts =>
        {
            opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline);
            opts.Projections.Snapshot<OrderSummary>(SnapshotLifecycle.Async);
            opts.Events.AggregateWriteCaching.Cache = theCache;
            opts.Events.CacheAggregatesForWriting<OrderAggregate>();
            opts.Events.CacheAggregatesForWriting<OrderSummary>();
        });
    }

    private async Task<Guid> startOrder(OrderNumber number)
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream<OrderAggregate>(streamId, new OrderCreated(number, "Alice"),
            new OrderItemAdded("Widget", 10m));
        await theSession.SaveChangesAsync();
        return streamId;
    }

    private async Task appendThroughFetchByStreamId(Guid streamId, object @event)
    {
        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<OrderAggregate>(streamId);
        stream.AppendOne(@event);
        await session.SaveChangesAsync();
    }

    private async Task appendThroughFetchByKey(OrderNumber number, object @event)
    {
        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(number);
        stream.AppendOne(@event);
        await session.SaveChangesAsync();
    }

    [Fact]
    public async Task an_aggregate_cached_by_a_fetch_by_stream_id_is_taken_by_its_natural_key()
    {
        CacheOrders();
        var number = new OrderNumber("CACHE-1");
        var streamId = await startOrder(number);

        // Writes the aggregate back to the cache after the commit, and points its key at it
        await appendThroughFetchByStreamId(streamId, new OrderItemAdded("Gadget", 5m));
        var hits = theCache.Hits;

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(number);

        theCache.Hits.ShouldBe(hits + 1);
        stream.Id.ShouldBe(streamId);
        stream.CurrentVersion.ShouldBe(3);
        stream.Aggregate!.TotalAmount.ShouldBe(15m);

        stream.AppendOne(new OrderItemAdded("Doohickey", 1m));
        await session.SaveChangesAsync();

        (await theSession.Events.FetchLatest<OrderAggregate, OrderNumber>(number))!.TotalAmount.ShouldBe(16m);
    }

    [Fact]
    public async Task a_fetch_by_natural_key_seeds_the_cache_for_the_next_one()
    {
        CacheOrders();
        var number = new OrderNumber("CACHE-2");
        await startOrder(number);

        await appendThroughFetchByKey(number, new OrderItemAdded("Gadget", 5m));
        var hits = theCache.Hits;

        await appendThroughFetchByKey(number, new OrderItemAdded("Gizmo", 2m));

        theCache.Hits.ShouldBe(hits + 1);
        (await theSession.Events.FetchLatest<OrderAggregate, OrderNumber>(number))!.TotalAmount.ShouldBe(17m);
    }

    [Fact]
    public async Task a_pointer_left_by_a_rename_does_not_return_the_renamed_stream()
    {
        CacheOrders();
        var original = new OrderNumber("CACHE-OLD");
        var renamed = new OrderNumber("CACHE-NEW");
        var streamId = await startOrder(original);

        await appendThroughFetchByStreamId(streamId, new OrderItemAdded("Gadget", 5m));

        // Renamed without going through the cache, so the old key still points at the stream
        theSession.Events.Append(streamId, new OrderNumberChanged(renamed));
        await theSession.SaveChangesAsync();

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(original);

        // No stream has the old key any more, so this is a new stream...
        stream.Id.ShouldNotBe(streamId);
        stream.Aggregate.ShouldBeNull();

        // ...and the snapshot the stale pointer led to went back into the cache for its own stream
        theCache.Keys.ShouldContain(x => streamId.Equals(x.Id));
    }

    [Fact]
    public async Task a_pointer_to_a_retired_key_does_not_return_its_old_stream()
    {
        CacheOrders();
        var number = new OrderNumber("CACHE-MOVED");
        var first = await startOrder(number);
        await appendThroughFetchByStreamId(first, new OrderItemAdded("Gadget", 5m));

        // The first stream gives the key up, and a second one claims it
        theSession.Events.Append(first, new OrderNumberChanged(null));
        await theSession.SaveChangesAsync();
        var second = await startOrder(number);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(number);

        stream.Id.ShouldBe(second);
        stream.CurrentVersion.ShouldBe(2);
        stream.Aggregate!.TotalAmount.ShouldBe(10m);
    }

    [Fact]
    public async Task an_async_aggregate_is_taken_from_the_cache_by_its_mapped_key()
    {
        CacheOrders();
        var number = new OrderNumber("CACHE-ASYNC");
        var streamId = await startOrder(number);

        // The Async plan stores the aggregate in the cache straight away, and points its keys at it
        await using (var first = theStore.LightweightSession())
        {
            (await first.Events.FetchForWriting<OrderSummary, OrderNumber>(number)).Aggregate!.Items.ShouldBe(1);
        }

        theSession.Events.Append(streamId, new OrderItemAdded("Gadget", 5m));
        await theSession.SaveChangesAsync();
        var hits = theCache.Hits;

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<OrderSummary, OrderNumber>(number);

        theCache.Hits.ShouldBe(hits + 1);
        stream.Id.ShouldBe(streamId);
        stream.CurrentVersion.ShouldBe(3);

        // The cached baseline, plus the event appended since it was cached
        stream.Aggregate!.Items.ShouldBe(2);
    }

    [Fact]
    public async Task fetch_latest_by_natural_key_is_answered_from_the_identity_map()
    {
        CacheOrders();
        var number = new OrderNumber("MAP-INLINE");
        var streamId = await startOrder(number);

        await using var session = theStore.IdentitySession();
        var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(number);

        var before = session.RequestCount;
        var latest = await session.Events.FetchLatest<OrderAggregate, OrderNumber>(number);

        session.RequestCount.ShouldBe(before);
        latest.ShouldBeSameAs(stream.Aggregate);
        latest!.Id.ShouldBe(streamId);
    }

    [Fact]
    public async Task fetch_latest_of_an_async_aggregate_by_natural_key_includes_events_appended_in_the_session()
    {
        CacheOrders();
        var number = new OrderNumber("MAP-ASYNC");
        await startOrder(number);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<OrderSummary, OrderNumber>(number);
        stream.AppendOne(new OrderItemAdded("Gadget", 5m));

        var before = session.RequestCount;
        var latest = await session.Events.FetchLatest<OrderSummary, OrderNumber>(number);

        session.RequestCount.ShouldBe(before);
        latest!.Items.ShouldBe(2);
    }

    [Fact]
    public async Task the_identity_map_is_searched_by_the_current_key()
    {
        CacheOrders();
        var original = new OrderNumber("MAP-OLD");
        var renamed = new OrderNumber("MAP-NEW");
        var streamId = await startOrder(original);

        await using var session = theStore.IdentitySession();
        var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(original);
        stream.AppendOne(new OrderNumberChanged(renamed));
        await session.SaveChangesAsync();

        // The aggregate in the map now carries the new key, so the old one is not answered from it
        (await session.Events.FetchLatest<OrderAggregate, OrderNumber>(original)).ShouldBeNull();
        (await session.Events.FetchLatest<OrderAggregate, OrderNumber>(renamed))!.Id.ShouldBe(streamId);
    }

    [Fact]
    public async Task a_batched_fetch_latest_by_natural_key_is_answered_from_the_identity_map()
    {
        CacheOrders();
        var number = new OrderNumber("MAP-BATCH");
        var streamId = await startOrder(number);

        await using var session = theStore.IdentitySession();
        var inMap = (await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(number)).Aggregate;

        var batch = session.CreateBatchQuery();
        var latest = batch.Events.FetchLatest<OrderAggregate, OrderNumber>(number);
        var byId = batch.Events.FetchLatest<OrderAggregate>(streamId);
        await batch.Execute();

        (await latest).ShouldBeSameAs(inMap);
        (await byId).ShouldBeSameAs(inMap);
    }
}
