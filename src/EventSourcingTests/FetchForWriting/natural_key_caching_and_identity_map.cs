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
}
