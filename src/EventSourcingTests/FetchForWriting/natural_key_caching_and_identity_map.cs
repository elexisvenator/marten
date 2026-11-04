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
