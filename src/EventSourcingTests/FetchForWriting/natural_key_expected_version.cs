using System;
using System.Threading.Tasks;
using JasperFx.Events.Projections;
using JasperFx;
using Marten.Testing.Harness;
using Shouldly;
using Xunit;

namespace EventSourcingTests.FetchForWriting;

public class natural_key_expected_version: OneOffConfigurationsContext
{
    private void configure()
    {
        StoreOptions(opts =>
        {
            opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline);
            opts.Projections.Snapshot<OrderSummary>(SnapshotLifecycle.Async);
        });
    }

    private async Task<Guid> startOrder(OrderNumber number, int items)
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream<OrderAggregate>(streamId, new OrderCreated(number, "Alice"));
        for (var i = 0; i < items; i++)
        {
            theSession.Events.Append(streamId, new OrderItemAdded("Widget", 10m));
        }

        await theSession.SaveChangesAsync();
        return streamId;
    }

    [Fact]
    public async Task fetch_for_writing_by_natural_key_at_the_expected_version()
    {
        configure();

        var order = new OrderNumber("EV-1");
        var orderId = await startOrder(order, 2);

        await using var session = theStore.LightweightSession();

        #region sample_fetch_for_writing_by_natural_key_with_expected_version
        var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(order, 3);
        #endregion

        stream.Id.ShouldBe(orderId);
        stream.CurrentVersion.ShouldBe(3);

        stream.AppendOne(new OrderItemAdded("Gadget", 5m));
        await session.SaveChangesAsync();

        (await theSession.Events.FetchLatest<OrderAggregate, OrderNumber>(order))!.TotalAmount.ShouldBe(25m);
    }

    [Fact]
    public async Task fetch_for_writing_by_natural_key_at_a_stale_version_fails()
    {
        configure();

        var order = new OrderNumber("EV-2");
        await startOrder(order, 2);

        await using var session = theStore.LightweightSession();
        var ex = await Should.ThrowAsync<ConcurrencyException>(() =>
            session.Events.FetchForWriting<OrderAggregate, OrderNumber>(order, 2));
        ex.Message.ShouldContain("Expected the existing version to be 2, but was 3");
    }

    [Fact]
    public async Task an_unknown_natural_key_is_at_version_zero()
    {
        configure();

        await using var session = theStore.LightweightSession();

        var stream = await session.Events.FetchForWriting<OrderAggregate, OrderNumber>(new OrderNumber("EV-NONE"), 0);
        stream.CurrentVersion.ShouldBe(0);
        stream.Aggregate.ShouldBeNull();

        await Should.ThrowAsync<ConcurrencyException>(() =>
            session.Events.FetchForWriting<OrderAggregate, OrderNumber>(new OrderNumber("EV-NONE"), 1));
    }

    [Fact]
    public async Task an_async_aggregate_by_mapped_key_checks_the_version()
    {
        configure();

        var order = new OrderNumber("EV-3");
        var orderId = await startOrder(order, 3);

        await using var session = theStore.LightweightSession();
        var stream = await session.Events.FetchForWriting<OrderSummary, OrderNumber>(order, 4);
        stream.Id.ShouldBe(orderId);
        stream.Aggregate!.Items.ShouldBe(3);

        await using var other = theStore.LightweightSession();
        await Should.ThrowAsync<ConcurrencyException>(() =>
            other.Events.FetchForWriting<OrderSummary, OrderNumber>(order, 1));
    }

    [Fact]
    public async Task fetch_for_writing_by_natural_key_with_expected_version_in_a_batch()
    {
        configure();

        var order = new OrderNumber("EV-4");
        var orderId = await startOrder(order, 1);

        await using var session = theStore.LightweightSession();
        var before = session.RequestCount;

        var batch = session.CreateBatchQuery();
        var streamTask = batch.Events.FetchForWriting<OrderAggregate, OrderNumber>(order, 2);
        await batch.Execute();

        session.RequestCount.ShouldBe(before + 1);

        var stream = await streamTask;
        stream.Id.ShouldBe(orderId);
        stream.CurrentVersion.ShouldBe(2);
    }

    [Fact]
    public async Task a_stale_version_in_a_batch_fails()
    {
        configure();

        var order = new OrderNumber("EV-5");
        await startOrder(order, 1);

        await using var session = theStore.LightweightSession();
        var batch = session.CreateBatchQuery();
        var streamTask = batch.Events.FetchForWriting<OrderAggregate, OrderNumber>(order, 5);

        await Should.ThrowAsync<ConcurrencyException>(async () =>
        {
            await batch.Execute();
            await streamTask;
        });
    }
}
