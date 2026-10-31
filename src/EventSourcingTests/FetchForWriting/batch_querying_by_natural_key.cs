using System;
using System.Threading.Tasks;
using JasperFx.Events.Projections;
using Marten.Testing.Harness;
using Shouldly;
using Xunit;

namespace EventSourcingTests.FetchForWriting;

public class batch_querying_by_natural_key: OneOffConfigurationsContext
{
    private void configure()
    {
        StoreOptions(opts =>
        {
            opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline);
            opts.Projections.Snapshot<OrderSummary>(SnapshotLifecycle.Async);
            opts.Projections.Snapshot<InvoiceAggregate>(SnapshotLifecycle.Inline);
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
    public async Task fetch_latest_by_natural_keys_in_one_round_trip()
    {
        configure();

        var order = new OrderNumber("BATCH-1");
        var orderId = await startOrder(order, 2);

        var invoice = new InvoiceNumber("INV-BATCH-1");
        theSession.Events.StartStream<InvoiceAggregate>(Guid.NewGuid(), new InvoiceCreated(invoice, 99m));
        await theSession.SaveChangesAsync();

        await using var session = theStore.LightweightSession();
        var before = session.RequestCount;

        var batch = session.CreateBatchQuery();

        // The async fetch brackets its reads in its own transaction, so it has to come first
        var summary = batch.Events.FetchLatest<OrderSummary, OrderNumber>(order);
        var owner = batch.Events.FetchLatest<OrderAggregate, OrderNumber>(order);
        var byStreamId = batch.Events.FetchLatest<OrderAggregate>(orderId);
        var invoiceAggregate = batch.Events.FetchLatest<InvoiceAggregate, InvoiceNumber>(invoice);
        var missing = batch.Events.FetchLatest<InvoiceAggregate, InvoiceNumber>(new InvoiceNumber("INV-NONE"));

        await batch.Execute();

        session.RequestCount.ShouldBe(before + 1);

        (await summary)!.Items.ShouldBe(2);
        (await owner)!.TotalAmount.ShouldBe(20m);
        (await byStreamId)!.Id.ShouldBe(orderId);
        (await invoiceAggregate)!.Amount.ShouldBe(99m);
        (await missing).ShouldBeNull();
    }

    [Fact]
    public async Task fetch_for_writing_by_natural_key_in_a_batch_and_append()
    {
        configure();

        var order = new OrderNumber("BATCH-2");
        var orderId = await startOrder(order, 1);

        await using var session = theStore.LightweightSession();
        var batch = session.CreateBatchQuery();
        var streamTask = batch.Events.FetchForWriting<OrderAggregate, OrderNumber>(order);
        var newStreamTask = batch.Events.FetchForWriting<OrderAggregate, OrderNumber>(new OrderNumber("BATCH-NEW"));
        await batch.Execute();

        var stream = await streamTask;
        stream.Id.ShouldBe(orderId);
        stream.CurrentVersion.ShouldBe(2);
        stream.Aggregate!.TotalAmount.ShouldBe(10m);

        var newStream = await newStreamTask;
        newStream.Aggregate.ShouldBeNull();
        newStream.CurrentVersion.ShouldBe(0);

        stream.AppendOne(new OrderItemAdded("Gadget", 5m));
        await session.SaveChangesAsync();

        (await theSession.Events.FetchLatest<OrderAggregate, OrderNumber>(order))!.TotalAmount.ShouldBe(15m);
    }

    [Fact]
    public async Task fetch_for_writing_an_async_aggregate_by_mapped_key_in_a_batch()
    {
        configure();

        var order = new OrderNumber("BATCH-3");
        var orderId = await startOrder(order, 3);

        await using var session = theStore.LightweightSession();
        var batch = session.CreateBatchQuery();
        var streamTask = batch.Events.FetchForWriting<OrderSummary, OrderNumber>(order);
        await batch.Execute();

        var stream = await streamTask;
        stream.Id.ShouldBe(orderId);
        stream.CurrentVersion.ShouldBe(4);
        stream.Aggregate!.Items.ShouldBe(3);
    }

    [Fact]
    public async Task fetch_for_exclusive_writing_by_natural_key_in_a_batch()
    {
        configure();

        var order = new OrderNumber("BATCH-4");
        var orderId = await startOrder(order, 1);

        await using var session = theStore.LightweightSession();
        var batch = session.CreateBatchQuery();
        var streamTask = batch.Events.FetchForExclusiveWriting<OrderAggregate, OrderNumber>(order);
        await batch.Execute();

        var stream = await streamTask;
        stream.Id.ShouldBe(orderId);
        stream.AppendOne(new OrderCompleted());
        await session.SaveChangesAsync();

        (await theSession.Events.FetchLatest<OrderAggregate, OrderNumber>(order))!.IsComplete.ShouldBeTrue();
    }

    [Fact]
    public async Task fetch_for_writing_by_natural_key_is_one_round_trip()
    {
        configure();

        var order = new OrderNumber("BATCH-5");
        await startOrder(order, 1);

        await using var session = theStore.LightweightSession();

        // Prime the schema checks so only the fetch itself is counted
        await session.Events.FetchLatest<OrderSummary, OrderNumber>(order);

        var before = session.RequestCount;
        var stream = await session.Events.FetchForWriting<OrderSummary, OrderNumber>(order);
        session.RequestCount.ShouldBe(before + 1);
        stream.Aggregate!.Items.ShouldBe(1);
    }
}
