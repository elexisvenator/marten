using System;
using System.Threading.Tasks;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Marten;
using Marten.Events.Aggregation;
using Marten.Storage;
using Marten.Testing.Harness;
using Shouldly;
using Xunit;

namespace EventSourcingTests.FetchForWriting;

#region sample_mapped_natural_key_aggregate

// An async read model of an order stream. It owns no lookup: the order number resolves through
// OrderAggregate's mt_natural_key table, which OrderAggregate's projection maintains inline.
public class OrderSummary
{
    public Guid Id { get; set; }

    [MappedNaturalKey<OrderAggregate>]
    public OrderNumber Number { get; set; }

    public int Items { get; set; }

    public static OrderSummary Create(OrderCreated e) => new() { Number = e.OrderNumber };

    public void Apply(OrderItemAdded e) => Items++;

    public void Apply(OrderNumberChanged e) => Number = e.NewOrderNumber;
}

#endregion

public class OrderSummaryAsString
{
    public string Id { get; set; }

    [MappedNaturalKey<OrderAggregateAsString>]
    public OrderNumber Number { get; set; }

    public int Items { get; set; }

    public static OrderSummaryAsString Create(OrderCreated e) => new() { Number = e.OrderNumber };

    public void Apply(OrderItemAdded e) => Items++;
}

public class mapped_and_async_natural_keys: OneOffConfigurationsContext
{
    [Fact]
    public async Task an_async_aggregate_that_owns_its_key_is_fetched_at_its_current_state()
    {
        // The lookup is maintained inline whatever the aggregate's lifecycle, but this used to load the
        // stored snapshot as is. With no daemon running there is none, so it came back null.
        StoreOptions(opts => opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Async));

        var orderNumber = new OrderNumber("ASYNC-OWNED-1");
        theSession.Events.StartStream<OrderAggregate>(Guid.NewGuid(),
            new OrderCreated(orderNumber, "Alice"),
            new OrderItemAdded("Widget", 9.99m));
        await theSession.SaveChangesAsync();

        var latest = await theSession.Events.FetchLatest<OrderAggregate, OrderNumber>(orderNumber);
        latest.ShouldNotBeNull();
        latest.TotalAmount.ShouldBe(9.99m);

        var stream = await theSession.Events.FetchForWriting<OrderAggregate, OrderNumber>(orderNumber);
        stream.Aggregate.ShouldNotBeNull();
        stream.Aggregate.TotalAmount.ShouldBe(9.99m);
        stream.CurrentVersion.ShouldBe(2);
    }

    [Fact]
    public async Task a_mapped_key_fetches_an_async_aggregate_through_the_owners_lookup()
    {
        StoreOptions(opts =>
        {
            opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline);
            opts.Projections.Snapshot<OrderSummary>(SnapshotLifecycle.Async);
        });

        var orderNumber = new OrderNumber("MAPPED-1");
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream<OrderAggregate>(streamId,
            new OrderCreated(orderNumber, "Bob"),
            new OrderItemAdded("Widget", 9.99m),
            new OrderItemAdded("Gadget", 19.99m));
        await theSession.SaveChangesAsync();

        var latest = await theSession.Events.FetchLatest<OrderSummary, OrderNumber>(orderNumber);
        latest.ShouldNotBeNull();
        latest.Id.ShouldBe(streamId);
        latest.Items.ShouldBe(2);

        var stream = await theSession.Events.FetchForWriting<OrderSummary, OrderNumber>(orderNumber);
        stream.Id.ShouldBe(streamId);
        stream.CurrentVersion.ShouldBe(3);
        stream.AppendOne(new OrderItemAdded("Doohickey", 5.50m));
        await theSession.SaveChangesAsync();

        (await theSession.Events.FetchLatest<OrderSummary, OrderNumber>(orderNumber))!.Items.ShouldBe(3);
    }

    [Fact]
    public async Task a_mapped_key_follows_the_owner_when_the_key_changes()
    {
        StoreOptions(opts =>
        {
            opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline);
            opts.Projections.Snapshot<OrderSummary>(SnapshotLifecycle.Async);
        });

        var original = new OrderNumber("MAPPED-OLD");
        var renamed = new OrderNumber("MAPPED-NEW");
        theSession.Events.StartStream<OrderAggregate>(Guid.NewGuid(), new OrderCreated(original, "Carol"));
        await theSession.SaveChangesAsync();

        var stream = await theSession.Events.FetchForWriting<OrderSummary, OrderNumber>(original);
        stream.AppendOne(new OrderNumberChanged(renamed));
        await theSession.SaveChangesAsync();

        (await theSession.Events.FetchLatest<OrderSummary, OrderNumber>(original)).ShouldBeNull();
        (await theSession.Events.FetchLatest<OrderSummary, OrderNumber>(renamed))!.Number.ShouldBe(renamed);
    }

    [Fact]
    public async Task a_mapped_key_with_string_stream_identity()
    {
        StoreOptions(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Snapshot<OrderAggregateAsString>(SnapshotLifecycle.Inline);
            opts.Projections.Snapshot<OrderSummaryAsString>(SnapshotLifecycle.Async);
        });

        var orderNumber = new OrderNumber("MAPPED-STR-1");
        var streamKey = "order-" + Guid.NewGuid();
        theSession.Events.StartStream<OrderAggregateAsString>(streamKey,
            new OrderCreated(orderNumber, "Dave"),
            new OrderItemAdded("Widget", 9.99m));
        await theSession.SaveChangesAsync();

        var latest = await theSession.Events.FetchLatest<OrderSummaryAsString, OrderNumber>(orderNumber);
        latest.ShouldNotBeNull();
        latest.Id.ShouldBe(streamKey);
        latest.Items.ShouldBe(1);

        var stream = await theSession.Events.FetchForWriting<OrderSummaryAsString, OrderNumber>(orderNumber);
        stream.Key.ShouldBe(streamKey);
        stream.CurrentVersion.ShouldBe(2);
    }

    [Fact]
    public async Task an_unknown_mapped_key_starts_a_new_stream()
    {
        StoreOptions(opts =>
        {
            opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline);
            opts.Projections.Snapshot<OrderSummary>(SnapshotLifecycle.Async);
        });

        var stream = await theSession.Events.FetchForWriting<OrderSummary, OrderNumber>(new OrderNumber("MAPPED-NONE"));
        stream.Aggregate.ShouldBeNull();
        stream.CurrentVersion.ShouldBe(0);
    }

    [Fact]
    public void a_mapped_key_whose_source_is_not_registered_fails_when_the_store_is_built()
    {
        var ex = Should.Throw<InvalidProjectionException>(() =>
        {
            using var store = DocumentStore.For(opts =>
            {
                opts.Connection(ConnectionSource.ConnectionString);
                opts.Projections.Snapshot<OrderSummary>(SnapshotLifecycle.Async);
            });
        });

        ex.Message.ShouldContain("OrderSummary.Number");
        ex.Message.ShouldContain("no aggregate projection for that type is registered");
    }

    [Fact]
    public void a_mapped_key_and_its_source_must_agree_on_global_tenancy()
    {
        var ex = Should.Throw<InvalidProjectionException>(() =>
        {
            using var store = DocumentStore.For(opts =>
            {
                opts.Connection(ConnectionSource.ConnectionString);
                opts.Events.TenancyStyle = TenancyStyle.Conjoined;
                opts.Policies.AllDocumentsAreMultiTenanted();
                opts.Projections.Snapshot<OrderAggregate>(SnapshotLifecycle.Inline);
                opts.Projections.AddGlobalProjection(new SingleStreamProjection<OrderSummary, Guid>(),
                    ProjectionLifecycle.Async);
            });
        });

        ex.Message.ShouldContain("only one of the two is a global aggregate");
    }
}
