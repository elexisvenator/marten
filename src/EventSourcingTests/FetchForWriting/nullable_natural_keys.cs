using System;
using System.Threading.Tasks;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Marten.Testing.Harness;
using Shouldly;
using Xunit;

namespace EventSourcingTests.FetchForWriting;

public readonly record struct Sku(string Value);

public record ProductListed(Sku Sku, string Name);

public record ProductSkuChanged(Sku? NewSku);

public record ProductRestocked(int Quantity);

// The key is a struct, declared nullable on the aggregate, and set by sources returning both Sku and Sku?
public class Product
{
    public Guid Id { get; set; }

    [NaturalKey]
    public Sku? Sku { get; set; }

    public string Name { get; set; } = string.Empty;
    public int Stock { get; set; }

    [NaturalKeySource]
    public static Sku KeyFor(ProductListed e) => e.Sku;

    [NaturalKeySource]
    public static Sku? KeyFor(ProductSkuChanged e) => e.NewSku;

    public void Apply(ProductListed e)
    {
        Sku = e.Sku;
        Name = e.Name;
    }

    public void Apply(ProductSkuChanged e) => Sku = e.NewSku;

    public void Apply(ProductRestocked e) => Stock += e.Quantity;
}

// An async view mapped to the owner's key, declared non-nullable where the owner's is nullable
public class ProductStock
{
    public Guid Id { get; set; }

    [MappedNaturalKey<Product>]
    public Sku Sku { get; set; }

    public int Stock { get; set; }

    public static ProductStock Create(ProductListed e) => new() { Sku = e.Sku };

    public void Apply(ProductRestocked e) => Stock += e.Quantity;
}

public record TicketOpened(int? Number);

public record TicketNoted(string Note);

public class Ticket
{
    public Guid Id { get; set; }

    [NaturalKey]
    public int? Number { get; set; }

    public int Notes { get; set; }

    [NaturalKeySource]
    public void Apply(TicketOpened e) => Number = e.Number;

    public void Apply(TicketNoted e) => Notes++;
}

public class nullable_natural_keys: OneOffConfigurationsContext
{
    [Fact]
    public async Task a_nullable_struct_key_is_fetched_by_its_underlying_type()
    {
        StoreOptions(opts => opts.Projections.Snapshot<Product>(SnapshotLifecycle.Inline));

        var sku = new Sku("SKU-1");
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream<Product>(streamId, new ProductListed(sku, "Widget"), new ProductRestocked(5));
        await theSession.SaveChangesAsync();

        var latest = await theSession.Events.FetchLatest<Product, Sku>(sku);
        latest.ShouldNotBeNull();
        latest.Id.ShouldBe(streamId);
        latest.Stock.ShouldBe(5);

        var stream = await theSession.Events.FetchForWriting<Product, Sku>(sku);
        stream.Id.ShouldBe(streamId);
        stream.CurrentVersion.ShouldBe(2);
    }

    [Fact]
    public async Task a_nullable_struct_key_can_be_asked_for_by_its_nullable_type()
    {
        StoreOptions(opts => opts.Projections.Snapshot<Product>(SnapshotLifecycle.Inline));

        var sku = new Sku("SKU-2");
        theSession.Events.StartStream<Product>(Guid.NewGuid(), new ProductListed(sku, "Gadget"));
        await theSession.SaveChangesAsync();

        Sku? asNullable = sku;
        (await theSession.Events.FetchLatest<Product, Sku?>(asNullable))!.Name.ShouldBe("Gadget");
        (await theSession.Events.FetchForWriting<Product, Sku?>(asNullable)).CurrentVersion.ShouldBe(1);
    }

    [Fact]
    public async Task a_source_returning_the_nullable_key_renames_the_stream()
    {
        StoreOptions(opts => opts.Projections.Snapshot<Product>(SnapshotLifecycle.Inline));

        var original = new Sku("SKU-OLD");
        var renamed = new Sku("SKU-NEW");
        theSession.Events.StartStream<Product>(Guid.NewGuid(), new ProductListed(original, "Doohickey"));
        await theSession.SaveChangesAsync();

        var stream = await theSession.Events.FetchForWriting<Product, Sku>(original);
        stream.AppendOne(new ProductSkuChanged(renamed));
        await theSession.SaveChangesAsync();

        (await theSession.Events.FetchLatest<Product, Sku>(original)).ShouldBeNull();
        (await theSession.Events.FetchLatest<Product, Sku>(renamed))!.Sku.ShouldBe(renamed);
    }

    [Fact]
    public async Task a_nullable_struct_key_is_auto_registered_on_first_fetch()
    {
        // No projection registered: the first FetchForWriting by Sku registers an Inline snapshot
        StoreOptions(_ => { });

        var sku = new Sku("SKU-AUTO");
        var stream = await theSession.Events.FetchForWriting<Product, Sku>(sku);
        stream.Aggregate.ShouldBeNull();
        stream.AppendOne(new ProductListed(sku, "Thingamajig"));
        await theSession.SaveChangesAsync();

        await using var session = theStore.LightweightSession();
        (await session.Events.FetchLatest<Product, Sku>(sku))!.Name.ShouldBe("Thingamajig");
    }

    [Fact]
    public async Task a_non_nullable_mapped_key_reads_a_nullable_owned_key()
    {
        StoreOptions(opts =>
        {
            opts.Projections.Snapshot<Product>(SnapshotLifecycle.Inline);
            opts.Projections.Snapshot<ProductStock>(SnapshotLifecycle.Async);
        });

        var sku = new Sku("SKU-MAPPED");
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream<Product>(streamId, new ProductListed(sku, "Gizmo"), new ProductRestocked(3));
        await theSession.SaveChangesAsync();

        var stock = await theSession.Events.FetchLatest<ProductStock, Sku>(sku);
        stock.ShouldNotBeNull();
        stock.Id.ShouldBe(streamId);
        stock.Stock.ShouldBe(3);
    }

    [Fact]
    public async Task a_nullable_primitive_key_is_stored_and_fetched_as_its_primitive()
    {
        StoreOptions(opts => opts.Projections.Snapshot<Ticket>(SnapshotLifecycle.Inline));

        var streamId = Guid.NewGuid();
        theSession.Events.StartStream<Ticket>(streamId, new TicketOpened(1042), new TicketNoted("first"));

        // A ticket opened without a number has no key, and so no lookup row
        theSession.Events.StartStream<Ticket>(Guid.NewGuid(), new TicketOpened(null));
        await theSession.SaveChangesAsync();

        var latest = await theSession.Events.FetchLatest<Ticket, int>(1042);
        latest.ShouldNotBeNull();
        latest.Id.ShouldBe(streamId);
        latest.Notes.ShouldBe(1);

        (await theSession.Events.FetchLatest<Ticket, int?>(1042))!.Id.ShouldBe(streamId);
    }
}
