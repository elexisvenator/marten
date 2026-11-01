using System;
using System.Threading.Tasks;
using EventSourcingTests.Aggregation;
using JasperFx.Events.Projections;
using Marten.Testing.Harness;
using Shouldly;
using Xunit;

namespace EventSourcingTests.FetchForWriting;

/// <summary>
/// An async-plan fetch brackets its reads in <c>begin transaction isolation level repeatable read read only</c>
/// … <c>end</c>, which PostgreSQL only accepts as a transaction's first statement. A batch used to refuse a
/// second one outright (#5535). Now every bracketed fetch at the start of a batch shares one bracket, and one
/// snapshot; one after anything else is still refused.
/// </summary>
public class batched_async_fetches_share_one_snapshot: OneOffConfigurationsContext
{
    private void configure()
    {
        StoreOptions(opts => opts.Projections.Snapshot<SimpleAggregate>(SnapshotLifecycle.Async));
    }

    private async Task<Guid> startStream(int aEvents)
    {
        var streamId = Guid.NewGuid();
        theSession.Events.StartStream<SimpleAggregate>(streamId, new BEvent());
        for (var i = 0; i < aEvents; i++)
        {
            theSession.Events.Append(streamId, new AEvent());
        }

        await theSession.SaveChangesAsync();
        return streamId;
    }

    [Fact]
    public async Task two_async_fetches_for_writing_at_the_start_of_a_batch()
    {
        configure();
        var first = await startStream(1);
        var second = await startStream(2);

        await using var session = theStore.LightweightSession();
        var before = session.RequestCount;

        var batch = session.CreateBatchQuery();
        var firstStream = batch.Events.FetchForWriting<SimpleAggregate>(first);
        var secondStream = batch.Events.FetchForWriting<SimpleAggregate>(second);
        await batch.Execute();

        session.RequestCount.ShouldBe(before + 1);
        (await firstStream).Aggregate!.ACount.ShouldBe(1);
        (await secondStream).Aggregate!.ACount.ShouldBe(2);

        // The handles still append and commit as usual
        (await firstStream).AppendOne(new AEvent());
        (await secondStream).AppendOne(new AEvent());
        await session.SaveChangesAsync();

        (await theSession.Events.FetchLatest<SimpleAggregate>(first))!.ACount.ShouldBe(2);
        (await theSession.Events.FetchLatest<SimpleAggregate>(second))!.ACount.ShouldBe(3);
    }

    [Fact]
    public async Task every_kind_of_bracketed_fetch_shares_the_start_with_other_items_after_it()
    {
        configure();
        var first = await startStream(1);
        var second = await startStream(2);
        var third = await startStream(3);

        await using var session = theStore.LightweightSession();
        var batch = session.CreateBatchQuery();

        var latest = batch.Events.FetchLatest<SimpleAggregate>(first);
        var writing = batch.Events.FetchForWriting<SimpleAggregate>(second);
        var expected = batch.Events.FetchForWriting<SimpleAggregate>(third, 4); // BEvent + 3 AEvents

        // Anything else can follow the bracketed fetches
        var load = batch.Load<SimpleAggregate>(Guid.NewGuid());

        await batch.Execute();

        (await latest)!.ACount.ShouldBe(1);
        (await writing).Aggregate!.ACount.ShouldBe(2);
        (await expected).Aggregate!.ACount.ShouldBe(3);
        (await load).ShouldBeNull();
    }

    [Fact]
    public async Task a_bracketed_fetch_after_another_kind_of_operation_is_still_refused()
    {
        configure();
        var first = await startStream(1);
        var second = await startStream(1);

        await using var session = theStore.LightweightSession();
        var batch = session.CreateBatchQuery();

        _ = batch.Events.FetchForWriting<SimpleAggregate>(first);
        _ = batch.Load<SimpleAggregate>(Guid.NewGuid());

        var ex = Should.Throw<InvalidOperationException>(() =>
            batch.Events.FetchForWriting<SimpleAggregate>(second));

        ex.Message.ShouldContain("has to come before every other kind of operation in its batch");
        ex.Message.ShouldContain("1 other operation(s)");
    }

    [Fact]
    public async Task several_async_fetches_on_a_session_holding_its_own_transaction()
    {
        // No bracket at all then (#5611), so nothing to share
        configure();
        var first = await startStream(1);
        var second = await startStream(2);

        await using var session = theStore.LightweightSession();
        await session.BeginTransactionAsync(default);

        var batch = session.CreateBatchQuery();
        _ = batch.Load<SimpleAggregate>(Guid.NewGuid());
        var firstStream = batch.Events.FetchForWriting<SimpleAggregate>(first);
        var secondStream = batch.Events.FetchForWriting<SimpleAggregate>(second);
        await batch.Execute();

        (await firstStream).Aggregate!.ACount.ShouldBe(1);
        (await secondStream).Aggregate!.ACount.ShouldBe(2);
    }

    [Fact]
    public async Task fetch_many_for_writing_an_async_aggregate_is_one_round_trip()
    {
        configure();
        var first = await startStream(1);
        var second = await startStream(2);
        var missing = Guid.NewGuid();

        await using var session = theStore.LightweightSession();
        var before = session.RequestCount;

        var streams = await session.Events.FetchManyForWriting<SimpleAggregate>([first, second, missing]);

        session.RequestCount.ShouldBe(before + 1);
        streams[0].Aggregate!.ACount.ShouldBe(1);
        streams[1].Aggregate!.ACount.ShouldBe(2);
        streams[2].Aggregate.ShouldBeNull();
        streams[2].CurrentVersion.ShouldBe(0);
    }
}
