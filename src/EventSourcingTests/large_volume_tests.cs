using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using JasperFx.Core;
using Marten;
using Marten.Events;
using Marten.Events.Projections;
using Marten.Schema;
using Marten.Storage;
using Marten.Testing.Harness;
using Xunit;
using Xunit.Abstractions;
using Shouldly;

namespace EventSourcingTests;

public class large_volume_tests : OneOffConfigurationsContext
{
    private readonly ITestOutputHelper _testOutputHelper;

    public large_volume_tests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
        StoreOptions(opts =>
        {
            opts.Events.AppendMode = EventAppendMode.Quick;
            opts.Events.TenancyStyle = TenancyStyle.Conjoined;
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Policies.AllDocumentsAreMultiTenanted();

            // every opportunity to make things work
            opts.Events.AddEventType<LoadTestEvent>();
            opts.Events.AddEventType<LoadTestUnrelatedEvent>();
            opts.Projections.Snapshot<LoadTestInlineProjection>(SnapshotLifecycle.Inline);
            opts.Projections.Snapshot<LoadTestUnrelatedInlineProjection>(SnapshotLifecycle.Inline);
        });
    }

    [Fact]
    public async Task create_1_stream_with_1000_events_20_times()
    {
        var tenant = "tenant-1";
        await using var session = theStore.LightweightSession(tenant);

        await Preload(session);

        var sw = new Stopwatch();
        foreach (var iteration in Enumerable.Range(1, 20))
        {
            var events = Enumerable.Range(1, 1000).Select(i => new LoadTestEvent(Guid.NewGuid(), i));

            var streamKey = CombGuidIdGeneration.NewGuid().ToString();
            session.Events.StartStream(streamKey, events.ToList());

            sw.Restart();
            await session.SaveChangesAsync();
            _testOutputHelper.WriteLine($"{iteration:D3}: {sw.Elapsed:g}");
        }
    }

    [Fact]
    public async Task update_1_stream_with_1000_events_20_times()
    {
        var tenant = "tenant-1";
        await using var session = theStore.LightweightSession(tenant);

        await Preload(session);

        var streamKey = CombGuidIdGeneration.NewGuid().ToString();
        var eventCount = 1;
        session.Events.StartStream(streamKey, new LoadTestEvent(Guid.NewGuid(), eventCount));
        await session.SaveChangesAsync();

        var sw = new Stopwatch();
        foreach (var iteration in Enumerable.Range(1, 20))
        {
            var events = Enumerable.Range(eventCount + 1, 1000).Select(i => new LoadTestEvent(Guid.NewGuid(), i));
            eventCount += 1000;

            session.Events.Append(streamKey, eventCount, events.ToList());

            sw.Restart();
            await session.SaveChangesAsync();
            _testOutputHelper.WriteLine($"{iteration:D3}: {sw.Elapsed:g}");
        }


        // verify
        var result = await session.LoadAsync<LoadTestInlineProjection>(streamKey);

        result.ShouldNotBeNull();
        result.Sum.ShouldBe(20_001 * (20_001 + 1) / 2);
        result.Version.ShouldBe(20_001);
    }

    [Fact]
    public async Task create_1000_streams_with_1_events_20_times()
    {
        var tenant = "tenant-1";
        await using var session = theStore.LightweightSession(tenant);

        await Preload(session);

        var sw = new Stopwatch();
        foreach (var iteration in Enumerable.Range(1, 20))
        {
            foreach (var i in Enumerable.Range(1, 1000))
            {
                var streamKey = CombGuidIdGeneration.NewGuid().ToString();
                session.Events.StartStream(streamKey, new LoadTestEvent(Guid.NewGuid(), i));
            }

            sw.Restart();
            await session.SaveChangesAsync();
            _testOutputHelper.WriteLine($"{iteration:D3}: {sw.Elapsed:g}");
        }
    }

    [Fact]
    public async Task update_1000_streams_with_1_events_20_times()
    {
        var tenant = "tenant-1";
        await using var session = theStore.LightweightSession(tenant);

        await Preload(session);

        var streamKeys = new string[1000];
        foreach (var i in Enumerable.Range(1, 1000))
        {
            streamKeys[i-1] = CombGuidIdGeneration.NewGuid().ToString();
            session.Events.StartStream(streamKeys[i - 1], new LoadTestEvent(Guid.NewGuid(), 0));
        }

        await session.SaveChangesAsync();

        var sw = new Stopwatch();
        foreach (var iteration in Enumerable.Range(1, 20))
        {
            foreach (var i in Enumerable.Range(1, 1000))
            {
                session.Events.Append(streamKeys[i - 1], iteration+1, new LoadTestEvent(Guid.NewGuid(), iteration));
            }

            sw.Restart();
            await session.SaveChangesAsync();
            _testOutputHelper.WriteLine($"{iteration:D3}: {sw.Elapsed:g}");
        }

        // verify
        var result = await session.LoadAsync<LoadTestInlineProjection>(streamKeys[0]);

        result.ShouldNotBeNull();
        result.Version.ShouldBe(21);
        result.Sum.ShouldBe(20 * (20 + 1) / 2);
    }

    private static Task Preload(IDocumentSession session)
    {
        session.Events.StartStream(CombGuidIdGeneration.NewGuid().ToString(),
            new LoadTestEvent(Guid.NewGuid(), 0),
            new LoadTestUnrelatedEvent());
        return session.SaveChangesAsync();
    }
}

public record LoadTestEvent(Guid Value, int Count);
public record LoadTestUnrelatedEvent;

public record LoadTestInlineProjection
{
    [Identity]
    public string StreamKey { get; init; }
    public Guid LastValue { get; init; }
    public long Sum { get; init; }
    [Version]
    public int Version { get; set; }

    public LoadTestInlineProjection Apply(LoadTestEvent @event, LoadTestInlineProjection current)
    {
        return current with { LastValue = @event.Value, Sum = current.Sum + @event.Count };
    }
}


public record LoadTestUnrelatedInlineProjection
{
    [Identity]
    public string StreamKey { get; init; }
    public long Count { get; init; }
    [Version]
    public int Version { get; set; }

    public LoadTestUnrelatedInlineProjection Apply(LoadTestUnrelatedEvent @event, LoadTestUnrelatedInlineProjection current)
    {
        return current with { Count = current.Count + 1 };
    }
}
