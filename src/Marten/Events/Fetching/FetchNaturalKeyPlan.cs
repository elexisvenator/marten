using System;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JasperFx;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Marten.Exceptions;
using Marten.Internal;
using Marten.Internal.Sessions;
using Marten.Linq.QueryHandlers;
using Weasel.Postgresql;

namespace Marten.Events.Fetching;

/// <summary>
/// Resolves a natural key to its stream through the natural key lookup table, then hands the fetch to the
/// ordinary plan for that stream identity.
/// </summary>
/// <remarks>
/// <para>
/// This used to be one plan that did its own document loading for every lifecycle. That made an Async
/// aggregate come back as whatever snapshot the daemon had last written, with none of the events after
/// it, and every later change to the Inline, Async and Live plans (aggregate write caching, the shared
/// snapshot read, the identity map) had to be made a second time here or was simply missing. Once the
/// key is resolved the fetch <em>is</em> an ordinary fetch by stream identity, so it forwards to the plan
/// for <typeparamref name="TId"/>, the same way <see cref="UnwrappedIdentityFetchPlan{TDoc,TId,TInner}"/>
/// forwards a strong-typed id.
/// </para>
/// <para>
/// The lookup table belongs to the aggregate that owns the key. For a mapped key
/// (<see cref="NaturalKeyAttribute.SourceProjection"/>) that is the source projection's aggregate, which
/// maintains the table inline, so an Async aggregate is found by its key as soon as its events are
/// committed, before the daemon has caught up.
/// </para>
/// </remarks>
internal class FetchNaturalKeyPlan<TDoc, TNaturalKey, TId>: IAggregateFetchPlan<TDoc, TNaturalKey>
    where TDoc : class where TNaturalKey : notnull where TId : notnull
{
    private readonly NaturalKeyDefinition _naturalKey;
    private readonly NaturalKeyQueryBuilder _lookup;

    public FetchNaturalKeyPlan(NaturalKeyDefinition naturalKey, NaturalKeyQueryBuilder lookup,
        ProjectionLifecycle lifecycle)
    {
        _naturalKey = naturalKey;
        _lookup = lookup;
        Lifecycle = lifecycle;
    }

    public ProjectionLifecycle Lifecycle { get; }

    public async Task<IEventStream<TDoc>> FetchForWriting(DocumentSessionBase session, TNaturalKey id,
        bool forUpdate, CancellationToken cancellation = default)
    {
        var (found, streamId) = await resolveAsync(session, id, cancellation).ConfigureAwait(false);
        if (!found) return startStream(session, cancellation);

        return await innerPlan(session).FetchForWriting(session, streamId, forUpdate, cancellation)
            .ConfigureAwait(false);
    }

    public async Task<IEventStream<TDoc>> FetchForWriting(DocumentSessionBase session, TNaturalKey id,
        long expectedStartingVersion, CancellationToken cancellation = default)
    {
        var (found, streamId) = await resolveAsync(session, id, cancellation).ConfigureAwait(false);
        if (!found) return startStream(session, id, expectedStartingVersion, cancellation);

        return await innerPlan(session).FetchForWriting(session, streamId, expectedStartingVersion, cancellation)
            .ConfigureAwait(false);
    }

    public async ValueTask<TDoc?> FetchForReading(DocumentSessionBase session, TNaturalKey id,
        CancellationToken cancellation)
    {
        var (found, streamId) = await resolveAsync(session, id, cancellation).ConfigureAwait(false);
        if (!found) return default;

        return await innerPlan(session).FetchForReading(session, streamId, cancellation).ConfigureAwait(false);
    }

    public async ValueTask<TDoc?> ProjectLatest(DocumentSessionBase session, TNaturalKey id,
        CancellationToken cancellation)
    {
        // A stream started in this session has no lookup row until SaveChanges runs the projection that
        // writes it, so it cannot be found here. A committed stream can, and the stream plan then folds in
        // this session's pending events for it.
        var (found, streamId) = await resolveAsync(session, id, cancellation).ConfigureAwait(false);
        if (!found) return default;

        return await innerPlan(session).ProjectLatest(session, streamId, cancellation).ConfigureAwait(false);
    }

    public async Task<bool> StreamForReading(DocumentSessionBase session, TNaturalKey id, Stream destination,
        CancellationToken cancellation)
    {
        var (found, streamId) = await resolveAsync(session, id, cancellation).ConfigureAwait(false);
        if (!found) return false;

        return await innerPlan(session).StreamForReading(session, streamId, destination, cancellation)
            .ConfigureAwait(false);
    }

    public IQueryHandler<IEventStream<TDoc>> BuildQueryHandler(QuerySession session, TNaturalKey id,
        long expectedStartingVersion)
    {
        session.AssertIsDocumentSession();
        return new ResolveThenFetch<IEventStream<TDoc>>(this, id,
            (s, found, streamId, token) => found
                ? innerPlan(s).FetchForWriting(s, streamId, expectedStartingVersion, token)
                : Task.FromResult(startStream(s, id, expectedStartingVersion, token)));
    }

    public IQueryHandler<IEventStream<TDoc>> BuildQueryHandler(QuerySession session, TNaturalKey id, bool forUpdate)
    {
        session.AssertIsDocumentSession();
        return new ResolveThenFetch<IEventStream<TDoc>>(this, id,
            (s, found, streamId, token) => found
                ? innerPlan(s).FetchForWriting(s, streamId, forUpdate, token)
                : Task.FromResult(startStream(s, token)));
    }

    public IQueryHandler<TDoc?> BuildQueryHandler(QuerySession session, TNaturalKey id)
    {
        return new ResolveThenFetch<TDoc?>(this, id,
            async (s, found, streamId, token) => found
                ? await innerPlan(s).FetchForReading(s, streamId, token).ConfigureAwait(false)
                : default);
    }

    private static IAggregateFetchPlan<TDoc, TId> innerPlan(DocumentSessionBase session)
        => ((EventStore)session.Events).FindFetchPlan<TDoc, TId>();

    private async Task<(bool found, TId streamId)> resolveAsync(DocumentSessionBase session, TNaturalKey id,
        CancellationToken cancellation)
    {
        await ensureEventStorage(session, cancellation).ConfigureAwait(false);

        var builder = new BatchBuilder { TenantId = session.TenantId };
        _lookup.WriteStreamLookup(builder, _naturalKey.Unwrap(id)!);

        await using var reader =
            await session.ExecuteReaderAsync(builder.Compile(), cancellation).ConfigureAwait(false);
        return await readStreamId(reader, cancellation).ConfigureAwait(false);
    }

    private static async Task<(bool found, TId streamId)> readStreamId(DbDataReader reader,
        CancellationToken cancellation)
    {
        if (!await reader.ReadAsync(cancellation).ConfigureAwait(false)) return (false, default!);

        return (true, await reader.GetFieldValueAsync<TId>(0, cancellation).ConfigureAwait(false));
    }

    private static Task ensureEventStorage(DocumentSessionBase session, CancellationToken cancellation)
        => ((IEventIdentityStrategy<TId>)session.Events).EnsureEventStorageExists<TDoc>(session, cancellation);

    private IEventStream<TDoc> startStream(DocumentSessionBase session, TNaturalKey id, long expectedStartingVersion,
        CancellationToken cancellation)
    {
        if (expectedStartingVersion != 0)
        {
            throw new ConcurrencyException(
                $"Expected the existing version to be {expectedStartingVersion}, but was 0",
                typeof(TDoc), id);
        }

        return startStream(session, cancellation);
    }

    // No stream has this key yet, so this is a new stream. Its key is claimed when the projection that owns
    // the key sees the first event that sets it.
    private static IEventStream<TDoc> startStream(DocumentSessionBase session, CancellationToken cancellation)
    {
        var newId = typeof(TId) == typeof(Guid)
            ? (TId)(object)Guid.NewGuid()
            : (TId)(object)Guid.NewGuid().ToString();

        return ((IEventIdentityStrategy<TId>)session.Events).StartStream<TDoc>(null, session, newId, cancellation);
    }

    /// <summary>
    /// The batched form: the lookup goes in the batch, and the fetch by stream identity follows once the
    /// batch has been read.
    /// </summary>
    private sealed class ResolveThenFetch<T>: IQueryHandler<T>
    {
        private readonly FetchNaturalKeyPlan<TDoc, TNaturalKey, TId> _parent;
        private readonly TNaturalKey _id;
        private readonly Func<DocumentSessionBase, bool, TId, CancellationToken, Task<T>> _fetch;

        public ResolveThenFetch(FetchNaturalKeyPlan<TDoc, TNaturalKey, TId> parent, TNaturalKey id,
            Func<DocumentSessionBase, bool, TId, CancellationToken, Task<T>> fetch)
        {
            _parent = parent;
            _id = id;
            _fetch = fetch;
        }

        public void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
        {
            _parent._lookup.WriteStreamLookup(builder, _parent._naturalKey.Unwrap(_id)!);
        }

        public async Task<T> HandleAsync(DbDataReader reader, IStorageSession session, CancellationToken token)
        {
            var (found, streamId) = await readStreamId(reader, token).ConfigureAwait(false);

            // Finish this result set before the fetch by stream identity issues its own command.
            while (await reader.ReadAsync(token).ConfigureAwait(false)) { }

            return await _fetch((DocumentSessionBase)session, found, streamId, token).ConfigureAwait(false);
        }

        public Task<int> StreamJson(Stream stream, DbDataReader reader, CancellationToken token)
        {
            throw new NotSupportedException();
        }
    }
}
