using System;
using System.Collections.Generic;
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
using Marten.Internal.Storage;
using Marten.Linq.QueryHandlers;
using Marten.Services.BatchQuerying;
using Npgsql;
using Weasel.Postgresql;

namespace Marten.Events.Fetching;

/// <summary>
/// Fetches an aggregate by natural key in one round trip: a JOIN from the lookup table to
/// <c>mt_streams</c> reads the stream's version and identity, and the lifecycle's own reads (the snapshot,
/// the events after it, or the whole stream) find the stream through a subselect on the same lookup. No
/// statement depends on the result of another, so the same SQL runs standalone and inside a batch query.
/// </summary>
/// <remarks>
/// <para>
/// The lookup table belongs to the aggregate that owns the key. For a mapped key
/// (<see cref="NaturalKeyAttribute.SourceProjection"/>) that is the source projection's aggregate, which
/// maintains the table inline, so an Async aggregate is found by its key as soon as its events are
/// committed, before the daemon has caught up.
/// </para>
/// <para>
/// The Inline, Async and Live subclasses mirror <see cref="FetchInlinedPlan{TDoc,TId}"/>,
/// <see cref="FetchAsyncPlan{TDoc,TId}"/> and <see cref="FetchLivePlan{TDoc,TId}"/>. What those plans do
/// before the first read because they know the stream id up front -- the aggregate write cache and the
/// identity map short cuts -- has no equivalent here, since the stream id is only known once the batch is
/// read.
/// </para>
/// </remarks>
internal abstract class FetchNaturalKeyPlan<TDoc, TNaturalKey, TId>: IAggregateFetchPlan<TDoc, TNaturalKey>
    where TDoc : class where TNaturalKey : notnull where TId : notnull
{
    protected readonly EventGraph _events;
    private readonly NaturalKeyDefinition _naturalKey;
    protected readonly NaturalKeyQueryBuilder _lookup;

    protected FetchNaturalKeyPlan(EventGraph events, NaturalKeyDefinition naturalKey, NaturalKeyQueryBuilder lookup)
    {
        _events = events;
        _naturalKey = naturalKey;
        _lookup = lookup;
        IsGlobal = events.GlobalAggregates.Contains(typeof(TDoc));
    }

    public bool IsGlobal { get; }

    public abstract ProjectionLifecycle Lifecycle { get; }

    /// <summary>
    /// Whether the reads are bracketed to share one snapshot of the database (see
    /// <see cref="FetchAsyncPlan{TDoc,TId}"/>): only needed where the snapshot document and the events
    /// after it are read by separate statements.
    /// </summary>
    protected virtual bool SharesOneSnapshot => false;

    /// <summary>
    /// Whether a fetched stream is remembered in the identity map for a later FetchLatest by stream id, as
    /// the Async and Live stream plans do.
    /// </summary>
    protected virtual bool RemembersStreamInIdentityMap => true;

    /// <summary>
    /// The statements after the version row, each finding the stream through
    /// <see cref="NaturalKeyQueryBuilder.WriteStreamIdSubselect"/>.
    /// </summary>
    protected abstract void writeAggregateStatements(ICommandBuilder builder, QuerySession session, object keyValue);

    /// <summary>
    /// Reads the result sets <see cref="writeAggregateStatements"/> produced, positioned on the first of them.
    /// Reads all of them whether or not the key was found, so that a batch can move on to its next item.
    /// </summary>
    protected abstract Task<TDoc?> readAggregateAsync(DbDataReader reader, DocumentSessionBase session, bool found,
        TId streamId, long version, bool detectSnapshotRace, CancellationToken cancellation);

    /// <summary>
    /// Folds this session's pending events for the stream into the aggregate, for <see cref="ProjectLatest"/>.
    /// </summary>
    protected abstract Task<TDoc?> applyPendingEventsAsync(DocumentSessionBase session, TId streamId,
        TDoc? aggregate, IReadOnlyList<IEvent> pending, CancellationToken cancellation);

    /// <summary>
    /// Called before the Async plan's redo after a <see cref="SnapshotRaceException"/>, to forget what the
    /// first attempt loaded.
    /// </summary>
    protected virtual void forgetSnapshot(DocumentSessionBase session, TId streamId)
    {
    }

    protected virtual async Task prepareAsync(DocumentSessionBase session, CancellationToken cancellation)
    {
        // The event storage includes the natural key lookup tables
        await identityStrategy(session).EnsureEventStorageExists<TDoc>(session, cancellation).ConfigureAwait(false);
        await session.Database.EnsureStorageExistsAsync(typeof(TDoc), cancellation).ConfigureAwait(false);
    }

    protected static IEventIdentityStrategy<TId> identityStrategy(QuerySession session)
        => (IEventIdentityStrategy<TId>)session.Events;

    protected object keyValueFor(TNaturalKey id) => _naturalKey.Unwrap(id)!;

    #region writing

    public async Task<IEventStream<TDoc>> FetchForWriting(DocumentSessionBase session, TNaturalKey id,
        bool forUpdate, CancellationToken cancellation = default)
    {
        // Only the Async plan detects the snapshot race. See FetchAsyncPlan.SnapshotRaceRedos for why two
        // redos, and why the last attempt runs with detection off.
        for (var attempt = 0; attempt < 2; attempt++)
        {
            try
            {
                return await fetchForWriting(session, id, forUpdate, true, cancellation).ConfigureAwait(false);
            }
            catch (SnapshotRaceException e)
            {
                forgetSnapshot(session, (TId)e.Id);
            }
        }

        return await fetchForWriting(session, id, forUpdate, false, cancellation).ConfigureAwait(false);
    }

    public async Task<IEventStream<TDoc>> FetchForWriting(DocumentSessionBase session, TNaturalKey id,
        long expectedStartingVersion, CancellationToken cancellation = default)
    {
        var stream = await FetchForWriting(session, id, false, cancellation).ConfigureAwait(false);
        assertExpectedVersion(id, expectedStartingVersion, stream.CurrentVersion ?? 0);
        return stream;
    }

    private async Task<IEventStream<TDoc>> fetchForWriting(DocumentSessionBase session, TNaturalKey id,
        bool forUpdate, bool detectSnapshotRace, CancellationToken cancellation)
    {
        await prepareAsync(session, cancellation).ConfigureAwait(false);

        if (forUpdate)
        {
            await session.BeginTransactionAsync(cancellation).ConfigureAwait(false);
        }

        var builder = new BatchBuilder { TenantId = session.TenantId };

        // The exclusive path holds a row lock in the session's own transaction, so it must not bracket.
        writeFetch(builder, session, keyValueFor(id), forUpdate, !forUpdate && sharesOneSnapshot(session));

        try
        {
            await using var reader =
                await session.ExecuteReaderAsync(builder.Compile(), cancellation).ConfigureAwait(false);

            return await readStreamAsync(reader, session, detectSnapshotRace, cancellation).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not SnapshotRaceException && isLockFailure(e))
        {
            throw new StreamLockedException(id, e.InnerException!);
        }
    }

    private async Task<IEventStream<TDoc>> readStreamAsync(DbDataReader reader, DocumentSessionBase session,
        bool detectSnapshotRace, CancellationToken cancellation)
    {
        var (found, streamId, version, aggregate) =
            await readFetchAsync(reader, session, detectSnapshotRace, cancellation).ConfigureAwait(false);

        var identity = identityStrategy(session);

        // No live stream has this key, so this is a new stream. Its key is claimed when the projection that
        // owns the key sees the first event that sets it.
        if (!found) return identity.StartStream<TDoc>(null, session, newStreamId(), cancellation);

        var stream = version == 0
            ? identity.StartStream(aggregate, session, streamId, cancellation)
            : identity.AppendToStream(aggregate, session, streamId, version, cancellation);

        if (RemembersStreamInIdentityMap && ((IMartenSession)session).Options.Events.UseIdentityMapForAggregates)
        {
            session.StoreDocumentInItemMap(streamId, stream);
        }

        return stream;
    }

    private static void assertExpectedVersion(TNaturalKey id, long expected, long actual)
    {
        if (expected != actual)
        {
            throw new ConcurrencyException(
                $"Expected the existing version to be {expected}, but was {actual}",
                typeof(TDoc), id);
        }
    }

    private static bool isLockFailure(Exception e)
    {
        return e.InnerException is NpgsqlException { SqlState: PostgresErrorCodes.InFailedSqlTransaction } ||
               (e.InnerException != null && e.Message.Contains(MartenCommandException.MaybeLockedRowsMessage));
    }

    private static TId newStreamId()
    {
        return typeof(TId) == typeof(Guid)
            ? (TId)(object)Guid.NewGuid()
            : (TId)(object)Guid.NewGuid().ToString();
    }

    #endregion

    #region reading

    public async ValueTask<TDoc?> FetchForReading(DocumentSessionBase session, TNaturalKey id,
        CancellationToken cancellation)
    {
        var (found, _, _, aggregate) = await fetchForReading(session, id, cancellation).ConfigureAwait(false);
        return found ? aggregate : default;
    }

    public async ValueTask<TDoc?> ProjectLatest(DocumentSessionBase session, TNaturalKey id,
        CancellationToken cancellation)
    {
        // A stream started in this session has no lookup row until SaveChanges runs the projection that
        // writes it, so it cannot be found here. A committed stream can, and this session's pending events
        // for it are folded in.
        var (found, streamId, _, aggregate) = await fetchForReading(session, id, cancellation).ConfigureAwait(false);
        if (!found) return default;

        var pending = FetchPlanHelper.FindPendingEvents<TId>(session, streamId);
        if (pending is not { Count: > 0 }) return aggregate;

        return await applyPendingEventsAsync(session, streamId, aggregate, pending, cancellation)
            .ConfigureAwait(false);
    }

    public virtual async Task<bool> StreamForReading(DocumentSessionBase session, TNaturalKey id,
        Stream destination, CancellationToken cancellation)
    {
        var aggregate = await FetchForReading(session, id, cancellation).ConfigureAwait(false);
        if (aggregate == null) return false;

        using var buffer = new Services.PooledByteBufferWriter();
        session.Serializer.WriteTo(buffer, aggregate);
        await destination.WriteAsync(buffer.WrittenMemory, cancellation).ConfigureAwait(false);
        return true;
    }

    private async Task<(bool found, TId streamId, long version, TDoc? aggregate)> fetchForReading(
        DocumentSessionBase session, TNaturalKey id, CancellationToken cancellation)
    {
        await prepareAsync(session, cancellation).ConfigureAwait(false);

        var builder = new BatchBuilder { TenantId = session.TenantId };
        writeFetch(builder, session, keyValueFor(id), false, sharesOneSnapshot(session));

        await using var reader =
            await session.ExecuteReaderAsync(builder.Compile(), cancellation).ConfigureAwait(false);

        return await readFetchAsync(reader, session, false, cancellation).ConfigureAwait(false);
    }

    #endregion

    #region the one batched command

    private bool sharesOneSnapshot(QuerySession session)
        => SharesOneSnapshot && FetchAsyncPlan<TDoc, TId>.canShareOneSnapshot(session);

    private void writeFetch(ICommandBuilder builder, QuerySession session, object keyValue, bool forUpdate,
        bool sharedSnapshot)
    {
        var opened = sharedSnapshot && FetchAsyncPlan<TDoc, TId>.beginSharedSnapshot(builder, session);

        _lookup.WriteVersionAndStreamId(builder, keyValue, forUpdate);

        builder.StartNewCommand();
        writeAggregateStatements(builder, session, keyValue);

        FetchAsyncPlan<TDoc, TId>.endSharedSnapshot(builder, opened);
    }

    private async Task<(bool found, TId streamId, long version, TDoc? aggregate)> readFetchAsync(
        DbDataReader reader, DocumentSessionBase session, bool detectSnapshotRace, CancellationToken cancellation)
    {
        var found = false;
        var version = 0L;
        TId streamId = default!;

        if (await reader.ReadAsync(cancellation).ConfigureAwait(false))
        {
            found = true;
            version = await reader.GetFieldValueAsync<long>(0, cancellation).ConfigureAwait(false);
            streamId = await reader.GetFieldValueAsync<TId>(1, cancellation).ConfigureAwait(false);
        }

        await reader.NextResultAsync(cancellation).ConfigureAwait(false);
        var aggregate = await readAggregateAsync(reader, session, found, streamId, version, detectSnapshotRace,
            cancellation).ConfigureAwait(false);

        return (found, streamId, version, aggregate);
    }

    #endregion

    #region batch query handlers

    public IQueryHandler<IEventStream<TDoc>> BuildQueryHandler(QuerySession session, TNaturalKey id,
        long expectedStartingVersion)
    {
        session.AssertIsDocumentSession();
        return new StreamHandler(this, id, false, expectedStartingVersion, sharesOneSnapshot(session));
    }

    public IQueryHandler<IEventStream<TDoc>> BuildQueryHandler(QuerySession session, TNaturalKey id,
        bool forUpdate)
    {
        session.AssertIsDocumentSession();
        return new StreamHandler(this, id, forUpdate, null, !forUpdate && sharesOneSnapshot(session));
    }

    public IQueryHandler<TDoc?> BuildQueryHandler(QuerySession session, TNaturalKey id)
    {
        return new LatestHandler(this, id, sharesOneSnapshot(session));
    }

    private sealed class StreamHandler: IQueryHandler<IEventStream<TDoc>>, IOpensItsOwnTransaction
    {
        private readonly FetchNaturalKeyPlan<TDoc, TNaturalKey, TId> _parent;
        private readonly TNaturalKey _id;
        private readonly bool _forUpdate;
        private readonly long? _expectedVersion;
        private readonly bool _sharedSnapshot;

        public StreamHandler(FetchNaturalKeyPlan<TDoc, TNaturalKey, TId> parent, TNaturalKey id, bool forUpdate,
            long? expectedVersion, bool sharedSnapshot)
        {
            _parent = parent;
            _id = id;
            _forUpdate = forUpdate;
            _expectedVersion = expectedVersion;
            _sharedSnapshot = sharedSnapshot;
        }

        /// <summary>
        /// True exactly when <see cref="ConfigureCommand"/> brackets its reads, as for
        /// <see cref="FetchAsyncPlan{TDoc,TId}.ForUpdateQueryHandler"/>.
        /// </summary>
        public bool OpensItsOwnTransaction => _sharedSnapshot;

        public string CallName => "FetchForWriting";

        public void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
        {
            // Asked again rather than reusing _sharedSnapshot, as FetchAsyncPlan's handlers do: the session can
            // acquire a transaction between enlistment and Execute().
            var querySession = (QuerySession)session;
            _parent.writeFetch(builder, querySession, _parent.keyValueFor(_id), _forUpdate,
                !_forUpdate && _parent.sharesOneSnapshot(querySession));
        }

        public async Task<IEventStream<TDoc>> HandleAsync(DbDataReader reader, IStorageSession session,
            CancellationToken token)
        {
            IEventStream<TDoc> stream;
            try
            {
                // A batch cannot redo itself, so a snapshot race surfaces as SnapshotRaceException, as it does
                // for a batched fetch by stream id.
                stream = await _parent.readStreamAsync(reader, (DocumentSessionBase)session, true, token)
                    .ConfigureAwait(false);
            }
            catch (Exception e) when (e is not SnapshotRaceException && isLockFailure(e))
            {
                throw new StreamLockedException(_id, e.InnerException!);
            }

            if (_expectedVersion.HasValue)
            {
                assertExpectedVersion(_id, _expectedVersion.Value, stream.CurrentVersion ?? 0);
            }

            return stream;
        }

        public Task<int> StreamJson(Stream stream, DbDataReader reader, CancellationToken token)
        {
            throw new NotSupportedException();
        }
    }

    private sealed class LatestHandler: IQueryHandler<TDoc?>, IOpensItsOwnTransaction
    {
        private readonly FetchNaturalKeyPlan<TDoc, TNaturalKey, TId> _parent;
        private readonly TNaturalKey _id;
        private readonly bool _sharedSnapshot;

        public LatestHandler(FetchNaturalKeyPlan<TDoc, TNaturalKey, TId> parent, TNaturalKey id,
            bool sharedSnapshot)
        {
            _parent = parent;
            _id = id;
            _sharedSnapshot = sharedSnapshot;
        }

        /// <summary>
        /// True exactly when <see cref="ConfigureCommand"/> brackets its reads, as for
        /// <see cref="FetchAsyncPlan{TDoc,TId}.QueryHandler"/>.
        /// </summary>
        public bool OpensItsOwnTransaction => _sharedSnapshot;

        public string CallName => "FetchLatest";

        public void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
        {
            var querySession = (QuerySession)session;
            _parent.writeFetch(builder, querySession, _parent.keyValueFor(_id), false,
                _parent.sharesOneSnapshot(querySession));
        }

        public async Task<TDoc?> HandleAsync(DbDataReader reader, IStorageSession session, CancellationToken token)
        {
            var (found, _, _, aggregate) = await _parent
                .readFetchAsync(reader, (DocumentSessionBase)session, false, token).ConfigureAwait(false);

            return found ? aggregate : default;
        }

        public Task<int> StreamJson(Stream stream, DbDataReader reader, CancellationToken token)
        {
            throw new NotSupportedException();
        }
    }

    #endregion

    #region shared statements

    /// <summary>
    /// <c>select [document fields] where id = (the key's stream id)</c>, for the storage's own selector.
    /// </summary>
    protected void writeDocumentLoad(ICommandBuilder builder, IDocumentStorage<TDoc, TId> storage,
        IStorageSession session, object keyValue)
    {
        builder.Append("select ");

        var fields = storage.SelectFields();
        builder.Append(fields[0]);
        for (var i = 1; i < fields.Length; i++)
        {
            builder.Append(", ");
            builder.Append(fields[i]);
        }

        builder.Append(" from ");
        builder.Append(storage.FromObject);
        builder.Append(" as d where id = ");
        _lookup.WriteStreamIdSubselect(builder, keyValue);

        storage.AddTenancyFilter(builder, session.TenantId);
    }

    /// <summary>
    /// Reads what <see cref="writeDocumentLoad"/> selected, through <see cref="LoadByIdHandler{T,TId}"/>,
    /// which uses its id only to write SQL.
    /// </summary>
    protected static Task<TDoc> readDocumentAsync(DbDataReader reader, IDocumentStorage<TDoc, TId> storage,
        DocumentSessionBase session, CancellationToken cancellation)
    {
        return new LoadByIdHandler<TDoc, TId>(storage, default!).HandleAsync(reader, session, cancellation);
    }

    #endregion
}
