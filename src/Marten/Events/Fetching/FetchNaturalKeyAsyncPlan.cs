using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Marten.Exceptions;
using Marten.Services;
using Marten.Internal.Sessions;
using Marten.Internal.Storage;
using Marten.Linq.QueryHandlers;
using Marten.Storage;
using Weasel.Postgresql;
using System.Diagnostics.CodeAnalysis;

namespace Marten.Events.Fetching;

/// <summary>
/// An Async snapshot lags its stream until the daemon catches up, so the fetch reads the stored snapshot and
/// the events after it, as <see cref="FetchAsyncPlan{TDoc,TId}"/> does, and folds them together.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026",
    Justification = "Class-level: as FetchAsyncPlan. Aggregate types are preserved at the projection-registration boundary.")]
[UnconditionalSuppressMessage("AOT", "IL3050",
    Justification = "Class-level: as FetchAsyncPlan. AOT consumers pre-generate codegen artifacts.")]
internal class FetchNaturalKeyAsyncPlan<TDoc, TNaturalKey, TId>: FetchNaturalKeyPlan<TDoc, TNaturalKey, TId>
    where TDoc : class where TNaturalKey : notnull where TId : notnull
{
    private readonly IAggregator<TDoc, TId, IQuerySession> _aggregator;
    private readonly IDocumentStorage<TDoc, TId> _storage;
    private readonly string _deltaJoinSql;
    private readonly string _aggregateTypeName = typeof(TDoc).FullNameInCode();
    private string? _initialSql;

    /// <param name="storage">IdentityOnly storage, as for <see cref="FetchAsyncPlan{TDoc,TId}"/>: its
    /// selector records the snapshot's revision, which the snapshot race detection reads.</param>
    public FetchNaturalKeyAsyncPlan(EventGraph events, NaturalKeyDefinition naturalKey,
        NaturalKeyQueryBuilder lookup, IDocumentStorage<TDoc, TId> storage): base(events, naturalKey, lookup)
    {
        _storage = storage;

        var raw = events.Options.Projections.AggregatorFor<TDoc>();
        _aggregator = raw as IAggregator<TDoc, TId, IQuerySession>
                      ?? typeof(IdentityForwardingAggregator<,,,>)
                          .CloseAndBuildAs<IAggregator<TDoc, TId, IQuerySession>>(raw, _storage, typeof(TDoc),
                              _storage.IdType, typeof(TId), typeof(IQuerySession));

        _deltaJoinSql = events.TenancyStyle == TenancyStyle.Single || IsGlobal
            ? $" left outer join {storage.TableName.QualifiedName} as a on d.stream_id = a.id where (a.mt_version is NULL or d.version > a.mt_version) and d.stream_id = "
            : $" left outer join {storage.TableName.QualifiedName} as a on d.stream_id = a.id and d.tenant_id = a.tenant_id where (a.mt_version is NULL or d.version > a.mt_version) and d.stream_id = ";
    }

    public override ProjectionLifecycle Lifecycle => ProjectionLifecycle.Async;

    protected override bool SharesOneSnapshot => true;

    protected override async Task prepareAsync(DocumentSessionBase session, CancellationToken cancellation)
    {
        await base.prepareAsync(session, cancellation).ConfigureAwait(false);
        ensureInitialSql(session.EventStorage());
    }

    private void ensureInitialSql(IEventStorage selector)
    {
        _initialSql ??=
            $"select {selector.SelectFields().Select(x => "d." + x).Join(", ")} from {_events.DatabaseSchemaName}.mt_events as d";
    }

    protected override void forgetSnapshot(DocumentSessionBase session, TId streamId)
        => FetchAsyncPlan<TDoc, TId>.forgetSnapshot(session, streamId);

    protected override void writeAggregateStatements(ICommandBuilder builder, QuerySession session,
        object keyValue)
    {
        // A batch enlists without going through prepareAsync
        ensureInitialSql(session.EventStorage());

        writeDocumentLoad(builder, _storage, session, keyValue);

        builder.StartNewCommand();

        builder.Append(_initialSql!);
        builder.Append(_deltaJoinSql);
        _lookup.WriteStreamIdSubselect(builder, keyValue);

        // You must do this for performance even if the stream ids were magically unique across tenants
        if (_events.TenancyStyle == TenancyStyle.Conjoined && !IsGlobal)
        {
            builder.Append(" and d.tenant_id = ");
            builder.AppendParameter(builder.TenantId);
        }

        builder.Append(" order by d.seq_id");
    }

    protected override async Task<TDoc?> readAggregateAsync(DbDataReader reader, DocumentSessionBase session,
        bool found, TId streamId, long version, bool detectSnapshotRace, CancellationToken cancellation)
    {
        TDoc? document = await readDocumentAsync(reader, _storage, session, cancellation).ConfigureAwait(false);

        await reader.NextResultAsync(cancellation).ConfigureAwait(false);
        var events = await new ListQueryHandler<IEvent>(null, session.EventStorage())
            .HandleAsync(reader, session, cancellation).ConfigureAwait(false);

        if (!found) return default;

        _events.Options.OpenTelemetry
            .RecordEventsReplayed(events.Count, _aggregateTypeName, OpenTelemetryOptions.AsyncPlan);

        // #5613: see FetchAsyncPlan.snapshotAndDeltaDisagree. Only the writing paths detect, as there.
        if (detectSnapshotRace &&
            FetchAsyncPlan<TDoc, TId>.snapshotAndDeltaDisagree(session, streamId, version, document, events))
        {
            throw new SnapshotRaceException(typeof(TDoc), streamId);
        }

        if (events.Any())
        {
            document = await _aggregator.BuildAsync(events, session, document, streamId, _storage, cancellation)
                .ConfigureAwait(false);
        }

        if (document != null)
        {
            _storage.SetIdentity(document, streamId);
        }

        return document;
    }

    protected override async Task<TDoc?> applyPendingEventsAsync(DocumentSessionBase session, TId streamId,
        TDoc? aggregate, IReadOnlyList<IEvent> pending, CancellationToken cancellation)
    {
        aggregate = await _aggregator.BuildAsync(pending, session, aggregate, streamId, _storage, cancellation)
            .ConfigureAwait(false);

        // As FetchAsyncPlan.ProjectLatest: store the updated document so it persists when the session commits
        if (aggregate != null)
        {
            session.Store(aggregate);
        }

        return aggregate;
    }
}
