using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Marten.Internal;
using Marten.Internal.Sessions;
using Marten.Internal.Storage;
using Weasel.Postgresql;
using System.Diagnostics.CodeAnalysis;

namespace Marten.Events.Fetching;

/// <summary>
/// An Inline snapshot is written in the same transaction as its events, so it is always at the stream head:
/// the version row and the stored document are everything there is to read.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026",
    Justification = "Class-level: as FetchInlinedPlan. Aggregate types are preserved at the projection-registration boundary.")]
[UnconditionalSuppressMessage("AOT", "IL3050",
    Justification = "Class-level: as FetchInlinedPlan. AOT consumers pre-generate codegen artifacts.")]
internal class FetchNaturalKeyInlinePlan<TDoc, TNaturalKey, TId>: FetchNaturalKeyPlan<TDoc, TNaturalKey, TId>
    where TDoc : class where TNaturalKey : notnull where TId : notnull
{
    public FetchNaturalKeyInlinePlan(EventGraph events, NaturalKeyDefinition naturalKey,
        NaturalKeyQueryBuilder lookup): base(events, naturalKey, lookup)
    {
    }

    public override ProjectionLifecycle Lifecycle => ProjectionLifecycle.Inline;

    // The document itself goes into the identity map through the IdentityOnly storage, as for
    // FetchInlinedPlan, rather than the stream.
    protected override bool RemembersStreamInIdentityMap => false;

    /// <summary>
    /// The same choice of storage as <see cref="FetchInlinedPlan{TDoc,TId}"/>.
    /// </summary>
    private static IDocumentStorage<TDoc, TId> findDocumentStorage(QuerySession session)
    {
        var options = ((IMartenSession)session).Options;
        if (options.Events.UseIdentityMapForAggregates)
        {
            // Opt into the identity map mechanics for this aggregate type just in case you're using a
            // lightweight session
            session.UseIdentityMapFor<TDoc>();
            return options.ResolveCorrectedDocumentStorage<TDoc, TId>(DocumentTracking.IdentityOnly);
        }

        return session.StorageFor<TDoc, TId>();
    }

    protected override void writeAggregateStatements(ICommandBuilder builder, QuerySession session,
        object keyValue)
    {
        writeDocumentLoad(builder, findDocumentStorage(session), session, keyValue);
    }

    protected override async Task<TDoc?> readAggregateAsync(DbDataReader reader, DocumentSessionBase session,
        bool found, TId streamId, long version, bool detectSnapshotRace, CancellationToken cancellation)
    {
        return await readDocumentAsync(reader, findDocumentStorage(session), session, cancellation)
            .ConfigureAwait(false);
    }

    protected override async Task<TDoc?> applyPendingEventsAsync(DocumentSessionBase session, TId streamId,
        TDoc? aggregate, IReadOnlyList<IEvent> pending, CancellationToken cancellation)
    {
        var storage = findDocumentStorage(session);
        var raw = ((IMartenSession)session).Options.Projections.AggregatorFor<TDoc>();
        var aggregator = raw as IAggregator<TDoc, TId, IQuerySession>
                         ?? typeof(IdentityForwardingAggregator<,,,>)
                             .CloseAndBuildAs<IAggregator<TDoc, TId, IQuerySession>>(raw, storage, typeof(TDoc),
                                 storage.IdType, typeof(TId), typeof(IQuerySession));

        aggregate = await aggregator.BuildAsync(pending, session, aggregate, streamId, storage, cancellation)
            .ConfigureAwait(false);

        // As FetchInlinedPlan.ProjectLatest: BuildAsync may have mutated the cached instance in place, and the
        // inline projection would re-apply the same events to it during SaveChangesAsync.
        session.EjectAggregateFromIdentityMap<TDoc, TId>(streamId);

        return aggregate;
    }

    /// <summary>
    /// The stored document is the aggregate, so its JSON streams straight from the database.
    /// </summary>
    public override async Task<bool> StreamForReading(DocumentSessionBase session, TNaturalKey id,
        Stream destination, CancellationToken cancellation)
    {
        await prepareAsync(session, cancellation).ConfigureAwait(false);

        // StreamOne needs a plain command, not a batch
        var builder = new CommandBuilder { TenantId = session.TenantId };
        writeDocumentLoad(builder, findDocumentStorage(session), session, keyValueFor(id));

        return await session.StreamOne(builder.Compile(), destination, cancellation).ConfigureAwait(false);
    }
}
