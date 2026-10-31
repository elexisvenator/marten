using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Marten.Exceptions;
using Marten.Internal.Storage;
using Marten.Schema;

namespace Marten.Events.Fetching;

/// <summary>
/// Fetch planner that activates when:
/// 1. The aggregate type has a natural key, owned or mapped, whose type is the TId being fetched
/// 2. That TId is not the store's stream identity type
///
/// This planner is registered BEFORE the built-in planners so it gets first crack at matching.
/// </summary>
internal class NaturalKeyFetchPlanner: IFetchPlanner
{
    public bool TryMatch<TDoc, TId>(IEventIdentityStrategy<TId> identity,
        StoreOptions options,
        [NotNullWhen(true)] out IAggregateFetchPlan<TDoc, TId>? plan) where TDoc : class where TId : notnull
    {
        // #5344: this used to exclude Guid and string outright, on the reasoning that both are stream
        // identity types handled by the built-in planners. Only one of them is, though -- whichever the
        // store actually uses. EventStore.IsNaturalKeyIdentity narrows the exclusion to the store's own
        // stream identity type, which keeps the ambiguous case (a string key on a string-identity store)
        // resolving to the stream as it always has.
        if (EventStore.IsNaturalKeyIdentity<TDoc, TId>(options) &&
            options.Projections.TryFindAggregate(typeof(TDoc), out var projection) &&
            FindNaturalKey(projection, typeof(TId)) is { } naturalKey)
        {
            var lookup = naturalKey.IsMapped
                ? naturalKey.SourceProjectionDefinition ?? throw new InvalidOperationException(
                    $"The mapped natural key {typeof(TDoc).FullNameInCode()}.{naturalKey.Member.Name} was never resolved to its source projection {naturalKey.SourceProjectionType!.FullNameInCode()}. Mapped keys are resolved when the DocumentStore is built.")
                : naturalKey;

            var builder = new NaturalKeyQueryBuilder(options.EventGraph, lookup);

            plan = options.EventGraph.StreamIdentity == StreamIdentity.AsGuid
                ? buildPlan<TDoc, TId, Guid>(options, projection, naturalKey, builder)
                : buildPlan<TDoc, TId, string>(options, projection, naturalKey, builder);
            return true;
        }

        plan = null;
        return false;
    }

    /// <summary>
    /// The same choice of lifecycle plan as the Inline, Async and Live planners make for a stream id. An Async
    /// aggregate without revision metadata is fetched by the Live plan there, so it is here too.
    /// </summary>
    private static IAggregateFetchPlan<TDoc, TNaturalKey> buildPlan<TDoc, TNaturalKey, TStreamId>(
        StoreOptions options, IAggregateProjection projection, NaturalKeyDefinition naturalKey,
        NaturalKeyQueryBuilder builder)
        where TDoc : class where TNaturalKey : notnull where TStreamId : notnull
    {
        var events = options.EventGraph;

        if (projection.Lifecycle == ProjectionLifecycle.Inline)
        {
            return new FetchNaturalKeyInlinePlan<TDoc, TNaturalKey, TStreamId>(events, naturalKey, builder);
        }

        if (projection.Lifecycle == ProjectionLifecycle.Async &&
            options.Storage.FindMapping(typeof(TDoc)) is DocumentMapping { Metadata.Revision.Enabled: true })
        {
            return new FetchNaturalKeyAsyncPlan<TDoc, TNaturalKey, TStreamId>(events, naturalKey, builder,
                options.ResolveCorrectedDocumentStorage<TDoc, TStreamId>(DocumentTracking.IdentityOnly));
        }

        IIdentitySetter<TDoc, TStreamId> identitySetter = new NulloIdentitySetter<TDoc, TStreamId>();
        try
        {
            identitySetter = options.ResolveCorrectedDocumentStorage<TDoc, TStreamId>(DocumentTracking.IdentityOnly);
        }
        catch (InvalidDocumentException)
        {
            // there's no identity, just use the nullo strategy
        }

        return new FetchNaturalKeyLivePlan<TDoc, TNaturalKey, TStreamId>(events, naturalKey, builder,
            identitySetter);
    }

    /// <summary>
    /// The aggregate's owned or mapped natural key of the given type. Keys on one aggregate have distinct
    /// types, so there is at most one.
    /// </summary>
    internal static NaturalKeyDefinition? FindNaturalKey(IAggregateProjection projection, Type keyType)
    {
        if (projection.NaturalKeyDefinition?.OuterType == keyType) return projection.NaturalKeyDefinition;

        return projection.MappedNaturalKeyDefinitions.FirstOrDefault(x => x.OuterType == keyType);
    }
}
