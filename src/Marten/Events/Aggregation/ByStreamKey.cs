#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JasperFx.Core.Reflection;
using JasperFx.Events;
using Marten.Events.Projections;
using Marten.Internal;
using Marten.Storage;

namespace Marten.Events.Aggregation;

/// <summary>
///     Slicer strategy by stream key (string identified streams)
/// </summary>
/// <typeparam name="TDoc"></typeparam>
public class ByStreamKey<TDoc>: IEventSlicer<TDoc, string>, ISingleStreamSlicer<string>
{
    public ValueTask<IReadOnlyList<EventSlice<TDoc, string>>> SliceInlineActions(IQuerySession querySession,
        IEnumerable<StreamAction> streams)
    {
        return new ValueTask<IReadOnlyList<EventSlice<TDoc, string>>>(streams.Select(s =>
        {
            return new EventSlice<TDoc, string>(s.Key!, s.TenantId, s.Events){ActionType = s.ActionType};
        }).ToList());
    }

    public void ArchiveStream(IDocumentOperations operations, string id)
    {
        operations.Events.ArchiveStream(id);
    }

    public ValueTask<IReadOnlyList<TenantSliceGroup<TDoc, string>>> SliceAsyncEvents(
        IQuerySession querySession,
        List<IEvent> events)
    {
        var list = new List<TenantSliceGroup<TDoc, string>>();
        var byTenant = events.GroupBy(x => x.TenantId);

        foreach (var tenantGroup in byTenant)
        {
            var tenant = new Tenant(tenantGroup.Key, querySession.Database);

            var slices = tenantGroup
                .GroupBy(x => x.StreamKey)
                .Select(x => new EventSlice<TDoc, string>(x.Key!, tenantGroup.Key, x));

            var group = new TenantSliceGroup<TDoc, string>(tenant, slices);

            list.Add(group);
        }

        return new ValueTask<IReadOnlyList<TenantSliceGroup<TDoc, string>>>(list);
    }
}

/// <summary>
///     Slicer strategy by stream key (string identified streams) for strong typed identifiers
/// </summary>
/// <typeparam name="TDoc"></typeparam>
public class ByStreamKey<TDoc, TId>: IEventSlicer<TDoc, TId>, ISingleStreamSlicer<TId>
{
    private readonly Func<string,TId> _converter;
    private readonly Func<TId,string> _unwrapper;

    public ByStreamKey(ValueTypeInfo valueType)
    {
        _converter = valueType.CreateConverter<TId, string>();
        _unwrapper = valueType.ValueAccessor<TId, string>();
    }

    public void ArchiveStream(IDocumentOperations operations, TId id)
    {
        operations.Events.ArchiveStream(_unwrapper(id));
    }

    public ValueTask<IReadOnlyList<EventSlice<TDoc, TId>>> SliceInlineActions(IQuerySession querySession,
        IEnumerable<StreamAction> streams)
    {
        return new ValueTask<IReadOnlyList<EventSlice<TDoc, TId>>>(streams.Select(s =>
        {
            return new EventSlice<TDoc, TId>(_converter(s.Key!), s.TenantId, s.Events){ActionType = s.ActionType};
        }).ToList());
    }

    public ValueTask<IReadOnlyList<TenantSliceGroup<TDoc, TId>>> SliceAsyncEvents(
        IQuerySession querySession,
        List<IEvent> events)
    {
        var list = new List<TenantSliceGroup<TDoc, TId>>();
        var byTenant = events.GroupBy(x => x.TenantId);

        foreach (var tenantGroup in byTenant)
        {
            var tenant = new Tenant(tenantGroup.Key, querySession.Database);

            var slices = tenantGroup
                .GroupBy(x => x.StreamKey)
                .Select(x => new EventSlice<TDoc, TId>(_converter(x.Key!), tenantGroup.Key, x));

            var group = new TenantSliceGroup<TDoc, TId>(tenant, slices);

            list.Add(group);
        }

        return new ValueTask<IReadOnlyList<TenantSliceGroup<TDoc, TId>>>(list);
    }
}

