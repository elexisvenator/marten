using JasperFx.Events;
using Marten.Events.Schema;
using Marten.Storage;
using Weasel.Postgresql;

namespace Marten.Events.Fetching;

/// <summary>
/// Writes the SQL that resolves a natural key to its stream, against the lookup table of the aggregate
/// that owns the key.
/// </summary>
internal class NaturalKeyQueryBuilder
{
    private readonly string _tableName;
    private readonly string _streamsTableName;
    private readonly string _streamIdColumn;
    private readonly bool _filterByTenant;

    /// <param name="events"></param>
    /// <param name="lookup">
    /// The owned key whose lookup table is read: the aggregate's own key, or the source projection's key
    /// for a mapped one.
    /// </param>
    public NaturalKeyQueryBuilder(EventGraph events, NaturalKeyDefinition lookup)
    {
        _tableName = NaturalKeyTable.TableNameFor(events, lookup).QualifiedName;
        _streamsTableName = $"{events.DatabaseSchemaName}.{StreamsTable.TableName}";
        _streamIdColumn = events.StreamIdentity == StreamIdentity.AsGuid ? "stream_id" : "stream_key";
        _filterByTenant = events.TenancyStyle == TenancyStyle.Conjoined &&
                          !events.GlobalAggregates.Contains(lookup.AggregateType);
    }

    /// <summary>
    /// <c>select stream id from the lookup where the key matches</c>, for a stream that is not archived.
    /// </summary>
    public void WriteStreamLookup(ICommandBuilder builder, object innerValue)
    {
        builder.Append("select nk.");
        builder.Append(_streamIdColumn);
        builder.Append(" from ");
        builder.Append(_tableName);
        builder.Append(" nk inner join ");
        builder.Append(_streamsTableName);
        builder.Append(" s on s.id = nk.");
        builder.Append(_streamIdColumn);

        // tenant_id is part of the primary key of both tables, so the join has to match it too or the same
        // key in two tenants would join across them.
        if (_filterByTenant)
        {
            builder.Append(" and s.tenant_id = nk.tenant_id");
        }

        builder.Append(" where nk.natural_key_value = ");
        builder.AppendParameter(innerValue);
        builder.Append(" and nk.is_archived = false");

        // #5344: archival is a property of the *stream*, and nothing on the write side copies it onto the
        // lookup row -- ArchiveStream() and the Archived event both go through mt_archive_stream, which has
        // never touched mt_natural_key_X. nk.is_archived above is kept because it is genuinely maintained
        // under UseArchivedStreamPartitioning, where the cascading FK moves the lookup row along with its
        // stream.
        builder.Append(" and s.is_archived = false");

        if (_filterByTenant)
        {
            builder.Append(" and nk.tenant_id = ");
            builder.AppendParameter(builder.TenantId);
        }
    }
}
