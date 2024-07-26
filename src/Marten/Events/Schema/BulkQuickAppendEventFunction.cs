using System.IO;
using System.Linq;
using Marten.Schema;
using Marten.Storage;
using Marten.Storage.Metadata;
using Weasel.Core;
using Weasel.Postgresql.Functions;

namespace Marten.Events.Schema;

public class BulkQuickAppendEventFunction: Function
{
    private readonly EventGraph _events;

    public BulkQuickAppendEventFunction(EventGraph events) : base(new DbObjectName(events.DatabaseSchemaName, "mt_bulk_quick_append_events"))
    {
        _events = events;
    }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        var streamIdType = _events.GetStreamIdDBType();
        var databaseSchema = _events.DatabaseSchemaName;

        var tenancyStyle = _events.TenancyStyle;
        var table = new EventsTable(_events);
        var metadataFunctionParams = "";
        var metadataSqlArgs = "";
        var maxArg = 7;
        var metadataInsertRefs = "";
        var metadataColumns = "";

        if (table.Columns.OfType<CausationIdColumn>().Any())
        {
            maxArg++;
            metadataFunctionParams += ", causation_ids text[]";
            metadataSqlArgs += $", ${maxArg}";
            metadataColumns += ", " + CausationIdColumn.ColumnName;
            metadataInsertRefs += ", e." + CausationIdColumn.ColumnName;
        }

        if (table.Columns.OfType<CorrelationIdColumn>().Any())
        {
            maxArg++;
            metadataFunctionParams += ", correlation_ids text[]";
            metadataColumns += ", " + CorrelationIdColumn.ColumnName;
            metadataInsertRefs += ", e." + CorrelationIdColumn.ColumnName;
        }

        if (table.Columns.OfType<HeadersColumn>().Any())
        {
            maxArg++;
            metadataFunctionParams += ", headers text[]";
            metadataColumns += ", " + HeadersColumn.ColumnName;
            metadataInsertRefs += ", e." + HeadersColumn.ColumnName;
        }

        maxArg = tenancyStyle == TenancyStyle.Conjoined ? maxArg + 1 : maxArg;
        var tenantFunctionArg = tenancyStyle == TenancyStyle.Conjoined ? ", tenant_id text" : "";
        var tenantSqlRef = tenancyStyle == TenancyStyle.Conjoined ? $"${maxArg} as tenant_id," : "";
        var tenantColumn = tenancyStyle == TenancyStyle.Conjoined ? "tenant_id, " : "";
        var tenantJoin = tenancyStyle == TenancyStyle.Conjoined ? $"and s.tenant_id = ${maxArg}" : "";

        writer.WriteLine(
            $"""
             CREATE OR REPLACE FUNCTION {Identifier}(streams {streamIdType}[], stream_types text[], event_ids uuid[], event_types text[], dotnet_types text[], bodies jsonb[], expected_versions bigint[]{metadataFunctionParams}{tenantFunctionArg})          
              RETURNS TABLE (id uuid, seq_id bigint, version bigint)
              LANGUAGE sql
             AS $function$
                /* At some threshold, this really should just be a tablecopy operation */
                with new_events as (
                 	select
                 	    row_number() over (partition by stream_id order by expected_version) as temp_version,
                 	    stream_id, 
                 	    stream_type, 
                 	    event_id, 
                 	    event_type, 
                 	    dotnet_type, 
                 	    body, 
                 	    expected_version{metadataColumns}
                 	from unnest($1, $2, $3, $4, $5, $6, $7{metadataSqlArgs})
                        as new_events(stream_id, stream_type, event_id, event_type, dotnet_type, body, expected_version{metadataColumns})
                ),
                updated_streams as (
                 	insert into {databaseSchema}.mt_streams (id, {tenantColumn}type, version, timestamp) 
                 	select
                 	    n.stream_id,
                 	    {tenantSqlRef}
                 	    n.stream_type,
                 	    count(1),
                 	    now()
                 	from new_events n
                 	left join {databaseSchema}.mt_streams s on s.id = n.stream_id {tenantJoin} 
                 	group by (1,2,3,5,s.version)
                 	/* Validate event sequence ids, if invalid then records wont be returned, resulting in errors in the next query */
                 	having
                 	    case min(n.expected_version)
                 	      when -1 then s.version is null                 /* stream should not exist */
                 	      when 0  then s.version is not null             /* stream should exist */
                 	      else (s.version + 1) = min(n.expected_version) /* stream should exist and be at expected version */
                 	    end
                 	on conflict ({tenantColumn}id)
                 	do update
                 	set version   = mt_streams.version + excluded.version,
                 	    timestamp = excluded.timestamp
                 	returning id, version
                 ),
                 updated_events as (
                 	insert into {databaseSchema}.mt_events
                 		(seq_id, id, stream_id, version, data, type, {tenantColumn}{SchemaConstants.DotNetTypeColumn}, is_archived{metadataColumns})
                 	select
                 	    nextval('{databaseSchema}.mt_events_sequence'),
                 	    e.event_id,
                 	    s.id,
                 	    /* stream version is updated before events are written, work backwards to get the event version */ 
                 	    s.version + 1 - (row_number() over (partition by s.id order by temp_version desc)),
                 	    e.body,
                 	    e.event_type,
                 	    {tenantSqlRef}
                 	    e.dotnet_type,
                 	    false{metadataInsertRefs}
                 	from new_events e
                 	/* Left join so we can insert nulls if there is no match - this will throw an error and abort the operation */
                 	left join updated_streams s on s.id = e.stream_id
                 	returning id, seq_id, version
                 )
                 select id, seq_id, version
                 from updated_events
             $function$;
             """);
    }

}
