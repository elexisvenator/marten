using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Core;
using Marten.Internal;
using Marten.Internal.Operations;
using NpgsqlTypes;
using Weasel.Postgresql;

namespace Marten.Events.Operations;

public abstract class BulkQuickAppendEventsOperationBase : IStorageOperation
{
    public BulkQuickAppendEventsOperationBase(IReadOnlyCollection<StreamAction> streams)
    {
        Streams = streams;
        _eventCount = streams.Sum(s => s.Events.Count);
    }

    public IReadOnlyCollection<StreamAction> Streams { get; }
    private readonly int _eventCount;

    public OperationRole Role()
    {
        return OperationRole.Events;
    }

    public Type DocumentType => typeof(IEvent);

    public override string ToString()
    {
        return
            $"Bulk append: {Streams.Select(s => $"Append {s.Events.Select(x => x.EventTypeName).Join(", ")} to event stream {s}").Join(", ")}";
    }

    public abstract void ConfigureCommand(ICommandBuilder builder, IMartenSession session);

    public void Postprocess(DbDataReader reader, IList<Exception> exceptions)
    {
        var streamDict = Streams.ToDictionary(s => s.Key ?? s.Id.ToString());
        var eventDict = Streams.SelectMany(s => s.Events).ToDictionary(e => e.Id);

        while (reader.Read())
        {
            var eventId = reader.GetFieldValue<Guid>(0);
            var sequenceId = reader.GetFieldValue<long>(1);
            var version = reader.GetFieldValue<long>(2);

            var @event = eventDict[eventId];
            // Only setting the sequence to aid in tombstone processing
            @event.Sequence = sequenceId;
            @event.Version = version;

            var stream = streamDict[@event.StreamKey ?? @event.StreamId.ToString()];
            stream.Version = stream.Version > version ? version : stream.Version;
        }
    }

    protected void writeIds(IGroupedParameterBuilder builder)
    {
        var ids = new Guid[_eventCount];
        var i = 0;
        foreach (var stream in Streams)
        {
            Array.Fill(ids, stream.Id, i, stream.Events.Count);
            i += stream.Events.Count;
        }

        var param = builder.AppendParameter(ids);
        param.NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Uuid;
    }

    protected void writeKeys(IGroupedParameterBuilder builder)
    {
        var keys = new string[_eventCount];
        var i = 0;
        foreach (var stream in Streams)
        {
            Array.Fill(keys, stream.Key, i, stream.Events.Count);
            i += stream.Events.Count;
        }

        var param = builder.AppendParameter(keys);
        param.NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text;
    }

    protected void writeBasicParameters(IGroupedParameterBuilder builder, IMartenSession session)
    {
        var aggregateTypeNameArray = new string[_eventCount];
        var eventIdArray = new Guid[_eventCount];
        var eventTypeNameArray = new string[_eventCount];
        var dotNetTypeNameArray = new string[_eventCount];
        var dataArray = new string[_eventCount];
        var versionArray = new long[_eventCount];

        var i = 0;
        foreach (var stream in Streams)
        {
            if (stream.Events.Count == 0)
            {
                continue;
            }

            Array.Fill(aggregateTypeNameArray, stream.AggregateTypeName, i, stream.Events.Count);

            var eventVersion = stream.ActionType == StreamActionType.Start
                ? -1 // new stream
                : stream.Events[0].Version <= 0
                    ? 0 // existing stream, unknown version
                    : stream.Events[0].Version; // existing stream, known version

            foreach (var @event in stream.Events)
            {
                eventIdArray[i] = @event.Id;
                eventTypeNameArray[i] = @event.EventTypeName;
                dotNetTypeNameArray[i] = @event.DotNetTypeName;
                dataArray[i] = session.Serializer.ToJson(@event.Data);
                versionArray[i] = eventVersion;

                i++;
                eventVersion++;
            }
        }

        var param1 = builder.AppendParameter(aggregateTypeNameArray);
        param1.NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text;

        var param2 = builder.AppendParameter(eventIdArray);
        param2.NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Uuid;

        var param3 = builder.AppendParameter(eventTypeNameArray);
        param3.NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text;

        var param4 = builder.AppendParameter(dotNetTypeNameArray);
        param4.NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text;

        var param5 = builder.AppendParameter(dataArray);
        param5.NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Jsonb;

        var param6 = builder.AppendParameter(versionArray);
        param6.NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint;
    }

    protected void writeCausationIds(IGroupedParameterBuilder builder)
    {
        var array = new string[_eventCount];
        var i = 0;
        foreach (var stream in Streams)
        {
            foreach (var @event in stream.Events)
            {
                array[i] = @event.CausationId;
                i++;
            }
        }

        var param = builder.AppendParameter(array);
        param.NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text;
    }

    protected void writeCorrelationIds(IGroupedParameterBuilder builder)
    {
        var array = new string[_eventCount];
        var i = 0;
        foreach (var stream in Streams)
        {
            foreach (var @event in stream.Events)
            {
                array[i] = @event.CorrelationId;
                i++;
            }
        }

        var param = builder.AppendParameter(array);
        param.NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text;
    }

    protected void writeHeaders(IGroupedParameterBuilder builder, IMartenSession session)
    {
        var array = new string[_eventCount];
        var i = 0;
        foreach (var stream in Streams)
        {
            foreach (var @event in stream.Events)
            {
                array[i] = session.Serializer.ToJson(@event.Headers);
                i++;
            }
        }

        var param = builder.AppendParameter(array);
        param.NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Jsonb;
    }

    protected void writeTenant(IGroupedParameterBuilder builder)
    {
        var param = builder.AppendParameter(Streams.First().TenantId);
        param.NpgsqlDbType = NpgsqlDbType.Text;
    }

    public async Task PostprocessAsync(DbDataReader reader, IList<Exception> exceptions, CancellationToken token)
    {
        var streamDict = Streams.ToDictionary(s => s.Key ?? s.Id.ToString());
        var eventDict = Streams.SelectMany(s => s.Events).ToDictionary(e => e.Id);

        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            var eventId = await reader.GetFieldValueAsync<Guid>(0, token).ConfigureAwait(false);
            var sequenceId = await reader.GetFieldValueAsync<long>(1, token).ConfigureAwait(false);
            var version = await reader.GetFieldValueAsync<long>(2, token).ConfigureAwait(false);

            var @event = eventDict[eventId];
            // Only setting the sequence to aid in tombstone processing
            @event.Sequence = sequenceId;
            @event.Version = version;

            var stream = streamDict[@event.StreamKey ?? @event.StreamId.ToString()];
            stream.Version = stream.Version > version ? version : stream.Version;
        }
    }
}
