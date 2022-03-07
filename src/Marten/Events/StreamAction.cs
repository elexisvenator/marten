using System;
using System.Collections.Generic;
using System.Linq;
using Marten.Events.Operations;
using Marten.Exceptions;
using Marten.Internal;
using Marten.Schema.Identity;

#nullable enable
namespace Marten.Events
{
    public enum StreamActionType
    {
        /// <summary>
        /// This is a new stream. This action will be rejected
        /// if a stream with the same identity exists in the database
        /// </summary>
        Start,

        /// <summary>
        /// Append these events to an existing stream. If the stream does not
        /// already exist, it will be created with these events
        /// </summary>
        Append
    }


    public class UncommittedEvent: IEventMetadata
    {
        public static UncommittedEvent FromExisting(IEvent existing)
        {
            return new UncommittedEvent(existing.Data).WithMetadata(existing);
        }

        public UncommittedEvent(object data)
        {
            Data = data;
        }

        public UncommittedEvent WithMetadata(IEventMetadata metadata)
        {
            CausationId = metadata.CausationId ?? CausationId;
            CorrelationId = metadata.CorrelationId ?? CorrelationId;

            if (!(metadata.Headers?.Count > 0))
            {
                return this;
            }

            if (!(Headers?.Count > 0))
            {
                Headers = new Dictionary<string, object>(metadata.Headers);
                return this;
            }

            foreach (var metadataHeader in metadata.Headers)
            {
                Headers.Add(metadataHeader.Key, metadataHeader.Value);
            }

            return this;
        }

        public object Data { get; }
        
        public string? CausationId { get; set; }

        public string? CorrelationId { get; set; }

        public Dictionary<string, object>? Headers { get; set; }

        public void SetHeader(string key, object? value)
        {
            if (value is null && Headers is null)
            {
                return;
            }

            Headers ??= new Dictionary<string, object>();

            if (value is null)
            {
                Headers.Remove(key);
                return;
            }

            Headers[key] = value;
        }


        /// <summary>
        /// Set an optional user defined metadata value by key.
        /// Fluent equivalent of <see cref="SetHeader"/>
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public UncommittedEvent WithHeader(string key, object? value)
        {
            SetHeader(key, value);
            return this;
        }

        public object? GetHeader(string key)
        {
            return Headers?.TryGetValue(key, out var value) ?? false ? value : null;
        }
    }

    /// <summary>
    /// Models a series of events to be appended to either a new or
    /// existing stream
    /// </summary>
    public class StreamAction : IEventMetadata
    {
        /// <summary>
        /// Identity of the stream if using Guid's as the identity
        /// </summary>
        public Guid Id { get; internal set; }

        /// <summary>
        /// The identity of this stream if using strings as the stream
        /// identity
        /// </summary>
        public string? Key { get; }

        /// <summary>
        /// Is this action the start of a new stream or appending
        /// to an existing stream?
        /// </summary>
        public StreamActionType ActionType { get; }

        /// <summary>
        /// If the stream was started as tagged to an aggregate type, that will
        /// be reflected in this property. May be null
        /// </summary>
        public Type? AggregateType { get; internal set; }

        /// <summary>
        /// Marten's name for the aggregate type that will be persisted
        /// to the streams table
        /// </summary>
        public string? AggregateTypeName { get; internal set; }

        /// <summary>
        /// The Id of the current tenant
        /// </summary>
        public string? TenantId { get; internal set; }
        
        private readonly List<UncommittedEvent> _events = new ();

        private IReadOnlyList<IEvent>? _preparedEvents = null;

        public bool EventsPrepared => _preparedEvents is not null;

        public string? CausationId { get; set; }

        public string? CorrelationId { get; set; }

        public Dictionary<string, object>? Headers { get; set; }

        public void SetHeader(string key, object? value)
        {
            if (value is null && Headers is null)
            {
                return;
            }

            Headers ??= new Dictionary<string, object>();

            if (value is null)
            {
                Headers.Remove(key);
                return;
            }

            Headers[key] = value;
        }

        public object? GetHeader(string key)
        {
            return Headers?.TryGetValue(key, out var value) ?? false ? value : null;
        }

        internal IReadOnlyList<IEvent> GetPreparedEvents()
        {
            if (!EventsPrepared)
            {
                // TODO: Custom error
                throw new InvalidOperationException(
                    $"Events have not been prepared, '{nameof(PrepareEvents)}' should be called first.");
            }

            return _preparedEvents!;
        }

        private StreamAction(Guid stream, StreamActionType actionType)
        {
            Id = stream;
            ActionType = actionType;
        }

        private StreamAction(string stream, StreamActionType actionType)
        {
            Key = stream;
            ActionType = actionType;
        }

        private StreamAction(Guid id, string key, StreamActionType actionType)
        {
            Id = id;
            Key = key;
            ActionType = actionType;
        }

        public StreamAction Append(params object[] events)
        {
            if (EventsPrepared)
            {
                // TODO: Custom error
                throw new InvalidOperationException(
                    $"Events have already been prepared, no further events can be added.");
            }

            _events.AddRange(events.Select(e => e is UncommittedEvent p ? p : new UncommittedEvent(e)));

            if (ExpectedVersionOnServer.HasValue)
            {
                ExpectedVersionOnServer += events.Length;
            }

            return this;
        }

        /// <summary>
        /// The events involved in this action
        /// </summary>
        public IReadOnlyList<UncommittedEvent> Events => _events;

        /// <summary>
        /// The expected starting version of the stream in the server. This is used
        /// to facilitate optimistic concurrency checks
        /// </summary>
        public long? ExpectedVersionOnServer { get; internal set; }

        /// <summary>
        /// The ending version of the stream for this action
        /// </summary>
        public long Version { get; internal set; }

        /// <summary>
        /// The recorded timestamp for these events
        /// </summary>
        public DateTimeOffset? Timestamp { get; internal set; }

        /// <summary>
        /// When was the stream created
        /// </summary>
        public DateTimeOffset? Created { get; internal set; }

        /// <summary>
        /// Create a new StreamAction for starting a new stream
        /// </summary>
        /// <param name="streamId"></param>
        /// <param name="events"></param>
        /// <returns></returns>
        /// <exception cref="EmptyEventStreamException"></exception>
        internal static StreamAction Start(Guid streamId, params object[] events)
        {
            return new StreamAction(streamId, StreamActionType.Start).Append(events);
        }

        /// <summary>
        /// Create a new StreamAction for starting a new stream
        /// </summary>
        /// <param name="streamKey"></param>
        /// <param name="events"></param>
        /// <returns></returns>
        /// <exception cref="EmptyEventStreamException"></exception>
        internal static StreamAction Start(string streamKey, params object[] events)
        {
            return new StreamAction(streamKey, StreamActionType.Start).Append(events);
        }

        /// <summary>
        /// Create a new StreamAction for appending to an existing stream
        /// </summary>
        /// <param name="streamId"></param>
        /// <param name="events"></param>
        /// <returns></returns>
        internal static StreamAction Append(Guid streamId, params object[] events)
        {
            return new StreamAction(streamId, StreamActionType.Append).Append(events);
        }

        /// <summary>
        /// Create a new StreamAction for appending to an existing stream
        /// </summary>
        /// <param name="streamKey"></param>
        /// <param name="events"></param>
        /// <returns></returns>
        internal static StreamAction Append(string streamKey, params object[] events)
        {
            return new StreamAction(streamKey, StreamActionType.Append).Append(events);
        }

        /// <summary>
        /// Applies versions, .Net type aliases, the reserved sequence numbers, timestamps, etc.
        /// to get the events ready to be inserted into the mt_events table
        /// </summary>
        /// <param name="currentVersion"></param>
        /// <param name="graph"></param>
        /// <param name="sequences"></param>
        /// <param name="session"></param>
        /// <exception cref="EventStreamUnexpectedMaxEventIdException"></exception>
        internal IReadOnlyList<IEvent> PrepareEvents(long currentVersion, EventGraph graph, Queue<long> sequences, IMartenSession session)
        {
            if (EventsPrepared)
            {
                // TODO: Custom error
                throw new InvalidOperationException(
                    $"Events have already been prepared, no further events can be added.");
            }

            ThrowIfStartWithNoEvents();
            var timestamp = DateTimeOffset.UtcNow;

            if (AggregateType != null)
            {
                AggregateTypeName = graph.AggregateAliasFor(AggregateType);
            }

            var i = currentVersion;

            if (currentVersion != 0)
            {
                // Guard logic for optimistic concurrency
                if (ExpectedVersionOnServer.HasValue)
                {
                    if (currentVersion != ExpectedVersionOnServer.Value)
                    {
                        throw new EventStreamUnexpectedMaxEventIdException((object?) Key ?? Id, AggregateType, ExpectedVersionOnServer.Value, currentVersion);
                    }
                }

                ExpectedVersionOnServer = currentVersion;
            }

            var preparedEvents = new List<IEvent>();
            
            foreach (var preEvent in _events)
            {
                var @event = graph.BuildEvent(preEvent.Data);
                @event.Version = ++i;
                @event.Id = CombGuidIdGeneration.NewGuid();

                @event.StreamId = Id;
                @event.StreamKey = Key;

                @event.Sequence = sequences.Dequeue();
                @event.TenantId = session.TenantId;
                @event.Timestamp = timestamp;

                ProcessMetadata(preEvent, @event, graph, session);
                preparedEvents.Add(@event);
            }

            _preparedEvents = preparedEvents;
            Version = i;
            return _preparedEvents;
        }

        internal static StreamAction ForReference(Guid streamId, string tenantId)
        {
            return new StreamAction(streamId, StreamActionType.Append)
            {
                TenantId = tenantId
            };
        }

        internal static StreamAction ForReference(string streamKey, string tenantId)
        {
            return new StreamAction(streamKey, StreamActionType.Append)
            {
                TenantId = tenantId
            };
        }

        internal static StreamAction ForTombstone()
        {
            return new StreamAction(EstablishTombstoneStream.StreamId, EstablishTombstoneStream.StreamKey, StreamActionType.Append)
            {

            };
        }

        private void ProcessMetadata(UncommittedEvent uncommittedEvent, IEvent @event, EventGraph graph, IMartenSession session)
        {
            // Order of precedence for metadata is always (pre)event, then stream, then session
            if (graph.Metadata.CausationId.Enabled)
            {
                @event.CausationId = uncommittedEvent.CausationId ?? CausationId ?? session.CausationId ;
            }

            if (graph.Metadata.CorrelationId.Enabled)
            {
                @event.CorrelationId = uncommittedEvent.CorrelationId ?? CorrelationId ?? session.CorrelationId;
            }

            if (!graph.Metadata.Headers.Enabled) return;

            if (session.Headers?.Count > 0)
            {
                foreach (var header in session.Headers)
                {
                    @event.SetHeader(header.Key, header.Value);
                }
            }

            if (Headers?.Count > 0)
            {
                foreach (var header in Headers)
                {
                    @event.SetHeader(header.Key, header.Value);
                }
            }

            if (!(uncommittedEvent.Headers?.Count > 0)) return;
            foreach (var header in uncommittedEvent.Headers)
            {
                @event.SetHeader(header.Key, header.Value);
            }
        }

        private void ThrowIfStartWithNoEvents()
        {
            if (ActionType != StreamActionType.Start || _events.Any())
            {
                return;
            }

            if (Key is not null)
            {
                throw new EmptyEventStreamException(Key);
            }

            throw new EmptyEventStreamException(Id);
        }

        internal static StreamAction For(Guid streamId, IReadOnlyList<IEvent> events)
        {
            var action = events[0].Version == 1 ? StreamActionType.Start : StreamActionType.Append;
            return new StreamAction(streamId, action)
                .Append(events);
        }

        internal static StreamAction For(string streamKey, IReadOnlyList<IEvent> events)
        {
            var action = events[0].Version == 1 ? StreamActionType.Start : StreamActionType.Append;
            return new StreamAction(streamKey, action)
                .Append(events);
        }
    }
}
