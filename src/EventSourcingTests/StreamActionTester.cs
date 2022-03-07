using System;
using System.Collections.Generic;
using EventSourcingTests.Aggregation;
using Marten;
using Marten.Events;
using Marten.Internal;
using Marten.Storage;
using NSubstitute;
using Shouldly;
using Xunit;

namespace EventSourcingTests
{
    public class StreamActionTester
    {
        private IMartenSession theSession;
        private Tenant theTenant;
        private EventGraph theEvents;

        public StreamActionTester()
        {
            theSession = Substitute.For<IMartenSession>();
            theSession.TenantId.Returns("TX");
            theSession.Database.Returns(Substitute.For<IMartenDatabase>());

            theEvents = new EventGraph(new StoreOptions());
        }

        [Fact]
        public void for_determines_action_type_guid()
        {
            var events = new List<IEvent>
            {
                new Event<AEvent>(new AEvent()),
                new Event<AEvent>(new AEvent()),
                new Event<AEvent>(new AEvent()),
                new Event<AEvent>(new AEvent()),
                new Event<AEvent>(new AEvent()),
            };

            events[0].Version = 5;

            StreamAction.For(Guid.NewGuid(), events)
                .ActionType.ShouldBe(StreamActionType.Append);

            events[0].Version = 1;

            StreamAction.For(Guid.NewGuid(), events)
                .ActionType.ShouldBe(StreamActionType.Start);
        }

        [Fact]
        public void for_determines_action_type_string()
        {
            var events = new List<IEvent>
            {
                new Event<AEvent>(new AEvent()),
                new Event<AEvent>(new AEvent()),
                new Event<AEvent>(new AEvent()),
                new Event<AEvent>(new AEvent()),
                new Event<AEvent>(new AEvent()),
            };

            events[0].Version = 5;

            StreamAction.For(Guid.NewGuid().ToString(), events)
                .ActionType.ShouldBe(StreamActionType.Append);

            events[0].Version = 1;

            StreamAction.For(Guid.NewGuid().ToString(), events)
                .ActionType.ShouldBe(StreamActionType.Start);
        }

        [Fact]
        public void ApplyServerVersion_for_new_streams()
        {
            var action = StreamAction.Start(Guid.NewGuid(), new AEvent(), new BEvent(), new CEvent(), new DEvent());

            var queue = new Queue<long>();
            queue.Enqueue(11);
            queue.Enqueue(12);
            queue.Enqueue(13);
            queue.Enqueue(14);

            var preparedEvents = action.PrepareEvents(0, theEvents, queue, theSession);
            
            preparedEvents[0].Version.ShouldBe(1);
            preparedEvents[1].Version.ShouldBe(2);
            preparedEvents[2].Version.ShouldBe(3);
            preparedEvents[3].Version.ShouldBe(4);

            preparedEvents[0].Sequence.ShouldBe(11);
            preparedEvents[1].Sequence.ShouldBe(12);
            preparedEvents[2].Sequence.ShouldBe(13);
            preparedEvents[3].Sequence.ShouldBe(14);
        }

        [Fact]
        public void ApplyServerVersion_for_existing_streams()
        {
            var action = StreamAction.Append(Guid.NewGuid(), new AEvent(), new BEvent(), new CEvent(), new DEvent());

            var queue = new Queue<long>();
            queue.Enqueue(11);
            queue.Enqueue(12);
            queue.Enqueue(13);
            queue.Enqueue(14);
            var preparedEvents = action.PrepareEvents(5, theEvents, queue, theSession);

            action.ExpectedVersionOnServer.ShouldBe(5);

            preparedEvents[0].Version.ShouldBe(6);
            preparedEvents[1].Version.ShouldBe(7);
            preparedEvents[2].Version.ShouldBe(8);
            preparedEvents[3].Version.ShouldBe(9);
        }
    }
}
