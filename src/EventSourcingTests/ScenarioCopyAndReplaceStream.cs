using System.Linq;
using System.Threading.Tasks;
using EventSourcingTests.Projections;
using Marten.Events;
using Marten.Testing.Harness;
using Xunit;

namespace EventSourcingTests
{
    [Collection("string_identified_streams")]
    public class ScenarioCopyAndReplaceStream : StoreContext<StringIdentifiedStreamsFixture>, IAsyncLifetime
    {
        public ScenarioCopyAndReplaceStream(StringIdentifiedStreamsFixture fixture) : base(fixture)
        {

        }

        public Task InitializeAsync()
        {
            return theStore.Advanced.Clean.DeleteAllDocumentsAsync();
        }

        public Task DisposeAsync()
        {
            Dispose();
            return Task.CompletedTask;
        }

        [Fact]
        public void SampleCopyAndTransformStream()
        {
            #region sample_scenario-copyandtransformstream-setup
            var started = new QuestStarted { Name = "Find the Orb" };
            var joined = new MembersJoined { Day = 2, Location = "Faldor's Farm", Members = new[] { "Garion", "Polgara", "Belgarath" } };
            var slayed1 = new MonsterSlayed { Name = "Troll" };
            var slayed2 = new MonsterSlayed { Name = "Dragon" };

            using (var session = theStore.OpenSession())
            {
                session.Events.StartStream<Quest>(started.Name,started, joined, slayed1, slayed2);
                session.SaveChanges();
            }
            #endregion

            #region sample_scenario-copyandtransformstream-transform
            using (var session = theStore.OpenSession())
            {
                var events = session.Events.FetchStream(started.Name);

                var transformedEvents = events.SelectMany(x =>
                {
                    void SetRestreamHeaders(UncommittedEvent @event)
                    {
                        @event.SetHeader("copied_from_event", x.Id);
                        @event.SetHeader("moved_from_stream", started.Name);
                    }

                    switch (x.Data)
                    {
                        case MonsterSlayed monster:
                        {
                            // Trolls we remove from our transformed stream
                            if (monster.Name.Equals("Troll")) return Enumerable.Empty<UncommittedEvent>();

                            var newEvent = UncommittedEvent.FromExisting(x);
                            SetRestreamHeaders(newEvent);
                            return new[] { newEvent };
                        }
                        case MembersJoined members:
                        {
                            // MembersJoined events we transform into a series of events
                            return MemberJoined.From(members).Select(member =>
                            {
                                var newEvent = new UncommittedEvent(member).WithMetadata(x);
                                SetRestreamHeaders(newEvent);
                                return newEvent;
                            });
                        }
                        default:
                        {
                            var newEvent = UncommittedEvent.FromExisting(x);
                            SetRestreamHeaders(newEvent);
                            return new[] { newEvent };
                        }
                    }
                }).Where(x => x != null).Cast<object>().ToArray();
                
                var moveTo = $"{started.Name} without Trolls";
                // We copy the transformed events to a new stream
                session.Events.StartStream<Quest>(moveTo, transformedEvents);
                // And additionally mark the old stream as moved. Furthermore, we assert on the new expected stream version to guard against any racing updates
                session.Events.Append(started.Name, events.Count + 1, new StreamMovedTo
                {
                    To = moveTo
                });

                // Transactionally update the streams.
                session.SaveChanges();
            }
            #endregion
        }

        #region sample_scenario-copyandtransformstream-newevent
        public class MemberJoined
        {
            public int Day { get; set; }
            public string Location { get; set; }
            public string Name { get; set; }

            public MemberJoined()
            {
            }

            public MemberJoined(int day, string location, string name)
            {
                Day = day;
                Location = location;
                Name = name;
            }

            public static MemberJoined[] From(MembersJoined @event)
            {
                return @event.Members.Select(x => new MemberJoined(@event.Day, @event.Location, x)).ToArray();
            }
        }
        #endregion

        #region sample_scenario-copyandtransformstream-streammoved
        public class StreamMovedTo
        {
            public string To { get; set; }
        }
        #endregion
    }
}
