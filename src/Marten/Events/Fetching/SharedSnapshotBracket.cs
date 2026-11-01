#nullable enable
using Marten.Internal.Sessions;

namespace Marten.Events.Fetching;

/// <summary>
///     The <c>begin transaction isolation level repeatable read read only</c> … <c>end</c> bracket an async-plan
///     fetch puts around its reads so they share one snapshot. Written by each fetch for itself, or once by a
///     batch around every bracketed fetch at its start.
/// </summary>
/// <remarks>
///     Only legal as the FIRST statement of a transaction (#5535), and only on a session that is not already
///     inside a transaction that outlives the call (#5611). See <c>FetchAsyncPlan.canShareOneSnapshot</c>.
/// </remarks>
internal static class SharedSnapshotBracket
{
    public const string Begin = "begin transaction isolation level repeatable read read only";

    /// <summary>Commits the transaction <see cref="Begin" /> opened.</summary>
    public const string End = "end";

    public static bool CanShare(QuerySession session) => !session.HasSurvivingTransaction;
}
