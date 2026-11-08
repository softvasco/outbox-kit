using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace OutboxKit.EntityFrameworkCore;

/// <summary>
/// Before each SaveChanges, moves the messages raised by tracked <see cref="IHasOutboxMessages"/> entities into the
/// outbox, so they are written in the same transaction as the entities.
/// </summary>
public sealed class OutboxSaveChangesInterceptor : SaveChangesInterceptor
{
    private readonly DbContextOutbox _outbox;

    public OutboxSaveChangesInterceptor(DbContextOutbox outbox)
    {
        ArgumentNullException.ThrowIfNull(outbox);
        _outbox = outbox;
    }

    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        EnqueueRaisedMessages(eventData.Context);
        return result;
    }

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        EnqueueRaisedMessages(eventData.Context);
        return ValueTask.FromResult(result);
    }

    private void EnqueueRaisedMessages(DbContext? context)
    {
        if (context is null)
        {
            return;
        }

        // ToList first: adding outbox rows changes the tracker we are enumerating
        var sources = context.ChangeTracker.Entries<IHasOutboxMessages>().Select(e => e.Entity).ToList();
        foreach (var source in sources)
        {
            // taken messages stay tracked as added rows, so if this save fails a retried save still writes them once
            foreach (var pending in source.TakeOutboxMessages())
            {
                _outbox.Enqueue(context, pending.Message, pending.OrderingKey);
            }
        }
    }
}
