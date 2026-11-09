namespace OutboxKit.Tests.Stores;

public abstract partial class OutboxStoreContract
{
    [Fact]
    public async Task Only_messages_dispatched_before_the_cutoff_are_deleted()
    {
        var old = await Dispatch(Now.AddDays(-8));
        var recent = await Dispatch(Now.AddDays(-1));

        var deleted = await _store.DeleteDispatchedAsync(Now.AddDays(-7), 10, Token);

        Assert.Equal(1, deleted);
        Assert.False(await Exists(old));
        Assert.True(await Exists(recent));
    }

    [Fact]
    public async Task A_delete_stops_at_the_batch_size()
    {
        for (var i = 0; i < 5; i++)
        {
            await Dispatch(Now.AddDays(-8));
        }

        var first = await _store.DeleteDispatchedAsync(Now, 3, Token);
        var second = await _store.DeleteDispatchedAsync(Now, 3, Token);

        Assert.Equal((3, 2), (first, second));
    }

    [Fact]
    public async Task Pending_and_dead_lettered_messages_are_left_alone()
    {
        var pending = await Insert(Now.AddDays(-30));
        await Insert(Now.AddDays(-31));
        var parked = Assert.Single(await _store.ClaimAsync(1, Now, Now + Claim, Token));
        await _store.MarkDeadLetteredAsync(parked, Now.AddDays(-30), "payload rejected", Token);

        var deleted = await _store.DeleteDispatchedAsync(Now, 10, Token);

        Assert.Equal(0, deleted);
        Assert.True(await Exists(pending));
        Assert.True(await Exists(parked.Id));
    }

    [Fact]
    public async Task A_row_another_transaction_holds_is_skipped_by_the_delete()
    {
        var held = await Dispatch(Now.AddDays(-8));
        var free = await Dispatch(Now.AddDays(-8));
        await using var connection = await DataSource.OpenConnectionAsync(Token);
        await using var transaction = await connection.BeginTransactionAsync(Token);
        await using var hold = connection.CreateCommand();
        hold.Transaction = transaction;
        hold.CommandText = $"UPDATE {_table.Name} SET payload = payload WHERE id = @id";
        Parameter(hold, "@id", held);
        await hold.ExecuteNonQueryAsync(Token);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        var deleted = await _store.DeleteDispatchedAsync(Now, 10, timeout.Token);
        await transaction.RollbackAsync(Token);

        Assert.Equal(1, deleted);
        Assert.False(await Exists(free));
        Assert.True(await Exists(held));
    }

    private async Task<Guid> Dispatch(DateTimeOffset dispatchedAt)
    {
        var id = await Insert(dispatchedAt.AddMinutes(-1));
        await _store.MarkDispatchedAsync(new OutboxMessage(id, "orders.placed", "{}", dispatchedAt), dispatchedAt, Token);
        return id;
    }

    private async Task<bool> Exists(Guid id)
    {
        await using var select = DataSource.CreateCommand($"SELECT COUNT(*) FROM {_table.Name} WHERE id = @id");
        Parameter(select, "@id", id);
        return Convert.ToInt32(await select.ExecuteScalarAsync(Token), System.Globalization.CultureInfo.InvariantCulture) == 1;
    }
}
