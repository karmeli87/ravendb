using System;
using System.Threading.Tasks;

namespace Tests.Infrastructure
{
    public static class TestExtensions
    {
        public static async Task<bool> WaitAsync(this Task task, TimeSpan timeout)
        {
            var delay = Task.Delay(timeout);
            if (await Task.WhenAny(delay, task).ConfigureAwait(false) == delay)
                return false;
            if (task.IsCompletedSuccessfully)
                return true;
            if (task.Exception != null)
                throw task.Exception;
            throw new Exception($"This make no sense {task.Status}, timeout: {timeout}");
        }

        public static async Task<bool> WaitAsync(this Task task, int timeout)
        {
            return await task.WaitAsync(TimeSpan.FromMilliseconds(timeout)).ConfigureAwait(false);
        }
    }
}
