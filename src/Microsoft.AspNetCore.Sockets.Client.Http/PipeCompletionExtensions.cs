using System.Threading.Tasks;

namespace System.IO.Pipelines
{
    internal static class PipeCompletionExtensions
    {
        public static Task WaitForWriterToComplete(this PipeReader reader)
        {
            var tcs = new TaskCompletionSource<object>();
            reader.OnWriterCompleted((ex, state) =>
            {
                if (ex != null)
                {
                    ((TaskCompletionSource<object>)state).TrySetException(ex);
                }
                else
                {
                    ((TaskCompletionSource<object>)state).TrySetResult(null);
                }
            }, tcs);
            return tcs.Task;
        }

        public static Task WaitForReaderToComplete(this PipeWriter writer)
        {
            var tcs = new TaskCompletionSource<object>();
            writer.OnReaderCompleted((ex, state) =>
            {
                if (ex != null)
                {
                    ((TaskCompletionSource<object>)state).TrySetException(ex);
                }
                else
                {
                    ((TaskCompletionSource<object>)state).TrySetResult(null);
                }
            }, tcs);
            return tcs.Task;
        }
    }
}
