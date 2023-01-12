using System.Threading;
using System.Threading.Tasks;

namespace Unity.Robotics.ROSTCPConnector
{
    public class WaitForRosConnectionPauser
    {
        CancellationTokenSource m_Source = new CancellationTokenSource();

        public async Task PauseUntilRosConnected()
        {
            try
            {
                ROSConnection.GetOrCreateInstance().connectionThreadStateUpdatedDelegate +=
                    OnConnectionThreadStateUpdated;
                while (!m_Source.Token.IsCancellationRequested)
                {
                    await Task.Delay(1000, m_Source.Token);
                }
            }
            catch (TaskCanceledException)
            {

            }

            ROSConnection.GetOrCreateInstance().connectionThreadStateUpdatedDelegate -=
                OnConnectionThreadStateUpdated;

            return;
        }

        private void OnConnectionThreadStateUpdated(ROSConnection.ConnectionThreadState connectionThreadState)
        {
            switch (connectionThreadState)
            {
                case ROSConnection.ConnectionThreadState.Connected:
                    m_Source.Cancel();
                    break;
            }
        }

    }
}
