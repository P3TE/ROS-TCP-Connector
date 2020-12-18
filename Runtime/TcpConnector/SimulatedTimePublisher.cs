using RosMessageTypes.Rosgraph;
using UnityEngine;

namespace Runtime.TcpConnector
{
    
    /// <summary>
    /// http://wiki.ros.org/Clock
    /// Used for simulation time.
    /// </summary>
    public class SimulatedTimePublisher : MonoBehaviour
    {

        // Update is called once per frame
        void Update()
        {
            Clock simulatedClock = new Clock(ROSConnection.CurrentSimTime);
            ROSConnection.Instance.Send("clock", simulatedClock);
        }
    }
}
