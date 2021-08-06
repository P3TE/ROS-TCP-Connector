using System;
using System.Collections.Concurrent;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector
{
    public class DebugTimingsLogger : MonoBehaviour
    {

        public static DebugTimingsLogger Instance
        {
            get;
            private set;
        }

        private void Awake()
        {
            Instance = this;
        }

        public ConcurrentQueue<string> toLog = new ConcurrentQueue<string>();

        private void LateUpdate()
        {
            while (toLog.TryDequeue(out string logMessage))
            {
                Debug.Log(logMessage);
            }
        }
    }
}
