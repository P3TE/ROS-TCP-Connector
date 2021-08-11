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

        private ConcurrentQueue<string> toLog = new ConcurrentQueue<string>();

        public static void AddToLog(string toLog)
        {
            if (Instance != null)
            {
                Instance.toLog.Enqueue(toLog);
            }
        }

        private void LateUpdate()
        {
            while (toLog.TryDequeue(out string logMessage))
            {
                Debug.Log(logMessage);
            }
        }
    }
}
