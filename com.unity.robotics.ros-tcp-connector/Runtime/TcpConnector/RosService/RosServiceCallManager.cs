using System;
using System.Collections.Generic;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector.RosService
{
    public class RosServiceCallManager
    {
        public static int nextServiceId = 101;

        public static readonly object m_ServiceRequestLock = new object();

        private static Dictionary<int, RosServiceCallInfoBase> m_ServicesWaiting = new Dictionary<int, RosServiceCallInfoBase>();

        public static int GetUniqueServiceId()
        {
            int result;
            lock (m_ServiceRequestLock)
            {
                result = nextServiceId;
                nextServiceId++;
            }
            return result;
        }

        public static void AddRosServiceCallInfoBase(RosServiceCallInfoBase rosServiceCallInfoBase)
        {
            lock (m_ServiceRequestLock)
            {
                m_ServicesWaiting.Add(rosServiceCallInfoBase.serviceId, rosServiceCallInfoBase);
            }
        }

        public static bool TryGetWaitingService(int serviceId, out RosServiceCallInfoBase rosServiceCallInfoBase)
        {
            bool result;
            lock (m_ServiceRequestLock)
            {
                result = m_ServicesWaiting.TryGetValue(serviceId, out rosServiceCallInfoBase);
            }
            return result;
        }

        public static bool TryRemoveWaitingService(int serviceId, out RosServiceCallInfoBase rosServiceCallInfoBase)
        {
            bool result;
            lock (m_ServiceRequestLock)
            {
                result = m_ServicesWaiting.Remove(serviceId, out rosServiceCallInfoBase);
            }
            return result;
        }

        public static void OnServiceFailed(int serviceId, Exception error)
        {
            if (TryRemoveWaitingService(serviceId,
                    out RosServiceCallInfoBase rosServiceCallInfoBase))
            {
                rosServiceCallInfoBase.OnServiceCallFailed(error);
            }
            else
            {
                Debug.LogWarning($"Unable to display service error for a unity service that doesn't exist, serviceId {serviceId} does not exist.");
            }
        }
    }
}
