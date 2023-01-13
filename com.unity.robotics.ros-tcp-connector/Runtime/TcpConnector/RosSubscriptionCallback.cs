using System;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using UnityEngine;

namespace Unity.Robotics.ROSTCPConnector
{

    public class RosSubscriptionCallbackBase
    {

        public readonly string topic;
        public readonly string rosMessageName;

        //Storing the original callback delegate so we can reference it when we unsubscribe.
        public readonly Delegate originalCallbackDelegate;

        public readonly Action<Message> callback;

        protected RosSubscriptionCallbackBase(string topic, string rosMessageName, Delegate originalCallbackDelegate, Action<Message> callback)
        {
            this.topic = topic;
            this.rosMessageName = rosMessageName;
            this.originalCallbackDelegate = originalCallbackDelegate;
            this.callback = callback;
        }

        public RosSubscriptionCallbackBase(string topic, string rosMessageName, Action<Message> callback) :
            this(topic, rosMessageName, callback, callback)
        {
        }

        public void OnMessageReceived(Message message)
        {
            if (message.RosMessageName == rosMessageName)
            {
                callback(message);
            }
            else
            {
                Debug.LogError($"Subscriber to '{topic}' expected '{rosMessageName}' but received '{message.RosMessageName}'!?");
            }
        }

        public bool CallbackMatches(Delegate otherCallback)
        {
            return originalCallbackDelegate == otherCallback;
        }
    }

    public class RosSubscriptionCallback<T> : RosSubscriptionCallbackBase where T : Message
    {
        public RosSubscriptionCallback(string topic, string rosMessageName, Action<T> callback) :
            base(topic, rosMessageName, callback, message => { callback((T)message); })
        {
        }
    }
}
