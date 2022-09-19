using System;
using System.Collections.Generic;
using RosMessageTypes.BuiltinInterfaces;
using RosMessageTypes.Std;
using RosMessageTypes.Tf2;
using Unity.Robotics.ROSTCPConnector;
using Unity.Robotics.ROSTCPConnector.MessageGeneration;
using Unity.Robotics.ROSTCPConnector.ROSGeometry;
using UnityEngine;

public class TFSystem
{
    public static bool instanceSet = false;
    public static TFSystem instance { get; private set; }
    Dictionary<string, TFTopicState> m_TFTopics = new Dictionary<string, TFTopicState>();
    private static TFTopicState tfTopicState = null;

    public class TFTopicState
    {
        private const string _StaticPostfix = "_static";

        string m_TFTopic;
        string m_TFTopic_static;
        Dictionary<string, TFStream> m_TransformTable = new Dictionary<string, TFStream>();
        List<Action<TFStream>> m_Listeners = new List<Action<TFStream>>();

        public TFTopicState(string tfTopic = "/tf", bool subscribeToStatic = true)
        {
            m_TFTopic = tfTopic;
            ROSConnection.GetOrCreateInstance().Subscribe<TFMessageMsg>(tfTopic, ReceiveTF);
            if (subscribeToStatic)
            {
                m_TFTopic_static = $"{tfTopic}{_StaticPostfix}";
                ROSConnection.GetOrCreateInstance().Subscribe<TFMessageMsg>(m_TFTopic_static, ReceiveTF);
            }
        }

        public TFStream GetOrCreateFrame(string frame_id)
        {
            TFStream tf;
            string frameIdTrimmed = RemoveLeadingAndTrailingSlashes(frame_id);
            if (!m_TransformTable.TryGetValue(frameIdTrimmed, out tf) || tf == null)
            {
                tf = new TFStream(null, frameIdTrimmed, m_TFTopic);
                m_TransformTable[frameIdTrimmed] = tf;
                NotifyChanged(tf);
            }
            return tf;
        }

        public static string RemoveLeadingAndTrailingSlashes(string rawString)
        {
            if (rawString == null)
            {
                return "";
            }
            string trimmedModelName = rawString.Trim();

            int startIndex = 0;
            int endIndex = trimmedModelName.Length;

            for (var i = 0; i < trimmedModelName.Length; i++)
            {
                char c = trimmedModelName[i];
                if (c == '/')
                {
                    startIndex++;
                }
                else
                {
                    break;
                }
            }

            for (var i = trimmedModelName.Length - 1; i >= 0; i--)
            {
                char c = trimmedModelName[i];
                if (c == '/')
                {
                    endIndex--;
                }
                else
                {
                    break;
                }
            }

            if (startIndex > endIndex)
            {
                return "";
            }

            return trimmedModelName.Substring(startIndex, endIndex - startIndex);
        }

        public void ReceiveTF(TFMessageMsg message)
        {
            foreach (var tf_message in message.transforms)
            {

                TFStream childTf = GetOrCreateFrame(tf_message.child_frame_id);
                TFStream parentTf = GetOrCreateFrame(tf_message.header.frame_id);

                childTf.SetParent(parentTf);

                childTf.Add(
                    tf_message.header.stamp.ToLongTime(),
                    tf_message.transform.translation.From<FLU>(),
                    tf_message.transform.rotation.From<FLU>()
                );

                NotifyChanged(childTf);
            }
        }

        public IEnumerable<string> GetTransformNames()
        {
            return m_TransformTable.Keys;
        }

        public IEnumerable<TFStream> GetTransforms()
        {
            return m_TransformTable.Values;
        }

        public TFStream GetTransformStream(string frame_id)
        {
            TFStream result = null;
            m_TransformTable.TryGetValue(frame_id, out result);
            return result;
        }

        public void AddListener(Action<TFStream> callback)
        {
            m_Listeners.Add(callback);
        }

        public void NotifyChanged(TFStream stream)
        {
            foreach (Action<TFStream> callback in m_Listeners)
            {
                callback(stream);
            }
        }

        public void NotifyAllChanged()
        {
            foreach (var stream in m_TransformTable.Values)
                NotifyChanged(stream);
        }
    }

    private TFSystem()
    {

    }

    public static TFSystem GetOrCreateInstance()
    {
        if (instanceSet)
            return instance;

        ROSConnection ros = ROSConnection.GetOrCreateInstance();
        instance = new TFSystem();
        instanceSet = true;
        foreach (string s in ros.TFTopics)
        {
            instance.GetOrCreateTFTopic(s, ros.SubscribeToTfStatic);
        }
        return instance;
    }

    public static IEnumerable<string> GetTransformNames(string tfTopic = "/tf")
    {
        return GetOrCreateInstance().GetOrCreateTFTopic(tfTopic).GetTransformNames();
    }

    public static IEnumerable<TFStream> GetTransforms(string tfTopic = "/tf")
    {
        return GetOrCreateInstance().GetOrCreateTFTopic(tfTopic).GetTransforms();
    }

    public static void AddListener(Action<TFStream> callback, bool notifyAllStreamsNow = true, string tfTopic = "/tf")
    {
        TFTopicState state = GetOrCreateInstance().GetOrCreateTFTopic(tfTopic);
        state.AddListener(callback);
        if (notifyAllStreamsNow)
            state.NotifyAllChanged();
    }

    public static void NotifyAllChanged(TFStream stream)
    {
        GetOrCreateInstance().GetOrCreateTFTopic(stream.TFTopic).NotifyAllChanged();
    }

    public static TFFrame GetTransform(HeaderMsg header, string tfTopic = "/tf")
    {
        return GetTransform(header.frame_id, header.stamp.ToLongTime(), tfTopic);
    }

    public static TFFrame GetTransform(string frame_id, long time, string tfTopic = "/tf")
    {
        var stream = GetTransformStream(frame_id, tfTopic);
        if (stream != null)
            return stream.GetWorldTF(time);
        return TFFrame.identity;
    }

    public static TFFrame GetTransform(string frame_id, TimeMsg time, string tfTopic = "/tf")
    {
        return GetTransform(frame_id, time.ToLongTime(), tfTopic);
    }

    public static TFFrame LookupRelativeTransform(string fromFrameId, string toFrameId, TimeMsg time, bool fallbackToIdentity = false, string tfTopic = "/tf")
    {
        TFStream stream = GetTransformStream(toFrameId, tfTopic);
        if (stream == null)
        {
            if (fallbackToIdentity)
            {
                return TFFrame.identity;
            }
            throw new Exception($"Unable to find transform '{toFrameId}' in tf tree");
        }
        return stream.GetRelativeTF(fromFrameId, time, fallbackToIdentity);
    }

    public static TFStream GetTransformStream(string frame_id, string tfTopic = "/tf")
    {
        return GetOrCreateInstance().GetOrCreateTFTopic(tfTopic).GetTransformStream(frame_id);
    }

    public static GameObject GetTransformObject(string frame_id, string tfTopic = "/tf")
    {
        TFStream stream = GetOrCreateInstance().GetOrCreateTFTopic(tfTopic).GetOrCreateFrame(frame_id);
        return stream.GameObject;
    }

    public TFTopicState GetOrCreateTFTopic(string tfTopic = "/tf", bool subscribeToStatic = true)
    {
        TFTopicState tfTopicState;
        if (!m_TFTopics.TryGetValue(tfTopic, out tfTopicState))
        {
            tfTopicState = new TFTopicState(tfTopic, subscribeToStatic);
            m_TFTopics[tfTopic] = tfTopicState;
        }
        return tfTopicState;
    }

    public static TFStream GetOrCreateFrame(string frame_id, string tfTopic = "/tf")
    {
        TFTopicState topicState = GetOrCreateInstance().GetOrCreateTFTopic(tfTopic);
        return topicState.GetOrCreateFrame(frame_id);
    }
}
