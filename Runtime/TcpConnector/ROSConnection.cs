using RosMessageGeneration;
using RosMessageTypes.RosTcpEndpoint;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Runtime.TcpConnector;
using UnityEngine;

public class ROSConnection : MonoBehaviour
{

    public static ROSConnection Instance
    {
        get;
        private set;
    }
    
    // Variables required for ROS communication
    public string hostName = "192.168.1.1";
    public int hostPort = 10000;
    [Tooltip("If blank, determine IP automatically.")]
    public string overrideUnityIP = "";
    public int unityPort = 5005;
    bool alreadyStartedServer = false;
    private volatile bool _unityServerReady = false;
    private bool unityHandshakeInProgress = false;
    private volatile bool shouldCheckConnection = false;
    private bool checkingConnection = false;

    private int networkTimeout = 2000;

    public int awaitDataMaxRetries = 10;
    public float awaitDataSleepSeconds = 1.0f;

    static object _lock = new object(); // sync lock 
    static List<Task> activeConnectionTasks = new List<Task>(); // pending connections

    const string ERROR_TOPIC_NAME = "__error";
    const string SYSCOMMAND_TOPIC_NAME = "__syscommand";
    const string HANDSHAKE_TOPIC_NAME = "__handshake";
    
    private Dictionary<string, PersistentTCPConnection> persistentConnectionPublishers = new Dictionary<string, PersistentTCPConnection>();
    private List<PersistentTCPConnection> aliveConnections = new List<PersistentTCPConnection>();
    
    private uint simTimeSeconds = 0;
    private uint simTimeNanoSeconds = 0;
    
    private TcpListener tcpListener;

    const string SYSCOMMAND_SUBSCRIBE = "subscribe";
    const string SYSCOMMAND_PUBLISH = "publish";

    struct SubscriberCallback
    {
        public ConstructorInfo messageConstructor;
        public List<Action<Message>> callbacks;
    }

    Dictionary<string, SubscriberCallback> subscribers = new Dictionary<string, SubscriberCallback>();
    HashSet<string> publishers = new HashSet<string>();
    
    public static RosMessageTypes.Std.Time CurrentSimTime => new RosMessageTypes.Std.Time(Instance.simTimeSeconds, Instance.simTimeNanoSeconds);
    
    public void RegisterPersistentPublisher(PersistentTCPConnection persistentTcpConnection)
    {
        aliveConnections.Add(persistentTcpConnection);
    }
    
    private void Awake()
    {
        if (Instance != null)
        {
            throw new Exception($"Multiple instances of a singleton found: {GetType().Name}");
        }
        Instance = this;
        
        Subscribe<RosUnityError>(ERROR_TOPIC_NAME, RosUnityErrorCallback);

        if (overrideUnityIP != "")
        {
            StartMessageServer(overrideUnityIP, unityPort); // no reason to wait, if we already know the IP
        }
        else
        {
            //We'll handshake to verify a connection with the server.
            Debug.Log($"Attempting handshake with ROS TCP connector at: {hostName}:{hostPort}");
            unityHandshakeInProgress = false;
        }
    }

    void RosUnityHandshakeCallback(UnityHandshakeResponse response)
    {
        Debug.Log($"Handshake response received, ip: {response.ip}");
        StartMessageServer(response.ip, unityPort);
    }

    void RosUnityErrorCallback(RosUnityError error)
    {
        Debug.LogError("ROS-Unity error: " + error.message);
    }

    public bool UnityServerReady
    {
        get
        {
            return _unityServerReady;
        }
        set
        {
            _unityServerReady = value;
        }
    }

    /// <summary>
    /// Note: For this to work properly, the ROSConnection should be set higher in
    /// the Script Execution Order.
    /// Also a note on why it's updated every frame rather than requesting Time values when needed:
    /// Time.time can only be run in the main thread, so this is to stop problems relating to that.
    /// </summary>
    private void Update()
    {
        ConnectToBridgeIfApplicable();
        UpdateSimTime();
        CheckConnectionIfApplicable();
    }

    private void ConnectToBridgeIfApplicable()
    {
        if (!UnityServerReady)
        {
            if (!unityHandshakeInProgress)
            {
                unityHandshakeInProgress = true;
                SendServiceMessage<UnityHandshakeResponse>(HANDSHAKE_TOPIC_NAME, 
                    new UnityHandshakeRequest(overrideUnityIP, (ushort)unityPort), 
                    RosUnityHandshakeCallback, OnRosUnityHandshakeFailed);    
            }
        }
    }

    public void RequestCheckConnection()
    {
        shouldCheckConnection = true;
    }

    private void CheckConnectionIfApplicable()
    {
        if (UnityServerReady && shouldCheckConnection)
        {
            if (!checkingConnection)
            {
                Debug.Log("Checking connection status...");
                checkingConnection = true;
                SendServiceMessage<UnityHandshakeResponse>(HANDSHAKE_TOPIC_NAME, 
                    new UnityHandshakeRequest(overrideUnityIP, (ushort)unityPort), 
                    RosUnityCheckConnectionHandshakeCallback, OnRosUnityCheckConnectionHandshakeCallbackFailed);   
            }
        }
    }
    
    
    private void RosUnityCheckConnectionHandshakeCallback(UnityHandshakeResponse obj)
    {
        //Do nothing.
        Debug.Log("Connection status ok.");
        checkingConnection = false;
        shouldCheckConnection = false;
    }

    private void OnRosUnityCheckConnectionHandshakeCallbackFailed(Exception obj)
    {
        Debug.Log("ROS TCP connector connection failure detected, restarting server.");
        checkingConnection = false;
        shouldCheckConnection = false;
        UnityServerReady = false;
        unityHandshakeInProgress = false;
        alreadyStartedServer = false;
        try
        {
            tcpListener?.Stop();
        }
        catch (Exception e)
        {
            Debug.LogError(e);
        }
    }

    private void OnRosUnityHandshakeFailed(Exception obj)
    {
        unityHandshakeInProgress = false;
    }

    private void UpdateSimTime()
    {
        float simulatedTime = UnityEngine.Time.time;
        simTimeSeconds = (uint) Mathf.FloorToInt(simulatedTime);
        simTimeNanoSeconds = (uint) ((simulatedTime - (double) simTimeSeconds) * 1000000000.0);
    }

    public PersistentTCPConnection GetPersistentPublisher(string topicName)
    {
        if (persistentConnectionPublishers.TryGetValue(topicName, out PersistentTCPConnection persistentTcpConnection))
        {
            return persistentTcpConnection;
        }
        PersistentTCPConnection newPersistentTcpConnection = new PersistentTCPConnection(hostName, hostPort);
        persistentConnectionPublishers.Add(topicName, newPersistentTcpConnection);
        return newPersistentTcpConnection;
    } 
    
    public void Subscribe<T>(string topic, Action<T> callback) where T : Message, new()
    {
        SubscriberCallback subCallbacks;
        if (!subscribers.TryGetValue(topic, out subCallbacks))
        {
            subCallbacks = new SubscriberCallback
            {
                messageConstructor = typeof(T).GetConstructor(new Type[0]),
                callbacks = new List<Action<Message>> { }
            };
            subscribers.Add(topic, subCallbacks);
        }

        subCallbacks.callbacks.Add((Message msg) => { callback((T)msg); });
    }
    
    public async void SendServiceMessage<RESPONSE>(string rosServiceName, Message serviceRequest, 
            Action<RESPONSE> callback,
            Action<Exception> serviceFailedCallback = null
        ) where RESPONSE : Message, new()
    {

        
        RESPONSE serviceResponse = new RESPONSE();
        bool serviceResponseSuccessful;
        Exception correspondingError = null;
        TcpClient client = new TcpClient();
        
        try
        {
            
            await client.ConnectAsync(hostName, hostPort);

            NetworkStream networkStream = client.GetStream();
            networkStream.ReadTimeout = networkTimeout;

            WriteDataStaggered(networkStream, rosServiceName, serviceRequest);

            if (!networkStream.CanRead)
            {
                throw new Exception("Unable to read from NetworkStream.");
            }

            // Poll every 1 second(s) for available data on the stream
            int attempts = 0;
            while (!networkStream.DataAvailable && attempts <= this.awaitDataMaxRetries)
            {
                if (attempts == this.awaitDataMaxRetries)
                {
                    Debug.LogError($"No data available on network stream after {awaitDataMaxRetries} attempts. ({(awaitDataMaxRetries * awaitDataSleepSeconds)} seconds)");
                }

                attempts++;
                await Task.Delay((int) (awaitDataSleepSeconds * 1000));
            }

            serviceResponseSuccessful = attempts <= this.awaitDataMaxRetries;
            
            if (serviceResponseSuccessful && ReadMessageData(networkStream, out string serviceName, out byte[] readBuffer))
            {
                // TODO: consider using the serviceName to confirm proper received location
                serviceResponse.Deserialize(readBuffer, 0);
            }
        }
        catch (Exception e)
        {
            serviceResponseSuccessful = false;
            correspondingError = e;
        }
        finally
        {
            //Try and close the clint connection if applicable.
            try
            {
                if (client.Connected)
                {
                    client.Close();
                }
            }
            catch (Exception)
            {
                //Ignored.
            }
        }

        try
        {
            if (serviceResponseSuccessful || serviceFailedCallback == null)
            {
                callback(serviceResponse);
            }
            else
            {
                serviceFailedCallback(correspondingError);
            }
        }
        catch (Exception e)
        {
            Debug.LogError("Unhandled internal exception! " + e);
        }
        
        
        //------------------------------
        //Old:
/*
        TcpClient client = new TcpClient();
        await client.ConnectAsync(hostName, hostPort);

        NetworkStream networkStream = client.GetStream();
        networkStream.ReadTimeout = networkTimeout;

        RESPONSE serviceResponse = new RESPONSE();

        // Send the message
        try
        {
            WriteDataStaggered(networkStream, rosServiceName, serviceRequest);
        }
        catch (Exception e)
        {
            Debug.LogError("SocketException: " + e);
            goto finish;
        }

        if (!networkStream.CanRead)
        {
            Debug.LogError("Sorry, you cannot read from this NetworkStream.");
            goto finish;
        }

        // Poll every 1 second(s) for available data on the stream
        int attempts = 0;
        while (!networkStream.DataAvailable && attempts <= this.awaitDataMaxRetries)
        {
            if (attempts == this.awaitDataMaxRetries)
            {
                Debug.LogError("No data available on network stream after " + awaitDataMaxRetries + " attempts.");
                goto finish;
            }
            attempts++;
            await Task.Delay((int)(awaitDataSleepSeconds * 1000));
        }

        try
        {
            if (ReadMessageData(networkStream, out string serviceName, out byte[] readBuffer))
            {
                // TODO: consider using the serviceName to confirm proper received location
                serviceResponse.Deserialize(readBuffer, 0);
            }
        }
        catch (Exception e)
        {
            Debug.LogError("Exception raised!! " + e);
        }

        finish:
        callback(serviceResponse);
        if (client.Connected)
            client.Close();
            */
    }

    public void RegisterSubscriber(string topic, string rosMessageName)
    {
        SendSysCommand(SYSCOMMAND_SUBSCRIBE, new SysCommand_Subscribe { topic = topic, message_name = rosMessageName });
    }

    public void RegisterPublisher(string topic, string rosMessageName)
    {
        SendSysCommand(SYSCOMMAND_PUBLISH, new SysCommand_Publish { topic = topic, message_name = rosMessageName });
    }

    /// <summary>
    /// 	Function is meant to be overridden by inheriting classes to specify how to handle read messages.
    /// </summary>
    /// <param name="tcpClient"></param> TcpClient to read byte stream from.
    protected async Task HandleConnectionAsync(TcpClient tcpClient)
    {
        await Task.Yield();
        // continue asynchronously on another threads

        ReadMessage(tcpClient.GetStream());
    }

    private byte[] ReadBytes(NetworkStream networkStream, int length)
    {
        byte[] result = new byte[length];
        int totalBytesRead = 0;
        while (totalBytesRead < length)
        {
            int bytesRead = networkStream.Read(result, totalBytesRead, length - totalBytesRead);
            if (bytesRead <= 0)
            {
                throw new Exception("Read stream closed unexpectedly!");
            }
            totalBytesRead += bytesRead;
        }

        return result;
    }
    
    private bool ReadMessageData(NetworkStream networkStream, out string topicName, out byte[] messageBytes)
    {
        topicName = "";
        messageBytes = null;
        try
        {
            if (!networkStream.CanRead)
            {
                throw new Exception("Unable to read from network stream!");
            }
            
            
            // Get first bytes to determine length of topic name
            byte[] topicLengthBytes = ReadBytes(networkStream, 4);
            int topicLength = BitConverter.ToInt32(topicLengthBytes, 0);

            // Read and convert topic name
            byte[] topicNameBytes = ReadBytes(networkStream, topicLength);
            topicName = Encoding.ASCII.GetString(topicNameBytes, 0, topicLength);

            byte[] fullMessageSizeBytes = ReadBytes(networkStream, 4);
            int fullMessageSize = BitConverter.ToInt32(fullMessageSizeBytes, 0);

            messageBytes = ReadBytes(networkStream, fullMessageSize);
        }
        catch (Exception e)
        {
            Debug.LogError("Exception raised!! " + e);
            return false;
        }

        return true;
    }

    void ReadMessage(NetworkStream networkStream)
    {
        try
        {
            if (ReadMessageData(networkStream, out string topicName, out byte[] readBuffer))
            {

                SubscriberCallback subs;
                if (subscribers.TryGetValue(topicName, out subs))
                {
                    Message msg = (Message)subs.messageConstructor.Invoke(new object[0]);
                    msg.Deserialize(readBuffer, 0);
                    if (!Application.isPlaying)
                    {
                        Debug.LogWarning("Message received when the application was not playing, ignoring...");
                        return;
                    }
                    foreach (Action<Message> callback in subs.callbacks)
                    {
                        try
                        {
                            callback(msg);
                        }
                        catch (Exception e)
                        {
                            Debug.LogError("Fatal error found and caught, " +
                                           "continuing so that other subscribers will still work, but this should be fixed! Error:");
                            Debug.LogError(e);
                        }
                    }
                }
            }
        }
        catch (Exception e)
        {
            Debug.LogError("Exception raised!! " + e);
        }
    }

    /// <summary>
    /// 	Handles multiple connections and locks.
    /// </summary>
    /// <param name="tcpClient"></param> TcpClient to read byte stream from.
    private async Task StartHandleConnectionAsync(TcpClient tcpClient)
    {
        var connectionTask = HandleConnectionAsync(tcpClient);

        lock (_lock)
            activeConnectionTasks.Add(connectionTask);

        try
        {
            await connectionTask;
            // we may be on another thread after "await"
        }
        catch (Exception ex)
        {
            Debug.LogError(ex.ToString());
        }
        finally
        {
            lock (_lock)
                activeConnectionTasks.Remove(connectionTask);
        }
    }

    protected async void StartMessageServer(string ip, int port)
    {
        if (alreadyStartedServer)
            return;

        alreadyStartedServer = true;
        while (true)
        {
            try
            {
                tcpListener = new TcpListener(IPAddress.Parse(ip), port);
                tcpListener.Start();
                UnityServerReady = true;
            }
            catch (Exception e)
            {
                Debug.LogError("Exception raised!! " + e);
                return;
            }

                Debug.Log("ROS-Unity server listening on " + ip + ":" + port);

            try
            {
                while (true)   //we wait for a connection
                {
                    var tcpClient = await tcpListener.AcceptTcpClientAsync();

                    var task = StartHandleConnectionAsync(tcpClient);
                    // if already faulted, re-throw any error on the calling context
                    if (task.IsFaulted)
                        await task;

                    // try to get through the message queue before doing another await
                    // but if messages are arriving faster than we can process them, don't freeze up
                    float abortAtRealtime = Time.realtimeSinceStartup + 0.1f;
                    while (tcpListener.Pending() && Time.realtimeSinceStartup < abortAtRealtime)
                    {
                        tcpClient = tcpListener.AcceptTcpClient();
                        task = StartHandleConnectionAsync(tcpClient);
                        if (task.IsFaulted)
                            await task;
                    }
                }
            }
            catch (ObjectDisposedException e)
            {
                if (!alreadyStartedServer || tcpListener == null)
                {
                    // we're shutting down this server, that's fine
                    break;
                } else
                {
                    //Something went wrong, attempt to restart the server.
                    Debug.LogError("Exception raised!! " + e);
                }
            }
            catch (Exception e)
            {
                Debug.LogError("Exception raised!! " + e);
            }
        }
    }


    /// <summary>
    ///    Given some input values, fill a byte array in the desired format to use with
    ///     https://github.com/Unity-Technologies/Robotics-Tutorials/tree/master/catkin_ws/src/tcp_endpoint
    ///
    /// 	All messages are expected to come in the format of:
    /// 		first four bytes: int32 of the length of following string value
    /// 		next N bytes determined from previous four bytes: ROS topic name as a string
    /// 		next four bytes: int32 of the length of the remaining bytes for the ROS Message
    /// 		last N bytes determined from previous four bytes: ROS Message variables
    /// </summary>
    /// <param name="offset"></param> Index of where to start writing output data
    /// <param name="serviceName"></param> The name of the ROS service or topic that the message data is meant for
    /// <param name="fullMessageSizeBytes"></param> The full size of the already serialized message in bytes
    /// <param name="messageToSend"></param> The serialized ROS message to send to ROS network
    /// <returns></returns>
    public void GetPrefixBytes(ref int offset, byte[] serviceName, byte[] fullMessageSizeBytes, byte[] messageBuffer)
    {
        AppendBytesToBuffer(ref offset, messageBuffer, PersistentTCPConnection._Preamble);
        AppendBytesToBuffer(ref offset, messageBuffer, serviceName);
        AppendBytesToBuffer(ref offset, messageBuffer, fullMessageSizeBytes);
    }

    public void AppendBytesToBuffer(ref int offset, byte[] messageBuffer, byte[] bytesToAppend, int messageLength = -1)
    {
        if (messageLength == -1)
        {
            messageLength = bytesToAppend.Length;
        }
        System.Buffer.BlockCopy(bytesToAppend, 0, messageBuffer, offset, messageLength);
        offset += messageLength;
    }

    /// <summary>
    ///    Serialize a ROS message in the expected format of
    ///     https://github.com/Unity-Technologies/Robotics-Tutorials/tree/master/catkin_ws/src/tcp_endpoint
    ///
    /// 	All messages are expected to come in the format of:
    /// 		first four bytes: int32 of the length of following string value
    /// 		next N bytes determined from previous four bytes: ROS topic name as a string
    /// 		next four bytes: int32 of the length of the remaining bytes for the ROS Message
    /// 		last N bytes determined from previous four bytes: ROS Message variables
    /// </summary>
    /// <param name="topicServiceName"></param> The ROS topic or service name that is receiving the messsage
    /// <param name="message"></param> The ROS message to send to a ROS publisher or service
    /// <returns> byte array with serialized ROS message in appropriate format</returns>
    [Obsolete("Use WriteDataStaggered instead")]
    public byte[] GetMessageBytes(string topicServiceName, Message message)
    {
        
        byte[] topicName = message.SerializeString(topicServiceName);
        byte[] bytesMsg = message.Serialize();
        byte[] fullMessageSizeBytes = BitConverter.GetBytes(bytesMsg.Length);

        byte[] messageBuffer = new byte[PersistentTCPConnection._Preamble.Length + topicName.Length + fullMessageSizeBytes.Length + bytesMsg.Length];
        // Copy preamble, topic name and message size in bytes to message buffer
        int offset = 0;
        GetPrefixBytes(ref offset, topicName, fullMessageSizeBytes, messageBuffer);
        // ROS message bytes
        AppendBytesToBuffer(ref offset, messageBuffer, bytesMsg);

        return messageBuffer;
    }

    struct SysCommand_Subscribe
    {
        public string topic;
        public string message_name;
    }

    struct SysCommand_Publish
    {
        public string topic;
        public string message_name;
    }

    void SendSysCommand(string command, object param)
    {
        Send(SYSCOMMAND_TOPIC_NAME, new RosUnitySysCommand(command, JsonUtility.ToJson(param)));
    }
    
    public void Send(string rosTopicName, Message message)
    {
        //We'll have one PersistentTCPConnection for each published topic.
        PersistentTCPConnection persistentTcpConnection = GetPersistentPublisher(rosTopicName);
        persistentTcpConnection.Send(rosTopicName, message);
        
        /*
        TcpClient client = null;
        try
        {

            client = new TcpClient();
            await client.ConnectAsync(hostName, hostPort);

            NetworkStream networkStream = client.GetStream();
            networkStream.ReadTimeout = networkTimeout;

            WriteDataStaggered(networkStream, rosTopicName, message);

        }
        catch (NullReferenceException e)
        {
            Debug.LogError("TCPConnector.SendMessage Null Reference Exception: " + e);
        }
        catch (Exception e)
        {
            Debug.LogError("TCPConnector Exception: " + e);
        }
        finally
        {
            if (client != null && client.Connected)
            {
                try
                {
                    client.Close();
                }
                catch (Exception)
                {
                    //Ignored.
                }
            }
        }
        */
    }

    private void WriteDataStaggered(NetworkStream networkStream, string rosTopicName, Message message)
    {
        
        networkStream.Write(PersistentTCPConnection._Preamble, 0, PersistentTCPConnection._Preamble.Length);
        
        byte[] topicName = message.SerializeString(rosTopicName);
        List<byte[]> segments = message.SerializationStatements();
        int messageLength = 0;
        for (int i = 0; i < segments.Count; i++)
        {
            messageLength += segments[i].Length;
        }
        byte[] fullMessageSizeBytes = BitConverter.GetBytes(messageLength);

        networkStream.Write(topicName, 0, topicName.Length);
        networkStream.Write(fullMessageSizeBytes, 0, fullMessageSizeBytes.Length);
        for (int i = 0; i < segments.Count; i++)
        {
            byte[] segmentData = segments[i];
            networkStream.Write(segmentData, 0, segmentData.Length);
        }
        
        networkStream.Flush();
    }

    private void OnDestroy()
    {
        foreach (PersistentTCPConnection persistentTcpConnection in aliveConnections)
        {
            persistentTcpConnection.Shutdown();
        }
        if (tcpListener != null)
            tcpListener.Stop();
        tcpListener = null;
    }
}
