using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using AmplitudeSharp.Api;
using AmplitudeSharp.Utils;
using Newtonsoft.Json;

namespace AmplitudeSharp
{
    public class AmplitudeService : IDisposable
    {
        private static AmplitudeService s_instance;
        internal static Action<LogLevel, string> s_logger;

        public static AmplitudeService Instance => s_instance;

        private readonly object lockObject;
        private readonly List<IAmplitudeEvent> eventQueue;
        private readonly CancellationTokenSource cancellationToken;
        private Thread sendThread;
        private readonly AmplitudeApi api;
        private AmplitudeIdentify identification;
        private readonly SemaphoreSlim eventsReady;
        private long sessionId = -1;
        private readonly JsonSerializerSettings apiJsonSerializerSettings = new JsonSerializerSettings
        {
            NullValueHandling = NullValueHandling.Ignore,
            Formatting = Formatting.None,
        };
        private readonly JsonSerializerSettings persistenceJsonSerializerSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Objects,
            NullValueHandling = NullValueHandling.Ignore,
            Formatting = Formatting.None,
        };

        /// <summary>
        /// Sets Offline mode, which means the events are never sent to actual amplitude service
        /// This is meant for testing
        /// </summary>
        public bool OfflineMode
        {
            get => api.OfflineMode;
            set => api.OfflineMode = value;
        }

        /// <summary>
        /// Additional properties to send with every event
        /// </summary>
        public Dictionary<string, object> ExtraEventProperties { get; } = new Dictionary<string, object>();

        private AmplitudeService(string apiKey)
        {
            lockObject = new object();
            api = new AmplitudeApi(apiKey, apiJsonSerializerSettings);
            eventQueue = new List<IAmplitudeEvent>();
            cancellationToken = new CancellationTokenSource();
            eventsReady = new SemaphoreSlim(0);
        }

        public void Dispose()
        {
            Uninitialize();
            s_instance = null;
        }

        /// <summary>
        /// Initialize AmplitudeSharp
        /// Takes an API key for the project and, optionally,
        /// a stream where offline/past events are stored
        /// </summary>
        /// <param name="apiKey">api key for the project to stream data to</param>
        /// <param name="persistenceStream">optional, stream with saved event data <seealso cref="Uninitialize(Stream)"/></param>
        /// <param name="logger">Action delegate for logging purposes, if none is specified <see cref="System.Diagnostics.Debug.WriteLine(object)"/> is used</param>
        /// <returns></returns>
        public static AmplitudeService Initialize(string apiKey, Action<LogLevel, string> logger = null, Stream persistenceStream = null)
        {
            if (apiKey == "<YOUR_API_KEY>")
            {
                throw new ArgumentOutOfRangeException(nameof(apiKey), "Please specify Amplitude API key");
            }

            AmplitudeService instance = new AmplitudeService(apiKey);
            instance.NewSession();

            if (Interlocked.CompareExchange(ref s_instance, instance, null) == null)
            {
                if (logger == null)
                {
                    logger = (level, message) => { System.Diagnostics.Debug.WriteLine($"Analytics: [{level}] {message}"); };
                }

                s_logger = logger;

                instance.StartSendThread();

                if (persistenceStream != null)
                {
                    instance.LoadPastEvents(persistenceStream);
                }
            }

            return Instance;
        }

        public void Uninitialize(Stream persistenceStore = null)
        {
            cancellationToken.Cancel();
            SaveEvents(persistenceStore);
        }

        /// <summary>
        /// Configure proxy settings.
        /// Many corporate users will be behind a proxy server <seealso cref="System.Net.HttpStatusCode.ProxyAuthenticationRequired"/>
        /// Supply the proxy credentials to use
        /// </summary>
        /// <param name="proxyUserName">proxy server username</param>
        /// <param name="proxyPassword">proxy server password</param>
        public void ConfigureProxy(string proxyUserName = null, string proxyPassword = null)
        {
            api.ConfigureProxy(proxyUserName, proxyPassword);
        }

        /// <summary>
        /// Set user and device identification parameters.
        /// </summary>
        public void Identify(UserProperties user, DeviceProperties device)
        {
            AmplitudeIdentify identify = new AmplitudeIdentify();

            foreach (var extraProps in new[] { user.ExtraProperties, device.ExtraProperties })
            {
                foreach (var value in extraProps)
                {
                    identify.UserProperties[value.Key] = value.Value;
                }
            }

            foreach (var obj in new object[] { user, device })
            {
                foreach (PropertyInfo property in obj.GetType().GetRuntimeProperties())
                {
                    object value = property.GetValue(obj);
                    if ((value != null) &&
                        (property.GetCustomAttribute<JsonIgnoreAttribute>() == null))
                    {
                        var jsonProp = property.GetCustomAttribute<JsonPropertyAttribute>();
                        string name = jsonProp?.PropertyName ?? property.Name;

                        identify[name] = value;
                    }
                }
            }

            identification = identify;
            QueueEvent(identify);
        }

        /// <summary>
        /// Begin new user session.
        /// Amplitude groups events into a single session if they have the same session_id
        /// The session_id is just the unix time stamp when the session began.
        /// Normally, you don't have to call it, but if you want to identify a specific new
        /// session (e.g. you are building a plugin an not an app)
        /// </summary>
        public void NewSession()
        {
            sessionId = DateTime.UtcNow.ToUnixEpoch();
        }


        /// <summary>
        /// Log an event with parameters
        /// </summary>
        /// <param name="eventName">the name of the event</param>
        /// <param name="properties">parameters for the event (this can just be a dynamic class)</param>
        public void Track(string eventName, object properties = null )
        {
            var identification = this.identification;

            if (identification != null)
            {
                AmplitudeEvent e = new AmplitudeEvent(eventName, properties, ExtraEventProperties)
                {
                    SessionId = sessionId,
                    UserId = identification.UserId,
                    DeviceId = identification.DeviceId,
                };

                QueueEvent(e);
            }
            else
            {
                s_logger(LogLevel.Error, "Must call Identify() before logging events");

                if (Debugger.IsAttached)
                {
                    throw new InvalidOperationException("Must call Identify() before logging events");
                }
            }
        }

        private void QueueEvent(IAmplitudeEvent e)
        {
            lock (lockObject)
            {
                eventQueue.Add(e);
            }

            eventsReady.Release();
        }

        private void SaveEvents(Stream persistenceStore)
        {
            if (persistenceStore != null)
            {
                try
                {
                    string persistedData = JsonConvert.SerializeObject(eventQueue, persistenceJsonSerializerSettings);
                    using (var writer = new StreamWriter(persistenceStore))
                    {
                        writer.Write(persistedData);
                    }
                }
                catch (Exception e)
                {
                    AmplitudeService.s_logger(LogLevel.Error, $"Failed to persist events: {e}");
                }
            }
        }

        private void LoadPastEvents(Stream persistenceStore)
        {
            try
            {
                using (var reader = new StreamReader(persistenceStore))
                {
                    string persistedData = reader.ReadLine();
                    var data = JsonConvert.DeserializeObject<List<IAmplitudeEvent>>(persistedData, persistenceJsonSerializerSettings);

                    eventQueue.InsertRange(0, data);
                    eventsReady.Release();
                }
            }
            catch (Exception e)
            {
                s_logger(LogLevel.Error, $"Failed to load persisted events: {e}");
            }
        }

        /// <summary>
        /// Configure and kick-off the background upload thread
        /// </summary>
        private void StartSendThread()
        {
            sendThread = new Thread(UploadThread)
            {
                Name = $"{nameof(AmplitudeSharp)} Upload Thread",
                Priority = ThreadPriority.BelowNormal,
            };
            sendThread.Start();
        }

        /// <summary>
        /// The background thread for uploading events
        /// </summary>
        private async void UploadThread()
        {
            // Start with this, and we can shrink it if we start hitting size limits
            int maxEventBatch = AmplitudeApi.MaxEventBatchSize;

            try
            {
                while (true)
                {
                    await eventsReady.WaitAsync(cancellationToken.Token);

                    List<IAmplitudeEvent> apiCallsToSend = new List<IAmplitudeEvent>();
                    bool backOff = false;

                    lock (lockObject)
                    {
                        foreach (IAmplitudeEvent ev in eventQueue)
                        {
                            // Events API supports bacthing, so grab a few (stop if we get to an identify - different API)
                            if ((apiCallsToSend.Count > 0 && ev is AmplitudeIdentify) || (apiCallsToSend.Count >= maxEventBatch))
                            {
                                break;
                            }
                            apiCallsToSend.Add(ev);
                        }
                    }

                    if (apiCallsToSend.Count > 0)
                    { 
                        // A little ugly, but we need to call different parts of the API, yet handle the response the same way
                        AmplitudeApi.SendResult result;
                        AmplitudeIdentify identification;
                        if ((identification = apiCallsToSend[0] as AmplitudeIdentify) != null)
                        {
                            result = await api.Identify(identification);
                        }
                        else
                        {
                            result = await api.SendEvents(apiCallsToSend.Cast<AmplitudeEvent>());
                        }

                        lock (lockObject)
                        {
                            if (result == AmplitudeApi.SendResult.Success)
                            {
                                // Success. Can remove those events from queue
                                eventQueue.RemoveRange(0, apiCallsToSend.Count);
                            }
                            else if (result == IAmplitudeApi.SendResult.BadData)
                            {
                                // TODO: For now, we also remove these from the list. In future we want to get the index of the
                                // events in the batch which failed and then only remove those from the queue.
                                eventQueue.RemoveRange(0, apiCallsToSend.Count);
                            }
                            else if (result == IAmplitudeApi.SendResult.InvalidApiKey)
                            {
                                // We cannot recover from this. The best we can do is save any events to the queue and hope for a
                                // new key API next time. We log the error, and then shutdown this thread.
                                s_logger(LogLevel.Error, $"Amplitude returned invalid API key. Further API calls will not be sent");
                                return;
                            }
                            else if (result == IAmplitudeApi.SendResult.TooLarge)
                            {
                                // Events only. For identity we cant reduce the batch size
                                if (identification == null)
                                {
                                    // If our payload was too large, we assume that future payloads also might be too large and hit
                                    // the cap. We reduce the batch size if possible and try again. If we are already at size 1,
                                    // then we have a problem.
                                    if (maxEventBatch > 0)
                                    {
                                        maxEventBatch = maxEventBatch / 2;
                                    }
                                    else
                                    {
                                        // Event was too large. Remove the event and continue
                                        s_logger(LogLevel.Error, $"Event data was too large for Amplitude (EventId = {((AmplitudeEvent)apiCallsToSend[0]).EventId})");
                                        eventQueue.RemoveRange(0, apiCallsToSend.Count);
                                    }
                                }
                                else
                                {
                                    s_logger(LogLevel.Error, $"Identify data was too large for Amplitude");
                                    eventQueue.RemoveRange(0, apiCallsToSend.Count);
                                }
                            }
                            else if (result == IAmplitudeApi.SendResult.Throttled)
                            {
                                if (result == IAmplitudeApi.SendResult.Throttled)
                                {
                                    s_logger(LogLevel.Warning, $"Amplitude is throttling API calls");
                                }
                                backOff = true;
                            }
                            else if (result == IAmplitudeApi.SendResult.ServerError)
                            {
                                // We treat retryable server errors in the same way as a throttle. Retry in a bit
                                backOff = true;
                            }
                        }
                    }

                    if (backOff)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken.Token);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                // No matter what exception happens, we just quit
                s_logger(LogLevel.Error, "Upload thread terminated with: " + e);
            }
        }
    }
}
