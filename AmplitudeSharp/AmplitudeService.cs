using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
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
        private bool eventQueueDirty;
        private readonly CancellationTokenSource cancellationToken;
        private Thread sendThread;
        private Thread persistenceThread;
        private readonly AmplitudeApi api;
        private AmplitudeIdentify identification;
        private readonly SemaphoreSlim eventsReady;
        private long sessionId = -1;
        private readonly Stream persistenceStream;
        private readonly AmplitudeServiceSettings settings;
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

        private AmplitudeService(string apiKey, Stream persistenceStream = null, AmplitudeServiceSettings settings = null)
        {
            lockObject = new object();

            // Use default settings if none given
            this.settings = settings ?? new AmplitudeServiceSettings();

            api = new AmplitudeApi(apiKey, apiJsonSerializerSettings);
            eventQueue = new List<IAmplitudeEvent>();
            cancellationToken = new CancellationTokenSource();
            eventsReady = new SemaphoreSlim(0);

            if (persistenceStream != null)
            {
                this.persistenceStream = persistenceStream;
                LoadPastEvents();
            }
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
        /// <param name="persistenceStream">optional, stream to persist/restore saved event data. This is written to periodically and should exlusive to the service.</param>
        /// <param name="logger">Action delegate for logging purposes, if none is specified <see cref="System.Diagnostics.Debug.WriteLine(object)"/> is used</param>
        /// <param name="settings">Settings to customize how the Amplitude service operates.</param>
        /// <returns></returns>
        public static AmplitudeService Initialize(string apiKey, Action<LogLevel, string> logger = null, Stream persistenceStream = null, AmplitudeServiceSettings settings = null)
        {
            // Deliberately catch the example key (stops copy/paste error from docs)
            if (apiKey == "<YOUR_API_KEY>")
            {
                throw new ArgumentOutOfRangeException(nameof(apiKey), "Please specify Amplitude API key");
            }

            AmplitudeService instance = new AmplitudeService(apiKey, persistenceStream, settings);
            instance.NewSession();

            if (Interlocked.CompareExchange(ref s_instance, instance, null) == null)
            {
                if (logger == null)
                {
                    logger = (level, message) => { System.Diagnostics.Debug.WriteLine($"Analytics: [{level}] {message}"); };
                }

                s_logger = logger;

                instance.StartBackgroundThreads();
            }

            return Instance;
        }

        public void Uninitialize()
        {
            cancellationToken.Cancel();
            // Force an immediate save outside our persistence thread
            SaveEvents();
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
        public void Track(string eventName, object properties = null)
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

                eventQueueDirty = true;
            }

            eventsReady.Release();
        }

        private void SaveEvents()
        {
            try
            {
                // We don't need to write anything in the common case that the event has been sent to the API before our 
                // save interval has expired. However, if there are already existing events in the store, we would need to
                // clear them. We can check this by seeing if our stream position is > 0 (i.e. it had some content previously).
                if (eventQueue.Any() || persistenceStream.Position > 0)
                {
                    String persistedData;
                    lock (lockObject)
                    {
                        persistedData = JsonConvert.SerializeObject(eventQueue, persistenceJsonSerializerSettings);
                        // Dirty flag is just an optimisation so we write less. Doesn't matter if we save events that our upload
                        // thread has since dispatched. We will update the store next cycle. If we crash, all our events have 
                        // insert_id's so they are fine to replay.
                        eventQueueDirty = false;
                    }
                    lock (persistenceStream)
                    {
                        // Reset us back to the start and truncate content (incase new data is shorter)
                        persistenceStream.Seek(0, SeekOrigin.Begin);
                        persistenceStream.SetLength(0);

                        using (var writer = new StreamWriter(persistenceStream, Encoding.UTF8, 1024, true))
                        {
                            writer.Write(persistedData);
                        }

                        persistenceStream.Flush();
                    }
                }
            }
            catch (Exception e)
            {
                AmplitudeService.s_logger(LogLevel.Error, $"Failed to persist events: {e}");
            }
        }

        private void LoadPastEvents()
        {
            try
            {
                lock (persistenceStream)
                {
                    using (var reader = new StreamReader(persistenceStream, Encoding.UTF8, false, 1024, true))
                    {
                        string persistedData = reader.ReadLine();
                        if (!String.IsNullOrEmpty(persistedData))
                        {
                            var data = JsonConvert.DeserializeObject<List<IAmplitudeEvent>>(persistedData, persistenceJsonSerializerSettings);
                            if(data.Any())
                            { 
                                lock (lockObject)
                                {
                                    eventQueue.InsertRange(0, data);
                                }
                                eventsReady.Release();
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                // We are safe to continue, we just won't have any of the previously saved data
                s_logger(LogLevel.Error, $"Failed to load persisted events: {e}");
            }
        }

        /// <summary>
        /// Configure and kick-off the background upload and persistence threads.
        /// </summary>
        private void StartBackgroundThreads()
        {
            sendThread = new Thread(UploadThread)
            {
                Name = $"{nameof(AmplitudeSharp)} Upload Thread",
                Priority = ThreadPriority.BelowNormal,
            };
            sendThread.Start();

            if (settings.BackgroundWritePeriodSeconds > 0)
            {
                persistenceThread = new Thread(PersistenceThread)
                {
                    Name = $"{nameof(AmplitudeSharp)} Persistence Thread",
                    Priority = ThreadPriority.BelowNormal,
                };
                persistenceThread.Start();
            }
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

                    // We have some events to dispatch. We might want to wait for more if mobile to group together
                    if (settings.DispatchBatchPeriodSeconds > 0 && eventQueue.Any())
                    {
                        await Task.Delay(TimeSpan.FromSeconds(settings.DispatchBatchPeriodSeconds), cancellationToken.Token);
                    }

                    List<IAmplitudeEvent> apiCallsToSend = new List<IAmplitudeEvent>();
                    bool backOff = false;

                    lock (lockObject)
                    {
                        // Remove any events that have been queued for too long (could include previously persisted events)
                        var now = DateTime.UtcNow.ToUnixEpoch();
                        var removed = eventQueue.RemoveAll(ev => ev is AmplitudeEvent &&
                            ((AmplitudeEvent)ev).Time + (settings.QueuedApiCallsTTLSeconds * 1000) < now);

                        // Events API supports bacthing, so grab a few (but stop if we get to an identify - different API)
                        foreach (IAmplitudeEvent ev in eventQueue)
                        {
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

                        if (result == AmplitudeApi.SendResult.Success)
                        {
                            // Success. Can remove those events from queue
                            EventQueueRemove(apiCallsToSend.Count);
                        }
                        else if (result == IAmplitudeApi.SendResult.BadData)
                        {
                            // TODO: For now, we also remove these from the list. In future we want to get the index of the
                            // events in the batch which failed and then only remove those from the queue.
                            EventQueueRemove(apiCallsToSend.Count);
                        }
                        else if (result == IAmplitudeApi.SendResult.InvalidApiKey)
                        {
                            // We cannot recover from this. The best we can do is save any events to the queue and hope for a
                            // new key API next time. We log the event, and then shutdown this thread.
                            s_logger(LogLevel.Error, $"Amplitude API returned invalid API key. Further API calls will not be sent");
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
                                    EventQueueRemove(apiCallsToSend.Count);
                                }
                            }
                            else
                            {
                                s_logger(LogLevel.Error, $"Identify data was too large for Amplitude");
                                EventQueueRemove(apiCallsToSend.Count);
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

                    if (backOff)
                    {
                        // 30s recommended by Amupltidue docs as backup time for throttling
                        await Task.Delay(TimeSpan.FromSeconds(settings.BackOffDelaySeconds), cancellationToken.Token);
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

        /// <summary>
        /// Removes the given number of API calls from the pending event queue (in a thread safe way).
        /// </summary>
        private void EventQueueRemove(int numberToRemove)
        {
            lock(lockObject)
            {
                eventQueue.RemoveRange(0, numberToRemove);

                eventQueueDirty = true;
            }
        }

        /// <summary>
        /// The background thread for persisting event state.
        /// </summary>
        private async void PersistenceThread()
        {
            try
            {
                while (true)
                {
                    await Task.Delay(TimeSpan.FromSeconds(settings.BackgroundWritePeriodSeconds), cancellationToken.Token);

                    if (eventQueueDirty)
                    {
                        // Will mark queue as non dirty
                        SaveEvents();
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                // No matter what exception happens, we just quit
                s_logger(LogLevel.Error, "Persistence thread terminated with: " + e);
            }
        }
    }
}
