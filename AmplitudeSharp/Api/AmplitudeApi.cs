using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;

namespace AmplitudeSharp.Api
{
    public abstract class IAmplitudeApi
    {
        public enum SendResult
        {
            Success,
            ProxyNeeded,
            Throttled,
            ServerError,
        }

        public bool OfflineMode { get; set; }

        public abstract Task<SendResult> Identify(AmplitudeIdentify identification);

        public abstract Task<SendResult> SendEvents(List<AmplitudeEvent> events);
    }

    class AmplitudeApi : IAmplitudeApi
    {
        private string apiKey;
        private HttpClient httpClient;
        private HttpClientHandler httpHandler;
        private readonly JsonSerializerSettings jsonSerializerSettings;

        public AmplitudeApi(string apiKey, JsonSerializerSettings jsonSerializerSettings)
        {
            this.apiKey = apiKey;
            this.jsonSerializerSettings = jsonSerializerSettings;

            httpHandler = new HttpClientHandler {
                AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip,
                Proxy = WebRequest.GetSystemWebProxy(),
                UseProxy = true,
            };
            httpClient = new HttpClient(httpHandler);
        }

        public void ConfigureProxy(string proxyUserName, string proxyPassword)
        {
            if ((string.IsNullOrEmpty(proxyPassword) && string.IsNullOrEmpty(proxyUserName)))
            {
                httpHandler.Proxy = WebRequest.GetSystemWebProxy();
            }
            else
            {
                httpHandler.Proxy.Credentials = new NetworkCredential(proxyUserName, proxyPassword);
            }
        }

        public async override Task<SendResult> Identify(AmplitudeIdentify identification)
        {
            SendResult result = SendResult.Success;

            if (!OfflineMode)
            {
                // Identify API format from https://developers.amplitude.com/docs/identify-api
                // Oddly it uses a different format to the events (2) API. It is similar to the original
                // v1 HTTP API - we use HTTP post params, of which one contains the id data as JSON.

                string json = JsonConvert.SerializeObject(identification, jsonSerializerSettings);

                string boundary = "----" + DateTime.Now.Ticks;
                using (var content = new MultipartFormDataContent(boundary))
                {
                    content.Add(new StringContent(apiKey, UTF8Encoding.UTF8, "text/plain"), "api_key");
                    content.Add(new StringContent(json, UTF8Encoding.UTF8, "application/json"), "identification");

                    try
                    {
                        using (var response = await httpClient
                            .PostAsync($"https://api.amplitude.com/identify", content)
                            .ConfigureAwait(false))
                        {
                            // Fortunately the response codes at least match (well, close enough) the v2 event API
                            result = await ResultFromAmplitudeHttpResponse(response);
                        }
                    }
                    catch (HttpRequestException)
                    {
                        // Connection error. Handle the same as 500 (retry in a little)
                        result = SendResult.ServerError;
                    }
                }

            }

            return result;
        }

        public async override Task<SendResult> SendEvents(List<AmplitudeEvent> events)
        {
            SendResult result = SendResult.Success;

            if (!OfflineMode)
            {

                // Events API format v2 from https://developers.amplitude.com/docs/http-api-v2
                // Multiple events can be combined into a single call, but docs recommend 10 per batch.
                // JSON body with format as follows:
                //
                // {
                //   "api_key": "foo",
                //   "events: [
                //     /* ... */
                //   ],
                //   "options": {
                //       "min_id_length": 5
                //   }
                // }

                var payload = new
                {
                    api_key = apiKey,
                    events = events,
                    options = new
                    {
                        // Amplitude puts a min length of 5 on user IDs. We want to be able to send numeric ID's < 10k, so we lower it
                        min_id_length = 1
                    }
                };

                using (var ms = new MemoryStream())
                {
                    SerializeJsonIntoStream(payload, ms);

                    using (var httpContent = new StreamContent(ms))
                    using (var request = new HttpRequestMessage(HttpMethod.Post, "https://api.amplitude.com/2/httpapi"))
                    {
                        httpContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                        request.Content = httpContent;

                        try
                        {
                            using (var response = await httpClient
                                   .SendAsync(request, HttpCompletionOption.ResponseContentRead)
                                   .ConfigureAwait(false))
                            {
                                result = await ResultFromAmplitudeHttpResponse(response);
                            }
                        }
                        catch (HttpRequestException)
                        {
                            // Connection error. Handle the same as 500 (retry in a little)
                            result = SendResult.ServerError;
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Assigns one of our internal SendResult statuses from the API response. 
        /// </summary>
        /// <param name="response">The HTTP response from an API call to Amplitude</param>
        private async Task<SendResult> ResultFromAmplitudeHttpResponse(HttpResponseMessage response)
        {
            // Amplitude documented HTTP API response codes as follows
            switch (response.StatusCode)
            {
                // 200
                case HttpStatusCode.OK:
                    return SendResult.Success;

                // 400
                case HttpStatusCode.BadRequest:
                    // Error message detailing what was wrong with the request will be in body
                    var responseJson = await response.Content.ReadAsStringAsync();
                    // We want to catch API key messages during integration, so we do some extra work to catch them
                    try
                    {
                        // Bad API key response example JSON
                        // { "code":400,"error":"Invalid API key: 1234567890"}
                        var payload = JsonConvert.DeserializeObject<Dictionary<string, string>>(responseJson);
                        String errorMessage;
                        if(payload.TryGetValue("error", out errorMessage))
                        {
                            if(errorMessage.StartsWith("Invalid API key"))
                            {
                                // TODO: Return a different exception for API keys
                            }
                        }
                    }
                    catch (Exception)
                    {
                        // We failed to decode the object format changed? something else? Just treat as error and log
                    }
                    // If we get here, there was something wrong with the data, but its not our API key
                    AmplitudeService.s_logger(LogLevel.Error, $"Amplitude API returned 400 (Bad Request): {responseJson}");
                    // TODO: Use a non retryable error
                    return SendResult.ServerError;

                // 413
                case HttpStatusCode.RequestEntityTooLarge:
                    AmplitudeService.s_logger(LogLevel.Error, $"Event data sent to Amplitude exceeded size limit");
                    // TODO: Use a non retryable error
                    return SendResult.ServerError;

                // 429
                case (HttpStatusCode)429:
                    // TODO: Test this
                    // TODO: Do we warn anywhere if we are throttled? Useful to know
                    return SendResult.Throttled;

                // 500, 502, 504
                case HttpStatusCode.InternalServerError:
                case HttpStatusCode.NotImplemented:
                case HttpStatusCode.BadGateway:
                    // Amplitude had an error when hanlding the request. State unknown. Not guaranteed to be processed,
                    //  but also not guaranteed to be not. Need to resend request using same insert_id
                    return SendResult.ServerError;

                // 503
                case HttpStatusCode.ServiceUnavailable:
                    // Failed, but guaranteed not commit event. We retry again
                    return SendResult.ServerError;

                // Not an Amplitude API response
                /*
                case HttpStatusCode.ProxyAuthenticationRequired:
                    // This was in the original AmplitudeSharp, though looks WIP as result wasn't handled anywhere
                    return SendResult.ProxyNeeded;
                */

                default:
                    // If we are getting somethign not defined in the docs then treat as a server error (retryable)
                    return SendResult.ServerError;
            }
        }

        /// <summary>
        /// Serializes the given object into a steam.
        /// </summary>
        /// <param name="value">The object to serialize</param>
        /// <param name="stream">The stream to serialize the object into</param>
        private static void SerializeJsonIntoStream(object value, Stream stream)
        {
            // On high throughput apps we seralize a lot. It's slightly friendlier to do as stream not string
            // Useful if we have many thousands queued. See https://johnthiriet.com/efficient-post-calls/

            using (var sw = new StreamWriter(stream, new UTF8Encoding(false), 1024, true))
            using (var jtw = new JsonTextWriter(sw) { Formatting = Formatting.None })
            {
                var js = new JsonSerializer();
                js.Serialize(jtw, value);
                // Must flush it else we'll get an empty stream
                jtw.Flush();
                // We also need to reset the stream back to the beginning
                stream.Seek(0, SeekOrigin.Begin);
            }
        }
    }
}
