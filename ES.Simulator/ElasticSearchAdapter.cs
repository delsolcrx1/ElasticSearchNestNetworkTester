using Elasticsearch.Net;
using Nest;
using Nest.JsonNetSerializer;
using Newtonsoft.Json;
using NLog;
using System;
using System.Configuration;
using System.Dynamic;
using System.Net;

namespace ES.Simulator
{
    class ElasticSearchAdapter
    {
        public static bool IndexTraceEnabled = Convert.ToBoolean(ConfigurationManager.AppSettings["IndexTraceEnabled"].ToString());
        public static string IndexUsername = ConfigurationManager.AppSettings["IndexUsername"].ToString();
        public static string IndexPassword = ConfigurationManager.AppSettings["IndexPassword"].ToString();
        public static bool IndexUntrustedSslCert = Convert.ToBoolean(ConfigurationManager.AppSettings["IndexUntrustedSslCert"].ToString());
        public static string IndexHostName = ConfigurationManager.AppSettings["IndexHost"].ToString();
        public static string IndexPrefix = ConfigurationManager.AppSettings["IndexPrefix"].ToString();
        public static int IndexId = Convert.ToInt32(ConfigurationManager.AppSettings["IndexId"].ToString());
        public static int IndexKeepAliveTime = Convert.ToInt32(ConfigurationManager.AppSettings["IndexKeepAliveTime"].ToString());
        public static int IndexKeepAliveInterval = Convert.ToInt32(ConfigurationManager.AppSettings["IndexKeepAliveInterval"].ToString());
        public static int ErrorCount = 0;
        public static int SuccessCount = 0;

        public static IElasticClient GetElasticClient(Logger logger)
        {
            try
            {
                // Initialise the ElasticClient
                var node = new Uri(IndexHostName);
                var pool = new SingleNodeConnectionPool(node);
                var connection = new HttpConnection();

                var settings = new ConnectionSettings(pool, connection,
                    sourceSerializer: (builtin, set) => new JsonNetSerializer(builtin, set,
                        () => new JsonSerializerSettings
                        {
                            NullValueHandling = NullValueHandling.Include,
                            TypeNameHandling = TypeNameHandling.Auto
                        }));

                settings.EnableTcpKeepAlive(
                    TimeSpan.FromMilliseconds(IndexKeepAliveTime),
                    TimeSpan.FromMilliseconds(IndexKeepAliveInterval));

                //settings.EnableDebugMode();

                if (IndexTraceEnabled)
                {
                    settings = settings.DisableDirectStreaming()
                    .OnRequestCompleted(details =>
                    {
                        if (details.RequestBodyInBytes != null)
                        {
                            logger.Info($"Elastic Search request for index: {IndexHostName}." + details.RequestBodyInBytes);
                        }

                        if (details.ResponseBodyInBytes != null)
                        {
                            logger.Info($"Elastic Search response for index: {IndexHostName}." + details.ResponseBodyInBytes);
                        }

                        if (details.DebugInformation != null)
                        {
                            logger.Info($"Elastic Search debug infromation response for index: {IndexHostName}." + details.DebugInformation);
                        }
                    });
                }

                if (IndexUntrustedSslCert &&
                    ServicePointManager.ServerCertificateValidationCallback == null)
                {
                    ServicePointManager.ServerCertificateValidationCallback += (sender, cert, chain, errors) => true;
                }
                settings = settings.BasicAuthentication(IndexUsername, IndexPassword);

                return new ElasticClient(settings);
            }
            catch (Exception ex)
            {
                logger.Error("Connection Exception" + ex);

            }
            return null;
        }

        public void PingES(IElasticClient client, Logger logger)
        {
            try
            {
                Elasticsearch.Net.IElasticsearchResponse response = client.Ping();

                if (response.ApiCall.HttpStatusCode == 200)
                {
                    logger.Info(IndexHostName + " ping success.");
                }
                else
                {
                    logger.Error(response.ApiCall.DebugInformation);
                    System.Environment.Exit(0);
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex);
                System.Environment.Exit(0);
            }
        }

        public void SearchES(IElasticClient client, Logger logger)
        {
            try
            {
                var response = client.Get<ExpandoObject>(new GetRequest(IndexPrefix + "task",
                                    "task", IndexId)) as GetResponse<ExpandoObject>;

                //var result = await client.GetIndexAsync(null, c => c
                //                     .AllIndices()
                //             );

                if (response.ApiCall.HttpStatusCode == 200)
                {
                    SuccessCount += 1;
                    logger.Info(IndexHostName + " search success. " + SuccessCount);
                }
                else
                {
                    ErrorCount += 1;
                    logger.Error("Error Count " + ErrorCount + " " + response.ApiCall.DebugInformation);
                }
            }
            catch (Exception ex)
            {
                logger.Error("General Catch" + ex);
                System.Environment.Exit(0);
            }
        }
    }
}
