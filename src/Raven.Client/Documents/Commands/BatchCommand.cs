using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using System.Reflection;
using System.Text;
using Raven.Abstractions.Commands;
using Raven.Client.Data;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;

namespace Raven.Client.Documents.Commands
{
    public class BatchCommand : RavenCommand<BatchResult>
    {
        public JsonOperationContext Context;
        public List<DynamicJsonValue> Commands;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
            };

            request.Content = new BlittableJsonContent(stream =>
            {
                using (var writer = new BlittableJsonTextWriter(Context, stream))
                {
                    /*writer.WriteStartObject();
                    writer.WritePropertyName("Commands");*/
                    writer.WriteStartArray();
                    bool first = false;
                    foreach (var command in Commands)
                    {
                        if (first)
                            writer.WriteComma();
                        first = true;
                        Context.Write(writer, command);
                    }
                    writer.WriteEndArray();
                    //writer.WriteEndObject();
                }
            });

            url = "bulk_docs";
            return request;
        }

      
        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
            {
                throw new InvalidOperationException("Got null response from the server after doing a batch, something is very wrong. Probably a garbled response. " +
                                                    "Commands: " + string.Join(",", Commands));
            }
            /*Object array;
            var b = response.TryGetMember("Commands", out array);
            foreach (var com in (array as BlittableJsonReaderArray))
            {
                var x = com as BlittableJsonReaderObject;
              
                Result = JsonDeserializationClient.Test(x);
            }*/
            Result = JsonDeserializationClient.BatchResult(response);

        }
    }
}