using System;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Data;
using Raven.Client.Documents.Commands;
using Sparrow.Json;

namespace Raven.Client.Documents
{
    public abstract class HiLoKeyGeneratorBase
    {
        protected const string RavenKeyGeneratorsHilo = "Raven/Hilo/";
        protected const string RavenKeyServerPrefix = "Raven/ServerPrefixForHilo";

        protected readonly string tag;
        protected long capacity;
        protected long baseCapacity;
        private volatile RangeValue range;

        protected string lastServerPrefix;
        protected DateTime lastRequestedUtc1, lastRequestedUtc2;

        protected HiLoKeyGeneratorBase(string tag, long capacity)
        {
            this.tag = tag;
            this.capacity = capacity;
            baseCapacity = capacity;
            this.range = new RangeValue(1, 0);
        }

        protected string GetDocumentKeyFromId(DocumentConvention convention, long nextId)
        {
            return string.Format("{0}{1}{2}{3}",
                                 tag,
                                 convention.IdentityPartsSeparator,
                                 lastServerPrefix,
                                 nextId);
        }

        protected long GetMaxFromDocument(BlittableJsonReaderObject document, long minMax)
        {
            //EFRAT
            long max = 0;
            object value;
            if (document.TryGetMember("ServerHi", out value))
            {
                var hi = value is long ? (long) value : 0;
                max = ((hi - 1) * capacity);

                /*document.DataAsJson.Remove("ServerHi");
                document.DataAsJson["Max"] = max;*/
            }
            //max = document.DataAsJson.Value<long>("Max");
            return Math.Max(max, minMax);
        }

        protected string HiLoDocumentKey
        {
            get { return RavenKeyGeneratorsHilo + tag; }
        }

        public bool DisableCapacityChanges { get; set; }

        protected void ModifyCapacityIfRequired()
        {
            if (DisableCapacityChanges)
                return;
            var span = SystemTime.UtcNow - lastRequestedUtc1;
            if (span.TotalSeconds < 5)
            {
                span = SystemTime.UtcNow - lastRequestedUtc2;
                if (span.TotalSeconds < 3)
                    capacity = Math.Max(capacity, Math.Max(capacity * 2, capacity * 4));
                else
                    capacity = Math.Max(capacity, capacity * 2);
            }
            else if (span.TotalMinutes > 1)
            {
                capacity = Math.Max(baseCapacity, capacity / 2);
            }

            lastRequestedUtc2 = lastRequestedUtc1;
            lastRequestedUtc1 = SystemTime.UtcNow;
        }

        protected InMemoryDocumentSessionOperations.DocumentInfo HandleGetDocumentResult(GetDocumentResult results)
        {
            if (results.Results.Length == 2  && results.Results[1] != null)
            {
                object value;
                ((BlittableJsonReaderObject) (results.Results[1])).TryGetMember("ServerPrefix", out value);
                lastServerPrefix = value.ToString();
            }
            else
            {
                lastServerPrefix = string.Empty;
            }

            if (results.Results.Length == 0 || results.Results[0] == null)
                return null;

            var document = (BlittableJsonReaderObject)results.Results[0];
            object metadata;
            document.TryGetMember(Constants.Metadata.Key, out metadata);

            object etag;
            document.TryGetMember(Constants.Metadata.Etag, out etag);

            //TODO - Efrat , we need this???
            /*foreach (var key in jsonDocument.Metadata.Keys.Where(x => x.StartsWith("@")).ToArray())
            {
                jsonDocument.Metadata.Remove(key);
            }*/

            return new InMemoryDocumentSessionOperations.DocumentInfo()
            {
                Document = document,
                ETag = etag as long?,
                Metadata = metadata as BlittableJsonReaderObject
            };
        }

        protected RangeValue Range
        {
            get { return range; }
            set { range = value; }
        }

        [System.Diagnostics.DebuggerDisplay("[{Min}-{Max}]: {Current}")]
        protected class RangeValue
        {
            public readonly long Min;
            public readonly long Max;
            public long Current;

            public RangeValue(long min, long max)
            {
                this.Min = min;
                this.Max = max;
                this.Current = min - 1;
            }
        }
    }
}
