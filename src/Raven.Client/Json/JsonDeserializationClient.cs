﻿using System;
using Raven.Abstractions.Data;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Sparrow.Json;
using BatchResult = Raven.Client.Documents.Commands.BatchResult;

namespace Raven.Client.Json
{
    public class JsonDeserializationClient : JsonDeserializationBase
    {
        public static readonly Func<BlittableJsonReaderObject, GetDocumentResult> GetDocumentResult = GenerateJsonDeserializationRoutine<GetDocumentResult>();

        public static readonly Func<BlittableJsonReaderObject, BatchResult> BatchResult = GenerateJsonDeserializationRoutine<BatchResult>();

        public static readonly Func<BlittableJsonReaderObject, PutResult> PutResult = GenerateJsonDeserializationRoutine<PutResult>();

        public static readonly Func<BlittableJsonReaderObject, AuthenticatorChallenge> AuthenticatorChallenge = GenerateJsonDeserializationRoutine<AuthenticatorChallenge>();

        public static readonly Func<BlittableJsonReaderObject, Topology> ClusterTopology = GenerateJsonDeserializationRoutine<Topology>();

        public static readonly Func<BlittableJsonReaderObject, TcpConnectionHeaderMessage> TcpConnectionHeaderMessage = GenerateJsonDeserializationRoutine<TcpConnectionHeaderMessage>();

        public static readonly Func<BlittableJsonReaderObject, DatabaseDocument> DatabaseDocument = GenerateJsonDeserializationRoutine<DatabaseDocument>();

        public static BatchResult Test(BlittableJsonReaderObject doc)
        {
            return GenerateJsonDeserializationRoutine<BatchResult>()(doc);
        }

        public static GetDocumentResult Test2(BlittableJsonReaderObject doc)
        {
            return GenerateJsonDeserializationRoutine<GetDocumentResult>()(doc);
        }
    }
}