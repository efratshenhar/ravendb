//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Client.Indexes;

namespace Raven.Client.Documents
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession : InMemoryDocumentSessionOperations, IDocumentQueryGenerator, ISyncAdvancedSessionOperation, IDocumentSessionImpl
    {
        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        public IDocumentQuery<T> DocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var index = new TIndexCreator();
            return DocumentQuery<T>(index.IndexName, index.IsMapReduce);
        }

        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        [Obsolete("Use DocumentQuery instead.")]
        public IDocumentQuery<T> LuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            return DocumentQuery<T, TIndexCreator>();
        }

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="indexName">Name of the index.</param>
        /// <param name="isMapReduce">Indicates if index is a map-reduce index.</param>
        /// <returns></returns>
        [Obsolete("Use DocumentQuery instead.")]
        public IDocumentQuery<T> LuceneQuery<T>(string indexName, bool isMapReduce = false)
        {
            return DocumentQuery<T>(indexName, isMapReduce);
        }

        /// <summary>
        /// Dynamically query RavenDB using Lucene syntax
        /// </summary>
        [Obsolete("Use DocumentQuery instead.")]
        public IDocumentQuery<T> LuceneQuery<T>()
        {
            return DocumentQuery<T>();
        }

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="indexName">Name of the index.</param>
        /// <param name="isMapReduce">Indicates if index is a map-reduce index.</param>
        /// <returns></returns>
        public IDocumentQuery<T> DocumentQuery<T>(string indexName, bool isMapReduce = false)
        {
            return new DocumentQuery<T>(this, indexName, null, null, isMapReduce);
        }

        /// <summary>
        /// Dynamically query RavenDB using Lucene syntax
        /// </summary>
        public IDocumentQuery<T> DocumentQuery<T>()
        {
            var indexName = CreateDynamicIndexName<T>();
            return Advanced.DocumentQuery<T>(indexName);
        }

        /// <summary>
        /// Query RavenDB dynamically using LINQ
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        public IRavenQueryable<T> Query<T>()
        {
            var indexName = CreateDynamicIndexName<T>();

            return Query<T>(indexName);
        }

        /// <summary>
        /// Queries the specified index using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <param name="indexName">Name of the index.</param>
        /// <param name="isMapReduce">Whatever we are querying a map/reduce index (modify how we treat identifier properties)</param>
        public IRavenQueryable<T> Query<T>(string indexName, bool isMapReduce = false)
        {
            var ravenQueryStatistics = new RavenQueryStatistics();
            var highlightings = new RavenQueryHighlightings();
            var ravenQueryProvider = new RavenQueryProvider<T>(this, indexName, ravenQueryStatistics, highlightings,  isMapReduce);
            var inspector = new RavenQueryInspector<T>();
            inspector.Init(ravenQueryProvider, ravenQueryStatistics, highlightings, indexName, null, this, isMapReduce);
            return inspector;
        }

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        public IAsyncDocumentQuery<T> AsyncQuery<T>(string indexName, bool isMapReduce)
        {
            throw new NotSupportedException();
        }

        public RavenQueryInspector<S> CreateRavenQueryInspector<S>()
        {
            return new RavenQueryInspector<S>();
        }

        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return Query<T>(indexCreator.IndexName, indexCreator.IsMapReduce);
        }

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName, bool isMapReduce)
        {
            return Advanced.DocumentQuery<T>(indexName, isMapReduce);
        }
    }
}