﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Config.Settings;

namespace Raven.Server.Documents.Indexes.Static.NuGet
{
    public sealed class MultiSourceNuGetFetcher
    {
        public static MultiSourceNuGetFetcher Instance = new MultiSourceNuGetFetcher();

        private bool _initialized;
        private PathSetting _rootPath;
        private readonly ConcurrentDictionary<string, Lazy<NuGetFetcher>> _fetchers = new ConcurrentDictionary<string, Lazy<NuGetFetcher>>(StringComparer.OrdinalIgnoreCase);

        private MultiSourceNuGetFetcher()
        {
        }

        public void Initialize(PathSetting rootPath)
        {
            if (rootPath is null)
                throw new ArgumentNullException(nameof(rootPath));

            if (_initialized)
                throw new InvalidOperationException("TODO ppekrol");

            _rootPath = rootPath;
            _initialized = true;
        }

        public async Task<List<string>> DownloadAsync(string packageName, string packageVersion, string packageSourceUrl, CancellationToken token = default)
        {
            AssertInitialized();

            var fetcherLazy = _fetchers.GetOrAdd(packageSourceUrl, url => new Lazy<NuGetFetcher>(() => new NuGetFetcher(url, _rootPath.FullPath)));

            NuGetFetcher fetcher;
            try
            {
                fetcher = fetcherLazy.Value;
                await fetcher.ValidateConnectivity();
            }
            catch
            {
                _fetchers.TryRemove(packageSourceUrl, out _);
                throw;
            }

            return await fetcher.DownloadAsync(packageName, packageVersion, token);
        }

        private void AssertInitialized()
        {
            if (_initialized)
                return;

            throw new InvalidOperationException("NuGet fetcher was not initialized with a proper root path.");
        }
    }
}