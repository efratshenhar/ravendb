﻿using System.IO;
using System.Threading.Tasks;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;
using Raven.Database.FileSystem.Util;

namespace Raven.Database.FileSystem.Synchronization.Rdc
{
	public class SignaturePartialAccess : IPartialDataAccess
	{
		private readonly string _sigName;
		private readonly ISignatureRepository _signatureRepository;

		public SignaturePartialAccess(string sigName, ISignatureRepository signatureRepository)
		{
			_sigName = sigName;
			_signatureRepository = signatureRepository;
		}

		public Task CopyToAsync(Stream target, long from, long length)
		{
			return new NarrowedStream(_signatureRepository.GetContentForReading(_sigName), from, from + length - 1).CopyToAsync(target);
		}
	}
}