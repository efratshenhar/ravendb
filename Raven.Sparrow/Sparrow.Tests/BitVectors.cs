﻿using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;

namespace Sparrow.Tests
{
    public class BitVectorsTest
    {
        public static IEnumerable<object[]> VectorSize
        {
            get
            {
                return new[]
				{
                    new object[] {1},
                    new object[] {4},
                    new object[] {7},
                    new object[] {8},
                    new object[] {9},
                    new object[] {16},
                    new object[] {17},
                    new object[] {65},
                    new object[] {90},
                    new object[] {244},
                    new object[] {513},
				};
            }
        }

        [Fact]
        public void Constants()
        {
            Assert.Equal(64, BitVector.BitsPerWord);
            Assert.Equal((uint) Math.Log(BitVector.BitsPerWord, 2), BitVector.Log2BitsPerWord);
        }


        [Fact]
        public void Operations_Bits()
        {
            Assert.Equal(0x0000000000000001UL, BitVector.BitInWord(63));
            Assert.Equal(0x0000000000000002UL, BitVector.BitInWord(62));
            Assert.Equal(0x0000000000000004UL, BitVector.BitInWord(61));
            Assert.Equal(0x0000000000000008UL, BitVector.BitInWord(60));
            Assert.Equal(0x8000000000000000UL, BitVector.BitInWord(0));
            
            //Assert.Equal(0x0000000000000001UL, BitVector.BitInWord(0));
            //Assert.Equal(0x0000000000000002UL, BitVector.BitInWord(1));
            //Assert.Equal(0x0000000000000004UL, BitVector.BitInWord(2));
            //Assert.Equal(0x0000000000000008UL, BitVector.BitInWord(3));
            //Assert.Equal(0x8000000000000000UL, BitVector.BitInWord(63));

            for ( int i = 0; i < BitVector.BitsPerWord; i++ )
                Assert.Equal(BitVector.BitInWord(i), BitVector.BitInWord(i + BitVector.BitsPerWord));

            Assert.Equal(0U, BitVector.WordForBit(0));
            Assert.Equal(0U, BitVector.WordForBit(1));
            Assert.Equal(0U, BitVector.WordForBit(63));
            Assert.Equal(1U, BitVector.WordForBit(64));
            Assert.Equal(1U, BitVector.WordForBit(127));
            Assert.Equal(2U, BitVector.WordForBit(128));

            Assert.Equal(0, BitVector.NumberOfWordsForBits(0));
            Assert.Equal(1, BitVector.NumberOfWordsForBits(1));
            Assert.Equal(2, BitVector.NumberOfWordsForBits(128));
            Assert.Equal(3, BitVector.NumberOfWordsForBits(129));
        }

        [Theory]
        [PropertyData("VectorSize")]
        public void Construction(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);

            Assert.Equal(vectorSize, vector.Count);
            for (int i = 0; i < vectorSize; i++)
                Assert.False(vector[i]);

            vector = new BitVector(vectorSize);

            Assert.Equal(vectorSize, vector.Count);
            for (int i = 0; i < vectorSize; i++)
                Assert.False(vector[i]);
        }

        [Fact]
        public void Construction_Explicit()
        {
            var vector = BitVector.Of(0xFFFFFFFF, 0x00000000);
            Assert.Equal(64, vector.Count);

            for (int i = 0; i < 32; i++)
            {
                Assert.True(vector[i]);
                Assert.False(vector[32 + i]);
            }
        }

        [Theory]
        [PropertyData("VectorSize")]
        public void SetBy_Index(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);
            for (int i = 0; i < vectorSize; i++)
            {
                Assert.False(vector[i]);
                vector[i] = true;

                for (int j = 0; j < vectorSize; j++)
                    Assert.True(vector[j] == (i == j));

                Assert.True(vector[i]);
                vector[i] = false;

                for (int j = 0; j < vectorSize; j++)
                    Assert.False(vector[j]);
            }
        }

        [Theory]
        [PropertyData("VectorSize")]
        public void SetBy_Method(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);
            for (int i = 0; i < vectorSize; i++)
            {
                Assert.False(vector.Get(i));
                vector.Set(i, true);

                for (int j = 0; j < vectorSize; j++)
                    Assert.True(vector.Get(j) == (i == j));

                Assert.True(vector[i]);
                vector.Set(i, false);

                for (int j = 0; j < vectorSize; j++)
                    Assert.False(vector.Get(j));
            }
        }

        [Theory]
        [PropertyData("VectorSize")]
        public void Flip_Bit(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);
            for (int i = 0; i < vectorSize; i++)
            {
                Assert.False(vector[i]);
                vector.Flip(i);

                for (int j = 0; j < vectorSize; j++)
                    Assert.True(vector[j] == (i == j));

                Assert.True(vector[i]);
                vector.Flip(i);

                for (int j = 0; j < vectorSize; j++)
                    Assert.False(vector[j]);
            }
        }

        [Fact]
        public void Flip_BitsExplicit()
        {
            var v1 = BitVector.Of(0xFFFFFFFF, 0x00000000);
            var v2 = BitVector.Of(0xFFFFFFFF, 0x000000FF);

            v1.Flip(64 - 2, 64);

            v1 = BitVector.Of(0xFFFFFFFF, 0x00000000);
            v2 = BitVector.Of(0xFFFFFF00, 0xFF000000);

            v1.Flip(32 - 2, 32 + 2);

            v1 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00001100);
            v2 = BitVector.Of(0xFFFFFFFF, 0x000000FF, 0xFF000000, 0x00001100);

            v1.Flip(64 - 8, 64 + 8);

            Assert.Equal(0, v1.CompareTo(v2));

            v1 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00001100);
            v2 = BitVector.Of(0xFFFFFFFF, 0x000000FF, 0xFFFFFFFF, 0xFFFFFFFF, 0xFF000000, 0x00001100);

            v1.Flip(64 - 8, 64 + 64 + 8);

            Assert.Equal(0, v1.CompareTo(v2));
        }

        [Theory]
        [PropertyData("VectorSize")]
        public void Flip_Bits(int vectorSize)
        {
            Random rnd = new Random(1000);

            var vector = BitVector.OfLength(vectorSize);
            for (int i = 0; i < vectorSize; i++)
            {
                int k = rnd.Next(vectorSize - i);

                Assert.False(vector[i]);
                vector.Flip(i, i + k);

                for (int j = 0; j < vectorSize; j++)
                {
                    if (j >= i && j < i + k)
                        Assert.True(vector[j]);
                    else if (j == i)
                        Assert.True(vector[j]);
                    else
                        Assert.False(vector[j]);
                }

                vector.Flip(i, i + k);

                for (int j = 0; j < vectorSize; j++)
                    Assert.False(vector[j]);
            }
        }

        [Theory]
        [PropertyData("VectorSize")]
        public void Fill_Clear(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);
            vector.Fill(true);
            for (int i = 0; i < vectorSize; i++)
                Assert.True(vector[i]);

            vector.Clear();
            for (int i = 0; i < vectorSize; i++)
                Assert.False(vector[i]);
        }

        [Theory]
        [PropertyData("VectorSize")]
        public void Operations_IdentityEqualSizes(int vectorSize)
        {
            Random rnd = new Random(1000);

            ulong[] data = new ulong[vectorSize];
            for (int i = 0; i < data.Length; i++)
                data[i] = (ulong)rnd.Next() << 32 | (ulong)rnd.Next();

            var v1 = BitVector.Of(data);
            var v2 = BitVector.OfLength(v1.Count);
            v1.CopyTo(v2);

            var rAnd = BitVector.And(v1, v2);
            var rOr = BitVector.Or(v1, v2);
            var rXor = BitVector.Xor(v1, v2);

            BitVector[] array = { rAnd, rOr, rXor };
            foreach (var r in array)
            {
                Assert.Equal(v1.Count, r.Count);
                Assert.Equal(v2.Count, r.Count);
            }

            for (int i = 0; i < v1.Count; i++)
            {
                Assert.Equal(rAnd[i], v1[i] && v2[i]);
                Assert.Equal(rOr[i], v1[i] || v2[i]);
                Assert.Equal(rXor[i], v1[i] ^ v2[i]);
            }
        }

        private static readonly Random generator = new Random(1000);

        [Theory]
        [PropertyData("VectorSize")]
        public void Operations_IdentityDifferentSizes(int vectorSize)
        {
            ulong[] data = GenerateRandomArray(vectorSize);
            ulong[] dataPlus = GenerateRandomArray(vectorSize + BitVector.BitsPerWord);

            var v1 = BitVector.Of(data);
            var v2 = BitVector.Of(dataPlus);

            var rAnd = BitVector.And(v1, v2);
            var rOr = BitVector.Or(v1, v2);
            var rXor = BitVector.Xor(v1, v2);

            BitVector[] array = { rAnd, rOr, rXor };
            foreach (var r in array)
            {
                Assert.Equal(v1.Count, r.Count);
                Assert.True(v2.Count > r.Count);
            }

            for (int i = 0; i < v1.Count; i++)
            {
                Assert.Equal(rAnd[i], v1[i] && v2[i]);
                Assert.Equal(rOr[i], v1[i] || v2[i]);
                Assert.Equal(rXor[i], v1[i] ^ v2[i]);
            }
        }

        [Theory]
        [PropertyData("VectorSize")]
        public void Operations_IdentityDifferentSizesInverted(int vectorSize)
        {
            ulong[] data = GenerateRandomArray(vectorSize);
            ulong[] dataPlus = GenerateRandomArray(vectorSize + BitVector.BitsPerWord);

            var v1 = BitVector.Of(data);
            var v2 = BitVector.Of(dataPlus);

            var rAnd = BitVector.And(v2, v1);
            var rOr = BitVector.Or(v2, v1);
            var rXor = BitVector.Xor(v2, v1);

            BitVector[] array = { rAnd, rOr, rXor };
            foreach (var r in array)
            {
                Assert.Equal(v1.Count, r.Count);
                Assert.True(v2.Count > r.Count);
            }

            for (int i = 0; i < v1.Count; i++)
            {
                Assert.Equal(rAnd[i], v1[i] && v2[i]);
                Assert.Equal(rOr[i], v1[i] || v2[i]);
                Assert.Equal(rXor[i], v1[i] ^ v2[i]);
            }
        }

        [Fact]
        public void Operations_Compare()
        {
            var v1 = BitVector.From("1");
            var v2 = BitVector.From("0");

            Assert.Equal(1, v1.CompareTo(v2));

            v1 = BitVector.From("10");
            v2 = BitVector.From("01");

            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));

            v1 = BitVector.From("10000000");
            v2 = BitVector.From("01000000");

            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));

            v1 = BitVector.From("11000000");
            v2 = BitVector.From("01000000");

            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));

            v1 = BitVector.From("11000000");
            v2 = BitVector.From("10000000");

            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));

            v1 = BitVector.From("11000000111");
            v2 = BitVector.From("11000000111");

            Assert.Equal(0, v1.CompareTo(v2));
            Assert.Equal(0, v2.CompareTo(v1));

            v1 = BitVector.From("01000000111");
            v2 = BitVector.From("01000000111");

            Assert.Equal(0, v1.CompareTo(v2));
            Assert.Equal(0, v2.CompareTo(v1));

            v1 = BitVector.From("01000000111");
            v2 = BitVector.From("01000000111");

            Assert.Equal(0, v1.CompareTo(v2));
            Assert.Equal(0, v2.CompareTo(v1));

            v1 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00001100);
            v2 = BitVector.Of(0xFFFFFFFF, 0x00000000, 0x00000000, 0x00000100);
        
            Assert.Equal(1, v1.CompareTo(v2));
            Assert.Equal(-1, v2.CompareTo(v1));
        }

        private static ulong[] GenerateRandomArray(int vectorSize, Random rnd = null)
        {
            if (rnd == null)
                rnd = generator;

            ulong[] data = new ulong[vectorSize];
            for (int i = 0; i < data.Length; i++)
                data[i] = (ulong)rnd.Next() << 32 | (ulong)rnd.Next();
            return data;
        }
    }
}
