/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections;
using System.Collections.Generic;

namespace Apache.NMS.ActiveMQ.State
{
	public class AtomicCollection<TValue>
		where TValue : class
	{
		private Object myLock = new Object();
		private ArrayList _collection;

		public AtomicCollection()
		{
			_collection = new ArrayList();
		}

		public AtomicCollection(ICollection c)
		{
			_collection = new ArrayList(c);
		}

		public int Count
		{
			get
			{
				lock(myLock)
				{
					return _collection.Count;
				}
			}
		}

		public bool IsReadOnly
		{
			get
			{
				return false;
			}
		}

		public int Add(TValue v)
		{
			lock(myLock)
			{
				return _collection.Add(v);
			}
		}

		public void Clear()
		{
			lock(myLock)
			{
				_collection.Clear();
			}
		}

		public bool Contains(TValue v)
		{
			lock(myLock)
			{
				return _collection.Contains(v);
			}
		}

		public void CopyTo(TValue[] a, int index)
		{
			lock(myLock)
			{
				_collection.CopyTo(a, index);
			}
		}

		public void Remove(TValue v)
		{
			lock(myLock)
			{
				_collection.Remove(v);
			}
		}

		public void RemoveAt(int index)
		{
			lock(myLock)
			{
				_collection.RemoveAt(index);
			}
		}

		public TValue this[int index]
		{
			get
			{
				TValue ret;
				lock(myLock)
				{
					ret = (TValue) _collection[index];
				}
				return (TValue) ret;
			}
			set
			{
				lock(myLock)
				{
					_collection[index] = value;
				}
			}
		}

		public IEnumerator GetEnumerator()
		{
			lock(myLock)
			{
				return _collection.GetEnumerator();
			}
		}

#if !NETCF
		public IEnumerator GetEnumerator(int index, int count)
		{
			lock(myLock)
			{
				return _collection.GetEnumerator(index, count);
			}
		}
#endif
	}

	public class AtomicDictionary<TKey, TValue>
		where TKey : class
		where TValue : class
	{
		private Object myLock = new Object();
		private Dictionary<TKey, TValue> _dictionary = new Dictionary<TKey, TValue>();

		public void Clear()
		{
			_dictionary.Clear();
		}

		public TValue this[TKey key]
		{
			get
			{
				TValue ret;
				lock(myLock)
				{
					ret = _dictionary[key];
				}
				return ret;
			}
			set
			{
				lock(myLock)
				{
					_dictionary[key] = value;
				}
			}
		}

		public AtomicCollection<TKey> Keys
		{
			get
			{
				lock(myLock)
				{
					return new AtomicCollection<TKey>(_dictionary.Keys);
				}
			}
		}

		public AtomicCollection<TValue> Values
		{
			get
			{
				lock(myLock)
				{
					return new AtomicCollection<TValue>(_dictionary.Values);
				}
			}
		}

		public void Add(TKey k, TValue v)
		{
			lock(myLock)
			{
				_dictionary.Add(k, v);
			}
		}

		public bool Remove(TKey v)
		{
			lock(myLock)
			{
				return _dictionary.Remove(v);
			}
		}
	}
}
