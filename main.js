"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __commonJS = (cb, mod2) => function __require() {
  return mod2 || (0, cb[__getOwnPropNames(cb)[0]])((mod2 = { exports: {} }).exports, mod2), mod2.exports;
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod2, isNodeMode, target) => (target = mod2 != null ? __create(__getProtoOf(mod2)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod2 || !mod2.__esModule ? __defProp(target, "default", { value: mod2, enumerable: true }) : target,
  mod2
));
var __toCommonJS = (mod2) => __copyProps(__defProp({}, "__esModule", { value: true }), mod2);

// node_modules/dataloader/index.js
var require_dataloader = __commonJS({
  "node_modules/dataloader/index.js"(exports, module2) {
    "use strict";
    var DataLoader3 = /* @__PURE__ */ function() {
      function DataLoader4(batchLoadFn, options) {
        if (typeof batchLoadFn !== "function") {
          throw new TypeError("DataLoader must be constructed with a function which accepts " + ("Array<key> and returns Promise<Array<value>>, but got: " + batchLoadFn + "."));
        }
        this._batchLoadFn = batchLoadFn;
        this._maxBatchSize = getValidMaxBatchSize2(options);
        this._batchScheduleFn = getValidBatchScheduleFn2(options);
        this._cacheKeyFn = getValidCacheKeyFn2(options);
        this._cacheMap = getValidCacheMap2(options);
        this._batch = null;
        this.name = getValidName2(options);
      }
      var _proto = DataLoader4.prototype;
      _proto.load = function load(key) {
        if (key === null || key === void 0) {
          throw new TypeError("The loader.load() function must be called with a value, " + ("but got: " + String(key) + "."));
        }
        var batch = getCurrentBatch2(this);
        var cacheMap2 = this._cacheMap;
        var cacheKey;
        if (cacheMap2) {
          cacheKey = this._cacheKeyFn(key);
          var cachedPromise = cacheMap2.get(cacheKey);
          if (cachedPromise) {
            var cacheHits = batch.cacheHits || (batch.cacheHits = []);
            return new Promise(function(resolve) {
              cacheHits.push(function() {
                resolve(cachedPromise);
              });
            });
          }
        }
        batch.keys.push(key);
        var promise = new Promise(function(resolve, reject) {
          batch.callbacks.push({
            resolve,
            reject
          });
        });
        if (cacheMap2) {
          cacheMap2.set(cacheKey, promise);
        }
        return promise;
      };
      _proto.loadMany = function loadMany(keys) {
        if (!isArrayLike2(keys)) {
          throw new TypeError("The loader.loadMany() function must be called with Array<key> " + ("but got: " + keys + "."));
        }
        var loadPromises = [];
        for (var i = 0; i < keys.length; i++) {
          loadPromises.push(this.load(keys[i])["catch"](function(error) {
            return error;
          }));
        }
        return Promise.all(loadPromises);
      };
      _proto.clear = function clear(key) {
        var cacheMap2 = this._cacheMap;
        if (cacheMap2) {
          var cacheKey = this._cacheKeyFn(key);
          cacheMap2["delete"](cacheKey);
        }
        return this;
      };
      _proto.clearAll = function clearAll() {
        var cacheMap2 = this._cacheMap;
        if (cacheMap2) {
          cacheMap2.clear();
        }
        return this;
      };
      _proto.prime = function prime(key, value) {
        var cacheMap2 = this._cacheMap;
        if (cacheMap2) {
          var cacheKey = this._cacheKeyFn(key);
          if (cacheMap2.get(cacheKey) === void 0) {
            var promise;
            if (value instanceof Error) {
              promise = Promise.reject(value);
              promise["catch"](function() {
              });
            } else {
              promise = Promise.resolve(value);
            }
            cacheMap2.set(cacheKey, promise);
          }
        }
        return this;
      };
      return DataLoader4;
    }();
    var enqueuePostPromiseJob2 = typeof process === "object" && typeof process.nextTick === "function" ? function(fn) {
      if (!resolvedPromise2) {
        resolvedPromise2 = Promise.resolve();
      }
      resolvedPromise2.then(function() {
        process.nextTick(fn);
      });
    } : typeof setImmediate === "function" ? function(fn) {
      setImmediate(fn);
    } : function(fn) {
      setTimeout(fn);
    };
    var resolvedPromise2;
    function getCurrentBatch2(loader) {
      var existingBatch = loader._batch;
      if (existingBatch !== null && !existingBatch.hasDispatched && existingBatch.keys.length < loader._maxBatchSize) {
        return existingBatch;
      }
      var newBatch = {
        hasDispatched: false,
        keys: [],
        callbacks: []
      };
      loader._batch = newBatch;
      loader._batchScheduleFn(function() {
        dispatchBatch2(loader, newBatch);
      });
      return newBatch;
    }
    function dispatchBatch2(loader, batch) {
      batch.hasDispatched = true;
      if (batch.keys.length === 0) {
        resolveCacheHits2(batch);
        return;
      }
      var batchPromise;
      try {
        batchPromise = loader._batchLoadFn(batch.keys);
      } catch (e) {
        return failedDispatch2(loader, batch, new TypeError("DataLoader must be constructed with a function which accepts Array<key> and returns Promise<Array<value>>, but the function " + ("errored synchronously: " + String(e) + ".")));
      }
      if (!batchPromise || typeof batchPromise.then !== "function") {
        return failedDispatch2(loader, batch, new TypeError("DataLoader must be constructed with a function which accepts Array<key> and returns Promise<Array<value>>, but the function did " + ("not return a Promise: " + String(batchPromise) + ".")));
      }
      batchPromise.then(function(values) {
        if (!isArrayLike2(values)) {
          throw new TypeError("DataLoader must be constructed with a function which accepts Array<key> and returns Promise<Array<value>>, but the function did " + ("not return a Promise of an Array: " + String(values) + "."));
        }
        if (values.length !== batch.keys.length) {
          throw new TypeError("DataLoader must be constructed with a function which accepts Array<key> and returns Promise<Array<value>>, but the function did not return a Promise of an Array of the same length as the Array of keys." + ("\n\nKeys:\n" + String(batch.keys)) + ("\n\nValues:\n" + String(values)));
        }
        resolveCacheHits2(batch);
        for (var i = 0; i < batch.callbacks.length; i++) {
          var _value = values[i];
          if (_value instanceof Error) {
            batch.callbacks[i].reject(_value);
          } else {
            batch.callbacks[i].resolve(_value);
          }
        }
      })["catch"](function(error) {
        failedDispatch2(loader, batch, error);
      });
    }
    function failedDispatch2(loader, batch, error) {
      resolveCacheHits2(batch);
      for (var i = 0; i < batch.keys.length; i++) {
        loader.clear(batch.keys[i]);
        batch.callbacks[i].reject(error);
      }
    }
    function resolveCacheHits2(batch) {
      if (batch.cacheHits) {
        for (var i = 0; i < batch.cacheHits.length; i++) {
          batch.cacheHits[i]();
        }
      }
    }
    function getValidMaxBatchSize2(options) {
      var shouldBatch = !options || options.batch !== false;
      if (!shouldBatch) {
        return 1;
      }
      var maxBatchSize = options && options.maxBatchSize;
      if (maxBatchSize === void 0) {
        return Infinity;
      }
      if (typeof maxBatchSize !== "number" || maxBatchSize < 1) {
        throw new TypeError("maxBatchSize must be a positive number: " + maxBatchSize);
      }
      return maxBatchSize;
    }
    function getValidBatchScheduleFn2(options) {
      var batchScheduleFn = options && options.batchScheduleFn;
      if (batchScheduleFn === void 0) {
        return enqueuePostPromiseJob2;
      }
      if (typeof batchScheduleFn !== "function") {
        throw new TypeError("batchScheduleFn must be a function: " + batchScheduleFn);
      }
      return batchScheduleFn;
    }
    function getValidCacheKeyFn2(options) {
      var cacheKeyFn = options && options.cacheKeyFn;
      if (cacheKeyFn === void 0) {
        return function(key) {
          return key;
        };
      }
      if (typeof cacheKeyFn !== "function") {
        throw new TypeError("cacheKeyFn must be a function: " + cacheKeyFn);
      }
      return cacheKeyFn;
    }
    function getValidCacheMap2(options) {
      var shouldCache = !options || options.cache !== false;
      if (!shouldCache) {
        return null;
      }
      var cacheMap2 = options && options.cacheMap;
      if (cacheMap2 === void 0) {
        return /* @__PURE__ */ new Map();
      }
      if (cacheMap2 !== null) {
        var cacheFunctions = ["get", "set", "delete", "clear"];
        var missingFunctions = cacheFunctions.filter(function(fnName) {
          return cacheMap2 && typeof cacheMap2[fnName] !== "function";
        });
        if (missingFunctions.length !== 0) {
          throw new TypeError("Custom cacheMap missing methods: " + missingFunctions.join(", "));
        }
      }
      return cacheMap2;
    }
    function getValidName2(options) {
      if (options && options.name) {
        return options.name;
      }
      return null;
    }
    function isArrayLike2(x) {
      return typeof x === "object" && x !== null && typeof x.length === "number" && (x.length === 0 || x.length > 0 && Object.prototype.hasOwnProperty.call(x, x.length - 1));
    }
    module2.exports = DataLoader3;
  }
});

// main.ts
var main_exports = {};
__export(main_exports, {
  default: () => WalrusStoragePlugin
});
module.exports = __toCommonJS(main_exports);
var import_obsidian = require("obsidian");

// node_modules/@scure/base/lib/esm/index.js
function isBytes(a) {
  return a instanceof Uint8Array || ArrayBuffer.isView(a) && a.constructor.name === "Uint8Array";
}
function isArrayOf(isString, arr) {
  if (!Array.isArray(arr))
    return false;
  if (arr.length === 0)
    return true;
  if (isString) {
    return arr.every((item) => typeof item === "string");
  } else {
    return arr.every((item) => Number.isSafeInteger(item));
  }
}
function afn(input) {
  if (typeof input !== "function")
    throw new Error("function expected");
  return true;
}
function astr(label, input) {
  if (typeof input !== "string")
    throw new Error(`${label}: string expected`);
  return true;
}
function anumber(n) {
  if (!Number.isSafeInteger(n))
    throw new Error(`invalid integer: ${n}`);
}
function aArr(input) {
  if (!Array.isArray(input))
    throw new Error("array expected");
}
function astrArr(label, input) {
  if (!isArrayOf(true, input))
    throw new Error(`${label}: array of strings expected`);
}
function anumArr(label, input) {
  if (!isArrayOf(false, input))
    throw new Error(`${label}: array of numbers expected`);
}
// @__NO_SIDE_EFFECTS__
function chain(...args) {
  const id = (a) => a;
  const wrap2 = (a, b) => (c) => a(b(c));
  const encode = args.map((x) => x.encode).reduceRight(wrap2, id);
  const decode = args.map((x) => x.decode).reduce(wrap2, id);
  return { encode, decode };
}
// @__NO_SIDE_EFFECTS__
function alphabet(letters) {
  const lettersA = typeof letters === "string" ? letters.split("") : letters;
  const len = lettersA.length;
  astrArr("alphabet", lettersA);
  const indexes = new Map(lettersA.map((l, i) => [l, i]));
  return {
    encode: (digits) => {
      aArr(digits);
      return digits.map((i) => {
        if (!Number.isSafeInteger(i) || i < 0 || i >= len)
          throw new Error(`alphabet.encode: digit index outside alphabet "${i}". Allowed: ${letters}`);
        return lettersA[i];
      });
    },
    decode: (input) => {
      aArr(input);
      return input.map((letter) => {
        astr("alphabet.decode", letter);
        const i = indexes.get(letter);
        if (i === void 0)
          throw new Error(`Unknown letter: "${letter}". Allowed: ${letters}`);
        return i;
      });
    }
  };
}
// @__NO_SIDE_EFFECTS__
function join(separator = "") {
  astr("join", separator);
  return {
    encode: (from) => {
      astrArr("join.decode", from);
      return from.join(separator);
    },
    decode: (to) => {
      astr("join.decode", to);
      return to.split(separator);
    }
  };
}
function convertRadix(data, from, to) {
  if (from < 2)
    throw new Error(`convertRadix: invalid from=${from}, base cannot be less than 2`);
  if (to < 2)
    throw new Error(`convertRadix: invalid to=${to}, base cannot be less than 2`);
  aArr(data);
  if (!data.length)
    return [];
  let pos = 0;
  const res = [];
  const digits = Array.from(data, (d) => {
    anumber(d);
    if (d < 0 || d >= from)
      throw new Error(`invalid integer: ${d}`);
    return d;
  });
  const dlen = digits.length;
  while (true) {
    let carry = 0;
    let done = true;
    for (let i = pos; i < dlen; i++) {
      const digit = digits[i];
      const fromCarry = from * carry;
      const digitBase = fromCarry + digit;
      if (!Number.isSafeInteger(digitBase) || fromCarry / from !== carry || digitBase - digit !== fromCarry) {
        throw new Error("convertRadix: carry overflow");
      }
      const div = digitBase / to;
      carry = digitBase % to;
      const rounded = Math.floor(div);
      digits[i] = rounded;
      if (!Number.isSafeInteger(rounded) || rounded * to + carry !== digitBase)
        throw new Error("convertRadix: carry overflow");
      if (!done)
        continue;
      else if (!rounded)
        pos = i;
      else
        done = false;
    }
    res.push(carry);
    if (done)
      break;
  }
  for (let i = 0; i < data.length - 1 && data[i] === 0; i++)
    res.push(0);
  return res.reverse();
}
var gcd = (a, b) => b === 0 ? a : gcd(b, a % b);
var radix2carry = /* @__NO_SIDE_EFFECTS__ */ (from, to) => from + (to - gcd(from, to));
var powers = /* @__PURE__ */ (() => {
  let res = [];
  for (let i = 0; i < 40; i++)
    res.push(2 ** i);
  return res;
})();
function convertRadix2(data, from, to, padding) {
  aArr(data);
  if (from <= 0 || from > 32)
    throw new Error(`convertRadix2: wrong from=${from}`);
  if (to <= 0 || to > 32)
    throw new Error(`convertRadix2: wrong to=${to}`);
  if (/* @__PURE__ */ radix2carry(from, to) > 32) {
    throw new Error(`convertRadix2: carry overflow from=${from} to=${to} carryBits=${/* @__PURE__ */ radix2carry(from, to)}`);
  }
  let carry = 0;
  let pos = 0;
  const max = powers[from];
  const mask = powers[to] - 1;
  const res = [];
  for (const n of data) {
    anumber(n);
    if (n >= max)
      throw new Error(`convertRadix2: invalid data word=${n} from=${from}`);
    carry = carry << from | n;
    if (pos + from > 32)
      throw new Error(`convertRadix2: carry overflow pos=${pos} from=${from}`);
    pos += from;
    for (; pos >= to; pos -= to)
      res.push((carry >> pos - to & mask) >>> 0);
    const pow = powers[pos];
    if (pow === void 0)
      throw new Error("invalid carry");
    carry &= pow - 1;
  }
  carry = carry << to - pos & mask;
  if (!padding && pos >= from)
    throw new Error("Excess padding");
  if (!padding && carry > 0)
    throw new Error(`Non-zero padding: ${carry}`);
  if (padding && pos > 0)
    res.push(carry >>> 0);
  return res;
}
// @__NO_SIDE_EFFECTS__
function radix(num) {
  anumber(num);
  const _256 = 2 ** 8;
  return {
    encode: (bytes) => {
      if (!isBytes(bytes))
        throw new Error("radix.encode input should be Uint8Array");
      return convertRadix(Array.from(bytes), _256, num);
    },
    decode: (digits) => {
      anumArr("radix.decode", digits);
      return Uint8Array.from(convertRadix(digits, num, _256));
    }
  };
}
// @__NO_SIDE_EFFECTS__
function radix2(bits, revPadding = false) {
  anumber(bits);
  if (bits <= 0 || bits > 32)
    throw new Error("radix2: bits should be in (0..32]");
  if (/* @__PURE__ */ radix2carry(8, bits) > 32 || /* @__PURE__ */ radix2carry(bits, 8) > 32)
    throw new Error("radix2: carry overflow");
  return {
    encode: (bytes) => {
      if (!isBytes(bytes))
        throw new Error("radix2.encode input should be Uint8Array");
      return convertRadix2(Array.from(bytes), 8, bits, !revPadding);
    },
    decode: (digits) => {
      anumArr("radix2.decode", digits);
      return Uint8Array.from(convertRadix2(digits, bits, 8, revPadding));
    }
  };
}
function unsafeWrapper(fn) {
  afn(fn);
  return function(...args) {
    try {
      return fn.apply(null, args);
    } catch (e) {
    }
  };
}
function checksum(len, fn) {
  anumber(len);
  afn(fn);
  return {
    encode(data) {
      if (!isBytes(data))
        throw new Error("checksum.encode: input should be Uint8Array");
      const sum = fn(data).slice(0, len);
      const res = new Uint8Array(data.length + len);
      res.set(data);
      res.set(sum, data.length);
      return res;
    },
    decode(data) {
      if (!isBytes(data))
        throw new Error("checksum.decode: input should be Uint8Array");
      const payload = data.slice(0, -len);
      const oldChecksum = data.slice(-len);
      const newChecksum = fn(payload).slice(0, len);
      for (let i = 0; i < len; i++)
        if (newChecksum[i] !== oldChecksum[i])
          throw new Error("Invalid checksum");
      return payload;
    }
  };
}
var genBase58 = /* @__NO_SIDE_EFFECTS__ */ (abc) => /* @__PURE__ */ chain(/* @__PURE__ */ radix(58), /* @__PURE__ */ alphabet(abc), /* @__PURE__ */ join(""));
var base58 = /* @__PURE__ */ genBase58("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz");
var createBase58check = (sha2563) => /* @__PURE__ */ chain(checksum(4, (data) => sha2563(sha2563(data))), base58);
var BECH_ALPHABET = /* @__PURE__ */ chain(/* @__PURE__ */ alphabet("qpzry9x8gf2tvdw0s3jn54khce6mua7l"), /* @__PURE__ */ join(""));
var POLYMOD_GENERATORS = [996825010, 642813549, 513874426, 1027748829, 705979059];
function bech32Polymod(pre) {
  const b = pre >> 25;
  let chk = (pre & 33554431) << 5;
  for (let i = 0; i < POLYMOD_GENERATORS.length; i++) {
    if ((b >> i & 1) === 1)
      chk ^= POLYMOD_GENERATORS[i];
  }
  return chk;
}
function bechChecksum(prefix, words, encodingConst = 1) {
  const len = prefix.length;
  let chk = 1;
  for (let i = 0; i < len; i++) {
    const c = prefix.charCodeAt(i);
    if (c < 33 || c > 126)
      throw new Error(`Invalid prefix (${prefix})`);
    chk = bech32Polymod(chk) ^ c >> 5;
  }
  chk = bech32Polymod(chk);
  for (let i = 0; i < len; i++)
    chk = bech32Polymod(chk) ^ prefix.charCodeAt(i) & 31;
  for (let v of words)
    chk = bech32Polymod(chk) ^ v;
  for (let i = 0; i < 6; i++)
    chk = bech32Polymod(chk);
  chk ^= encodingConst;
  return BECH_ALPHABET.encode(convertRadix2([chk % powers[30]], 30, 5, false));
}
// @__NO_SIDE_EFFECTS__
function genBech32(encoding) {
  const ENCODING_CONST = encoding === "bech32" ? 1 : 734539939;
  const _words = /* @__PURE__ */ radix2(5);
  const fromWords = _words.decode;
  const toWords = _words.encode;
  const fromWordsUnsafe = unsafeWrapper(fromWords);
  function encode(prefix, words, limit = 90) {
    astr("bech32.encode prefix", prefix);
    if (isBytes(words))
      words = Array.from(words);
    anumArr("bech32.encode", words);
    const plen = prefix.length;
    if (plen === 0)
      throw new TypeError(`Invalid prefix length ${plen}`);
    const actualLength = plen + 7 + words.length;
    if (limit !== false && actualLength > limit)
      throw new TypeError(`Length ${actualLength} exceeds limit ${limit}`);
    const lowered = prefix.toLowerCase();
    const sum = bechChecksum(lowered, words, ENCODING_CONST);
    return `${lowered}1${BECH_ALPHABET.encode(words)}${sum}`;
  }
  function decode(str, limit = 90) {
    astr("bech32.decode input", str);
    const slen = str.length;
    if (slen < 8 || limit !== false && slen > limit)
      throw new TypeError(`invalid string length: ${slen} (${str}). Expected (8..${limit})`);
    const lowered = str.toLowerCase();
    if (str !== lowered && str !== str.toUpperCase())
      throw new Error(`String must be lowercase or uppercase`);
    const sepIndex = lowered.lastIndexOf("1");
    if (sepIndex === 0 || sepIndex === -1)
      throw new Error(`Letter "1" must be present between prefix and data only`);
    const prefix = lowered.slice(0, sepIndex);
    const data = lowered.slice(sepIndex + 1);
    if (data.length < 6)
      throw new Error("Data must be at least 6 characters long");
    const words = BECH_ALPHABET.decode(data).slice(0, -6);
    const sum = bechChecksum(prefix, words, ENCODING_CONST);
    if (!data.endsWith(sum))
      throw new Error(`Invalid checksum in ${str}: expected "${sum}"`);
    return { prefix, words };
  }
  const decodeUnsafe = unsafeWrapper(decode);
  function decodeToBytes(str) {
    const { prefix, words } = decode(str, false);
    return { prefix, words, bytes: fromWords(words) };
  }
  function encodeFromBytes(prefix, bytes) {
    return encode(prefix, toWords(bytes));
  }
  return {
    encode,
    decode,
    encodeFromBytes,
    decodeToBytes,
    decodeUnsafe,
    fromWords,
    fromWordsUnsafe,
    toWords
  };
}
var bech32 = /* @__PURE__ */ genBech32("bech32");

// node_modules/@mysten/utils/dist/esm/b58.js
var toBase58 = (buffer) => base58.encode(buffer);
var fromBase58 = (str) => base58.decode(str);

// node_modules/@mysten/utils/dist/esm/b64.js
function fromBase64(base64String2) {
  return Uint8Array.from(atob(base64String2), (char) => char.charCodeAt(0));
}
var CHUNK_SIZE = 8192;
function toBase64(bytes) {
  if (bytes.length < CHUNK_SIZE) {
    return btoa(String.fromCharCode(...bytes));
  }
  let output = "";
  for (var i = 0; i < bytes.length; i += CHUNK_SIZE) {
    const chunk2 = bytes.slice(i, i + CHUNK_SIZE);
    output += String.fromCharCode(...chunk2);
  }
  return btoa(output);
}

// node_modules/@mysten/utils/dist/esm/hex.js
function fromHex(hexStr) {
  const normalized = hexStr.startsWith("0x") ? hexStr.slice(2) : hexStr;
  const padded = normalized.length % 2 === 0 ? normalized : `0${normalized}`;
  const intArr = padded.match(/[0-9a-fA-F]{2}/g)?.map((byte) => parseInt(byte, 16)) ?? [];
  if (intArr.length !== padded.length / 2) {
    throw new Error(`Invalid hex string ${hexStr}`);
  }
  return Uint8Array.from(intArr);
}
function toHex(bytes) {
  return bytes.reduce((str, byte) => str + byte.toString(16).padStart(2, "0"), "");
}

// node_modules/@mysten/utils/dist/esm/chunk.js
function chunk(array2, size) {
  return Array.from({ length: Math.ceil(array2.length / size) }, (_, i) => {
    return array2.slice(i * size, (i + 1) * size);
  });
}

// node_modules/@mysten/utils/dist/esm/dataloader.js
var DataLoader = class {
  constructor(batchLoadFn, options) {
    if (typeof batchLoadFn !== "function") {
      throw new TypeError(
        `DataLoader must be constructed with a function which accepts Array<key> and returns Promise<Array<value>>, but got: ${batchLoadFn}.`
      );
    }
    this._batchLoadFn = batchLoadFn;
    this._maxBatchSize = getValidMaxBatchSize(options);
    this._batchScheduleFn = getValidBatchScheduleFn(options);
    this._cacheKeyFn = getValidCacheKeyFn(options);
    this._cacheMap = getValidCacheMap(options);
    this._batch = null;
    this.name = getValidName(options);
  }
  /**
   * Loads a key, returning a `Promise` for the value represented by that key.
   */
  load(key) {
    if (key === null || key === void 0) {
      throw new TypeError(
        `The loader.load() function must be called with a value, but got: ${String(key)}.`
      );
    }
    const batch = getCurrentBatch(this);
    const cacheMap2 = this._cacheMap;
    let cacheKey;
    if (cacheMap2) {
      cacheKey = this._cacheKeyFn(key);
      const cachedPromise = cacheMap2.get(cacheKey);
      if (cachedPromise) {
        const cacheHits = batch.cacheHits || (batch.cacheHits = []);
        return new Promise((resolve) => {
          cacheHits.push(() => {
            resolve(cachedPromise);
          });
        });
      }
    }
    batch.keys.push(key);
    const promise = new Promise((resolve, reject) => {
      batch.callbacks.push({ resolve, reject });
    });
    if (cacheMap2) {
      cacheMap2.set(cacheKey, promise);
    }
    return promise;
  }
  /**
   * Loads multiple keys, promising an array of values:
   *
   *     var [ a, b ] = await myLoader.loadMany([ 'a', 'b' ]);
   *
   * This is similar to the more verbose:
   *
   *     var [ a, b ] = await Promise.all([
   *       myLoader.load('a'),
   *       myLoader.load('b')
   *     ]);
   *
   * However it is different in the case where any load fails. Where
   * Promise.all() would reject, loadMany() always resolves, however each result
   * is either a value or an Error instance.
   *
   *     var [ a, b, c ] = await myLoader.loadMany([ 'a', 'b', 'badkey' ]);
   *     // c instanceof Error
   *
   */
  loadMany(keys) {
    if (!isArrayLike(keys)) {
      throw new TypeError(
        `The loader.loadMany() function must be called with Array<key>, but got: ${keys}.`
      );
    }
    const loadPromises = [];
    for (let i = 0; i < keys.length; i++) {
      loadPromises.push(this.load(keys[i]).catch((error) => error));
    }
    return Promise.all(loadPromises);
  }
  /**
   * Clears the value at `key` from the cache, if it exists. Returns itself for
   * method chaining.
   */
  clear(key) {
    const cacheMap2 = this._cacheMap;
    if (cacheMap2) {
      const cacheKey = this._cacheKeyFn(key);
      cacheMap2.delete(cacheKey);
    }
    return this;
  }
  /**
   * Clears the entire cache. To be used when some event results in unknown
   * invalidations across this particular `DataLoader`. Returns itself for
   * method chaining.
   */
  clearAll() {
    const cacheMap2 = this._cacheMap;
    if (cacheMap2) {
      cacheMap2.clear();
    }
    return this;
  }
  /**
   * Adds the provided key and value to the cache. If the key already
   * exists, no change is made. Returns itself for method chaining.
   *
   * To prime the cache with an error at a key, provide an Error instance.
   */
  prime(key, value) {
    const cacheMap2 = this._cacheMap;
    if (cacheMap2) {
      const cacheKey = this._cacheKeyFn(key);
      if (cacheMap2.get(cacheKey) === void 0) {
        let promise;
        if (value instanceof Error) {
          promise = Promise.reject(value);
          promise.catch(() => {
          });
        } else {
          promise = Promise.resolve(value);
        }
        cacheMap2.set(cacheKey, promise);
      }
    }
    return this;
  }
};
var enqueuePostPromiseJob = (
  /** @ts-ignore */
  typeof process === "object" && typeof process.nextTick === "function" ? function(fn) {
    if (!resolvedPromise) {
      resolvedPromise = Promise.resolve();
    }
    resolvedPromise.then(() => {
      process.nextTick(fn);
    });
  } : (
    // @ts-ignore
    typeof setImmediate === "function" ? function(fn) {
      setImmediate(fn);
    } : function(fn) {
      setTimeout(fn);
    }
  )
);
var resolvedPromise;
function getCurrentBatch(loader) {
  const existingBatch = loader._batch;
  if (existingBatch !== null && !existingBatch.hasDispatched && existingBatch.keys.length < loader._maxBatchSize) {
    return existingBatch;
  }
  const newBatch = { hasDispatched: false, keys: [], callbacks: [] };
  loader._batch = newBatch;
  loader._batchScheduleFn(() => {
    dispatchBatch(loader, newBatch);
  });
  return newBatch;
}
function dispatchBatch(loader, batch) {
  batch.hasDispatched = true;
  if (batch.keys.length === 0) {
    resolveCacheHits(batch);
    return;
  }
  let batchPromise;
  try {
    batchPromise = loader._batchLoadFn(batch.keys);
  } catch (e) {
    return failedDispatch(
      loader,
      batch,
      new TypeError(
        `DataLoader must be constructed with a function which accepts Array<key> and returns Promise<Array<value>>, but the function errored synchronously: ${String(e)}.`
      )
    );
  }
  if (!batchPromise || typeof batchPromise.then !== "function") {
    return failedDispatch(
      loader,
      batch,
      new TypeError(
        `DataLoader must be constructed with a function which accepts Array<key> and returns Promise<Array<value>>, but the function did not return a Promise: ${String(batchPromise)}.`
      )
    );
  }
  Promise.resolve(batchPromise).then((values) => {
    if (!isArrayLike(values)) {
      throw new TypeError(
        `DataLoader must be constructed with a function which accepts Array<key> and returns Promise<Array<value>>, but the function did not return a Promise of an Array: ${String(values)}.`
      );
    }
    if (values.length !== batch.keys.length) {
      throw new TypeError(
        `DataLoader must be constructed with a function which accepts Array<key> and returns Promise<Array<value>>, but the function did not return a Promise of an Array of the same length as the Array of keys.

Keys:
${String(batch.keys)}

Values:
${String(values)}`
      );
    }
    resolveCacheHits(batch);
    for (let i = 0; i < batch.callbacks.length; i++) {
      const value = values[i];
      if (value instanceof Error) {
        batch.callbacks[i].reject(value);
      } else {
        batch.callbacks[i].resolve(value);
      }
    }
  }).catch((error) => {
    failedDispatch(loader, batch, error);
  });
}
function failedDispatch(loader, batch, error) {
  resolveCacheHits(batch);
  for (let i = 0; i < batch.keys.length; i++) {
    loader.clear(batch.keys[i]);
    batch.callbacks[i].reject(error);
  }
}
function resolveCacheHits(batch) {
  if (batch.cacheHits) {
    for (let i = 0; i < batch.cacheHits.length; i++) {
      batch.cacheHits[i]();
    }
  }
}
function getValidMaxBatchSize(options) {
  const shouldBatch = !options || options.batch !== false;
  if (!shouldBatch) {
    return 1;
  }
  const maxBatchSize = options && options.maxBatchSize;
  if (maxBatchSize === void 0) {
    return Infinity;
  }
  if (typeof maxBatchSize !== "number" || maxBatchSize < 1) {
    throw new TypeError(`maxBatchSize must be a positive number: ${maxBatchSize}`);
  }
  return maxBatchSize;
}
function getValidBatchScheduleFn(options) {
  const batchScheduleFn = options && options.batchScheduleFn;
  if (batchScheduleFn === void 0) {
    return enqueuePostPromiseJob;
  }
  if (typeof batchScheduleFn !== "function") {
    throw new TypeError(`batchScheduleFn must be a function: ${batchScheduleFn}`);
  }
  return batchScheduleFn;
}
function getValidCacheKeyFn(options) {
  const cacheKeyFn = options && options.cacheKeyFn;
  if (cacheKeyFn === void 0) {
    return (key) => key;
  }
  if (typeof cacheKeyFn !== "function") {
    throw new TypeError(`cacheKeyFn must be a function: ${cacheKeyFn}`);
  }
  return cacheKeyFn;
}
function getValidCacheMap(options) {
  const shouldCache = !options || options.cache !== false;
  if (!shouldCache) {
    return null;
  }
  const cacheMap2 = options && options.cacheMap;
  if (cacheMap2 === void 0) {
    return /* @__PURE__ */ new Map();
  }
  if (cacheMap2 !== null) {
    const cacheFunctions = ["get", "set", "delete", "clear"];
    const missingFunctions = cacheFunctions.filter(
      (fnName) => cacheMap2 && typeof cacheMap2[fnName] !== "function"
    );
    if (missingFunctions.length !== 0) {
      throw new TypeError("Custom cacheMap missing methods: " + missingFunctions.join(", "));
    }
  }
  return cacheMap2;
}
function getValidName(options) {
  if (options && options.name) {
    return options.name;
  }
  return null;
}
function isArrayLike(x) {
  return typeof x === "object" && x !== null && "length" in x && typeof x.length === "number" && (x.length === 0 || x.length > 0 && Object.prototype.hasOwnProperty.call(x, x.length - 1));
}

// node_modules/@mysten/bcs/dist/esm/uleb.js
function ulebEncode(num) {
  let bigNum = BigInt(num);
  const arr = [];
  let len = 0;
  if (bigNum === 0n) {
    return [0];
  }
  while (bigNum > 0) {
    arr[len] = Number(bigNum & 0x7fn);
    bigNum >>= 7n;
    if (bigNum > 0n) {
      arr[len] |= 128;
    }
    len += 1;
  }
  return arr;
}
function ulebDecode(arr) {
  let total = 0n;
  let shift = 0n;
  let len = 0;
  while (true) {
    if (len >= arr.length) {
      throw new Error("ULEB decode error: buffer overflow");
    }
    const byte = arr[len];
    len += 1;
    total += BigInt(byte & 127) << shift;
    if ((byte & 128) === 0) {
      break;
    }
    shift += 7n;
  }
  if (total > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error("ULEB decode error: value exceeds MAX_SAFE_INTEGER");
  }
  return {
    value: Number(total),
    length: len
  };
}

// node_modules/@mysten/bcs/dist/esm/reader.js
var BcsReader = class {
  /**
   * @param {Uint8Array} data Data to use as a buffer.
   */
  constructor(data) {
    this.bytePosition = 0;
    this.dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
  }
  /**
   * Shift current cursor position by `bytes`.
   *
   * @param {Number} bytes Number of bytes to
   * @returns {this} Self for possible chaining.
   */
  shift(bytes) {
    this.bytePosition += bytes;
    return this;
  }
  /**
   * Read U8 value from the buffer and shift cursor by 1.
   * @returns
   */
  read8() {
    const value = this.dataView.getUint8(this.bytePosition);
    this.shift(1);
    return value;
  }
  /**
   * Read U16 value from the buffer and shift cursor by 2.
   * @returns
   */
  read16() {
    const value = this.dataView.getUint16(this.bytePosition, true);
    this.shift(2);
    return value;
  }
  /**
   * Read U32 value from the buffer and shift cursor by 4.
   * @returns
   */
  read32() {
    const value = this.dataView.getUint32(this.bytePosition, true);
    this.shift(4);
    return value;
  }
  /**
   * Read U64 value from the buffer and shift cursor by 8.
   * @returns
   */
  read64() {
    const value1 = this.read32();
    const value2 = this.read32();
    const result = value2.toString(16) + value1.toString(16).padStart(8, "0");
    return BigInt("0x" + result).toString(10);
  }
  /**
   * Read U128 value from the buffer and shift cursor by 16.
   */
  read128() {
    const value1 = BigInt(this.read64());
    const value2 = BigInt(this.read64());
    const result = value2.toString(16) + value1.toString(16).padStart(16, "0");
    return BigInt("0x" + result).toString(10);
  }
  /**
   * Read U128 value from the buffer and shift cursor by 32.
   * @returns
   */
  read256() {
    const value1 = BigInt(this.read128());
    const value2 = BigInt(this.read128());
    const result = value2.toString(16) + value1.toString(16).padStart(32, "0");
    return BigInt("0x" + result).toString(10);
  }
  /**
   * Read `num` number of bytes from the buffer and shift cursor by `num`.
   * @param num Number of bytes to read.
   */
  readBytes(num) {
    const start = this.bytePosition + this.dataView.byteOffset;
    const value = new Uint8Array(this.dataView.buffer, start, num);
    this.shift(num);
    return value;
  }
  /**
   * Read ULEB value - an integer of varying size. Used for enum indexes and
   * vector lengths.
   * @returns {Number} The ULEB value.
   */
  readULEB() {
    const start = this.bytePosition + this.dataView.byteOffset;
    const buffer = new Uint8Array(this.dataView.buffer, start);
    const { value, length } = ulebDecode(buffer);
    this.shift(length);
    return value;
  }
  /**
   * Read a BCS vector: read a length and then apply function `cb` X times
   * where X is the length of the vector, defined as ULEB in BCS bytes.
   * @param cb Callback to process elements of vector.
   * @returns {Array<Any>} Array of the resulting values, returned by callback.
   */
  readVec(cb) {
    const length = this.readULEB();
    const result = [];
    for (let i = 0; i < length; i++) {
      result.push(cb(this, i, length));
    }
    return result;
  }
};

// node_modules/@mysten/bcs/dist/esm/utils.js
function encodeStr(data, encoding) {
  switch (encoding) {
    case "base58":
      return toBase58(data);
    case "base64":
      return toBase64(data);
    case "hex":
      return toHex(data);
    default:
      throw new Error("Unsupported encoding, supported values are: base64, hex");
  }
}
function splitGenericParameters(str, genericSeparators = ["<", ">"]) {
  const [left, right] = genericSeparators;
  const tok = [];
  let word = "";
  let nestedAngleBrackets = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str[i];
    if (char === left) {
      nestedAngleBrackets++;
    }
    if (char === right) {
      nestedAngleBrackets--;
    }
    if (nestedAngleBrackets === 0 && char === ",") {
      tok.push(word.trim());
      word = "";
      continue;
    }
    word += char;
  }
  tok.push(word.trim());
  return tok;
}

// node_modules/@mysten/bcs/dist/esm/writer.js
var BcsWriter = class {
  constructor({
    initialSize = 1024,
    maxSize = Infinity,
    allocateSize = 1024
  } = {}) {
    this.bytePosition = 0;
    this.size = initialSize;
    this.maxSize = maxSize;
    this.allocateSize = allocateSize;
    this.dataView = new DataView(new ArrayBuffer(initialSize));
  }
  ensureSizeOrGrow(bytes) {
    const requiredSize = this.bytePosition + bytes;
    if (requiredSize > this.size) {
      const nextSize = Math.min(
        this.maxSize,
        Math.max(this.size + requiredSize, this.size + this.allocateSize)
      );
      if (requiredSize > nextSize) {
        throw new Error(
          `Attempting to serialize to BCS, but buffer does not have enough size. Allocated size: ${this.size}, Max size: ${this.maxSize}, Required size: ${requiredSize}`
        );
      }
      this.size = nextSize;
      const nextBuffer = new ArrayBuffer(this.size);
      new Uint8Array(nextBuffer).set(new Uint8Array(this.dataView.buffer));
      this.dataView = new DataView(nextBuffer);
    }
  }
  /**
   * Shift current cursor position by `bytes`.
   *
   * @param {Number} bytes Number of bytes to
   * @returns {this} Self for possible chaining.
   */
  shift(bytes) {
    this.bytePosition += bytes;
    return this;
  }
  /**
   * Write a U8 value into a buffer and shift cursor position by 1.
   * @param {Number} value Value to write.
   * @returns {this}
   */
  write8(value) {
    this.ensureSizeOrGrow(1);
    this.dataView.setUint8(this.bytePosition, Number(value));
    return this.shift(1);
  }
  /**
   * Write a U8 value into a buffer and shift cursor position by 1.
   * @param {Number} value Value to write.
   * @returns {this}
   */
  writeBytes(bytes) {
    this.ensureSizeOrGrow(bytes.length);
    for (let i = 0; i < bytes.length; i++) {
      this.dataView.setUint8(this.bytePosition + i, bytes[i]);
    }
    return this.shift(bytes.length);
  }
  /**
   * Write a U16 value into a buffer and shift cursor position by 2.
   * @param {Number} value Value to write.
   * @returns {this}
   */
  write16(value) {
    this.ensureSizeOrGrow(2);
    this.dataView.setUint16(this.bytePosition, Number(value), true);
    return this.shift(2);
  }
  /**
   * Write a U32 value into a buffer and shift cursor position by 4.
   * @param {Number} value Value to write.
   * @returns {this}
   */
  write32(value) {
    this.ensureSizeOrGrow(4);
    this.dataView.setUint32(this.bytePosition, Number(value), true);
    return this.shift(4);
  }
  /**
   * Write a U64 value into a buffer and shift cursor position by 8.
   * @param {bigint} value Value to write.
   * @returns {this}
   */
  write64(value) {
    toLittleEndian(BigInt(value), 8).forEach((el) => this.write8(el));
    return this;
  }
  /**
   * Write a U128 value into a buffer and shift cursor position by 16.
   *
   * @param {bigint} value Value to write.
   * @returns {this}
   */
  write128(value) {
    toLittleEndian(BigInt(value), 16).forEach((el) => this.write8(el));
    return this;
  }
  /**
   * Write a U256 value into a buffer and shift cursor position by 16.
   *
   * @param {bigint} value Value to write.
   * @returns {this}
   */
  write256(value) {
    toLittleEndian(BigInt(value), 32).forEach((el) => this.write8(el));
    return this;
  }
  /**
   * Write a ULEB value into a buffer and shift cursor position by number of bytes
   * written.
   * @param {Number} value Value to write.
   * @returns {this}
   */
  writeULEB(value) {
    ulebEncode(value).forEach((el) => this.write8(el));
    return this;
  }
  /**
   * Write a vector into a buffer by first writing the vector length and then calling
   * a callback on each passed value.
   *
   * @param {Array<Any>} vector Array of elements to write.
   * @param {WriteVecCb} cb Callback to call on each element of the vector.
   * @returns {this}
   */
  writeVec(vector2, cb) {
    this.writeULEB(vector2.length);
    Array.from(vector2).forEach((el, i) => cb(this, el, i, vector2.length));
    return this;
  }
  /**
   * Adds support for iterations over the object.
   * @returns {Uint8Array}
   */
  // oxlint-disable-next-line require-yields
  *[Symbol.iterator]() {
    for (let i = 0; i < this.bytePosition; i++) {
      yield this.dataView.getUint8(i);
    }
    return this.toBytes();
  }
  /**
   * Get underlying buffer taking only value bytes (in case initial buffer size was bigger).
   * @returns {Uint8Array} Resulting bcs.
   */
  toBytes() {
    return new Uint8Array(this.dataView.buffer.slice(0, this.bytePosition));
  }
  /**
   * Represent data as 'hex' or 'base64'
   * @param encoding Encoding to use: 'base64' or 'hex'
   */
  toString(encoding) {
    return encodeStr(this.toBytes(), encoding);
  }
};
function toLittleEndian(bigint2, size) {
  const result = new Uint8Array(size);
  let i = 0;
  while (bigint2 > 0) {
    result[i] = Number(bigint2 % BigInt(256));
    bigint2 = bigint2 / BigInt(256);
    i += 1;
  }
  return result;
}

// node_modules/@mysten/bcs/dist/esm/bcs-type.js
var __typeError = (msg) => {
  throw TypeError(msg);
};
var __accessCheck = (obj, member, msg) => member.has(obj) || __typeError("Cannot " + msg);
var __privateGet = (obj, member, getter) => (__accessCheck(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd = (obj, member, value) => member.has(obj) ? __typeError("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet = (obj, member, value, setter) => (__accessCheck(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var _write;
var _serialize;
var _schema;
var _bytes;
var _BcsType = class _BcsType2 {
  constructor(options) {
    __privateAdd(this, _write);
    __privateAdd(this, _serialize);
    this.name = options.name;
    this.read = options.read;
    this.serializedSize = options.serializedSize ?? (() => null);
    __privateSet(this, _write, options.write);
    __privateSet(this, _serialize, options.serialize ?? ((value, options2) => {
      const writer = new BcsWriter({
        initialSize: this.serializedSize(value) ?? void 0,
        ...options2
      });
      __privateGet(this, _write).call(this, value, writer);
      return writer.toBytes();
    }));
    this.validate = options.validate ?? (() => {
    });
  }
  write(value, writer) {
    this.validate(value);
    __privateGet(this, _write).call(this, value, writer);
  }
  serialize(value, options) {
    this.validate(value);
    return new SerializedBcs(this, __privateGet(this, _serialize).call(this, value, options));
  }
  parse(bytes) {
    const reader = new BcsReader(bytes);
    return this.read(reader);
  }
  fromHex(hex) {
    return this.parse(fromHex(hex));
  }
  fromBase58(b64) {
    return this.parse(fromBase58(b64));
  }
  fromBase64(b64) {
    return this.parse(fromBase64(b64));
  }
  transform({
    name,
    input,
    output,
    validate: validate2
  }) {
    return new _BcsType2({
      name: name ?? this.name,
      read: (reader) => output ? output(this.read(reader)) : this.read(reader),
      write: (value, writer) => __privateGet(this, _write).call(this, input ? input(value) : value, writer),
      serializedSize: (value) => this.serializedSize(input ? input(value) : value),
      serialize: (value, options) => __privateGet(this, _serialize).call(this, input ? input(value) : value, options),
      validate: (value) => {
        validate2?.(value);
        this.validate(input ? input(value) : value);
      }
    });
  }
};
_write = /* @__PURE__ */ new WeakMap();
_serialize = /* @__PURE__ */ new WeakMap();
var BcsType = _BcsType;
var SERIALIZED_BCS_BRAND = Symbol.for("@mysten/serialized-bcs");
function isSerializedBcs(obj) {
  return !!obj && typeof obj === "object" && obj[SERIALIZED_BCS_BRAND] === true;
}
var SerializedBcs = class {
  constructor(schema, bytes) {
    __privateAdd(this, _schema);
    __privateAdd(this, _bytes);
    __privateSet(this, _schema, schema);
    __privateSet(this, _bytes, bytes);
  }
  // Used to brand SerializedBcs so that they can be identified, even between multiple copies
  // of the @mysten/bcs package are installed
  get [SERIALIZED_BCS_BRAND]() {
    return true;
  }
  toBytes() {
    return __privateGet(this, _bytes);
  }
  toHex() {
    return toHex(__privateGet(this, _bytes));
  }
  toBase64() {
    return toBase64(__privateGet(this, _bytes));
  }
  toBase58() {
    return toBase58(__privateGet(this, _bytes));
  }
  parse() {
    return __privateGet(this, _schema).parse(__privateGet(this, _bytes));
  }
};
_schema = /* @__PURE__ */ new WeakMap();
_bytes = /* @__PURE__ */ new WeakMap();
function fixedSizeBcsType({
  size,
  ...options
}) {
  return new BcsType({
    ...options,
    serializedSize: () => size
  });
}
function uIntBcsType({
  readMethod,
  writeMethod,
  ...options
}) {
  return fixedSizeBcsType({
    ...options,
    read: (reader) => reader[readMethod](),
    write: (value, writer) => writer[writeMethod](value),
    validate: (value) => {
      if (value < 0 || value > options.maxValue) {
        throw new TypeError(
          `Invalid ${options.name} value: ${value}. Expected value in range 0-${options.maxValue}`
        );
      }
      options.validate?.(value);
    }
  });
}
function bigUIntBcsType({
  readMethod,
  writeMethod,
  ...options
}) {
  return fixedSizeBcsType({
    ...options,
    read: (reader) => reader[readMethod](),
    write: (value, writer) => writer[writeMethod](BigInt(value)),
    validate: (val) => {
      const value = BigInt(val);
      if (value < 0 || value > options.maxValue) {
        throw new TypeError(
          `Invalid ${options.name} value: ${value}. Expected value in range 0-${options.maxValue}`
        );
      }
      options.validate?.(value);
    }
  });
}
function dynamicSizeBcsType({
  serialize,
  ...options
}) {
  const type = new BcsType({
    ...options,
    serialize,
    write: (value, writer) => {
      for (const byte of type.serialize(value).toBytes()) {
        writer.write8(byte);
      }
    }
  });
  return type;
}
function stringLikeBcsType({
  toBytes: toBytes2,
  fromBytes,
  ...options
}) {
  return new BcsType({
    ...options,
    read: (reader) => {
      const length = reader.readULEB();
      const bytes = reader.readBytes(length);
      return fromBytes(bytes);
    },
    write: (hex, writer) => {
      const bytes = toBytes2(hex);
      writer.writeULEB(bytes.length);
      for (let i = 0; i < bytes.length; i++) {
        writer.write8(bytes[i]);
      }
    },
    serialize: (value) => {
      const bytes = toBytes2(value);
      const size = ulebEncode(bytes.length);
      const result = new Uint8Array(size.length + bytes.length);
      result.set(size, 0);
      result.set(bytes, size.length);
      return result;
    },
    validate: (value) => {
      if (typeof value !== "string") {
        throw new TypeError(`Invalid ${options.name} value: ${value}. Expected string`);
      }
      options.validate?.(value);
    }
  });
}
function lazyBcsType(cb) {
  let lazyType = null;
  function getType() {
    if (!lazyType) {
      lazyType = cb();
    }
    return lazyType;
  }
  return new BcsType({
    name: "lazy",
    read: (data) => getType().read(data),
    serializedSize: (value) => getType().serializedSize(value),
    write: (value, writer) => getType().write(value, writer),
    serialize: (value, options) => getType().serialize(value, options).toBytes()
  });
}
var BcsStruct = class extends BcsType {
  constructor({ name, fields, ...options }) {
    const canonicalOrder = Object.entries(fields);
    super({
      name,
      serializedSize: (values) => {
        let total = 0;
        for (const [field, type] of canonicalOrder) {
          const size = type.serializedSize(values[field]);
          if (size == null) {
            return null;
          }
          total += size;
        }
        return total;
      },
      read: (reader) => {
        const result = {};
        for (const [field, type] of canonicalOrder) {
          result[field] = type.read(reader);
        }
        return result;
      },
      write: (value, writer) => {
        for (const [field, type] of canonicalOrder) {
          type.write(value[field], writer);
        }
      },
      ...options,
      validate: (value) => {
        options?.validate?.(value);
        if (typeof value !== "object" || value == null) {
          throw new TypeError(`Expected object, found ${typeof value}`);
        }
      }
    });
  }
};
var BcsEnum = class extends BcsType {
  constructor({ fields, ...options }) {
    const canonicalOrder = Object.entries(fields);
    super({
      read: (reader) => {
        const index = reader.readULEB();
        const enumEntry = canonicalOrder[index];
        if (!enumEntry) {
          throw new TypeError(`Unknown value ${index} for enum ${options.name}`);
        }
        const [kind, type] = enumEntry;
        return {
          [kind]: type?.read(reader) ?? true,
          $kind: kind
        };
      },
      write: (value, writer) => {
        const [name, val] = Object.entries(value).filter(
          ([name2]) => Object.hasOwn(fields, name2)
        )[0];
        for (let i = 0; i < canonicalOrder.length; i++) {
          const [optionName, optionType] = canonicalOrder[i];
          if (optionName === name) {
            writer.writeULEB(i);
            optionType?.write(val, writer);
            return;
          }
        }
      },
      ...options,
      validate: (value) => {
        options?.validate?.(value);
        if (typeof value !== "object" || value == null) {
          throw new TypeError(`Expected object, found ${typeof value}`);
        }
        const keys = Object.keys(value).filter(
          (k) => value[k] !== void 0 && Object.hasOwn(fields, k)
        );
        if (keys.length !== 1) {
          throw new TypeError(
            `Expected object with one key, but found ${keys.length} for type ${options.name}}`
          );
        }
        const [variant] = keys;
        if (!Object.hasOwn(fields, variant)) {
          throw new TypeError(`Invalid enum variant ${variant}`);
        }
      }
    });
  }
};
var BcsTuple = class extends BcsType {
  constructor({ fields, name, ...options }) {
    super({
      name: name ?? `(${fields.map((t) => t.name).join(", ")})`,
      serializedSize: (values) => {
        let total = 0;
        for (let i = 0; i < fields.length; i++) {
          const size = fields[i].serializedSize(values[i]);
          if (size == null) {
            return null;
          }
          total += size;
        }
        return total;
      },
      read: (reader) => {
        const result = [];
        for (const field of fields) {
          result.push(field.read(reader));
        }
        return result;
      },
      write: (value, writer) => {
        for (let i = 0; i < fields.length; i++) {
          fields[i].write(value[i], writer);
        }
      },
      ...options,
      validate: (value) => {
        options?.validate?.(value);
        if (!Array.isArray(value)) {
          throw new TypeError(`Expected array, found ${typeof value}`);
        }
        if (value.length !== fields.length) {
          throw new TypeError(`Expected array of length ${fields.length}, found ${value.length}`);
        }
      }
    });
  }
};

// node_modules/@mysten/bcs/dist/esm/bcs.js
function fixedArray(size, type, options) {
  return new BcsType({
    read: (reader) => {
      const result = new Array(size);
      for (let i = 0; i < size; i++) {
        result[i] = type.read(reader);
      }
      return result;
    },
    write: (value, writer) => {
      for (const item of value) {
        type.write(item, writer);
      }
    },
    ...options,
    name: options?.name ?? `${type.name}[${size}]`,
    validate: (value) => {
      options?.validate?.(value);
      if (!value || typeof value !== "object" || !("length" in value)) {
        throw new TypeError(`Expected array, found ${typeof value}`);
      }
      if (value.length !== size) {
        throw new TypeError(`Expected array of length ${size}, found ${value.length}`);
      }
    }
  });
}
function option(type) {
  return bcs.enum(`Option<${type.name}>`, {
    None: null,
    Some: type
  }).transform({
    input: (value) => {
      if (value == null) {
        return { None: true };
      }
      return { Some: value };
    },
    output: (value) => {
      if (value.$kind === "Some") {
        return value.Some;
      }
      return null;
    }
  });
}
function vector(type, options) {
  return new BcsType({
    read: (reader) => {
      const length = reader.readULEB();
      const result = new Array(length);
      for (let i = 0; i < length; i++) {
        result[i] = type.read(reader);
      }
      return result;
    },
    write: (value, writer) => {
      writer.writeULEB(value.length);
      for (const item of value) {
        type.write(item, writer);
      }
    },
    ...options,
    name: options?.name ?? `vector<${type.name}>`,
    validate: (value) => {
      options?.validate?.(value);
      if (!value || typeof value !== "object" || !("length" in value)) {
        throw new TypeError(`Expected array, found ${typeof value}`);
      }
    }
  });
}
function map(keyType, valueType) {
  return bcs.vector(bcs.tuple([keyType, valueType])).transform({
    name: `Map<${keyType.name}, ${valueType.name}>`,
    input: (value) => {
      return [...value.entries()];
    },
    output: (value) => {
      const result = /* @__PURE__ */ new Map();
      for (const [key, val] of value) {
        result.set(key, val);
      }
      return result;
    }
  });
}
var bcs = {
  /**
   * Creates a BcsType that can be used to read and write an 8-bit unsigned integer.
   * @example
   * bcs.u8().serialize(255).toBytes() // Uint8Array [ 255 ]
   */
  u8(options) {
    return uIntBcsType({
      readMethod: "read8",
      writeMethod: "write8",
      size: 1,
      maxValue: 2 ** 8 - 1,
      ...options,
      name: options?.name ?? "u8"
    });
  },
  /**
   * Creates a BcsType that can be used to read and write a 16-bit unsigned integer.
   * @example
   * bcs.u16().serialize(65535).toBytes() // Uint8Array [ 255, 255 ]
   */
  u16(options) {
    return uIntBcsType({
      readMethod: "read16",
      writeMethod: "write16",
      size: 2,
      maxValue: 2 ** 16 - 1,
      ...options,
      name: options?.name ?? "u16"
    });
  },
  /**
   * Creates a BcsType that can be used to read and write a 32-bit unsigned integer.
   * @example
   * bcs.u32().serialize(4294967295).toBytes() // Uint8Array [ 255, 255, 255, 255 ]
   */
  u32(options) {
    return uIntBcsType({
      readMethod: "read32",
      writeMethod: "write32",
      size: 4,
      maxValue: 2 ** 32 - 1,
      ...options,
      name: options?.name ?? "u32"
    });
  },
  /**
   * Creates a BcsType that can be used to read and write a 64-bit unsigned integer.
   * @example
   * bcs.u64().serialize(1).toBytes() // Uint8Array [ 1, 0, 0, 0, 0, 0, 0, 0 ]
   */
  u64(options) {
    return bigUIntBcsType({
      readMethod: "read64",
      writeMethod: "write64",
      size: 8,
      maxValue: 2n ** 64n - 1n,
      ...options,
      name: options?.name ?? "u64"
    });
  },
  /**
   * Creates a BcsType that can be used to read and write a 128-bit unsigned integer.
   * @example
   * bcs.u128().serialize(1).toBytes() // Uint8Array [ 1, ..., 0 ]
   */
  u128(options) {
    return bigUIntBcsType({
      readMethod: "read128",
      writeMethod: "write128",
      size: 16,
      maxValue: 2n ** 128n - 1n,
      ...options,
      name: options?.name ?? "u128"
    });
  },
  /**
   * Creates a BcsType that can be used to read and write a 256-bit unsigned integer.
   * @example
   * bcs.u256().serialize(1).toBytes() // Uint8Array [ 1, ..., 0 ]
   */
  u256(options) {
    return bigUIntBcsType({
      readMethod: "read256",
      writeMethod: "write256",
      size: 32,
      maxValue: 2n ** 256n - 1n,
      ...options,
      name: options?.name ?? "u256"
    });
  },
  /**
   * Creates a BcsType that can be used to read and write boolean values.
   * @example
   * bcs.bool().serialize(true).toBytes() // Uint8Array [ 1 ]
   */
  bool(options) {
    return fixedSizeBcsType({
      size: 1,
      read: (reader) => reader.read8() === 1,
      write: (value, writer) => writer.write8(value ? 1 : 0),
      ...options,
      name: options?.name ?? "bool",
      validate: (value) => {
        options?.validate?.(value);
        if (typeof value !== "boolean") {
          throw new TypeError(`Expected boolean, found ${typeof value}`);
        }
      }
    });
  },
  /**
   * Creates a BcsType that can be used to read and write unsigned LEB encoded integers
   * @example
   *
   */
  uleb128(options) {
    return dynamicSizeBcsType({
      read: (reader) => reader.readULEB(),
      serialize: (value) => {
        return Uint8Array.from(ulebEncode(value));
      },
      ...options,
      name: options?.name ?? "uleb128"
    });
  },
  /**
   * Creates a BcsType representing a fixed length byte array
   * @param size The number of bytes this types represents
   * @example
   * bcs.bytes(3).serialize(new Uint8Array([1, 2, 3])).toBytes() // Uint8Array [1, 2, 3]
   */
  bytes(size, options) {
    return fixedSizeBcsType({
      size,
      read: (reader) => reader.readBytes(size),
      write: (value, writer) => {
        writer.writeBytes(new Uint8Array(value));
      },
      ...options,
      name: options?.name ?? `bytes[${size}]`,
      validate: (value) => {
        options?.validate?.(value);
        if (!value || typeof value !== "object" || !("length" in value)) {
          throw new TypeError(`Expected array, found ${typeof value}`);
        }
        if (value.length !== size) {
          throw new TypeError(`Expected array of length ${size}, found ${value.length}`);
        }
      }
    });
  },
  /**
   * Creates a BcsType representing a variable length byte array
   *
   * @example
   * bcs.byteVector().serialize([1, 2, 3]).toBytes() // Uint8Array [3, 1, 2, 3]
   */
  byteVector(options) {
    return new BcsType({
      read: (reader) => {
        const length = reader.readULEB();
        return reader.readBytes(length);
      },
      write: (value, writer) => {
        const array2 = new Uint8Array(value);
        writer.writeULEB(array2.length);
        writer.writeBytes(array2);
      },
      ...options,
      name: options?.name ?? "vector<u8>",
      serializedSize: (value) => {
        const length = "length" in value ? value.length : null;
        return length == null ? null : ulebEncode(length).length + length;
      },
      validate: (value) => {
        options?.validate?.(value);
        if (!value || typeof value !== "object" || !("length" in value)) {
          throw new TypeError(`Expected array, found ${typeof value}`);
        }
      }
    });
  },
  /**
   * Creates a BcsType that can ser/de string values.  Strings will be UTF-8 encoded
   * @example
   * bcs.string().serialize('a').toBytes() // Uint8Array [ 1, 97 ]
   */
  string(options) {
    return stringLikeBcsType({
      toBytes: (value) => new TextEncoder().encode(value),
      fromBytes: (bytes) => new TextDecoder().decode(bytes),
      ...options,
      name: options?.name ?? "string"
    });
  },
  /**
   * Creates a BcsType that represents a fixed length array of a given type
   * @param size The number of elements in the array
   * @param type The BcsType of each element in the array
   * @example
   * bcs.fixedArray(3, bcs.u8()).serialize([1, 2, 3]).toBytes() // Uint8Array [ 1, 2, 3 ]
   */
  fixedArray,
  /**
   * Creates a BcsType representing an optional value
   * @param type The BcsType of the optional value
   * @example
   * bcs.option(bcs.u8()).serialize(null).toBytes() // Uint8Array [ 0 ]
   * bcs.option(bcs.u8()).serialize(1).toBytes() // Uint8Array [ 1, 1 ]
   */
  option,
  /**
   * Creates a BcsType representing a variable length vector of a given type
   * @param type The BcsType of each element in the vector
   *
   * @example
   * bcs.vector(bcs.u8()).toBytes([1, 2, 3]) // Uint8Array [ 3, 1, 2, 3 ]
   */
  vector,
  /**
   * Creates a BcsType representing a tuple of a given set of types
   * @param types The BcsTypes for each element in the tuple
   *
   * @example
   * const tuple = bcs.tuple([bcs.u8(), bcs.string(), bcs.bool()])
   * tuple.serialize([1, 'a', true]).toBytes() // Uint8Array [ 1, 1, 97, 1 ]
   */
  tuple(fields, options) {
    return new BcsTuple({
      fields,
      ...options
    });
  },
  /**
   * Creates a BcsType representing a struct of a given set of fields
   * @param name The name of the struct
   * @param fields The fields of the struct. The order of the fields affects how data is serialized and deserialized
   *
   * @example
   * const struct = bcs.struct('MyStruct', {
   *  a: bcs.u8(),
   *  b: bcs.string(),
   * })
   * struct.serialize({ a: 1, b: 'a' }).toBytes() // Uint8Array [ 1, 1, 97 ]
   */
  struct(name, fields, options) {
    return new BcsStruct({
      name,
      fields,
      ...options
    });
  },
  /**
   * Creates a BcsType representing an enum of a given set of options
   * @param name The name of the enum
   * @param values The values of the enum. The order of the values affects how data is serialized and deserialized.
   * null can be used to represent a variant with no data.
   *
   * @example
   * const enum = bcs.enum('MyEnum', {
   *   A: bcs.u8(),
   *   B: bcs.string(),
   *   C: null,
   * })
   * enum.serialize({ A: 1 }).toBytes() // Uint8Array [ 0, 1 ]
   * enum.serialize({ B: 'a' }).toBytes() // Uint8Array [ 1, 1, 97 ]
   * enum.serialize({ C: true }).toBytes() // Uint8Array [ 2 ]
   */
  enum(name, fields, options) {
    return new BcsEnum({
      name,
      fields,
      ...options
    });
  },
  /**
   * Creates a BcsType representing a map of a given key and value type
   * @param keyType The BcsType of the key
   * @param valueType The BcsType of the value
   * @example
   * const map = bcs.map(bcs.u8(), bcs.string())
   * map.serialize(new Map([[2, 'a']])).toBytes() // Uint8Array [ 1, 2, 1, 97 ]
   */
  map,
  /**
   * Creates a BcsType that wraps another BcsType which is lazily evaluated. This is useful for creating recursive types.
   * @param cb A callback that returns the BcsType
   */
  lazy(cb) {
    return lazyBcsType(cb);
  }
};

// node_modules/@mysten/sui/dist/esm/utils/suins.js
var SUI_NS_NAME_REGEX = /^(?!.*(^(?!@)|[-.@])($|[-.@]))(?:[a-z0-9-]{0,63}(?:\.[a-z0-9-]{0,63})*)?@[a-z0-9-]{0,63}$/i;
var SUI_NS_DOMAIN_REGEX = /^(?!.*(^|[-.])($|[-.]))(?:[a-z0-9-]{0,63}\.)+sui$/i;
var MAX_SUI_NS_NAME_LENGTH = 235;
function isValidSuiNSName(name) {
  if (name.length > MAX_SUI_NS_NAME_LENGTH) {
    return false;
  }
  if (name.includes("@")) {
    return SUI_NS_NAME_REGEX.test(name);
  }
  return SUI_NS_DOMAIN_REGEX.test(name);
}
function normalizeSuiNSName(name, format = "at") {
  const lowerCase = name.toLowerCase();
  let parts;
  if (lowerCase.includes("@")) {
    if (!SUI_NS_NAME_REGEX.test(lowerCase)) {
      throw new Error(`Invalid SuiNS name ${name}`);
    }
    const [labels, domain] = lowerCase.split("@");
    parts = [...labels ? labels.split(".") : [], domain];
  } else {
    if (!SUI_NS_DOMAIN_REGEX.test(lowerCase)) {
      throw new Error(`Invalid SuiNS name ${name}`);
    }
    parts = lowerCase.split(".").slice(0, -1);
  }
  if (format === "dot") {
    return `${parts.join(".")}.sui`;
  }
  return `${parts.slice(0, -1).join(".")}@${parts[parts.length - 1]}`;
}

// node_modules/@mysten/sui/dist/esm/utils/move-registry.js
var NAME_PATTERN = /^([a-z0-9]+(?:-[a-z0-9]+)*)$/;
var VERSION_REGEX = /^\d+$/;
var MAX_APP_SIZE = 64;
var NAME_SEPARATOR = "/";
var isValidNamedPackage = (name) => {
  const parts = name.split(NAME_SEPARATOR);
  if (parts.length < 2 || parts.length > 3) return false;
  const [org, app, version] = parts;
  if (version !== void 0 && !VERSION_REGEX.test(version)) return false;
  if (!isValidSuiNSName(org)) return false;
  return NAME_PATTERN.test(app) && app.length < MAX_APP_SIZE;
};
var isValidNamedType = (type) => {
  const splitType = type.split(/::|<|>|,/);
  for (const t of splitType) {
    if (t.includes(NAME_SEPARATOR) && !isValidNamedPackage(t)) return false;
  }
  return true;
};

// node_modules/@mysten/sui/dist/esm/utils/sui-types.js
var TX_DIGEST_LENGTH = 32;
function isValidTransactionDigest(value) {
  try {
    const buffer = fromBase58(value);
    return buffer.length === TX_DIGEST_LENGTH;
  } catch {
    return false;
  }
}
var SUI_ADDRESS_LENGTH = 32;
function isValidSuiAddress(value) {
  return isHex(value) && getHexByteLength(value) === SUI_ADDRESS_LENGTH;
}
function isValidSuiObjectId(value) {
  return isValidSuiAddress(value);
}
function parseTypeTag(type) {
  if (!type.includes("::")) return type;
  return parseStructTag(type);
}
function parseStructTag(type) {
  const [address, module2] = type.split("::");
  const isMvrPackage = isValidNamedPackage(address);
  const rest = type.slice(address.length + module2.length + 4);
  const name = rest.includes("<") ? rest.slice(0, rest.indexOf("<")) : rest;
  const typeParams = rest.includes("<") ? splitGenericParameters(rest.slice(rest.indexOf("<") + 1, rest.lastIndexOf(">"))).map(
    (typeParam) => parseTypeTag(typeParam.trim())
  ) : [];
  return {
    address: isMvrPackage ? address : normalizeSuiAddress(address),
    module: module2,
    name,
    typeParams
  };
}
function normalizeStructTag(type) {
  const { address, module: module2, name, typeParams } = typeof type === "string" ? parseStructTag(type) : type;
  const formattedTypeParams = typeParams?.length > 0 ? `<${typeParams.map(
    (typeParam) => typeof typeParam === "string" ? typeParam : normalizeStructTag(typeParam)
  ).join(",")}>` : "";
  return `${address}::${module2}::${name}${formattedTypeParams}`;
}
function normalizeSuiAddress(value, forceAdd0x = false) {
  let address = value.toLowerCase();
  if (!forceAdd0x && address.startsWith("0x")) {
    address = address.slice(2);
  }
  return `0x${address.padStart(SUI_ADDRESS_LENGTH * 2, "0")}`;
}
function normalizeSuiObjectId(value, forceAdd0x = false) {
  return normalizeSuiAddress(value, forceAdd0x);
}
function isHex(value) {
  return /^(0x|0X)?[a-fA-F0-9]+$/.test(value) && value.length % 2 === 0;
}
function getHexByteLength(value) {
  return /^(0x|0X)/.test(value) ? (value.length - 2) / 2 : value.length / 2;
}

// node_modules/@mysten/sui/dist/esm/bcs/type-tag-serializer.js
var VECTOR_REGEX = /^vector<(.+)>$/;
var STRUCT_REGEX = /^([^:]+)::([^:]+)::([^<]+)(<(.+)>)?/;
var TypeTagSerializer = class _TypeTagSerializer {
  static parseFromStr(str, normalizeAddress = false) {
    if (str === "address") {
      return { address: null };
    } else if (str === "bool") {
      return { bool: null };
    } else if (str === "u8") {
      return { u8: null };
    } else if (str === "u16") {
      return { u16: null };
    } else if (str === "u32") {
      return { u32: null };
    } else if (str === "u64") {
      return { u64: null };
    } else if (str === "u128") {
      return { u128: null };
    } else if (str === "u256") {
      return { u256: null };
    } else if (str === "signer") {
      return { signer: null };
    }
    const vectorMatch = str.match(VECTOR_REGEX);
    if (vectorMatch) {
      return {
        vector: _TypeTagSerializer.parseFromStr(vectorMatch[1], normalizeAddress)
      };
    }
    const structMatch = str.match(STRUCT_REGEX);
    if (structMatch) {
      const address = normalizeAddress ? normalizeSuiAddress(structMatch[1]) : structMatch[1];
      return {
        struct: {
          address,
          module: structMatch[2],
          name: structMatch[3],
          typeParams: structMatch[5] === void 0 ? [] : _TypeTagSerializer.parseStructTypeArgs(structMatch[5], normalizeAddress)
        }
      };
    }
    throw new Error(`Encountered unexpected token when parsing type args for ${str}`);
  }
  static parseStructTypeArgs(str, normalizeAddress = false) {
    return splitGenericParameters(str).map(
      (tok) => _TypeTagSerializer.parseFromStr(tok, normalizeAddress)
    );
  }
  static tagToString(tag) {
    if ("bool" in tag) {
      return "bool";
    }
    if ("u8" in tag) {
      return "u8";
    }
    if ("u16" in tag) {
      return "u16";
    }
    if ("u32" in tag) {
      return "u32";
    }
    if ("u64" in tag) {
      return "u64";
    }
    if ("u128" in tag) {
      return "u128";
    }
    if ("u256" in tag) {
      return "u256";
    }
    if ("address" in tag) {
      return "address";
    }
    if ("signer" in tag) {
      return "signer";
    }
    if ("vector" in tag) {
      return `vector<${_TypeTagSerializer.tagToString(tag.vector)}>`;
    }
    if ("struct" in tag) {
      const struct = tag.struct;
      const typeParams = struct.typeParams.map(_TypeTagSerializer.tagToString).join(", ");
      return `${struct.address}::${struct.module}::${struct.name}${typeParams ? `<${typeParams}>` : ""}`;
    }
    throw new Error("Invalid TypeTag");
  }
};

// node_modules/@mysten/sui/dist/esm/bcs/bcs.js
function unsafe_u64(options) {
  return bcs.u64({
    name: "unsafe_u64",
    ...options
  }).transform({
    input: (val) => val,
    output: (val) => Number(val)
  });
}
function optionEnum(type) {
  return bcs.enum("Option", {
    None: null,
    Some: type
  });
}
var Address = bcs.bytes(SUI_ADDRESS_LENGTH).transform({
  validate: (val) => {
    const address = typeof val === "string" ? val : toHex(val);
    if (!address || !isValidSuiAddress(normalizeSuiAddress(address))) {
      throw new Error(`Invalid Sui address ${address}`);
    }
  },
  input: (val) => typeof val === "string" ? fromHex(normalizeSuiAddress(val)) : val,
  output: (val) => normalizeSuiAddress(toHex(val))
});
var ObjectDigest = bcs.byteVector().transform({
  name: "ObjectDigest",
  input: (value) => fromBase58(value),
  output: (value) => toBase58(new Uint8Array(value)),
  validate: (value) => {
    if (fromBase58(value).length !== 32) {
      throw new Error("ObjectDigest must be 32 bytes");
    }
  }
});
var SuiObjectRef = bcs.struct("SuiObjectRef", {
  objectId: Address,
  version: bcs.u64(),
  digest: ObjectDigest
});
var SharedObjectRef = bcs.struct("SharedObjectRef", {
  objectId: Address,
  initialSharedVersion: bcs.u64(),
  mutable: bcs.bool()
});
var ObjectArg = bcs.enum("ObjectArg", {
  ImmOrOwnedObject: SuiObjectRef,
  SharedObject: SharedObjectRef,
  Receiving: SuiObjectRef
});
var Owner = bcs.enum("Owner", {
  AddressOwner: Address,
  ObjectOwner: Address,
  Shared: bcs.struct("Shared", {
    initialSharedVersion: bcs.u64()
  }),
  Immutable: null,
  ConsensusAddressOwner: bcs.struct("ConsensusAddressOwner", {
    owner: Address,
    startVersion: bcs.u64()
  })
});
var CallArg = bcs.enum("CallArg", {
  Pure: bcs.struct("Pure", {
    bytes: bcs.byteVector().transform({
      input: (val) => typeof val === "string" ? fromBase64(val) : val,
      output: (val) => toBase64(new Uint8Array(val))
    })
  }),
  Object: ObjectArg
});
var InnerTypeTag = bcs.enum("TypeTag", {
  bool: null,
  u8: null,
  u64: null,
  u128: null,
  address: null,
  signer: null,
  vector: bcs.lazy(() => InnerTypeTag),
  struct: bcs.lazy(() => StructTag),
  u16: null,
  u32: null,
  u256: null
});
var TypeTag = InnerTypeTag.transform({
  input: (typeTag) => typeof typeTag === "string" ? TypeTagSerializer.parseFromStr(typeTag, true) : typeTag,
  output: (typeTag) => TypeTagSerializer.tagToString(typeTag)
});
var Argument = bcs.enum("Argument", {
  GasCoin: null,
  Input: bcs.u16(),
  Result: bcs.u16(),
  NestedResult: bcs.tuple([bcs.u16(), bcs.u16()])
});
var ProgrammableMoveCall = bcs.struct("ProgrammableMoveCall", {
  package: Address,
  module: bcs.string(),
  function: bcs.string(),
  typeArguments: bcs.vector(TypeTag),
  arguments: bcs.vector(Argument)
});
var Command = bcs.enum("Command", {
  /**
   * A Move Call - any public Move function can be called via
   * this transaction. The results can be used that instant to pass
   * into the next transaction.
   */
  MoveCall: ProgrammableMoveCall,
  /**
   * Transfer vector of objects to a receiver.
   */
  TransferObjects: bcs.struct("TransferObjects", {
    objects: bcs.vector(Argument),
    address: Argument
  }),
  // /**
  //  * Split `amount` from a `coin`.
  //  */
  SplitCoins: bcs.struct("SplitCoins", {
    coin: Argument,
    amounts: bcs.vector(Argument)
  }),
  // /**
  //  * Merge Vector of Coins (`sources`) into a `destination`.
  //  */
  MergeCoins: bcs.struct("MergeCoins", {
    destination: Argument,
    sources: bcs.vector(Argument)
  }),
  // /**
  //  * Publish a Move module.
  //  */
  Publish: bcs.struct("Publish", {
    modules: bcs.vector(
      bcs.byteVector().transform({
        input: (val) => typeof val === "string" ? fromBase64(val) : val,
        output: (val) => toBase64(new Uint8Array(val))
      })
    ),
    dependencies: bcs.vector(Address)
  }),
  // /**
  //  * Build a vector of objects using the input arguments.
  //  * It is impossible to export construct a `vector<T: key>` otherwise,
  //  * so this call serves a utility function.
  //  */
  MakeMoveVec: bcs.struct("MakeMoveVec", {
    type: optionEnum(TypeTag).transform({
      input: (val) => val === null ? {
        None: true
      } : {
        Some: val
      },
      output: (val) => val.Some ?? null
    }),
    elements: bcs.vector(Argument)
  }),
  Upgrade: bcs.struct("Upgrade", {
    modules: bcs.vector(
      bcs.byteVector().transform({
        input: (val) => typeof val === "string" ? fromBase64(val) : val,
        output: (val) => toBase64(new Uint8Array(val))
      })
    ),
    dependencies: bcs.vector(Address),
    package: Address,
    ticket: Argument
  })
});
var ProgrammableTransaction = bcs.struct("ProgrammableTransaction", {
  inputs: bcs.vector(CallArg),
  commands: bcs.vector(Command)
});
var TransactionKind = bcs.enum("TransactionKind", {
  ProgrammableTransaction,
  ChangeEpoch: null,
  Genesis: null,
  ConsensusCommitPrologue: null
});
var TransactionExpiration = bcs.enum("TransactionExpiration", {
  None: null,
  Epoch: unsafe_u64()
});
var StructTag = bcs.struct("StructTag", {
  address: Address,
  module: bcs.string(),
  name: bcs.string(),
  typeParams: bcs.vector(InnerTypeTag)
});
var GasData = bcs.struct("GasData", {
  payment: bcs.vector(SuiObjectRef),
  owner: Address,
  price: bcs.u64(),
  budget: bcs.u64()
});
var TransactionDataV1 = bcs.struct("TransactionDataV1", {
  kind: TransactionKind,
  sender: Address,
  gasData: GasData,
  expiration: TransactionExpiration
});
var TransactionData = bcs.enum("TransactionData", {
  V1: TransactionDataV1
});
var IntentScope = bcs.enum("IntentScope", {
  TransactionData: null,
  TransactionEffects: null,
  CheckpointSummary: null,
  PersonalMessage: null
});
var IntentVersion = bcs.enum("IntentVersion", {
  V0: null
});
var AppId = bcs.enum("AppId", {
  Sui: null
});
var Intent = bcs.struct("Intent", {
  scope: IntentScope,
  version: IntentVersion,
  appId: AppId
});
function IntentMessage(T) {
  return bcs.struct(`IntentMessage<${T.name}>`, {
    intent: Intent,
    value: T
  });
}
var CompressedSignature = bcs.enum("CompressedSignature", {
  ED25519: bcs.bytes(64),
  Secp256k1: bcs.bytes(64),
  Secp256r1: bcs.bytes(64),
  ZkLogin: bcs.byteVector(),
  Passkey: bcs.byteVector()
});
var PublicKey = bcs.enum("PublicKey", {
  ED25519: bcs.bytes(32),
  Secp256k1: bcs.bytes(33),
  Secp256r1: bcs.bytes(33),
  ZkLogin: bcs.byteVector(),
  Passkey: bcs.bytes(33)
});
var MultiSigPkMap = bcs.struct("MultiSigPkMap", {
  pubKey: PublicKey,
  weight: bcs.u8()
});
var MultiSigPublicKey = bcs.struct("MultiSigPublicKey", {
  pk_map: bcs.vector(MultiSigPkMap),
  threshold: bcs.u16()
});
var MultiSig = bcs.struct("MultiSig", {
  sigs: bcs.vector(CompressedSignature),
  bitmap: bcs.u16(),
  multisig_pk: MultiSigPublicKey
});
var base64String = bcs.byteVector().transform({
  input: (val) => typeof val === "string" ? fromBase64(val) : val,
  output: (val) => toBase64(new Uint8Array(val))
});
var SenderSignedTransaction = bcs.struct("SenderSignedTransaction", {
  intentMessage: IntentMessage(TransactionData),
  txSignatures: bcs.vector(base64String)
});
var SenderSignedData = bcs.vector(SenderSignedTransaction, {
  name: "SenderSignedData"
});
var PasskeyAuthenticator = bcs.struct("PasskeyAuthenticator", {
  authenticatorData: bcs.byteVector(),
  clientDataJson: bcs.string(),
  userSignature: bcs.byteVector()
});

// node_modules/@mysten/sui/dist/esm/bcs/effects.js
var PackageUpgradeError = bcs.enum("PackageUpgradeError", {
  UnableToFetchPackage: bcs.struct("UnableToFetchPackage", { packageId: Address }),
  NotAPackage: bcs.struct("NotAPackage", { objectId: Address }),
  IncompatibleUpgrade: null,
  DigestDoesNotMatch: bcs.struct("DigestDoesNotMatch", { digest: bcs.byteVector() }),
  UnknownUpgradePolicy: bcs.struct("UnknownUpgradePolicy", { policy: bcs.u8() }),
  PackageIDDoesNotMatch: bcs.struct("PackageIDDoesNotMatch", {
    packageId: Address,
    ticketId: Address
  })
});
var ModuleId = bcs.struct("ModuleId", {
  address: Address,
  name: bcs.string()
});
var MoveLocation = bcs.struct("MoveLocation", {
  module: ModuleId,
  function: bcs.u16(),
  instruction: bcs.u16(),
  functionName: bcs.option(bcs.string())
});
var CommandArgumentError = bcs.enum("CommandArgumentError", {
  TypeMismatch: null,
  InvalidBCSBytes: null,
  InvalidUsageOfPureArg: null,
  InvalidArgumentToPrivateEntryFunction: null,
  IndexOutOfBounds: bcs.struct("IndexOutOfBounds", { idx: bcs.u16() }),
  SecondaryIndexOutOfBounds: bcs.struct("SecondaryIndexOutOfBounds", {
    resultIdx: bcs.u16(),
    secondaryIdx: bcs.u16()
  }),
  InvalidResultArity: bcs.struct("InvalidResultArity", { resultIdx: bcs.u16() }),
  InvalidGasCoinUsage: null,
  InvalidValueUsage: null,
  InvalidObjectByValue: null,
  InvalidObjectByMutRef: null,
  SharedObjectOperationNotAllowed: null
});
var TypeArgumentError = bcs.enum("TypeArgumentError", {
  TypeNotFound: null,
  ConstraintNotSatisfied: null
});
var ExecutionFailureStatus = bcs.enum("ExecutionFailureStatus", {
  InsufficientGas: null,
  InvalidGasObject: null,
  InvariantViolation: null,
  FeatureNotYetSupported: null,
  MoveObjectTooBig: bcs.struct("MoveObjectTooBig", {
    objectSize: bcs.u64(),
    maxObjectSize: bcs.u64()
  }),
  MovePackageTooBig: bcs.struct("MovePackageTooBig", {
    objectSize: bcs.u64(),
    maxObjectSize: bcs.u64()
  }),
  CircularObjectOwnership: bcs.struct("CircularObjectOwnership", { object: Address }),
  InsufficientCoinBalance: null,
  CoinBalanceOverflow: null,
  PublishErrorNonZeroAddress: null,
  SuiMoveVerificationError: null,
  MovePrimitiveRuntimeError: bcs.option(MoveLocation),
  MoveAbort: bcs.tuple([MoveLocation, bcs.u64()]),
  VMVerificationOrDeserializationError: null,
  VMInvariantViolation: null,
  FunctionNotFound: null,
  ArityMismatch: null,
  TypeArityMismatch: null,
  NonEntryFunctionInvoked: null,
  CommandArgumentError: bcs.struct("CommandArgumentError", {
    argIdx: bcs.u16(),
    kind: CommandArgumentError
  }),
  TypeArgumentError: bcs.struct("TypeArgumentError", {
    argumentIdx: bcs.u16(),
    kind: TypeArgumentError
  }),
  UnusedValueWithoutDrop: bcs.struct("UnusedValueWithoutDrop", {
    resultIdx: bcs.u16(),
    secondaryIdx: bcs.u16()
  }),
  InvalidPublicFunctionReturnType: bcs.struct("InvalidPublicFunctionReturnType", {
    idx: bcs.u16()
  }),
  InvalidTransferObject: null,
  EffectsTooLarge: bcs.struct("EffectsTooLarge", { currentSize: bcs.u64(), maxSize: bcs.u64() }),
  PublishUpgradeMissingDependency: null,
  PublishUpgradeDependencyDowngrade: null,
  PackageUpgradeError: bcs.struct("PackageUpgradeError", { upgradeError: PackageUpgradeError }),
  WrittenObjectsTooLarge: bcs.struct("WrittenObjectsTooLarge", {
    currentSize: bcs.u64(),
    maxSize: bcs.u64()
  }),
  CertificateDenied: null,
  SuiMoveVerificationTimedout: null,
  SharedObjectOperationNotAllowed: null,
  InputObjectDeleted: null,
  ExecutionCancelledDueToSharedObjectCongestion: bcs.struct(
    "ExecutionCancelledDueToSharedObjectCongestion",
    {
      congestedObjects: bcs.vector(Address)
    }
  ),
  AddressDeniedForCoin: bcs.struct("AddressDeniedForCoin", {
    address: Address,
    coinType: bcs.string()
  }),
  CoinTypeGlobalPause: bcs.struct("CoinTypeGlobalPause", { coinType: bcs.string() }),
  ExecutionCancelledDueToRandomnessUnavailable: null
});
var ExecutionStatus = bcs.enum("ExecutionStatus", {
  Success: null,
  Failed: bcs.struct("ExecutionFailed", {
    error: ExecutionFailureStatus,
    command: bcs.option(bcs.u64())
  })
});
var GasCostSummary = bcs.struct("GasCostSummary", {
  computationCost: bcs.u64(),
  storageCost: bcs.u64(),
  storageRebate: bcs.u64(),
  nonRefundableStorageFee: bcs.u64()
});
var TransactionEffectsV1 = bcs.struct("TransactionEffectsV1", {
  status: ExecutionStatus,
  executedEpoch: bcs.u64(),
  gasUsed: GasCostSummary,
  modifiedAtVersions: bcs.vector(bcs.tuple([Address, bcs.u64()])),
  sharedObjects: bcs.vector(SuiObjectRef),
  transactionDigest: ObjectDigest,
  created: bcs.vector(bcs.tuple([SuiObjectRef, Owner])),
  mutated: bcs.vector(bcs.tuple([SuiObjectRef, Owner])),
  unwrapped: bcs.vector(bcs.tuple([SuiObjectRef, Owner])),
  deleted: bcs.vector(SuiObjectRef),
  unwrappedThenDeleted: bcs.vector(SuiObjectRef),
  wrapped: bcs.vector(SuiObjectRef),
  gasObject: bcs.tuple([SuiObjectRef, Owner]),
  eventsDigest: bcs.option(ObjectDigest),
  dependencies: bcs.vector(ObjectDigest)
});
var VersionDigest = bcs.tuple([bcs.u64(), ObjectDigest]);
var ObjectIn = bcs.enum("ObjectIn", {
  NotExist: null,
  Exist: bcs.tuple([VersionDigest, Owner])
});
var ObjectOut = bcs.enum("ObjectOut", {
  NotExist: null,
  ObjectWrite: bcs.tuple([ObjectDigest, Owner]),
  PackageWrite: VersionDigest
});
var IDOperation = bcs.enum("IDOperation", {
  None: null,
  Created: null,
  Deleted: null
});
var EffectsObjectChange = bcs.struct("EffectsObjectChange", {
  inputState: ObjectIn,
  outputState: ObjectOut,
  idOperation: IDOperation
});
var UnchangedSharedKind = bcs.enum("UnchangedSharedKind", {
  ReadOnlyRoot: VersionDigest,
  // TODO: these have been renamed to MutateConsensusStreamEnded and ReadConsensusStreamEnded
  MutateDeleted: bcs.u64(),
  ReadDeleted: bcs.u64(),
  Cancelled: bcs.u64(),
  PerEpochConfig: null
});
var TransactionEffectsV2 = bcs.struct("TransactionEffectsV2", {
  status: ExecutionStatus,
  executedEpoch: bcs.u64(),
  gasUsed: GasCostSummary,
  transactionDigest: ObjectDigest,
  gasObjectIndex: bcs.option(bcs.u32()),
  eventsDigest: bcs.option(ObjectDigest),
  dependencies: bcs.vector(ObjectDigest),
  lamportVersion: bcs.u64(),
  changedObjects: bcs.vector(bcs.tuple([Address, EffectsObjectChange])),
  unchangedSharedObjects: bcs.vector(bcs.tuple([Address, UnchangedSharedKind])),
  auxDataDigest: bcs.option(ObjectDigest)
});
var TransactionEffects = bcs.enum("TransactionEffects", {
  V1: TransactionEffectsV1,
  V2: TransactionEffectsV2
});

// node_modules/@mysten/sui/dist/esm/bcs/pure.js
function pureBcsSchemaFromTypeName(name) {
  switch (name) {
    case "u8":
      return bcs.u8();
    case "u16":
      return bcs.u16();
    case "u32":
      return bcs.u32();
    case "u64":
      return bcs.u64();
    case "u128":
      return bcs.u128();
    case "u256":
      return bcs.u256();
    case "bool":
      return bcs.bool();
    case "string":
      return bcs.string();
    case "id":
    case "address":
      return Address;
  }
  const generic = name.match(/^(vector|option)<(.+)>$/);
  if (generic) {
    const [kind, inner] = generic.slice(1);
    if (kind === "vector") {
      return bcs.vector(pureBcsSchemaFromTypeName(inner));
    } else {
      return bcs.option(pureBcsSchemaFromTypeName(inner));
    }
  }
  throw new Error(`Invalid Pure type name: ${name}`);
}

// node_modules/@mysten/sui/dist/esm/bcs/index.js
var suiBcs = {
  ...bcs,
  U8: bcs.u8(),
  U16: bcs.u16(),
  U32: bcs.u32(),
  U64: bcs.u64(),
  U128: bcs.u128(),
  U256: bcs.u256(),
  ULEB128: bcs.uleb128(),
  Bool: bcs.bool(),
  String: bcs.string(),
  Address,
  AppId,
  Argument,
  CallArg,
  Command,
  CompressedSignature,
  GasData,
  Intent,
  IntentMessage,
  IntentScope,
  IntentVersion,
  MultiSig,
  MultiSigPkMap,
  MultiSigPublicKey,
  ObjectArg,
  ObjectDigest,
  Owner,
  PasskeyAuthenticator,
  ProgrammableMoveCall,
  ProgrammableTransaction,
  PublicKey,
  SenderSignedData,
  SenderSignedTransaction,
  SharedObjectRef,
  StructTag,
  SuiObjectRef,
  TransactionData,
  TransactionDataV1,
  TransactionEffects,
  TransactionExpiration,
  TransactionKind,
  TypeTag
};

// node_modules/@noble/hashes/esm/crypto.js
var crypto2 = typeof globalThis === "object" && "crypto" in globalThis ? globalThis.crypto : void 0;

// node_modules/@noble/hashes/esm/utils.js
function isBytes2(a) {
  return a instanceof Uint8Array || ArrayBuffer.isView(a) && a.constructor.name === "Uint8Array";
}
function anumber2(n) {
  if (!Number.isSafeInteger(n) || n < 0)
    throw new Error("positive integer expected, got " + n);
}
function abytes(b, ...lengths) {
  if (!isBytes2(b))
    throw new Error("Uint8Array expected");
  if (lengths.length > 0 && !lengths.includes(b.length))
    throw new Error("Uint8Array expected of length " + lengths + ", got length=" + b.length);
}
function ahash(h) {
  if (typeof h !== "function" || typeof h.create !== "function")
    throw new Error("Hash should be wrapped by utils.createHasher");
  anumber2(h.outputLen);
  anumber2(h.blockLen);
}
function aexists(instance, checkFinished = true) {
  if (instance.destroyed)
    throw new Error("Hash instance has been destroyed");
  if (checkFinished && instance.finished)
    throw new Error("Hash#digest() has already been called");
}
function aoutput(out, instance) {
  abytes(out);
  const min = instance.outputLen;
  if (out.length < min) {
    throw new Error("digestInto() expects output buffer of length at least " + min);
  }
}
function u32(arr) {
  return new Uint32Array(arr.buffer, arr.byteOffset, Math.floor(arr.byteLength / 4));
}
function clean(...arrays) {
  for (let i = 0; i < arrays.length; i++) {
    arrays[i].fill(0);
  }
}
function createView(arr) {
  return new DataView(arr.buffer, arr.byteOffset, arr.byteLength);
}
function rotr(word, shift) {
  return word << 32 - shift | word >>> shift;
}
function rotl(word, shift) {
  return word << shift | word >>> 32 - shift >>> 0;
}
var isLE = /* @__PURE__ */ (() => new Uint8Array(new Uint32Array([287454020]).buffer)[0] === 68)();
function byteSwap(word) {
  return word << 24 & 4278190080 | word << 8 & 16711680 | word >>> 8 & 65280 | word >>> 24 & 255;
}
var swap8IfBE = isLE ? (n) => n : (n) => byteSwap(n);
function byteSwap32(arr) {
  for (let i = 0; i < arr.length; i++) {
    arr[i] = byteSwap(arr[i]);
  }
  return arr;
}
var swap32IfBE = isLE ? (u) => u : byteSwap32;
var hasHexBuiltin = /* @__PURE__ */ (() => (
  // @ts-ignore
  typeof Uint8Array.from([]).toHex === "function" && typeof Uint8Array.fromHex === "function"
))();
var hexes = /* @__PURE__ */ Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, "0"));
function bytesToHex(bytes) {
  abytes(bytes);
  if (hasHexBuiltin)
    return bytes.toHex();
  let hex = "";
  for (let i = 0; i < bytes.length; i++) {
    hex += hexes[bytes[i]];
  }
  return hex;
}
var asciis = { _0: 48, _9: 57, A: 65, F: 70, a: 97, f: 102 };
function asciiToBase16(ch) {
  if (ch >= asciis._0 && ch <= asciis._9)
    return ch - asciis._0;
  if (ch >= asciis.A && ch <= asciis.F)
    return ch - (asciis.A - 10);
  if (ch >= asciis.a && ch <= asciis.f)
    return ch - (asciis.a - 10);
  return;
}
function hexToBytes(hex) {
  if (typeof hex !== "string")
    throw new Error("hex string expected, got " + typeof hex);
  if (hasHexBuiltin)
    return Uint8Array.fromHex(hex);
  const hl = hex.length;
  const al = hl / 2;
  if (hl % 2)
    throw new Error("hex string expected, got unpadded hex of length " + hl);
  const array2 = new Uint8Array(al);
  for (let ai = 0, hi = 0; ai < al; ai++, hi += 2) {
    const n1 = asciiToBase16(hex.charCodeAt(hi));
    const n2 = asciiToBase16(hex.charCodeAt(hi + 1));
    if (n1 === void 0 || n2 === void 0) {
      const char = hex[hi] + hex[hi + 1];
      throw new Error('hex string expected, got non-hex character "' + char + '" at index ' + hi);
    }
    array2[ai] = n1 * 16 + n2;
  }
  return array2;
}
function utf8ToBytes(str) {
  if (typeof str !== "string")
    throw new Error("string expected");
  return new Uint8Array(new TextEncoder().encode(str));
}
function toBytes(data) {
  if (typeof data === "string")
    data = utf8ToBytes(data);
  abytes(data);
  return data;
}
function kdfInputToBytes(data) {
  if (typeof data === "string")
    data = utf8ToBytes(data);
  abytes(data);
  return data;
}
function concatBytes(...arrays) {
  let sum = 0;
  for (let i = 0; i < arrays.length; i++) {
    const a = arrays[i];
    abytes(a);
    sum += a.length;
  }
  const res = new Uint8Array(sum);
  for (let i = 0, pad = 0; i < arrays.length; i++) {
    const a = arrays[i];
    res.set(a, pad);
    pad += a.length;
  }
  return res;
}
function checkOpts(defaults, opts) {
  if (opts !== void 0 && {}.toString.call(opts) !== "[object Object]")
    throw new Error("options should be object or undefined");
  const merged = Object.assign(defaults, opts);
  return merged;
}
var Hash = class {
};
function createHasher(hashCons) {
  const hashC = (msg) => hashCons().update(toBytes(msg)).digest();
  const tmp = hashCons();
  hashC.outputLen = tmp.outputLen;
  hashC.blockLen = tmp.blockLen;
  hashC.create = () => hashCons();
  return hashC;
}
function createOptHasher(hashCons) {
  const hashC = (msg, opts) => hashCons(opts).update(toBytes(msg)).digest();
  const tmp = hashCons({});
  hashC.outputLen = tmp.outputLen;
  hashC.blockLen = tmp.blockLen;
  hashC.create = (opts) => hashCons(opts);
  return hashC;
}
function randomBytes(bytesLength = 32) {
  if (crypto2 && typeof crypto2.getRandomValues === "function") {
    return crypto2.getRandomValues(new Uint8Array(bytesLength));
  }
  if (crypto2 && typeof crypto2.randomBytes === "function") {
    return Uint8Array.from(crypto2.randomBytes(bytesLength));
  }
  throw new Error("crypto.getRandomValues must be defined");
}

// node_modules/@noble/curves/esm/utils.js
var _0n = /* @__PURE__ */ BigInt(0);
var _1n = /* @__PURE__ */ BigInt(1);
function abool(title, value) {
  if (typeof value !== "boolean")
    throw new Error(title + " boolean expected, got " + value);
}
function numberToHexUnpadded(num) {
  const hex = num.toString(16);
  return hex.length & 1 ? "0" + hex : hex;
}
function hexToNumber(hex) {
  if (typeof hex !== "string")
    throw new Error("hex string expected, got " + typeof hex);
  return hex === "" ? _0n : BigInt("0x" + hex);
}
function bytesToNumberBE(bytes) {
  return hexToNumber(bytesToHex(bytes));
}
function bytesToNumberLE(bytes) {
  abytes(bytes);
  return hexToNumber(bytesToHex(Uint8Array.from(bytes).reverse()));
}
function numberToBytesBE(n, len) {
  return hexToBytes(n.toString(16).padStart(len * 2, "0"));
}
function numberToBytesLE(n, len) {
  return numberToBytesBE(n, len).reverse();
}
function ensureBytes(title, hex, expectedLength) {
  let res;
  if (typeof hex === "string") {
    try {
      res = hexToBytes(hex);
    } catch (e) {
      throw new Error(title + " must be hex string or Uint8Array, cause: " + e);
    }
  } else if (isBytes2(hex)) {
    res = Uint8Array.from(hex);
  } else {
    throw new Error(title + " must be hex string or Uint8Array");
  }
  const len = res.length;
  if (typeof expectedLength === "number" && len !== expectedLength)
    throw new Error(title + " of length " + expectedLength + " expected, got " + len);
  return res;
}
function equalBytes(a, b) {
  if (a.length !== b.length)
    return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++)
    diff |= a[i] ^ b[i];
  return diff === 0;
}
var isPosBig = (n) => typeof n === "bigint" && _0n <= n;
function inRange(n, min, max) {
  return isPosBig(n) && isPosBig(min) && isPosBig(max) && min <= n && n < max;
}
function aInRange(title, n, min, max) {
  if (!inRange(n, min, max))
    throw new Error("expected valid " + title + ": " + min + " <= n < " + max + ", got " + n);
}
function bitLen(n) {
  let len;
  for (len = 0; n > _0n; n >>= _1n, len += 1)
    ;
  return len;
}
var bitMask = (n) => (_1n << BigInt(n)) - _1n;
function createHmacDrbg(hashLen, qByteLen, hmacFn) {
  if (typeof hashLen !== "number" || hashLen < 2)
    throw new Error("hashLen must be a number");
  if (typeof qByteLen !== "number" || qByteLen < 2)
    throw new Error("qByteLen must be a number");
  if (typeof hmacFn !== "function")
    throw new Error("hmacFn must be a function");
  const u8n = (len) => new Uint8Array(len);
  const u8of = (byte) => Uint8Array.of(byte);
  let v = u8n(hashLen);
  let k = u8n(hashLen);
  let i = 0;
  const reset = () => {
    v.fill(1);
    k.fill(0);
    i = 0;
  };
  const h = (...b) => hmacFn(k, v, ...b);
  const reseed = (seed = u8n(0)) => {
    k = h(u8of(0), seed);
    v = h();
    if (seed.length === 0)
      return;
    k = h(u8of(1), seed);
    v = h();
  };
  const gen = () => {
    if (i++ >= 1e3)
      throw new Error("drbg: tried 1000 values");
    let len = 0;
    const out = [];
    while (len < qByteLen) {
      v = h();
      const sl = v.slice();
      out.push(sl);
      len += v.length;
    }
    return concatBytes(...out);
  };
  const genUntil = (seed, pred) => {
    reset();
    reseed(seed);
    let res = void 0;
    while (!(res = pred(gen())))
      reseed();
    reset();
    return res;
  };
  return genUntil;
}
function _validateObject(object2, fields, optFields = {}) {
  if (!object2 || typeof object2 !== "object")
    throw new Error("expected valid options object");
  function checkField(fieldName, expectedType, isOpt) {
    const val = object2[fieldName];
    if (isOpt && val === void 0)
      return;
    const current = typeof val;
    if (current !== expectedType || val === null)
      throw new Error(`param "${fieldName}" is invalid: expected ${expectedType}, got ${current}`);
  }
  Object.entries(fields).forEach(([k, v]) => checkField(k, v, false));
  Object.entries(optFields).forEach(([k, v]) => checkField(k, v, true));
}
function memoized(fn) {
  const map2 = /* @__PURE__ */ new WeakMap();
  return (arg, ...args) => {
    const val = map2.get(arg);
    if (val !== void 0)
      return val;
    const computed = fn(arg, ...args);
    map2.set(arg, computed);
    return computed;
  };
}

// node_modules/@noble/curves/esm/abstract/modular.js
var _0n2 = BigInt(0);
var _1n2 = BigInt(1);
var _2n = /* @__PURE__ */ BigInt(2);
var _3n = /* @__PURE__ */ BigInt(3);
var _4n = /* @__PURE__ */ BigInt(4);
var _5n = /* @__PURE__ */ BigInt(5);
var _7n = /* @__PURE__ */ BigInt(7);
var _8n = /* @__PURE__ */ BigInt(8);
var _9n = /* @__PURE__ */ BigInt(9);
var _16n = /* @__PURE__ */ BigInt(16);
function mod(a, b) {
  const result = a % b;
  return result >= _0n2 ? result : b + result;
}
function pow2(x, power, modulo) {
  let res = x;
  while (power-- > _0n2) {
    res *= res;
    res %= modulo;
  }
  return res;
}
function invert(number2, modulo) {
  if (number2 === _0n2)
    throw new Error("invert: expected non-zero number");
  if (modulo <= _0n2)
    throw new Error("invert: expected positive modulus, got " + modulo);
  let a = mod(number2, modulo);
  let b = modulo;
  let x = _0n2, y = _1n2, u = _1n2, v = _0n2;
  while (a !== _0n2) {
    const q = b / a;
    const r = b % a;
    const m = x - u * q;
    const n = y - v * q;
    b = a, a = r, x = u, y = v, u = m, v = n;
  }
  const gcd2 = b;
  if (gcd2 !== _1n2)
    throw new Error("invert: does not exist");
  return mod(x, modulo);
}
function assertIsSquare(Fp2, root, n) {
  if (!Fp2.eql(Fp2.sqr(root), n))
    throw new Error("Cannot find square root");
}
function sqrt3mod4(Fp2, n) {
  const p1div4 = (Fp2.ORDER + _1n2) / _4n;
  const root = Fp2.pow(n, p1div4);
  assertIsSquare(Fp2, root, n);
  return root;
}
function sqrt5mod8(Fp2, n) {
  const p5div8 = (Fp2.ORDER - _5n) / _8n;
  const n2 = Fp2.mul(n, _2n);
  const v = Fp2.pow(n2, p5div8);
  const nv = Fp2.mul(n, v);
  const i = Fp2.mul(Fp2.mul(nv, _2n), v);
  const root = Fp2.mul(nv, Fp2.sub(i, Fp2.ONE));
  assertIsSquare(Fp2, root, n);
  return root;
}
function sqrt9mod16(P) {
  const Fp_ = Field(P);
  const tn = tonelliShanks(P);
  const c1 = tn(Fp_, Fp_.neg(Fp_.ONE));
  const c2 = tn(Fp_, c1);
  const c3 = tn(Fp_, Fp_.neg(c1));
  const c4 = (P + _7n) / _16n;
  return (Fp2, n) => {
    let tv1 = Fp2.pow(n, c4);
    let tv2 = Fp2.mul(tv1, c1);
    const tv3 = Fp2.mul(tv1, c2);
    const tv4 = Fp2.mul(tv1, c3);
    const e1 = Fp2.eql(Fp2.sqr(tv2), n);
    const e2 = Fp2.eql(Fp2.sqr(tv3), n);
    tv1 = Fp2.cmov(tv1, tv2, e1);
    tv2 = Fp2.cmov(tv4, tv3, e2);
    const e3 = Fp2.eql(Fp2.sqr(tv2), n);
    const root = Fp2.cmov(tv1, tv2, e3);
    assertIsSquare(Fp2, root, n);
    return root;
  };
}
function tonelliShanks(P) {
  if (P < _3n)
    throw new Error("sqrt is not defined for small field");
  let Q = P - _1n2;
  let S = 0;
  while (Q % _2n === _0n2) {
    Q /= _2n;
    S++;
  }
  let Z = _2n;
  const _Fp = Field(P);
  while (FpLegendre(_Fp, Z) === 1) {
    if (Z++ > 1e3)
      throw new Error("Cannot find square root: probably non-prime P");
  }
  if (S === 1)
    return sqrt3mod4;
  let cc = _Fp.pow(Z, Q);
  const Q1div2 = (Q + _1n2) / _2n;
  return function tonelliSlow(Fp2, n) {
    if (Fp2.is0(n))
      return n;
    if (FpLegendre(Fp2, n) !== 1)
      throw new Error("Cannot find square root");
    let M = S;
    let c = Fp2.mul(Fp2.ONE, cc);
    let t = Fp2.pow(n, Q);
    let R = Fp2.pow(n, Q1div2);
    while (!Fp2.eql(t, Fp2.ONE)) {
      if (Fp2.is0(t))
        return Fp2.ZERO;
      let i = 1;
      let t_tmp = Fp2.sqr(t);
      while (!Fp2.eql(t_tmp, Fp2.ONE)) {
        i++;
        t_tmp = Fp2.sqr(t_tmp);
        if (i === M)
          throw new Error("Cannot find square root");
      }
      const exponent = _1n2 << BigInt(M - i - 1);
      const b = Fp2.pow(c, exponent);
      M = i;
      c = Fp2.sqr(b);
      t = Fp2.mul(t, c);
      R = Fp2.mul(R, b);
    }
    return R;
  };
}
function FpSqrt(P) {
  if (P % _4n === _3n)
    return sqrt3mod4;
  if (P % _8n === _5n)
    return sqrt5mod8;
  if (P % _16n === _9n)
    return sqrt9mod16(P);
  return tonelliShanks(P);
}
var isNegativeLE = (num, modulo) => (mod(num, modulo) & _1n2) === _1n2;
var FIELD_FIELDS = [
  "create",
  "isValid",
  "is0",
  "neg",
  "inv",
  "sqrt",
  "sqr",
  "eql",
  "add",
  "sub",
  "mul",
  "pow",
  "div",
  "addN",
  "subN",
  "mulN",
  "sqrN"
];
function validateField(field) {
  const initial = {
    ORDER: "bigint",
    MASK: "bigint",
    BYTES: "number",
    BITS: "number"
  };
  const opts = FIELD_FIELDS.reduce((map2, val) => {
    map2[val] = "function";
    return map2;
  }, initial);
  _validateObject(field, opts);
  return field;
}
function FpPow(Fp2, num, power) {
  if (power < _0n2)
    throw new Error("invalid exponent, negatives unsupported");
  if (power === _0n2)
    return Fp2.ONE;
  if (power === _1n2)
    return num;
  let p = Fp2.ONE;
  let d = num;
  while (power > _0n2) {
    if (power & _1n2)
      p = Fp2.mul(p, d);
    d = Fp2.sqr(d);
    power >>= _1n2;
  }
  return p;
}
function FpInvertBatch(Fp2, nums, passZero = false) {
  const inverted = new Array(nums.length).fill(passZero ? Fp2.ZERO : void 0);
  const multipliedAcc = nums.reduce((acc, num, i) => {
    if (Fp2.is0(num))
      return acc;
    inverted[i] = acc;
    return Fp2.mul(acc, num);
  }, Fp2.ONE);
  const invertedAcc = Fp2.inv(multipliedAcc);
  nums.reduceRight((acc, num, i) => {
    if (Fp2.is0(num))
      return acc;
    inverted[i] = Fp2.mul(acc, inverted[i]);
    return Fp2.mul(acc, num);
  }, invertedAcc);
  return inverted;
}
function FpLegendre(Fp2, n) {
  const p1mod2 = (Fp2.ORDER - _1n2) / _2n;
  const powered = Fp2.pow(n, p1mod2);
  const yes = Fp2.eql(powered, Fp2.ONE);
  const zero = Fp2.eql(powered, Fp2.ZERO);
  const no = Fp2.eql(powered, Fp2.neg(Fp2.ONE));
  if (!yes && !zero && !no)
    throw new Error("invalid Legendre symbol result");
  return yes ? 1 : zero ? 0 : -1;
}
function nLength(n, nBitLength) {
  if (nBitLength !== void 0)
    anumber2(nBitLength);
  const _nBitLength = nBitLength !== void 0 ? nBitLength : n.toString(2).length;
  const nByteLength = Math.ceil(_nBitLength / 8);
  return { nBitLength: _nBitLength, nByteLength };
}
function Field(ORDER, bitLenOrOpts, isLE2 = false, opts = {}) {
  if (ORDER <= _0n2)
    throw new Error("invalid field: expected ORDER > 0, got " + ORDER);
  let _nbitLength = void 0;
  let _sqrt = void 0;
  let modOnDecode = false;
  let allowedLengths = void 0;
  if (typeof bitLenOrOpts === "object" && bitLenOrOpts != null) {
    if (opts.sqrt || isLE2)
      throw new Error("cannot specify opts in two arguments");
    const _opts = bitLenOrOpts;
    if (_opts.BITS)
      _nbitLength = _opts.BITS;
    if (_opts.sqrt)
      _sqrt = _opts.sqrt;
    if (typeof _opts.isLE === "boolean")
      isLE2 = _opts.isLE;
    if (typeof _opts.modOnDecode === "boolean")
      modOnDecode = _opts.modOnDecode;
    allowedLengths = _opts.allowedLengths;
  } else {
    if (typeof bitLenOrOpts === "number")
      _nbitLength = bitLenOrOpts;
    if (opts.sqrt)
      _sqrt = opts.sqrt;
  }
  const { nBitLength: BITS, nByteLength: BYTES } = nLength(ORDER, _nbitLength);
  if (BYTES > 2048)
    throw new Error("invalid field: expected ORDER of <= 2048 bytes");
  let sqrtP;
  const f = Object.freeze({
    ORDER,
    isLE: isLE2,
    BITS,
    BYTES,
    MASK: bitMask(BITS),
    ZERO: _0n2,
    ONE: _1n2,
    allowedLengths,
    create: (num) => mod(num, ORDER),
    isValid: (num) => {
      if (typeof num !== "bigint")
        throw new Error("invalid field element: expected bigint, got " + typeof num);
      return _0n2 <= num && num < ORDER;
    },
    is0: (num) => num === _0n2,
    // is valid and invertible
    isValidNot0: (num) => !f.is0(num) && f.isValid(num),
    isOdd: (num) => (num & _1n2) === _1n2,
    neg: (num) => mod(-num, ORDER),
    eql: (lhs, rhs) => lhs === rhs,
    sqr: (num) => mod(num * num, ORDER),
    add: (lhs, rhs) => mod(lhs + rhs, ORDER),
    sub: (lhs, rhs) => mod(lhs - rhs, ORDER),
    mul: (lhs, rhs) => mod(lhs * rhs, ORDER),
    pow: (num, power) => FpPow(f, num, power),
    div: (lhs, rhs) => mod(lhs * invert(rhs, ORDER), ORDER),
    // Same as above, but doesn't normalize
    sqrN: (num) => num * num,
    addN: (lhs, rhs) => lhs + rhs,
    subN: (lhs, rhs) => lhs - rhs,
    mulN: (lhs, rhs) => lhs * rhs,
    inv: (num) => invert(num, ORDER),
    sqrt: _sqrt || ((n) => {
      if (!sqrtP)
        sqrtP = FpSqrt(ORDER);
      return sqrtP(f, n);
    }),
    toBytes: (num) => isLE2 ? numberToBytesLE(num, BYTES) : numberToBytesBE(num, BYTES),
    fromBytes: (bytes, skipValidation = true) => {
      if (allowedLengths) {
        if (!allowedLengths.includes(bytes.length) || bytes.length > BYTES) {
          throw new Error("Field.fromBytes: expected " + allowedLengths + " bytes, got " + bytes.length);
        }
        const padded = new Uint8Array(BYTES);
        padded.set(bytes, isLE2 ? 0 : padded.length - bytes.length);
        bytes = padded;
      }
      if (bytes.length !== BYTES)
        throw new Error("Field.fromBytes: expected " + BYTES + " bytes, got " + bytes.length);
      let scalar = isLE2 ? bytesToNumberLE(bytes) : bytesToNumberBE(bytes);
      if (modOnDecode)
        scalar = mod(scalar, ORDER);
      if (!skipValidation) {
        if (!f.isValid(scalar))
          throw new Error("invalid field element: outside of range 0..ORDER");
      }
      return scalar;
    },
    // TODO: we don't need it here, move out to separate fn
    invertBatch: (lst) => FpInvertBatch(f, lst),
    // We can't move this out because Fp6, Fp12 implement it
    // and it's unclear what to return in there.
    cmov: (a, b, c) => c ? b : a
  });
  return Object.freeze(f);
}
function getFieldBytesLength(fieldOrder) {
  if (typeof fieldOrder !== "bigint")
    throw new Error("field order must be bigint");
  const bitLength = fieldOrder.toString(2).length;
  return Math.ceil(bitLength / 8);
}
function getMinHashLength(fieldOrder) {
  const length = getFieldBytesLength(fieldOrder);
  return length + Math.ceil(length / 2);
}
function mapHashToField(key, fieldOrder, isLE2 = false) {
  const len = key.length;
  const fieldLen = getFieldBytesLength(fieldOrder);
  const minLen = getMinHashLength(fieldOrder);
  if (len < 16 || len < minLen || len > 1024)
    throw new Error("expected " + minLen + "-1024 bytes of input, got " + len);
  const num = isLE2 ? bytesToNumberLE(key) : bytesToNumberBE(key);
  const reduced = mod(num, fieldOrder - _1n2) + _1n2;
  return isLE2 ? numberToBytesLE(reduced, fieldLen) : numberToBytesBE(reduced, fieldLen);
}

// node_modules/@noble/hashes/esm/_md.js
function setBigUint64(view, byteOffset, value, isLE2) {
  if (typeof view.setBigUint64 === "function")
    return view.setBigUint64(byteOffset, value, isLE2);
  const _32n2 = BigInt(32);
  const _u32_max = BigInt(4294967295);
  const wh = Number(value >> _32n2 & _u32_max);
  const wl = Number(value & _u32_max);
  const h = isLE2 ? 4 : 0;
  const l = isLE2 ? 0 : 4;
  view.setUint32(byteOffset + h, wh, isLE2);
  view.setUint32(byteOffset + l, wl, isLE2);
}
function Chi(a, b, c) {
  return a & b ^ ~a & c;
}
function Maj(a, b, c) {
  return a & b ^ a & c ^ b & c;
}
var HashMD = class extends Hash {
  constructor(blockLen, outputLen, padOffset, isLE2) {
    super();
    this.finished = false;
    this.length = 0;
    this.pos = 0;
    this.destroyed = false;
    this.blockLen = blockLen;
    this.outputLen = outputLen;
    this.padOffset = padOffset;
    this.isLE = isLE2;
    this.buffer = new Uint8Array(blockLen);
    this.view = createView(this.buffer);
  }
  update(data) {
    aexists(this);
    data = toBytes(data);
    abytes(data);
    const { view, buffer, blockLen } = this;
    const len = data.length;
    for (let pos = 0; pos < len; ) {
      const take = Math.min(blockLen - this.pos, len - pos);
      if (take === blockLen) {
        const dataView = createView(data);
        for (; blockLen <= len - pos; pos += blockLen)
          this.process(dataView, pos);
        continue;
      }
      buffer.set(data.subarray(pos, pos + take), this.pos);
      this.pos += take;
      pos += take;
      if (this.pos === blockLen) {
        this.process(view, 0);
        this.pos = 0;
      }
    }
    this.length += data.length;
    this.roundClean();
    return this;
  }
  digestInto(out) {
    aexists(this);
    aoutput(out, this);
    this.finished = true;
    const { buffer, view, blockLen, isLE: isLE2 } = this;
    let { pos } = this;
    buffer[pos++] = 128;
    clean(this.buffer.subarray(pos));
    if (this.padOffset > blockLen - pos) {
      this.process(view, 0);
      pos = 0;
    }
    for (let i = pos; i < blockLen; i++)
      buffer[i] = 0;
    setBigUint64(view, blockLen - 8, BigInt(this.length * 8), isLE2);
    this.process(view, 0);
    const oview = createView(out);
    const len = this.outputLen;
    if (len % 4)
      throw new Error("_sha2: outputLen should be aligned to 32bit");
    const outLen = len / 4;
    const state = this.get();
    if (outLen > state.length)
      throw new Error("_sha2: outputLen bigger than state");
    for (let i = 0; i < outLen; i++)
      oview.setUint32(4 * i, state[i], isLE2);
  }
  digest() {
    const { buffer, outputLen } = this;
    this.digestInto(buffer);
    const res = buffer.slice(0, outputLen);
    this.destroy();
    return res;
  }
  _cloneInto(to) {
    to || (to = new this.constructor());
    to.set(...this.get());
    const { blockLen, buffer, length, finished, destroyed, pos } = this;
    to.destroyed = destroyed;
    to.finished = finished;
    to.length = length;
    to.pos = pos;
    if (length % blockLen)
      to.buffer.set(buffer);
    return to;
  }
  clone() {
    return this._cloneInto();
  }
};
var SHA256_IV = /* @__PURE__ */ Uint32Array.from([
  1779033703,
  3144134277,
  1013904242,
  2773480762,
  1359893119,
  2600822924,
  528734635,
  1541459225
]);
var SHA384_IV = /* @__PURE__ */ Uint32Array.from([
  3418070365,
  3238371032,
  1654270250,
  914150663,
  2438529370,
  812702999,
  355462360,
  4144912697,
  1731405415,
  4290775857,
  2394180231,
  1750603025,
  3675008525,
  1694076839,
  1203062813,
  3204075428
]);
var SHA512_IV = /* @__PURE__ */ Uint32Array.from([
  1779033703,
  4089235720,
  3144134277,
  2227873595,
  1013904242,
  4271175723,
  2773480762,
  1595750129,
  1359893119,
  2917565137,
  2600822924,
  725511199,
  528734635,
  4215389547,
  1541459225,
  327033209
]);

// node_modules/@noble/hashes/esm/_u64.js
var U32_MASK64 = /* @__PURE__ */ BigInt(2 ** 32 - 1);
var _32n = /* @__PURE__ */ BigInt(32);
function fromBig(n, le = false) {
  if (le)
    return { h: Number(n & U32_MASK64), l: Number(n >> _32n & U32_MASK64) };
  return { h: Number(n >> _32n & U32_MASK64) | 0, l: Number(n & U32_MASK64) | 0 };
}
function split(lst, le = false) {
  const len = lst.length;
  let Ah = new Uint32Array(len);
  let Al = new Uint32Array(len);
  for (let i = 0; i < len; i++) {
    const { h, l } = fromBig(lst[i], le);
    [Ah[i], Al[i]] = [h, l];
  }
  return [Ah, Al];
}
var shrSH = (h, _l, s) => h >>> s;
var shrSL = (h, l, s) => h << 32 - s | l >>> s;
var rotrSH = (h, l, s) => h >>> s | l << 32 - s;
var rotrSL = (h, l, s) => h << 32 - s | l >>> s;
var rotrBH = (h, l, s) => h << 64 - s | l >>> s - 32;
var rotrBL = (h, l, s) => h >>> s - 32 | l << 64 - s;
var rotr32H = (_h, l) => l;
var rotr32L = (h, _l) => h;
function add(Ah, Al, Bh, Bl) {
  const l = (Al >>> 0) + (Bl >>> 0);
  return { h: Ah + Bh + (l / 2 ** 32 | 0) | 0, l: l | 0 };
}
var add3L = (Al, Bl, Cl) => (Al >>> 0) + (Bl >>> 0) + (Cl >>> 0);
var add3H = (low, Ah, Bh, Ch) => Ah + Bh + Ch + (low / 2 ** 32 | 0) | 0;
var add4L = (Al, Bl, Cl, Dl) => (Al >>> 0) + (Bl >>> 0) + (Cl >>> 0) + (Dl >>> 0);
var add4H = (low, Ah, Bh, Ch, Dh) => Ah + Bh + Ch + Dh + (low / 2 ** 32 | 0) | 0;
var add5L = (Al, Bl, Cl, Dl, El) => (Al >>> 0) + (Bl >>> 0) + (Cl >>> 0) + (Dl >>> 0) + (El >>> 0);
var add5H = (low, Ah, Bh, Ch, Dh, Eh) => Ah + Bh + Ch + Dh + Eh + (low / 2 ** 32 | 0) | 0;

// node_modules/@noble/hashes/esm/sha2.js
var SHA256_K = /* @__PURE__ */ Uint32Array.from([
  1116352408,
  1899447441,
  3049323471,
  3921009573,
  961987163,
  1508970993,
  2453635748,
  2870763221,
  3624381080,
  310598401,
  607225278,
  1426881987,
  1925078388,
  2162078206,
  2614888103,
  3248222580,
  3835390401,
  4022224774,
  264347078,
  604807628,
  770255983,
  1249150122,
  1555081692,
  1996064986,
  2554220882,
  2821834349,
  2952996808,
  3210313671,
  3336571891,
  3584528711,
  113926993,
  338241895,
  666307205,
  773529912,
  1294757372,
  1396182291,
  1695183700,
  1986661051,
  2177026350,
  2456956037,
  2730485921,
  2820302411,
  3259730800,
  3345764771,
  3516065817,
  3600352804,
  4094571909,
  275423344,
  430227734,
  506948616,
  659060556,
  883997877,
  958139571,
  1322822218,
  1537002063,
  1747873779,
  1955562222,
  2024104815,
  2227730452,
  2361852424,
  2428436474,
  2756734187,
  3204031479,
  3329325298
]);
var SHA256_W = /* @__PURE__ */ new Uint32Array(64);
var SHA256 = class extends HashMD {
  constructor(outputLen = 32) {
    super(64, outputLen, 8, false);
    this.A = SHA256_IV[0] | 0;
    this.B = SHA256_IV[1] | 0;
    this.C = SHA256_IV[2] | 0;
    this.D = SHA256_IV[3] | 0;
    this.E = SHA256_IV[4] | 0;
    this.F = SHA256_IV[5] | 0;
    this.G = SHA256_IV[6] | 0;
    this.H = SHA256_IV[7] | 0;
  }
  get() {
    const { A, B, C, D, E, F, G, H } = this;
    return [A, B, C, D, E, F, G, H];
  }
  // prettier-ignore
  set(A, B, C, D, E, F, G, H) {
    this.A = A | 0;
    this.B = B | 0;
    this.C = C | 0;
    this.D = D | 0;
    this.E = E | 0;
    this.F = F | 0;
    this.G = G | 0;
    this.H = H | 0;
  }
  process(view, offset) {
    for (let i = 0; i < 16; i++, offset += 4)
      SHA256_W[i] = view.getUint32(offset, false);
    for (let i = 16; i < 64; i++) {
      const W15 = SHA256_W[i - 15];
      const W2 = SHA256_W[i - 2];
      const s0 = rotr(W15, 7) ^ rotr(W15, 18) ^ W15 >>> 3;
      const s1 = rotr(W2, 17) ^ rotr(W2, 19) ^ W2 >>> 10;
      SHA256_W[i] = s1 + SHA256_W[i - 7] + s0 + SHA256_W[i - 16] | 0;
    }
    let { A, B, C, D, E, F, G, H } = this;
    for (let i = 0; i < 64; i++) {
      const sigma1 = rotr(E, 6) ^ rotr(E, 11) ^ rotr(E, 25);
      const T1 = H + sigma1 + Chi(E, F, G) + SHA256_K[i] + SHA256_W[i] | 0;
      const sigma0 = rotr(A, 2) ^ rotr(A, 13) ^ rotr(A, 22);
      const T2 = sigma0 + Maj(A, B, C) | 0;
      H = G;
      G = F;
      F = E;
      E = D + T1 | 0;
      D = C;
      C = B;
      B = A;
      A = T1 + T2 | 0;
    }
    A = A + this.A | 0;
    B = B + this.B | 0;
    C = C + this.C | 0;
    D = D + this.D | 0;
    E = E + this.E | 0;
    F = F + this.F | 0;
    G = G + this.G | 0;
    H = H + this.H | 0;
    this.set(A, B, C, D, E, F, G, H);
  }
  roundClean() {
    clean(SHA256_W);
  }
  destroy() {
    this.set(0, 0, 0, 0, 0, 0, 0, 0);
    clean(this.buffer);
  }
};
var K512 = /* @__PURE__ */ (() => split([
  "0x428a2f98d728ae22",
  "0x7137449123ef65cd",
  "0xb5c0fbcfec4d3b2f",
  "0xe9b5dba58189dbbc",
  "0x3956c25bf348b538",
  "0x59f111f1b605d019",
  "0x923f82a4af194f9b",
  "0xab1c5ed5da6d8118",
  "0xd807aa98a3030242",
  "0x12835b0145706fbe",
  "0x243185be4ee4b28c",
  "0x550c7dc3d5ffb4e2",
  "0x72be5d74f27b896f",
  "0x80deb1fe3b1696b1",
  "0x9bdc06a725c71235",
  "0xc19bf174cf692694",
  "0xe49b69c19ef14ad2",
  "0xefbe4786384f25e3",
  "0x0fc19dc68b8cd5b5",
  "0x240ca1cc77ac9c65",
  "0x2de92c6f592b0275",
  "0x4a7484aa6ea6e483",
  "0x5cb0a9dcbd41fbd4",
  "0x76f988da831153b5",
  "0x983e5152ee66dfab",
  "0xa831c66d2db43210",
  "0xb00327c898fb213f",
  "0xbf597fc7beef0ee4",
  "0xc6e00bf33da88fc2",
  "0xd5a79147930aa725",
  "0x06ca6351e003826f",
  "0x142929670a0e6e70",
  "0x27b70a8546d22ffc",
  "0x2e1b21385c26c926",
  "0x4d2c6dfc5ac42aed",
  "0x53380d139d95b3df",
  "0x650a73548baf63de",
  "0x766a0abb3c77b2a8",
  "0x81c2c92e47edaee6",
  "0x92722c851482353b",
  "0xa2bfe8a14cf10364",
  "0xa81a664bbc423001",
  "0xc24b8b70d0f89791",
  "0xc76c51a30654be30",
  "0xd192e819d6ef5218",
  "0xd69906245565a910",
  "0xf40e35855771202a",
  "0x106aa07032bbd1b8",
  "0x19a4c116b8d2d0c8",
  "0x1e376c085141ab53",
  "0x2748774cdf8eeb99",
  "0x34b0bcb5e19b48a8",
  "0x391c0cb3c5c95a63",
  "0x4ed8aa4ae3418acb",
  "0x5b9cca4f7763e373",
  "0x682e6ff3d6b2b8a3",
  "0x748f82ee5defb2fc",
  "0x78a5636f43172f60",
  "0x84c87814a1f0ab72",
  "0x8cc702081a6439ec",
  "0x90befffa23631e28",
  "0xa4506cebde82bde9",
  "0xbef9a3f7b2c67915",
  "0xc67178f2e372532b",
  "0xca273eceea26619c",
  "0xd186b8c721c0c207",
  "0xeada7dd6cde0eb1e",
  "0xf57d4f7fee6ed178",
  "0x06f067aa72176fba",
  "0x0a637dc5a2c898a6",
  "0x113f9804bef90dae",
  "0x1b710b35131c471b",
  "0x28db77f523047d84",
  "0x32caab7b40c72493",
  "0x3c9ebe0a15c9bebc",
  "0x431d67c49c100d4c",
  "0x4cc5d4becb3e42b6",
  "0x597f299cfc657e2a",
  "0x5fcb6fab3ad6faec",
  "0x6c44198c4a475817"
].map((n) => BigInt(n))))();
var SHA512_Kh = /* @__PURE__ */ (() => K512[0])();
var SHA512_Kl = /* @__PURE__ */ (() => K512[1])();
var SHA512_W_H = /* @__PURE__ */ new Uint32Array(80);
var SHA512_W_L = /* @__PURE__ */ new Uint32Array(80);
var SHA512 = class extends HashMD {
  constructor(outputLen = 64) {
    super(128, outputLen, 16, false);
    this.Ah = SHA512_IV[0] | 0;
    this.Al = SHA512_IV[1] | 0;
    this.Bh = SHA512_IV[2] | 0;
    this.Bl = SHA512_IV[3] | 0;
    this.Ch = SHA512_IV[4] | 0;
    this.Cl = SHA512_IV[5] | 0;
    this.Dh = SHA512_IV[6] | 0;
    this.Dl = SHA512_IV[7] | 0;
    this.Eh = SHA512_IV[8] | 0;
    this.El = SHA512_IV[9] | 0;
    this.Fh = SHA512_IV[10] | 0;
    this.Fl = SHA512_IV[11] | 0;
    this.Gh = SHA512_IV[12] | 0;
    this.Gl = SHA512_IV[13] | 0;
    this.Hh = SHA512_IV[14] | 0;
    this.Hl = SHA512_IV[15] | 0;
  }
  // prettier-ignore
  get() {
    const { Ah, Al, Bh, Bl, Ch, Cl, Dh, Dl, Eh, El, Fh, Fl, Gh, Gl, Hh, Hl } = this;
    return [Ah, Al, Bh, Bl, Ch, Cl, Dh, Dl, Eh, El, Fh, Fl, Gh, Gl, Hh, Hl];
  }
  // prettier-ignore
  set(Ah, Al, Bh, Bl, Ch, Cl, Dh, Dl, Eh, El, Fh, Fl, Gh, Gl, Hh, Hl) {
    this.Ah = Ah | 0;
    this.Al = Al | 0;
    this.Bh = Bh | 0;
    this.Bl = Bl | 0;
    this.Ch = Ch | 0;
    this.Cl = Cl | 0;
    this.Dh = Dh | 0;
    this.Dl = Dl | 0;
    this.Eh = Eh | 0;
    this.El = El | 0;
    this.Fh = Fh | 0;
    this.Fl = Fl | 0;
    this.Gh = Gh | 0;
    this.Gl = Gl | 0;
    this.Hh = Hh | 0;
    this.Hl = Hl | 0;
  }
  process(view, offset) {
    for (let i = 0; i < 16; i++, offset += 4) {
      SHA512_W_H[i] = view.getUint32(offset);
      SHA512_W_L[i] = view.getUint32(offset += 4);
    }
    for (let i = 16; i < 80; i++) {
      const W15h = SHA512_W_H[i - 15] | 0;
      const W15l = SHA512_W_L[i - 15] | 0;
      const s0h = rotrSH(W15h, W15l, 1) ^ rotrSH(W15h, W15l, 8) ^ shrSH(W15h, W15l, 7);
      const s0l = rotrSL(W15h, W15l, 1) ^ rotrSL(W15h, W15l, 8) ^ shrSL(W15h, W15l, 7);
      const W2h = SHA512_W_H[i - 2] | 0;
      const W2l = SHA512_W_L[i - 2] | 0;
      const s1h = rotrSH(W2h, W2l, 19) ^ rotrBH(W2h, W2l, 61) ^ shrSH(W2h, W2l, 6);
      const s1l = rotrSL(W2h, W2l, 19) ^ rotrBL(W2h, W2l, 61) ^ shrSL(W2h, W2l, 6);
      const SUMl = add4L(s0l, s1l, SHA512_W_L[i - 7], SHA512_W_L[i - 16]);
      const SUMh = add4H(SUMl, s0h, s1h, SHA512_W_H[i - 7], SHA512_W_H[i - 16]);
      SHA512_W_H[i] = SUMh | 0;
      SHA512_W_L[i] = SUMl | 0;
    }
    let { Ah, Al, Bh, Bl, Ch, Cl, Dh, Dl, Eh, El, Fh, Fl, Gh, Gl, Hh, Hl } = this;
    for (let i = 0; i < 80; i++) {
      const sigma1h = rotrSH(Eh, El, 14) ^ rotrSH(Eh, El, 18) ^ rotrBH(Eh, El, 41);
      const sigma1l = rotrSL(Eh, El, 14) ^ rotrSL(Eh, El, 18) ^ rotrBL(Eh, El, 41);
      const CHIh = Eh & Fh ^ ~Eh & Gh;
      const CHIl = El & Fl ^ ~El & Gl;
      const T1ll = add5L(Hl, sigma1l, CHIl, SHA512_Kl[i], SHA512_W_L[i]);
      const T1h = add5H(T1ll, Hh, sigma1h, CHIh, SHA512_Kh[i], SHA512_W_H[i]);
      const T1l = T1ll | 0;
      const sigma0h = rotrSH(Ah, Al, 28) ^ rotrBH(Ah, Al, 34) ^ rotrBH(Ah, Al, 39);
      const sigma0l = rotrSL(Ah, Al, 28) ^ rotrBL(Ah, Al, 34) ^ rotrBL(Ah, Al, 39);
      const MAJh = Ah & Bh ^ Ah & Ch ^ Bh & Ch;
      const MAJl = Al & Bl ^ Al & Cl ^ Bl & Cl;
      Hh = Gh | 0;
      Hl = Gl | 0;
      Gh = Fh | 0;
      Gl = Fl | 0;
      Fh = Eh | 0;
      Fl = El | 0;
      ({ h: Eh, l: El } = add(Dh | 0, Dl | 0, T1h | 0, T1l | 0));
      Dh = Ch | 0;
      Dl = Cl | 0;
      Ch = Bh | 0;
      Cl = Bl | 0;
      Bh = Ah | 0;
      Bl = Al | 0;
      const All = add3L(T1l, sigma0l, MAJl);
      Ah = add3H(All, T1h, sigma0h, MAJh);
      Al = All | 0;
    }
    ({ h: Ah, l: Al } = add(this.Ah | 0, this.Al | 0, Ah | 0, Al | 0));
    ({ h: Bh, l: Bl } = add(this.Bh | 0, this.Bl | 0, Bh | 0, Bl | 0));
    ({ h: Ch, l: Cl } = add(this.Ch | 0, this.Cl | 0, Ch | 0, Cl | 0));
    ({ h: Dh, l: Dl } = add(this.Dh | 0, this.Dl | 0, Dh | 0, Dl | 0));
    ({ h: Eh, l: El } = add(this.Eh | 0, this.El | 0, Eh | 0, El | 0));
    ({ h: Fh, l: Fl } = add(this.Fh | 0, this.Fl | 0, Fh | 0, Fl | 0));
    ({ h: Gh, l: Gl } = add(this.Gh | 0, this.Gl | 0, Gh | 0, Gl | 0));
    ({ h: Hh, l: Hl } = add(this.Hh | 0, this.Hl | 0, Hh | 0, Hl | 0));
    this.set(Ah, Al, Bh, Bl, Ch, Cl, Dh, Dl, Eh, El, Fh, Fl, Gh, Gl, Hh, Hl);
  }
  roundClean() {
    clean(SHA512_W_H, SHA512_W_L);
  }
  destroy() {
    clean(this.buffer);
    this.set(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
  }
};
var SHA384 = class extends SHA512 {
  constructor() {
    super(48);
    this.Ah = SHA384_IV[0] | 0;
    this.Al = SHA384_IV[1] | 0;
    this.Bh = SHA384_IV[2] | 0;
    this.Bl = SHA384_IV[3] | 0;
    this.Ch = SHA384_IV[4] | 0;
    this.Cl = SHA384_IV[5] | 0;
    this.Dh = SHA384_IV[6] | 0;
    this.Dl = SHA384_IV[7] | 0;
    this.Eh = SHA384_IV[8] | 0;
    this.El = SHA384_IV[9] | 0;
    this.Fh = SHA384_IV[10] | 0;
    this.Fl = SHA384_IV[11] | 0;
    this.Gh = SHA384_IV[12] | 0;
    this.Gl = SHA384_IV[13] | 0;
    this.Hh = SHA384_IV[14] | 0;
    this.Hl = SHA384_IV[15] | 0;
  }
};
var sha256 = /* @__PURE__ */ createHasher(() => new SHA256());
var sha512 = /* @__PURE__ */ createHasher(() => new SHA512());
var sha384 = /* @__PURE__ */ createHasher(() => new SHA384());

// node_modules/@noble/hashes/esm/hmac.js
var HMAC = class extends Hash {
  constructor(hash, _key) {
    super();
    this.finished = false;
    this.destroyed = false;
    ahash(hash);
    const key = toBytes(_key);
    this.iHash = hash.create();
    if (typeof this.iHash.update !== "function")
      throw new Error("Expected instance of class which extends utils.Hash");
    this.blockLen = this.iHash.blockLen;
    this.outputLen = this.iHash.outputLen;
    const blockLen = this.blockLen;
    const pad = new Uint8Array(blockLen);
    pad.set(key.length > blockLen ? hash.create().update(key).digest() : key);
    for (let i = 0; i < pad.length; i++)
      pad[i] ^= 54;
    this.iHash.update(pad);
    this.oHash = hash.create();
    for (let i = 0; i < pad.length; i++)
      pad[i] ^= 54 ^ 92;
    this.oHash.update(pad);
    clean(pad);
  }
  update(buf) {
    aexists(this);
    this.iHash.update(buf);
    return this;
  }
  digestInto(out) {
    aexists(this);
    abytes(out, this.outputLen);
    this.finished = true;
    this.iHash.digestInto(out);
    this.oHash.update(out);
    this.oHash.digestInto(out);
    this.destroy();
  }
  digest() {
    const out = new Uint8Array(this.oHash.outputLen);
    this.digestInto(out);
    return out;
  }
  _cloneInto(to) {
    to || (to = Object.create(Object.getPrototypeOf(this), {}));
    const { oHash, iHash, finished, destroyed, blockLen, outputLen } = this;
    to = to;
    to.finished = finished;
    to.destroyed = destroyed;
    to.blockLen = blockLen;
    to.outputLen = outputLen;
    to.oHash = oHash._cloneInto(to.oHash);
    to.iHash = iHash._cloneInto(to.iHash);
    return to;
  }
  clone() {
    return this._cloneInto();
  }
  destroy() {
    this.destroyed = true;
    this.oHash.destroy();
    this.iHash.destroy();
  }
};
var hmac = (hash, key, message) => new HMAC(hash, key).update(message).digest();
hmac.create = (hash, key) => new HMAC(hash, key);

// node_modules/@noble/curves/esm/abstract/curve.js
var _0n3 = BigInt(0);
var _1n3 = BigInt(1);
function negateCt(condition, item) {
  const neg = item.negate();
  return condition ? neg : item;
}
function normalizeZ(c, points) {
  const invertedZs = FpInvertBatch(c.Fp, points.map((p) => p.Z));
  return points.map((p, i) => c.fromAffine(p.toAffine(invertedZs[i])));
}
function validateW(W, bits) {
  if (!Number.isSafeInteger(W) || W <= 0 || W > bits)
    throw new Error("invalid window size, expected [1.." + bits + "], got W=" + W);
}
function calcWOpts(W, scalarBits) {
  validateW(W, scalarBits);
  const windows = Math.ceil(scalarBits / W) + 1;
  const windowSize = 2 ** (W - 1);
  const maxNumber = 2 ** W;
  const mask = bitMask(W);
  const shiftBy = BigInt(W);
  return { windows, windowSize, mask, maxNumber, shiftBy };
}
function calcOffsets(n, window, wOpts) {
  const { windowSize, mask, maxNumber, shiftBy } = wOpts;
  let wbits = Number(n & mask);
  let nextN = n >> shiftBy;
  if (wbits > windowSize) {
    wbits -= maxNumber;
    nextN += _1n3;
  }
  const offsetStart = window * windowSize;
  const offset = offsetStart + Math.abs(wbits) - 1;
  const isZero = wbits === 0;
  const isNeg = wbits < 0;
  const isNegF = window % 2 !== 0;
  const offsetF = offsetStart;
  return { nextN, offset, isZero, isNeg, isNegF, offsetF };
}
function validateMSMPoints(points, c) {
  if (!Array.isArray(points))
    throw new Error("array expected");
  points.forEach((p, i) => {
    if (!(p instanceof c))
      throw new Error("invalid point at index " + i);
  });
}
function validateMSMScalars(scalars, field) {
  if (!Array.isArray(scalars))
    throw new Error("array of scalars expected");
  scalars.forEach((s, i) => {
    if (!field.isValid(s))
      throw new Error("invalid scalar at index " + i);
  });
}
var pointPrecomputes = /* @__PURE__ */ new WeakMap();
var pointWindowSizes = /* @__PURE__ */ new WeakMap();
function getW(P) {
  return pointWindowSizes.get(P) || 1;
}
function assert0(n) {
  if (n !== _0n3)
    throw new Error("invalid wNAF");
}
var wNAF = class {
  // Parametrized with a given Point class (not individual point)
  constructor(Point2, bits) {
    this.BASE = Point2.BASE;
    this.ZERO = Point2.ZERO;
    this.Fn = Point2.Fn;
    this.bits = bits;
  }
  // non-const time multiplication ladder
  _unsafeLadder(elm, n, p = this.ZERO) {
    let d = elm;
    while (n > _0n3) {
      if (n & _1n3)
        p = p.add(d);
      d = d.double();
      n >>= _1n3;
    }
    return p;
  }
  /**
   * Creates a wNAF precomputation window. Used for caching.
   * Default window size is set by `utils.precompute()` and is equal to 8.
   * Number of precomputed points depends on the curve size:
   * 2^(𝑊−1) * (Math.ceil(𝑛 / 𝑊) + 1), where:
   * - 𝑊 is the window size
   * - 𝑛 is the bitlength of the curve order.
   * For a 256-bit curve and window size 8, the number of precomputed points is 128 * 33 = 4224.
   * @param point Point instance
   * @param W window size
   * @returns precomputed point tables flattened to a single array
   */
  precomputeWindow(point, W) {
    const { windows, windowSize } = calcWOpts(W, this.bits);
    const points = [];
    let p = point;
    let base = p;
    for (let window = 0; window < windows; window++) {
      base = p;
      points.push(base);
      for (let i = 1; i < windowSize; i++) {
        base = base.add(p);
        points.push(base);
      }
      p = base.double();
    }
    return points;
  }
  /**
   * Implements ec multiplication using precomputed tables and w-ary non-adjacent form.
   * More compact implementation:
   * https://github.com/paulmillr/noble-secp256k1/blob/47cb1669b6e506ad66b35fe7d76132ae97465da2/index.ts#L502-L541
   * @returns real and fake (for const-time) points
   */
  wNAF(W, precomputes, n) {
    if (!this.Fn.isValid(n))
      throw new Error("invalid scalar");
    let p = this.ZERO;
    let f = this.BASE;
    const wo = calcWOpts(W, this.bits);
    for (let window = 0; window < wo.windows; window++) {
      const { nextN, offset, isZero, isNeg, isNegF, offsetF } = calcOffsets(n, window, wo);
      n = nextN;
      if (isZero) {
        f = f.add(negateCt(isNegF, precomputes[offsetF]));
      } else {
        p = p.add(negateCt(isNeg, precomputes[offset]));
      }
    }
    assert0(n);
    return { p, f };
  }
  /**
   * Implements ec unsafe (non const-time) multiplication using precomputed tables and w-ary non-adjacent form.
   * @param acc accumulator point to add result of multiplication
   * @returns point
   */
  wNAFUnsafe(W, precomputes, n, acc = this.ZERO) {
    const wo = calcWOpts(W, this.bits);
    for (let window = 0; window < wo.windows; window++) {
      if (n === _0n3)
        break;
      const { nextN, offset, isZero, isNeg } = calcOffsets(n, window, wo);
      n = nextN;
      if (isZero) {
        continue;
      } else {
        const item = precomputes[offset];
        acc = acc.add(isNeg ? item.negate() : item);
      }
    }
    assert0(n);
    return acc;
  }
  getPrecomputes(W, point, transform2) {
    let comp = pointPrecomputes.get(point);
    if (!comp) {
      comp = this.precomputeWindow(point, W);
      if (W !== 1) {
        if (typeof transform2 === "function")
          comp = transform2(comp);
        pointPrecomputes.set(point, comp);
      }
    }
    return comp;
  }
  cached(point, scalar, transform2) {
    const W = getW(point);
    return this.wNAF(W, this.getPrecomputes(W, point, transform2), scalar);
  }
  unsafe(point, scalar, transform2, prev) {
    const W = getW(point);
    if (W === 1)
      return this._unsafeLadder(point, scalar, prev);
    return this.wNAFUnsafe(W, this.getPrecomputes(W, point, transform2), scalar, prev);
  }
  // We calculate precomputes for elliptic curve point multiplication
  // using windowed method. This specifies window size and
  // stores precomputed values. Usually only base point would be precomputed.
  createCache(P, W) {
    validateW(W, this.bits);
    pointWindowSizes.set(P, W);
    pointPrecomputes.delete(P);
  }
  hasCache(elm) {
    return getW(elm) !== 1;
  }
};
function mulEndoUnsafe(Point2, point, k1, k2) {
  let acc = point;
  let p1 = Point2.ZERO;
  let p2 = Point2.ZERO;
  while (k1 > _0n3 || k2 > _0n3) {
    if (k1 & _1n3)
      p1 = p1.add(acc);
    if (k2 & _1n3)
      p2 = p2.add(acc);
    acc = acc.double();
    k1 >>= _1n3;
    k2 >>= _1n3;
  }
  return { p1, p2 };
}
function pippenger(c, fieldN, points, scalars) {
  validateMSMPoints(points, c);
  validateMSMScalars(scalars, fieldN);
  const plength = points.length;
  const slength = scalars.length;
  if (plength !== slength)
    throw new Error("arrays of points and scalars must have equal length");
  const zero = c.ZERO;
  const wbits = bitLen(BigInt(plength));
  let windowSize = 1;
  if (wbits > 12)
    windowSize = wbits - 3;
  else if (wbits > 4)
    windowSize = wbits - 2;
  else if (wbits > 0)
    windowSize = 2;
  const MASK = bitMask(windowSize);
  const buckets = new Array(Number(MASK) + 1).fill(zero);
  const lastBits = Math.floor((fieldN.BITS - 1) / windowSize) * windowSize;
  let sum = zero;
  for (let i = lastBits; i >= 0; i -= windowSize) {
    buckets.fill(zero);
    for (let j = 0; j < slength; j++) {
      const scalar = scalars[j];
      const wbits2 = Number(scalar >> BigInt(i) & MASK);
      buckets[wbits2] = buckets[wbits2].add(points[j]);
    }
    let resI = zero;
    for (let j = buckets.length - 1, sumI = zero; j > 0; j--) {
      sumI = sumI.add(buckets[j]);
      resI = resI.add(sumI);
    }
    sum = sum.add(resI);
    if (i !== 0)
      for (let j = 0; j < windowSize; j++)
        sum = sum.double();
  }
  return sum;
}
function createField(order, field) {
  if (field) {
    if (field.ORDER !== order)
      throw new Error("Field.ORDER must match order: Fp == p, Fn == n");
    validateField(field);
    return field;
  } else {
    return Field(order);
  }
}
function _createCurveFields(type, CURVE, curveOpts = {}) {
  if (!CURVE || typeof CURVE !== "object")
    throw new Error(`expected valid ${type} CURVE object`);
  for (const p of ["p", "n", "h"]) {
    const val = CURVE[p];
    if (!(typeof val === "bigint" && val > _0n3))
      throw new Error(`CURVE.${p} must be positive bigint`);
  }
  const Fp2 = createField(CURVE.p, curveOpts.Fp);
  const Fn2 = createField(CURVE.n, curveOpts.Fn);
  const _b = type === "weierstrass" ? "b" : "d";
  const params = ["Gx", "Gy", "a", _b];
  for (const p of params) {
    if (!Fp2.isValid(CURVE[p]))
      throw new Error(`CURVE.${p} must be valid field element of CURVE.Fp`);
  }
  return { Fp: Fp2, Fn: Fn2 };
}

// node_modules/@noble/curves/esm/abstract/weierstrass.js
var divNearest = (num, den) => (num + (num >= 0 ? den : -den) / _2n2) / den;
function _splitEndoScalar(k, basis, n) {
  const [[a1, b1], [a2, b2]] = basis;
  const c1 = divNearest(b2 * k, n);
  const c2 = divNearest(-b1 * k, n);
  let k1 = k - c1 * a1 - c2 * a2;
  let k2 = -c1 * b1 - c2 * b2;
  const k1neg = k1 < _0n4;
  const k2neg = k2 < _0n4;
  if (k1neg)
    k1 = -k1;
  if (k2neg)
    k2 = -k2;
  const MAX_NUM = bitMask(Math.ceil(bitLen(n) / 2)) + _1n4;
  if (k1 < _0n4 || k1 >= MAX_NUM || k2 < _0n4 || k2 >= MAX_NUM) {
    throw new Error("splitScalar (endomorphism): failed, k=" + k);
  }
  return { k1neg, k1, k2neg, k2 };
}
function validateSigVerOpts(opts) {
  if (opts.lowS !== void 0)
    abool("lowS", opts.lowS);
  if (opts.prehash !== void 0)
    abool("prehash", opts.prehash);
}
var DERErr = class extends Error {
  constructor(m = "") {
    super(m);
  }
};
var DER = {
  // asn.1 DER encoding utils
  Err: DERErr,
  // Basic building block is TLV (Tag-Length-Value)
  _tlv: {
    encode: (tag, data) => {
      const { Err: E } = DER;
      if (tag < 0 || tag > 256)
        throw new E("tlv.encode: wrong tag");
      if (data.length & 1)
        throw new E("tlv.encode: unpadded data");
      const dataLen = data.length / 2;
      const len = numberToHexUnpadded(dataLen);
      if (len.length / 2 & 128)
        throw new E("tlv.encode: long form length too big");
      const lenLen = dataLen > 127 ? numberToHexUnpadded(len.length / 2 | 128) : "";
      const t = numberToHexUnpadded(tag);
      return t + lenLen + len + data;
    },
    // v - value, l - left bytes (unparsed)
    decode(tag, data) {
      const { Err: E } = DER;
      let pos = 0;
      if (tag < 0 || tag > 256)
        throw new E("tlv.encode: wrong tag");
      if (data.length < 2 || data[pos++] !== tag)
        throw new E("tlv.decode: wrong tlv");
      const first = data[pos++];
      const isLong = !!(first & 128);
      let length = 0;
      if (!isLong)
        length = first;
      else {
        const lenLen = first & 127;
        if (!lenLen)
          throw new E("tlv.decode(long): indefinite length not supported");
        if (lenLen > 4)
          throw new E("tlv.decode(long): byte length is too big");
        const lengthBytes = data.subarray(pos, pos + lenLen);
        if (lengthBytes.length !== lenLen)
          throw new E("tlv.decode: length bytes not complete");
        if (lengthBytes[0] === 0)
          throw new E("tlv.decode(long): zero leftmost byte");
        for (const b of lengthBytes)
          length = length << 8 | b;
        pos += lenLen;
        if (length < 128)
          throw new E("tlv.decode(long): not minimal encoding");
      }
      const v = data.subarray(pos, pos + length);
      if (v.length !== length)
        throw new E("tlv.decode: wrong value length");
      return { v, l: data.subarray(pos + length) };
    }
  },
  // https://crypto.stackexchange.com/a/57734 Leftmost bit of first byte is 'negative' flag,
  // since we always use positive integers here. It must always be empty:
  // - add zero byte if exists
  // - if next byte doesn't have a flag, leading zero is not allowed (minimal encoding)
  _int: {
    encode(num) {
      const { Err: E } = DER;
      if (num < _0n4)
        throw new E("integer: negative integers are not allowed");
      let hex = numberToHexUnpadded(num);
      if (Number.parseInt(hex[0], 16) & 8)
        hex = "00" + hex;
      if (hex.length & 1)
        throw new E("unexpected DER parsing assertion: unpadded hex");
      return hex;
    },
    decode(data) {
      const { Err: E } = DER;
      if (data[0] & 128)
        throw new E("invalid signature integer: negative");
      if (data[0] === 0 && !(data[1] & 128))
        throw new E("invalid signature integer: unnecessary leading zero");
      return bytesToNumberBE(data);
    }
  },
  toSig(hex) {
    const { Err: E, _int: int, _tlv: tlv } = DER;
    const data = ensureBytes("signature", hex);
    const { v: seqBytes, l: seqLeftBytes } = tlv.decode(48, data);
    if (seqLeftBytes.length)
      throw new E("invalid signature: left bytes after parsing");
    const { v: rBytes, l: rLeftBytes } = tlv.decode(2, seqBytes);
    const { v: sBytes, l: sLeftBytes } = tlv.decode(2, rLeftBytes);
    if (sLeftBytes.length)
      throw new E("invalid signature: left bytes after parsing");
    return { r: int.decode(rBytes), s: int.decode(sBytes) };
  },
  hexFromSig(sig) {
    const { _tlv: tlv, _int: int } = DER;
    const rs = tlv.encode(2, int.encode(sig.r));
    const ss = tlv.encode(2, int.encode(sig.s));
    const seq = rs + ss;
    return tlv.encode(48, seq);
  }
};
var _0n4 = BigInt(0);
var _1n4 = BigInt(1);
var _2n2 = BigInt(2);
var _3n2 = BigInt(3);
var _4n2 = BigInt(4);
function _legacyHelperEquat(Fp2, a, b) {
  function weierstrassEquation(x) {
    const x2 = Fp2.sqr(x);
    const x3 = Fp2.mul(x2, x);
    return Fp2.add(Fp2.add(x3, Fp2.mul(x, a)), b);
  }
  return weierstrassEquation;
}
function _normFnElement(Fn2, key) {
  const { BYTES: expected } = Fn2;
  let num;
  if (typeof key === "bigint") {
    num = key;
  } else {
    let bytes = ensureBytes("private key", key);
    try {
      num = Fn2.fromBytes(bytes);
    } catch (error) {
      throw new Error(`invalid private key: expected ui8a of size ${expected}, got ${typeof key}`);
    }
  }
  if (!Fn2.isValidNot0(num))
    throw new Error("invalid private key: out of range [1..N-1]");
  return num;
}
function weierstrassN(CURVE, curveOpts = {}) {
  const { Fp: Fp2, Fn: Fn2 } = _createCurveFields("weierstrass", CURVE, curveOpts);
  const { h: cofactor, n: CURVE_ORDER } = CURVE;
  _validateObject(curveOpts, {}, {
    allowInfinityPoint: "boolean",
    clearCofactor: "function",
    isTorsionFree: "function",
    fromBytes: "function",
    toBytes: "function",
    endo: "object",
    wrapPrivateKey: "boolean"
  });
  const { endo } = curveOpts;
  if (endo) {
    if (!Fp2.is0(CURVE.a) || typeof endo.beta !== "bigint" || !Array.isArray(endo.basises)) {
      throw new Error('invalid endo: expected "beta": bigint and "basises": array');
    }
  }
  function assertCompressionIsSupported() {
    if (!Fp2.isOdd)
      throw new Error("compression is not supported: Field does not have .isOdd()");
  }
  function pointToBytes(_c, point, isCompressed) {
    const { x, y } = point.toAffine();
    const bx = Fp2.toBytes(x);
    abool("isCompressed", isCompressed);
    if (isCompressed) {
      assertCompressionIsSupported();
      const hasEvenY = !Fp2.isOdd(y);
      return concatBytes(pprefix(hasEvenY), bx);
    } else {
      return concatBytes(Uint8Array.of(4), bx, Fp2.toBytes(y));
    }
  }
  function pointFromBytes(bytes) {
    abytes(bytes);
    const L = Fp2.BYTES;
    const LC = L + 1;
    const LU = 2 * L + 1;
    const length = bytes.length;
    const head = bytes[0];
    const tail = bytes.subarray(1);
    if (length === LC && (head === 2 || head === 3)) {
      const x = Fp2.fromBytes(tail);
      if (!Fp2.isValid(x))
        throw new Error("bad point: is not on curve, wrong x");
      const y2 = weierstrassEquation(x);
      let y;
      try {
        y = Fp2.sqrt(y2);
      } catch (sqrtError) {
        const err = sqrtError instanceof Error ? ": " + sqrtError.message : "";
        throw new Error("bad point: is not on curve, sqrt error" + err);
      }
      assertCompressionIsSupported();
      const isYOdd = Fp2.isOdd(y);
      const isHeadOdd = (head & 1) === 1;
      if (isHeadOdd !== isYOdd)
        y = Fp2.neg(y);
      return { x, y };
    } else if (length === LU && head === 4) {
      const x = Fp2.fromBytes(tail.subarray(L * 0, L * 1));
      const y = Fp2.fromBytes(tail.subarray(L * 1, L * 2));
      if (!isValidXY(x, y))
        throw new Error("bad point: is not on curve");
      return { x, y };
    } else {
      throw new Error(`bad point: got length ${length}, expected compressed=${LC} or uncompressed=${LU}`);
    }
  }
  const toBytes2 = curveOpts.toBytes || pointToBytes;
  const fromBytes = curveOpts.fromBytes || pointFromBytes;
  const weierstrassEquation = _legacyHelperEquat(Fp2, CURVE.a, CURVE.b);
  function isValidXY(x, y) {
    const left = Fp2.sqr(y);
    const right = weierstrassEquation(x);
    return Fp2.eql(left, right);
  }
  if (!isValidXY(CURVE.Gx, CURVE.Gy))
    throw new Error("bad curve params: generator point");
  const _4a3 = Fp2.mul(Fp2.pow(CURVE.a, _3n2), _4n2);
  const _27b2 = Fp2.mul(Fp2.sqr(CURVE.b), BigInt(27));
  if (Fp2.is0(Fp2.add(_4a3, _27b2)))
    throw new Error("bad curve params: a or b");
  function acoord(title, n, banZero = false) {
    if (!Fp2.isValid(n) || banZero && Fp2.is0(n))
      throw new Error(`bad point coordinate ${title}`);
    return n;
  }
  function aprjpoint(other) {
    if (!(other instanceof Point2))
      throw new Error("ProjectivePoint expected");
  }
  function splitEndoScalarN(k) {
    if (!endo || !endo.basises)
      throw new Error("no endo");
    return _splitEndoScalar(k, endo.basises, Fn2.ORDER);
  }
  const toAffineMemo = memoized((p, iz) => {
    const { X, Y, Z } = p;
    if (Fp2.eql(Z, Fp2.ONE))
      return { x: X, y: Y };
    const is0 = p.is0();
    if (iz == null)
      iz = is0 ? Fp2.ONE : Fp2.inv(Z);
    const x = Fp2.mul(X, iz);
    const y = Fp2.mul(Y, iz);
    const zz = Fp2.mul(Z, iz);
    if (is0)
      return { x: Fp2.ZERO, y: Fp2.ZERO };
    if (!Fp2.eql(zz, Fp2.ONE))
      throw new Error("invZ was invalid");
    return { x, y };
  });
  const assertValidMemo = memoized((p) => {
    if (p.is0()) {
      if (curveOpts.allowInfinityPoint && !Fp2.is0(p.Y))
        return;
      throw new Error("bad point: ZERO");
    }
    const { x, y } = p.toAffine();
    if (!Fp2.isValid(x) || !Fp2.isValid(y))
      throw new Error("bad point: x or y not field elements");
    if (!isValidXY(x, y))
      throw new Error("bad point: equation left != right");
    if (!p.isTorsionFree())
      throw new Error("bad point: not in prime-order subgroup");
    return true;
  });
  function finishEndo(endoBeta, k1p, k2p, k1neg, k2neg) {
    k2p = new Point2(Fp2.mul(k2p.X, endoBeta), k2p.Y, k2p.Z);
    k1p = negateCt(k1neg, k1p);
    k2p = negateCt(k2neg, k2p);
    return k1p.add(k2p);
  }
  class Point2 {
    /** Does NOT validate if the point is valid. Use `.assertValidity()`. */
    constructor(X, Y, Z) {
      this.X = acoord("x", X);
      this.Y = acoord("y", Y, true);
      this.Z = acoord("z", Z);
      Object.freeze(this);
    }
    /** Does NOT validate if the point is valid. Use `.assertValidity()`. */
    static fromAffine(p) {
      const { x, y } = p || {};
      if (!p || !Fp2.isValid(x) || !Fp2.isValid(y))
        throw new Error("invalid affine point");
      if (p instanceof Point2)
        throw new Error("projective point not allowed");
      if (Fp2.is0(x) && Fp2.is0(y))
        return Point2.ZERO;
      return new Point2(x, y, Fp2.ONE);
    }
    get x() {
      return this.toAffine().x;
    }
    get y() {
      return this.toAffine().y;
    }
    // TODO: remove
    get px() {
      return this.X;
    }
    get py() {
      return this.X;
    }
    get pz() {
      return this.Z;
    }
    static normalizeZ(points) {
      return normalizeZ(Point2, points);
    }
    static fromBytes(bytes) {
      abytes(bytes);
      return Point2.fromHex(bytes);
    }
    /** Converts hash string or Uint8Array to Point. */
    static fromHex(hex) {
      const P = Point2.fromAffine(fromBytes(ensureBytes("pointHex", hex)));
      P.assertValidity();
      return P;
    }
    /** Multiplies generator point by privateKey. */
    static fromPrivateKey(privateKey) {
      return Point2.BASE.multiply(_normFnElement(Fn2, privateKey));
    }
    // TODO: remove
    static msm(points, scalars) {
      return pippenger(Point2, Fn2, points, scalars);
    }
    _setWindowSize(windowSize) {
      this.precompute(windowSize);
    }
    /**
     *
     * @param windowSize
     * @param isLazy true will defer table computation until the first multiplication
     * @returns
     */
    precompute(windowSize = 8, isLazy = true) {
      wnaf.createCache(this, windowSize);
      if (!isLazy)
        this.multiply(_3n2);
      return this;
    }
    // TODO: return `this`
    /** A point on curve is valid if it conforms to equation. */
    assertValidity() {
      assertValidMemo(this);
    }
    hasEvenY() {
      const { y } = this.toAffine();
      if (!Fp2.isOdd)
        throw new Error("Field doesn't support isOdd");
      return !Fp2.isOdd(y);
    }
    /** Compare one point to another. */
    equals(other) {
      aprjpoint(other);
      const { X: X1, Y: Y1, Z: Z1 } = this;
      const { X: X2, Y: Y2, Z: Z2 } = other;
      const U1 = Fp2.eql(Fp2.mul(X1, Z2), Fp2.mul(X2, Z1));
      const U2 = Fp2.eql(Fp2.mul(Y1, Z2), Fp2.mul(Y2, Z1));
      return U1 && U2;
    }
    /** Flips point to one corresponding to (x, -y) in Affine coordinates. */
    negate() {
      return new Point2(this.X, Fp2.neg(this.Y), this.Z);
    }
    // Renes-Costello-Batina exception-free doubling formula.
    // There is 30% faster Jacobian formula, but it is not complete.
    // https://eprint.iacr.org/2015/1060, algorithm 3
    // Cost: 8M + 3S + 3*a + 2*b3 + 15add.
    double() {
      const { a, b } = CURVE;
      const b3 = Fp2.mul(b, _3n2);
      const { X: X1, Y: Y1, Z: Z1 } = this;
      let X3 = Fp2.ZERO, Y3 = Fp2.ZERO, Z3 = Fp2.ZERO;
      let t0 = Fp2.mul(X1, X1);
      let t1 = Fp2.mul(Y1, Y1);
      let t2 = Fp2.mul(Z1, Z1);
      let t3 = Fp2.mul(X1, Y1);
      t3 = Fp2.add(t3, t3);
      Z3 = Fp2.mul(X1, Z1);
      Z3 = Fp2.add(Z3, Z3);
      X3 = Fp2.mul(a, Z3);
      Y3 = Fp2.mul(b3, t2);
      Y3 = Fp2.add(X3, Y3);
      X3 = Fp2.sub(t1, Y3);
      Y3 = Fp2.add(t1, Y3);
      Y3 = Fp2.mul(X3, Y3);
      X3 = Fp2.mul(t3, X3);
      Z3 = Fp2.mul(b3, Z3);
      t2 = Fp2.mul(a, t2);
      t3 = Fp2.sub(t0, t2);
      t3 = Fp2.mul(a, t3);
      t3 = Fp2.add(t3, Z3);
      Z3 = Fp2.add(t0, t0);
      t0 = Fp2.add(Z3, t0);
      t0 = Fp2.add(t0, t2);
      t0 = Fp2.mul(t0, t3);
      Y3 = Fp2.add(Y3, t0);
      t2 = Fp2.mul(Y1, Z1);
      t2 = Fp2.add(t2, t2);
      t0 = Fp2.mul(t2, t3);
      X3 = Fp2.sub(X3, t0);
      Z3 = Fp2.mul(t2, t1);
      Z3 = Fp2.add(Z3, Z3);
      Z3 = Fp2.add(Z3, Z3);
      return new Point2(X3, Y3, Z3);
    }
    // Renes-Costello-Batina exception-free addition formula.
    // There is 30% faster Jacobian formula, but it is not complete.
    // https://eprint.iacr.org/2015/1060, algorithm 1
    // Cost: 12M + 0S + 3*a + 3*b3 + 23add.
    add(other) {
      aprjpoint(other);
      const { X: X1, Y: Y1, Z: Z1 } = this;
      const { X: X2, Y: Y2, Z: Z2 } = other;
      let X3 = Fp2.ZERO, Y3 = Fp2.ZERO, Z3 = Fp2.ZERO;
      const a = CURVE.a;
      const b3 = Fp2.mul(CURVE.b, _3n2);
      let t0 = Fp2.mul(X1, X2);
      let t1 = Fp2.mul(Y1, Y2);
      let t2 = Fp2.mul(Z1, Z2);
      let t3 = Fp2.add(X1, Y1);
      let t4 = Fp2.add(X2, Y2);
      t3 = Fp2.mul(t3, t4);
      t4 = Fp2.add(t0, t1);
      t3 = Fp2.sub(t3, t4);
      t4 = Fp2.add(X1, Z1);
      let t5 = Fp2.add(X2, Z2);
      t4 = Fp2.mul(t4, t5);
      t5 = Fp2.add(t0, t2);
      t4 = Fp2.sub(t4, t5);
      t5 = Fp2.add(Y1, Z1);
      X3 = Fp2.add(Y2, Z2);
      t5 = Fp2.mul(t5, X3);
      X3 = Fp2.add(t1, t2);
      t5 = Fp2.sub(t5, X3);
      Z3 = Fp2.mul(a, t4);
      X3 = Fp2.mul(b3, t2);
      Z3 = Fp2.add(X3, Z3);
      X3 = Fp2.sub(t1, Z3);
      Z3 = Fp2.add(t1, Z3);
      Y3 = Fp2.mul(X3, Z3);
      t1 = Fp2.add(t0, t0);
      t1 = Fp2.add(t1, t0);
      t2 = Fp2.mul(a, t2);
      t4 = Fp2.mul(b3, t4);
      t1 = Fp2.add(t1, t2);
      t2 = Fp2.sub(t0, t2);
      t2 = Fp2.mul(a, t2);
      t4 = Fp2.add(t4, t2);
      t0 = Fp2.mul(t1, t4);
      Y3 = Fp2.add(Y3, t0);
      t0 = Fp2.mul(t5, t4);
      X3 = Fp2.mul(t3, X3);
      X3 = Fp2.sub(X3, t0);
      t0 = Fp2.mul(t3, t1);
      Z3 = Fp2.mul(t5, Z3);
      Z3 = Fp2.add(Z3, t0);
      return new Point2(X3, Y3, Z3);
    }
    subtract(other) {
      return this.add(other.negate());
    }
    is0() {
      return this.equals(Point2.ZERO);
    }
    /**
     * Constant time multiplication.
     * Uses wNAF method. Windowed method may be 10% faster,
     * but takes 2x longer to generate and consumes 2x memory.
     * Uses precomputes when available.
     * Uses endomorphism for Koblitz curves.
     * @param scalar by which the point would be multiplied
     * @returns New point
     */
    multiply(scalar) {
      const { endo: endo2 } = curveOpts;
      if (!Fn2.isValidNot0(scalar))
        throw new Error("invalid scalar: out of range");
      let point, fake;
      const mul = (n) => wnaf.cached(this, n, (p) => normalizeZ(Point2, p));
      if (endo2) {
        const { k1neg, k1, k2neg, k2 } = splitEndoScalarN(scalar);
        const { p: k1p, f: k1f } = mul(k1);
        const { p: k2p, f: k2f } = mul(k2);
        fake = k1f.add(k2f);
        point = finishEndo(endo2.beta, k1p, k2p, k1neg, k2neg);
      } else {
        const { p, f } = mul(scalar);
        point = p;
        fake = f;
      }
      return normalizeZ(Point2, [point, fake])[0];
    }
    /**
     * Non-constant-time multiplication. Uses double-and-add algorithm.
     * It's faster, but should only be used when you don't care about
     * an exposed secret key e.g. sig verification, which works over *public* keys.
     */
    multiplyUnsafe(sc) {
      const { endo: endo2 } = curveOpts;
      const p = this;
      if (!Fn2.isValid(sc))
        throw new Error("invalid scalar: out of range");
      if (sc === _0n4 || p.is0())
        return Point2.ZERO;
      if (sc === _1n4)
        return p;
      if (wnaf.hasCache(this))
        return this.multiply(sc);
      if (endo2) {
        const { k1neg, k1, k2neg, k2 } = splitEndoScalarN(sc);
        const { p1, p2 } = mulEndoUnsafe(Point2, p, k1, k2);
        return finishEndo(endo2.beta, p1, p2, k1neg, k2neg);
      } else {
        return wnaf.unsafe(p, sc);
      }
    }
    multiplyAndAddUnsafe(Q, a, b) {
      const sum = this.multiplyUnsafe(a).add(Q.multiplyUnsafe(b));
      return sum.is0() ? void 0 : sum;
    }
    /**
     * Converts Projective point to affine (x, y) coordinates.
     * @param invertedZ Z^-1 (inverted zero) - optional, precomputation is useful for invertBatch
     */
    toAffine(invertedZ) {
      return toAffineMemo(this, invertedZ);
    }
    /**
     * Checks whether Point is free of torsion elements (is in prime subgroup).
     * Always torsion-free for cofactor=1 curves.
     */
    isTorsionFree() {
      const { isTorsionFree } = curveOpts;
      if (cofactor === _1n4)
        return true;
      if (isTorsionFree)
        return isTorsionFree(Point2, this);
      return wnaf.unsafe(this, CURVE_ORDER).is0();
    }
    clearCofactor() {
      const { clearCofactor } = curveOpts;
      if (cofactor === _1n4)
        return this;
      if (clearCofactor)
        return clearCofactor(Point2, this);
      return this.multiplyUnsafe(cofactor);
    }
    isSmallOrder() {
      return this.multiplyUnsafe(cofactor).is0();
    }
    toBytes(isCompressed = true) {
      abool("isCompressed", isCompressed);
      this.assertValidity();
      return toBytes2(Point2, this, isCompressed);
    }
    /** @deprecated use `toBytes` */
    toRawBytes(isCompressed = true) {
      return this.toBytes(isCompressed);
    }
    toHex(isCompressed = true) {
      return bytesToHex(this.toBytes(isCompressed));
    }
    toString() {
      return `<Point ${this.is0() ? "ZERO" : this.toHex()}>`;
    }
  }
  Point2.BASE = new Point2(CURVE.Gx, CURVE.Gy, Fp2.ONE);
  Point2.ZERO = new Point2(Fp2.ZERO, Fp2.ONE, Fp2.ZERO);
  Point2.Fp = Fp2;
  Point2.Fn = Fn2;
  const bits = Fn2.BITS;
  const wnaf = new wNAF(Point2, curveOpts.endo ? Math.ceil(bits / 2) : bits);
  return Point2;
}
function pprefix(hasEvenY) {
  return Uint8Array.of(hasEvenY ? 2 : 3);
}
function ecdsa(Point2, hash, ecdsaOpts = {}) {
  ahash(hash);
  _validateObject(ecdsaOpts, {}, {
    hmac: "function",
    lowS: "boolean",
    randomBytes: "function",
    bits2int: "function",
    bits2int_modN: "function"
  });
  const randomBytes_ = ecdsaOpts.randomBytes || randomBytes;
  const hmac_ = ecdsaOpts.hmac || ((key, ...msgs) => hmac(hash, key, concatBytes(...msgs)));
  const { Fp: Fp2, Fn: Fn2 } = Point2;
  const { ORDER: CURVE_ORDER, BITS: fnBits } = Fn2;
  const seedLen = getMinHashLength(CURVE_ORDER);
  const lengths = {
    secret: Fn2.BYTES,
    public: 1 + Fp2.BYTES,
    publicUncompressed: 1 + 2 * Fp2.BYTES,
    signature: 2 * Fn2.BYTES,
    seed: seedLen
  };
  function isBiggerThanHalfOrder(number2) {
    const HALF = CURVE_ORDER >> _1n4;
    return number2 > HALF;
  }
  function normalizeS(s) {
    return isBiggerThanHalfOrder(s) ? Fn2.neg(s) : s;
  }
  function aValidRS(title, num) {
    if (!Fn2.isValidNot0(num))
      throw new Error(`invalid signature ${title}: out of range 1..CURVE.n`);
  }
  class Signature {
    constructor(r, s, recovery) {
      aValidRS("r", r);
      aValidRS("s", s);
      this.r = r;
      this.s = s;
      if (recovery != null)
        this.recovery = recovery;
      Object.freeze(this);
    }
    static fromBytes(bytes, format = "compact") {
      if (format === "compact") {
        const L = Fn2.BYTES;
        abytes(bytes, L * 2);
        const r = bytes.subarray(0, L);
        const s = bytes.subarray(L, L * 2);
        return new Signature(Fn2.fromBytes(r), Fn2.fromBytes(s));
      }
      if (format === "der") {
        abytes(bytes);
        const { r, s } = DER.toSig(bytes);
        return new Signature(r, s);
      }
      throw new Error("invalid format");
    }
    static fromHex(hex, format) {
      return this.fromBytes(hexToBytes(hex), format);
    }
    addRecoveryBit(recovery) {
      return new Signature(this.r, this.s, recovery);
    }
    // ProjPointType<bigint>
    recoverPublicKey(msgHash) {
      const FIELD_ORDER = Fp2.ORDER;
      const { r, s, recovery: rec } = this;
      if (rec == null || ![0, 1, 2, 3].includes(rec))
        throw new Error("recovery id invalid");
      const hasCofactor = CURVE_ORDER * _2n2 < FIELD_ORDER;
      if (hasCofactor && rec > 1)
        throw new Error("recovery id is ambiguous for h>1 curve");
      const radj = rec === 2 || rec === 3 ? r + CURVE_ORDER : r;
      if (!Fp2.isValid(radj))
        throw new Error("recovery id 2 or 3 invalid");
      const x = Fp2.toBytes(radj);
      const R = Point2.fromHex(concatBytes(pprefix((rec & 1) === 0), x));
      const ir = Fn2.inv(radj);
      const h = bits2int_modN(ensureBytes("msgHash", msgHash));
      const u1 = Fn2.create(-h * ir);
      const u2 = Fn2.create(s * ir);
      const Q = Point2.BASE.multiplyUnsafe(u1).add(R.multiplyUnsafe(u2));
      if (Q.is0())
        throw new Error("point at infinify");
      Q.assertValidity();
      return Q;
    }
    // Signatures should be low-s, to prevent malleability.
    hasHighS() {
      return isBiggerThanHalfOrder(this.s);
    }
    normalizeS() {
      return this.hasHighS() ? new Signature(this.r, Fn2.neg(this.s), this.recovery) : this;
    }
    toBytes(format = "compact") {
      if (format === "compact")
        return concatBytes(Fn2.toBytes(this.r), Fn2.toBytes(this.s));
      if (format === "der")
        return hexToBytes(DER.hexFromSig(this));
      throw new Error("invalid format");
    }
    toHex(format) {
      return bytesToHex(this.toBytes(format));
    }
    // TODO: remove
    assertValidity() {
    }
    static fromCompact(hex) {
      return Signature.fromBytes(ensureBytes("sig", hex), "compact");
    }
    static fromDER(hex) {
      return Signature.fromBytes(ensureBytes("sig", hex), "der");
    }
    toDERRawBytes() {
      return this.toBytes("der");
    }
    toDERHex() {
      return bytesToHex(this.toBytes("der"));
    }
    toCompactRawBytes() {
      return this.toBytes("compact");
    }
    toCompactHex() {
      return bytesToHex(this.toBytes("compact"));
    }
  }
  function isValidSecretKey(privateKey) {
    try {
      return !!_normFnElement(Fn2, privateKey);
    } catch (error) {
      return false;
    }
  }
  function isValidPublicKey(publicKey, isCompressed) {
    try {
      const l = publicKey.length;
      if (isCompressed === true && l !== lengths.public)
        return false;
      if (isCompressed === false && l !== lengths.publicUncompressed)
        return false;
      return !!Point2.fromBytes(publicKey);
    } catch (error) {
      return false;
    }
  }
  function randomSecretKey(seed = randomBytes_(seedLen)) {
    return mapHashToField(seed, CURVE_ORDER);
  }
  const utils = {
    isValidSecretKey,
    isValidPublicKey,
    randomSecretKey,
    // TODO: remove
    isValidPrivateKey: isValidSecretKey,
    randomPrivateKey: randomSecretKey,
    normPrivateKeyToScalar: (key) => _normFnElement(Fn2, key),
    precompute(windowSize = 8, point = Point2.BASE) {
      return point.precompute(windowSize, false);
    }
  };
  function getPublicKey(secretKey, isCompressed = true) {
    return Point2.BASE.multiply(_normFnElement(Fn2, secretKey)).toBytes(isCompressed);
  }
  function isProbPub(item) {
    if (typeof item === "bigint")
      return false;
    if (item instanceof Point2)
      return true;
    if (Fn2.allowedLengths || lengths.secret === lengths.public)
      return void 0;
    const l = ensureBytes("key", item).length;
    return l === lengths.public || l === lengths.publicUncompressed;
  }
  function getSharedSecret(secretKeyA, publicKeyB, isCompressed = true) {
    if (isProbPub(secretKeyA) === true)
      throw new Error("first arg must be private key");
    if (isProbPub(publicKeyB) === false)
      throw new Error("second arg must be public key");
    const s = _normFnElement(Fn2, secretKeyA);
    const b = Point2.fromHex(publicKeyB);
    return b.multiply(s).toBytes(isCompressed);
  }
  const bits2int = ecdsaOpts.bits2int || function(bytes) {
    if (bytes.length > 8192)
      throw new Error("input is too large");
    const num = bytesToNumberBE(bytes);
    const delta = bytes.length * 8 - fnBits;
    return delta > 0 ? num >> BigInt(delta) : num;
  };
  const bits2int_modN = ecdsaOpts.bits2int_modN || function(bytes) {
    return Fn2.create(bits2int(bytes));
  };
  const ORDER_MASK = bitMask(fnBits);
  function int2octets(num) {
    aInRange("num < 2^" + fnBits, num, _0n4, ORDER_MASK);
    return Fn2.toBytes(num);
  }
  function prepSig(msgHash, privateKey, opts = defaultSigOpts) {
    if (["recovered", "canonical"].some((k) => k in opts))
      throw new Error("sign() legacy options not supported");
    let { lowS, prehash, extraEntropy: ent } = opts;
    if (lowS == null)
      lowS = true;
    msgHash = ensureBytes("msgHash", msgHash);
    validateSigVerOpts(opts);
    if (prehash)
      msgHash = ensureBytes("prehashed msgHash", hash(msgHash));
    const h1int = bits2int_modN(msgHash);
    const d = _normFnElement(Fn2, privateKey);
    const seedArgs = [int2octets(d), int2octets(h1int)];
    if (ent != null && ent !== false) {
      const e = ent === true ? randomBytes_(lengths.secret) : ent;
      seedArgs.push(ensureBytes("extraEntropy", e));
    }
    const seed = concatBytes(...seedArgs);
    const m = h1int;
    function k2sig(kBytes) {
      const k = bits2int(kBytes);
      if (!Fn2.isValidNot0(k))
        return;
      const ik = Fn2.inv(k);
      const q = Point2.BASE.multiply(k).toAffine();
      const r = Fn2.create(q.x);
      if (r === _0n4)
        return;
      const s = Fn2.create(ik * Fn2.create(m + r * d));
      if (s === _0n4)
        return;
      let recovery = (q.x === r ? 0 : 2) | Number(q.y & _1n4);
      let normS = s;
      if (lowS && isBiggerThanHalfOrder(s)) {
        normS = normalizeS(s);
        recovery ^= 1;
      }
      return new Signature(r, normS, recovery);
    }
    return { seed, k2sig };
  }
  const defaultSigOpts = { lowS: ecdsaOpts.lowS, prehash: false };
  const defaultVerOpts = { lowS: ecdsaOpts.lowS, prehash: false };
  function sign(msgHash, secretKey, opts = defaultSigOpts) {
    const { seed, k2sig } = prepSig(msgHash, secretKey, opts);
    const drbg = createHmacDrbg(hash.outputLen, Fn2.BYTES, hmac_);
    return drbg(seed, k2sig);
  }
  Point2.BASE.precompute(8);
  function verify(signature, msgHash, publicKey, opts = defaultVerOpts) {
    const sg = signature;
    msgHash = ensureBytes("msgHash", msgHash);
    publicKey = ensureBytes("publicKey", publicKey);
    validateSigVerOpts(opts);
    const { lowS, prehash, format } = opts;
    if ("strict" in opts)
      throw new Error("options.strict was renamed to lowS");
    let _sig = void 0;
    let P;
    if (format === void 0) {
      const isHex2 = typeof sg === "string" || isBytes2(sg);
      const isObj = !isHex2 && sg !== null && typeof sg === "object" && typeof sg.r === "bigint" && typeof sg.s === "bigint";
      if (!isHex2 && !isObj)
        throw new Error("invalid signature, expected Uint8Array, hex string or Signature instance");
      if (isObj) {
        _sig = new Signature(sg.r, sg.s);
      } else if (isHex2) {
        try {
          _sig = Signature.fromDER(sg);
        } catch (derError) {
          if (!(derError instanceof DER.Err))
            throw derError;
        }
        if (!_sig) {
          try {
            _sig = Signature.fromCompact(sg);
          } catch (error) {
            return false;
          }
        }
      }
    } else {
      if (format === "compact" || format === "der") {
        if (typeof sg !== "string" && !isBytes2(sg))
          throw new Error('"der" / "compact" format expects Uint8Array signature');
        _sig = Signature.fromBytes(ensureBytes("sig", sg), format);
      } else if (format === "js") {
        if (!(sg instanceof Signature))
          throw new Error('"js" format expects Signature instance');
        _sig = sg;
      } else {
        throw new Error('format must be "compact", "der" or "js"');
      }
    }
    if (!_sig)
      return false;
    try {
      P = Point2.fromHex(publicKey);
      if (lowS && _sig.hasHighS())
        return false;
      if (prehash)
        msgHash = hash(msgHash);
      const { r, s } = _sig;
      const h = bits2int_modN(msgHash);
      const is2 = Fn2.inv(s);
      const u1 = Fn2.create(h * is2);
      const u2 = Fn2.create(r * is2);
      const R = Point2.BASE.multiplyUnsafe(u1).add(P.multiplyUnsafe(u2));
      if (R.is0())
        return false;
      const v = Fn2.create(R.x);
      return v === r;
    } catch (e) {
      return false;
    }
  }
  function keygen(seed) {
    const secretKey = utils.randomSecretKey(seed);
    return { secretKey, publicKey: getPublicKey(secretKey) };
  }
  return Object.freeze({
    keygen,
    getPublicKey,
    sign,
    verify,
    getSharedSecret,
    utils,
    Point: Point2,
    Signature,
    info: { type: "weierstrass", lengths, publicKeyHasPrefix: true }
  });
}
function _weierstrass_legacy_opts_to_new(c) {
  const CURVE = {
    a: c.a,
    b: c.b,
    p: c.Fp.ORDER,
    n: c.n,
    h: c.h,
    Gx: c.Gx,
    Gy: c.Gy
  };
  const Fp2 = c.Fp;
  let allowedLengths = c.allowedPrivateKeyLengths ? Array.from(new Set(c.allowedPrivateKeyLengths.map((l) => Math.ceil(l / 2)))) : void 0;
  const Fn2 = Field(CURVE.n, {
    BITS: c.nBitLength,
    allowedLengths,
    modOnDecode: c.wrapPrivateKey
  });
  const curveOpts = {
    Fp: Fp2,
    Fn: Fn2,
    allowInfinityPoint: c.allowInfinityPoint,
    endo: c.endo,
    isTorsionFree: c.isTorsionFree,
    clearCofactor: c.clearCofactor,
    fromBytes: c.fromBytes,
    toBytes: c.toBytes
  };
  return { CURVE, curveOpts };
}
function _ecdsa_legacy_opts_to_new(c) {
  const { CURVE, curveOpts } = _weierstrass_legacy_opts_to_new(c);
  const ecdsaOpts = {
    hmac: c.hmac,
    randomBytes: c.randomBytes,
    lowS: c.lowS,
    bits2int: c.bits2int,
    bits2int_modN: c.bits2int_modN
  };
  return { CURVE, curveOpts, hash: c.hash, ecdsaOpts };
}
function _ecdsa_new_output_to_legacy(c, ecdsa2) {
  return Object.assign({}, ecdsa2, {
    ProjectivePoint: ecdsa2.Point,
    CURVE: c
  });
}
function weierstrass(c) {
  const { CURVE, curveOpts, hash, ecdsaOpts } = _ecdsa_legacy_opts_to_new(c);
  const Point2 = weierstrassN(CURVE, curveOpts);
  const signs = ecdsa(Point2, hash, ecdsaOpts);
  return _ecdsa_new_output_to_legacy(c, signs);
}

// node_modules/@noble/curves/esm/_shortw_utils.js
function createCurve(curveDef, defHash) {
  const create = (hash) => weierstrass({ ...curveDef, hash });
  return { ...create(defHash), create };
}

// node_modules/@noble/curves/esm/nist.js
var p256_CURVE = {
  p: BigInt("0xffffffff00000001000000000000000000000000ffffffffffffffffffffffff"),
  n: BigInt("0xffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551"),
  h: BigInt(1),
  a: BigInt("0xffffffff00000001000000000000000000000000fffffffffffffffffffffffc"),
  b: BigInt("0x5ac635d8aa3a93e7b3ebbd55769886bc651d06b0cc53b0f63bce3c3e27d2604b"),
  Gx: BigInt("0x6b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"),
  Gy: BigInt("0x4fe342e2fe1a7f9b8ee7eb4a7c0f9e162bce33576b315ececbb6406837bf51f5")
};
var p384_CURVE = {
  p: BigInt("0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffeffffffff0000000000000000ffffffff"),
  n: BigInt("0xffffffffffffffffffffffffffffffffffffffffffffffffc7634d81f4372ddf581a0db248b0a77aecec196accc52973"),
  h: BigInt(1),
  a: BigInt("0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffeffffffff0000000000000000fffffffc"),
  b: BigInt("0xb3312fa7e23ee7e4988e056be3f82d19181d9c6efe8141120314088f5013875ac656398d8a2ed19d2a85c8edd3ec2aef"),
  Gx: BigInt("0xaa87ca22be8b05378eb1c71ef320ad746e1d3b628ba79b9859f741e082542a385502f25dbf55296c3a545e3872760ab7"),
  Gy: BigInt("0x3617de4a96262c6f5d9e98bf9292dc29f8f41dbd289a147ce9da3113b5f0b8c00a60b1ce1d7e819d7a431d7c90ea0e5f")
};
var p521_CURVE = {
  p: BigInt("0x1ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
  n: BigInt("0x01fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffa51868783bf2f966b7fcc0148f709a5d03bb5c9b8899c47aebb6fb71e91386409"),
  h: BigInt(1),
  a: BigInt("0x1fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc"),
  b: BigInt("0x0051953eb9618e1c9a1f929a21a0b68540eea2da725b99b315f3b8b489918ef109e156193951ec7e937b1652c0bd3bb1bf073573df883d2c34f1ef451fd46b503f00"),
  Gx: BigInt("0x00c6858e06b70404e9cd9e3ecb662395b4429c648139053fb521f828af606b4d3dbaa14b5e77efe75928fe1dc127a2ffa8de3348b3c1856a429bf97e7e31c2e5bd66"),
  Gy: BigInt("0x011839296a789a3bc0045c8a5fb42c7d1bd998f54449579b446817afbd17273e662c97ee72995ef42640c550b9013fad0761353c7086a272c24088be94769fd16650")
};
var Fp256 = Field(p256_CURVE.p);
var Fp384 = Field(p384_CURVE.p);
var Fp521 = Field(p521_CURVE.p);
var p256 = createCurve({ ...p256_CURVE, Fp: Fp256, lowS: false }, sha256);
var p384 = createCurve({ ...p384_CURVE, Fp: Fp384, lowS: false }, sha384);
var p521 = createCurve({ ...p521_CURVE, Fp: Fp521, lowS: false, allowedPrivateKeyLengths: [130, 131, 132] }, sha512);

// node_modules/@noble/curves/esm/p256.js
var secp256r1 = p256;

// node_modules/@noble/hashes/esm/sha256.js
var sha2562 = sha256;

// node_modules/@noble/hashes/esm/_blake.js
var BSIGMA = /* @__PURE__ */ Uint8Array.from([
  0,
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13,
  14,
  15,
  14,
  10,
  4,
  8,
  9,
  15,
  13,
  6,
  1,
  12,
  0,
  2,
  11,
  7,
  5,
  3,
  11,
  8,
  12,
  0,
  5,
  2,
  15,
  13,
  10,
  14,
  3,
  6,
  7,
  1,
  9,
  4,
  7,
  9,
  3,
  1,
  13,
  12,
  11,
  14,
  2,
  6,
  5,
  10,
  4,
  0,
  15,
  8,
  9,
  0,
  5,
  7,
  2,
  4,
  10,
  15,
  14,
  1,
  11,
  12,
  6,
  8,
  3,
  13,
  2,
  12,
  6,
  10,
  0,
  11,
  8,
  3,
  4,
  13,
  7,
  5,
  15,
  14,
  1,
  9,
  12,
  5,
  1,
  15,
  14,
  13,
  4,
  10,
  0,
  7,
  6,
  3,
  9,
  2,
  8,
  11,
  13,
  11,
  7,
  14,
  12,
  1,
  3,
  9,
  5,
  0,
  15,
  4,
  8,
  6,
  2,
  10,
  6,
  15,
  14,
  9,
  11,
  3,
  0,
  8,
  12,
  2,
  13,
  7,
  1,
  4,
  10,
  5,
  10,
  2,
  8,
  4,
  7,
  6,
  1,
  5,
  15,
  11,
  9,
  14,
  3,
  12,
  13,
  0,
  0,
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13,
  14,
  15,
  14,
  10,
  4,
  8,
  9,
  15,
  13,
  6,
  1,
  12,
  0,
  2,
  11,
  7,
  5,
  3,
  // Blake1, unused in others
  11,
  8,
  12,
  0,
  5,
  2,
  15,
  13,
  10,
  14,
  3,
  6,
  7,
  1,
  9,
  4,
  7,
  9,
  3,
  1,
  13,
  12,
  11,
  14,
  2,
  6,
  5,
  10,
  4,
  0,
  15,
  8,
  9,
  0,
  5,
  7,
  2,
  4,
  10,
  15,
  14,
  1,
  11,
  12,
  6,
  8,
  3,
  13,
  2,
  12,
  6,
  10,
  0,
  11,
  8,
  3,
  4,
  13,
  7,
  5,
  15,
  14,
  1,
  9
]);

// node_modules/@noble/hashes/esm/blake2.js
var B2B_IV = /* @__PURE__ */ Uint32Array.from([
  4089235720,
  1779033703,
  2227873595,
  3144134277,
  4271175723,
  1013904242,
  1595750129,
  2773480762,
  2917565137,
  1359893119,
  725511199,
  2600822924,
  4215389547,
  528734635,
  327033209,
  1541459225
]);
var BBUF = /* @__PURE__ */ new Uint32Array(32);
function G1b(a, b, c, d, msg, x) {
  const Xl = msg[x], Xh = msg[x + 1];
  let Al = BBUF[2 * a], Ah = BBUF[2 * a + 1];
  let Bl = BBUF[2 * b], Bh = BBUF[2 * b + 1];
  let Cl = BBUF[2 * c], Ch = BBUF[2 * c + 1];
  let Dl = BBUF[2 * d], Dh = BBUF[2 * d + 1];
  let ll = add3L(Al, Bl, Xl);
  Ah = add3H(ll, Ah, Bh, Xh);
  Al = ll | 0;
  ({ Dh, Dl } = { Dh: Dh ^ Ah, Dl: Dl ^ Al });
  ({ Dh, Dl } = { Dh: rotr32H(Dh, Dl), Dl: rotr32L(Dh, Dl) });
  ({ h: Ch, l: Cl } = add(Ch, Cl, Dh, Dl));
  ({ Bh, Bl } = { Bh: Bh ^ Ch, Bl: Bl ^ Cl });
  ({ Bh, Bl } = { Bh: rotrSH(Bh, Bl, 24), Bl: rotrSL(Bh, Bl, 24) });
  BBUF[2 * a] = Al, BBUF[2 * a + 1] = Ah;
  BBUF[2 * b] = Bl, BBUF[2 * b + 1] = Bh;
  BBUF[2 * c] = Cl, BBUF[2 * c + 1] = Ch;
  BBUF[2 * d] = Dl, BBUF[2 * d + 1] = Dh;
}
function G2b(a, b, c, d, msg, x) {
  const Xl = msg[x], Xh = msg[x + 1];
  let Al = BBUF[2 * a], Ah = BBUF[2 * a + 1];
  let Bl = BBUF[2 * b], Bh = BBUF[2 * b + 1];
  let Cl = BBUF[2 * c], Ch = BBUF[2 * c + 1];
  let Dl = BBUF[2 * d], Dh = BBUF[2 * d + 1];
  let ll = add3L(Al, Bl, Xl);
  Ah = add3H(ll, Ah, Bh, Xh);
  Al = ll | 0;
  ({ Dh, Dl } = { Dh: Dh ^ Ah, Dl: Dl ^ Al });
  ({ Dh, Dl } = { Dh: rotrSH(Dh, Dl, 16), Dl: rotrSL(Dh, Dl, 16) });
  ({ h: Ch, l: Cl } = add(Ch, Cl, Dh, Dl));
  ({ Bh, Bl } = { Bh: Bh ^ Ch, Bl: Bl ^ Cl });
  ({ Bh, Bl } = { Bh: rotrBH(Bh, Bl, 63), Bl: rotrBL(Bh, Bl, 63) });
  BBUF[2 * a] = Al, BBUF[2 * a + 1] = Ah;
  BBUF[2 * b] = Bl, BBUF[2 * b + 1] = Bh;
  BBUF[2 * c] = Cl, BBUF[2 * c + 1] = Ch;
  BBUF[2 * d] = Dl, BBUF[2 * d + 1] = Dh;
}
function checkBlake2Opts(outputLen, opts = {}, keyLen, saltLen, persLen) {
  anumber2(keyLen);
  if (outputLen < 0 || outputLen > keyLen)
    throw new Error("outputLen bigger than keyLen");
  const { key, salt, personalization } = opts;
  if (key !== void 0 && (key.length < 1 || key.length > keyLen))
    throw new Error("key length must be undefined or 1.." + keyLen);
  if (salt !== void 0 && salt.length !== saltLen)
    throw new Error("salt must be undefined or " + saltLen);
  if (personalization !== void 0 && personalization.length !== persLen)
    throw new Error("personalization must be undefined or " + persLen);
}
var BLAKE2 = class extends Hash {
  constructor(blockLen, outputLen) {
    super();
    this.finished = false;
    this.destroyed = false;
    this.length = 0;
    this.pos = 0;
    anumber2(blockLen);
    anumber2(outputLen);
    this.blockLen = blockLen;
    this.outputLen = outputLen;
    this.buffer = new Uint8Array(blockLen);
    this.buffer32 = u32(this.buffer);
  }
  update(data) {
    aexists(this);
    data = toBytes(data);
    abytes(data);
    const { blockLen, buffer, buffer32 } = this;
    const len = data.length;
    const offset = data.byteOffset;
    const buf = data.buffer;
    for (let pos = 0; pos < len; ) {
      if (this.pos === blockLen) {
        swap32IfBE(buffer32);
        this.compress(buffer32, 0, false);
        swap32IfBE(buffer32);
        this.pos = 0;
      }
      const take = Math.min(blockLen - this.pos, len - pos);
      const dataOffset = offset + pos;
      if (take === blockLen && !(dataOffset % 4) && pos + take < len) {
        const data32 = new Uint32Array(buf, dataOffset, Math.floor((len - pos) / 4));
        swap32IfBE(data32);
        for (let pos32 = 0; pos + blockLen < len; pos32 += buffer32.length, pos += blockLen) {
          this.length += blockLen;
          this.compress(data32, pos32, false);
        }
        swap32IfBE(data32);
        continue;
      }
      buffer.set(data.subarray(pos, pos + take), this.pos);
      this.pos += take;
      this.length += take;
      pos += take;
    }
    return this;
  }
  digestInto(out) {
    aexists(this);
    aoutput(out, this);
    const { pos, buffer32 } = this;
    this.finished = true;
    clean(this.buffer.subarray(pos));
    swap32IfBE(buffer32);
    this.compress(buffer32, 0, true);
    swap32IfBE(buffer32);
    const out32 = u32(out);
    this.get().forEach((v, i) => out32[i] = swap8IfBE(v));
  }
  digest() {
    const { buffer, outputLen } = this;
    this.digestInto(buffer);
    const res = buffer.slice(0, outputLen);
    this.destroy();
    return res;
  }
  _cloneInto(to) {
    const { buffer, length, finished, destroyed, outputLen, pos } = this;
    to || (to = new this.constructor({ dkLen: outputLen }));
    to.set(...this.get());
    to.buffer.set(buffer);
    to.destroyed = destroyed;
    to.finished = finished;
    to.length = length;
    to.pos = pos;
    to.outputLen = outputLen;
    return to;
  }
  clone() {
    return this._cloneInto();
  }
};
var BLAKE2b = class extends BLAKE2 {
  constructor(opts = {}) {
    const olen = opts.dkLen === void 0 ? 64 : opts.dkLen;
    super(128, olen);
    this.v0l = B2B_IV[0] | 0;
    this.v0h = B2B_IV[1] | 0;
    this.v1l = B2B_IV[2] | 0;
    this.v1h = B2B_IV[3] | 0;
    this.v2l = B2B_IV[4] | 0;
    this.v2h = B2B_IV[5] | 0;
    this.v3l = B2B_IV[6] | 0;
    this.v3h = B2B_IV[7] | 0;
    this.v4l = B2B_IV[8] | 0;
    this.v4h = B2B_IV[9] | 0;
    this.v5l = B2B_IV[10] | 0;
    this.v5h = B2B_IV[11] | 0;
    this.v6l = B2B_IV[12] | 0;
    this.v6h = B2B_IV[13] | 0;
    this.v7l = B2B_IV[14] | 0;
    this.v7h = B2B_IV[15] | 0;
    checkBlake2Opts(olen, opts, 64, 16, 16);
    let { key, personalization, salt } = opts;
    let keyLength = 0;
    if (key !== void 0) {
      key = toBytes(key);
      keyLength = key.length;
    }
    this.v0l ^= this.outputLen | keyLength << 8 | 1 << 16 | 1 << 24;
    if (salt !== void 0) {
      salt = toBytes(salt);
      const slt = u32(salt);
      this.v4l ^= swap8IfBE(slt[0]);
      this.v4h ^= swap8IfBE(slt[1]);
      this.v5l ^= swap8IfBE(slt[2]);
      this.v5h ^= swap8IfBE(slt[3]);
    }
    if (personalization !== void 0) {
      personalization = toBytes(personalization);
      const pers = u32(personalization);
      this.v6l ^= swap8IfBE(pers[0]);
      this.v6h ^= swap8IfBE(pers[1]);
      this.v7l ^= swap8IfBE(pers[2]);
      this.v7h ^= swap8IfBE(pers[3]);
    }
    if (key !== void 0) {
      const tmp = new Uint8Array(this.blockLen);
      tmp.set(key);
      this.update(tmp);
    }
  }
  // prettier-ignore
  get() {
    let { v0l, v0h, v1l, v1h, v2l, v2h, v3l, v3h, v4l, v4h, v5l, v5h, v6l, v6h, v7l, v7h } = this;
    return [v0l, v0h, v1l, v1h, v2l, v2h, v3l, v3h, v4l, v4h, v5l, v5h, v6l, v6h, v7l, v7h];
  }
  // prettier-ignore
  set(v0l, v0h, v1l, v1h, v2l, v2h, v3l, v3h, v4l, v4h, v5l, v5h, v6l, v6h, v7l, v7h) {
    this.v0l = v0l | 0;
    this.v0h = v0h | 0;
    this.v1l = v1l | 0;
    this.v1h = v1h | 0;
    this.v2l = v2l | 0;
    this.v2h = v2h | 0;
    this.v3l = v3l | 0;
    this.v3h = v3h | 0;
    this.v4l = v4l | 0;
    this.v4h = v4h | 0;
    this.v5l = v5l | 0;
    this.v5h = v5h | 0;
    this.v6l = v6l | 0;
    this.v6h = v6h | 0;
    this.v7l = v7l | 0;
    this.v7h = v7h | 0;
  }
  compress(msg, offset, isLast) {
    this.get().forEach((v, i) => BBUF[i] = v);
    BBUF.set(B2B_IV, 16);
    let { h, l } = fromBig(BigInt(this.length));
    BBUF[24] = B2B_IV[8] ^ l;
    BBUF[25] = B2B_IV[9] ^ h;
    if (isLast) {
      BBUF[28] = ~BBUF[28];
      BBUF[29] = ~BBUF[29];
    }
    let j = 0;
    const s = BSIGMA;
    for (let i = 0; i < 12; i++) {
      G1b(0, 4, 8, 12, msg, offset + 2 * s[j++]);
      G2b(0, 4, 8, 12, msg, offset + 2 * s[j++]);
      G1b(1, 5, 9, 13, msg, offset + 2 * s[j++]);
      G2b(1, 5, 9, 13, msg, offset + 2 * s[j++]);
      G1b(2, 6, 10, 14, msg, offset + 2 * s[j++]);
      G2b(2, 6, 10, 14, msg, offset + 2 * s[j++]);
      G1b(3, 7, 11, 15, msg, offset + 2 * s[j++]);
      G2b(3, 7, 11, 15, msg, offset + 2 * s[j++]);
      G1b(0, 5, 10, 15, msg, offset + 2 * s[j++]);
      G2b(0, 5, 10, 15, msg, offset + 2 * s[j++]);
      G1b(1, 6, 11, 12, msg, offset + 2 * s[j++]);
      G2b(1, 6, 11, 12, msg, offset + 2 * s[j++]);
      G1b(2, 7, 8, 13, msg, offset + 2 * s[j++]);
      G2b(2, 7, 8, 13, msg, offset + 2 * s[j++]);
      G1b(3, 4, 9, 14, msg, offset + 2 * s[j++]);
      G2b(3, 4, 9, 14, msg, offset + 2 * s[j++]);
    }
    this.v0l ^= BBUF[0] ^ BBUF[16];
    this.v0h ^= BBUF[1] ^ BBUF[17];
    this.v1l ^= BBUF[2] ^ BBUF[18];
    this.v1h ^= BBUF[3] ^ BBUF[19];
    this.v2l ^= BBUF[4] ^ BBUF[20];
    this.v2h ^= BBUF[5] ^ BBUF[21];
    this.v3l ^= BBUF[6] ^ BBUF[22];
    this.v3h ^= BBUF[7] ^ BBUF[23];
    this.v4l ^= BBUF[8] ^ BBUF[24];
    this.v4h ^= BBUF[9] ^ BBUF[25];
    this.v5l ^= BBUF[10] ^ BBUF[26];
    this.v5h ^= BBUF[11] ^ BBUF[27];
    this.v6l ^= BBUF[12] ^ BBUF[28];
    this.v6h ^= BBUF[13] ^ BBUF[29];
    this.v7l ^= BBUF[14] ^ BBUF[30];
    this.v7h ^= BBUF[15] ^ BBUF[31];
    clean(BBUF);
  }
  destroy() {
    this.destroyed = true;
    clean(this.buffer32);
    this.set(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
  }
};
var blake2b = /* @__PURE__ */ createOptHasher((opts) => new BLAKE2b(opts));

// node_modules/@noble/hashes/esm/blake2b.js
var blake2b2 = blake2b;

// node_modules/@mysten/sui/dist/esm/cryptography/intent.js
function messageWithIntent(scope, message) {
  return suiBcs.IntentMessage(suiBcs.bytes(message.length)).serialize({
    intent: {
      scope: { [scope]: true },
      version: { V0: true },
      appId: { Sui: true }
    },
    value: message
  }).toBytes();
}

// node_modules/@mysten/sui/dist/esm/cryptography/signature-scheme.js
var SIGNATURE_SCHEME_TO_FLAG = {
  ED25519: 0,
  Secp256k1: 1,
  Secp256r1: 2,
  MultiSig: 3,
  ZkLogin: 5,
  Passkey: 6
};
var SIGNATURE_SCHEME_TO_SIZE = {
  ED25519: 32,
  Secp256k1: 33,
  Secp256r1: 33,
  Passkey: 33
};
var SIGNATURE_FLAG_TO_SCHEME = {
  0: "ED25519",
  1: "Secp256k1",
  2: "Secp256r1",
  3: "MultiSig",
  5: "ZkLogin",
  6: "Passkey"
};

// node_modules/@mysten/sui/dist/esm/cryptography/publickey.js
function bytesEqual(a, b) {
  if (a === b) return true;
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}
var PublicKey2 = class {
  /**
   * Checks if two public keys are equal
   */
  equals(publicKey) {
    return bytesEqual(this.toRawBytes(), publicKey.toRawBytes());
  }
  /**
   * Return the base-64 representation of the public key
   */
  toBase64() {
    return toBase64(this.toRawBytes());
  }
  toString() {
    throw new Error(
      "`toString` is not implemented on public keys. Use `toBase64()` or `toRawBytes()` instead."
    );
  }
  /**
   * Return the Sui representation of the public key encoded in
   * base-64. A Sui public key is formed by the concatenation
   * of the scheme flag with the raw bytes of the public key
   */
  toSuiPublicKey() {
    const bytes = this.toSuiBytes();
    return toBase64(bytes);
  }
  verifyWithIntent(bytes, signature, intent) {
    const intentMessage = messageWithIntent(intent, bytes);
    const digest = blake2b2(intentMessage, { dkLen: 32 });
    return this.verify(digest, signature);
  }
  /**
   * Verifies that the signature is valid for for the provided PersonalMessage
   */
  verifyPersonalMessage(message, signature) {
    return this.verifyWithIntent(
      suiBcs.byteVector().serialize(message).toBytes(),
      signature,
      "PersonalMessage"
    );
  }
  /**
   * Verifies that the signature is valid for for the provided Transaction
   */
  verifyTransaction(transaction, signature) {
    return this.verifyWithIntent(transaction, signature, "TransactionData");
  }
  /**
   * Verifies that the public key is associated with the provided address
   */
  verifyAddress(address) {
    return this.toSuiAddress() === address;
  }
  /**
   * Returns the bytes representation of the public key
   * prefixed with the signature scheme flag
   */
  toSuiBytes() {
    const rawBytes = this.toRawBytes();
    const suiBytes = new Uint8Array(rawBytes.length + 1);
    suiBytes.set([this.flag()]);
    suiBytes.set(rawBytes, 1);
    return suiBytes;
  }
  /**
   * Return the Sui address associated with this Ed25519 public key
   */
  toSuiAddress() {
    return normalizeSuiAddress(
      bytesToHex(blake2b2(this.toSuiBytes(), { dkLen: 32 })).slice(0, SUI_ADDRESS_LENGTH * 2)
    );
  }
};
function parseSerializedKeypairSignature(serializedSignature) {
  const bytes = fromBase64(serializedSignature);
  const signatureScheme = SIGNATURE_FLAG_TO_SCHEME[bytes[0]];
  switch (signatureScheme) {
    case "ED25519":
    case "Secp256k1":
    case "Secp256r1":
      const size = SIGNATURE_SCHEME_TO_SIZE[signatureScheme];
      const signature = bytes.slice(1, bytes.length - size);
      const publicKey = bytes.slice(1 + signature.length);
      return {
        serializedSignature,
        signatureScheme,
        signature,
        publicKey,
        bytes
      };
    default:
      throw new Error("Unsupported signature scheme");
  }
}

// node_modules/@mysten/sui/dist/esm/keypairs/passkey/publickey.js
var PASSKEY_PUBLIC_KEY_SIZE = 33;
var PASSKEY_SIGNATURE_SIZE = 64;
var SECP256R1_SPKI_HEADER = new Uint8Array([
  48,
  89,
  // SEQUENCE, length 89
  48,
  19,
  // SEQUENCE, length 19
  6,
  7,
  // OID, length 7
  42,
  134,
  72,
  206,
  61,
  2,
  1,
  // OID: 1.2.840.10045.2.1 (ecPublicKey)
  6,
  8,
  // OID, length 8
  42,
  134,
  72,
  206,
  61,
  3,
  1,
  7,
  // OID: 1.2.840.10045.3.1.7 (prime256v1/secp256r1)
  3,
  66,
  // BIT STRING, length 66
  0
  // no unused bits
]);
var PasskeyPublicKey = class extends PublicKey2 {
  /**
   * Create a new PasskeyPublicKey object
   * @param value passkey public key as buffer or base-64 encoded string
   */
  constructor(value) {
    super();
    if (typeof value === "string") {
      this.data = fromBase64(value);
    } else if (value instanceof Uint8Array) {
      this.data = value;
    } else {
      this.data = Uint8Array.from(value);
    }
    if (this.data.length !== PASSKEY_PUBLIC_KEY_SIZE) {
      throw new Error(
        `Invalid public key input. Expected ${PASSKEY_PUBLIC_KEY_SIZE} bytes, got ${this.data.length}`
      );
    }
  }
  /**
   * Checks if two passkey public keys are equal
   */
  equals(publicKey) {
    return super.equals(publicKey);
  }
  /**
   * Return the byte array representation of the Secp256r1 public key
   */
  toRawBytes() {
    return this.data;
  }
  /**
   * Return the Sui address associated with this Secp256r1 public key
   */
  flag() {
    return SIGNATURE_SCHEME_TO_FLAG["Passkey"];
  }
  /**
   * Verifies that the signature is valid for for the provided message
   */
  async verify(message, signature) {
    const parsed = parseSerializedPasskeySignature(signature);
    const clientDataJSON = JSON.parse(parsed.clientDataJson);
    if (clientDataJSON.type !== "webauthn.get") {
      return false;
    }
    const parsedChallenge = fromBase64(
      clientDataJSON.challenge.replace(/-/g, "+").replace(/_/g, "/")
    );
    if (!bytesEqual(message, parsedChallenge)) {
      return false;
    }
    const pk = parsed.userSignature.slice(1 + PASSKEY_SIGNATURE_SIZE);
    if (!bytesEqual(this.toRawBytes(), pk)) {
      return false;
    }
    const payload = new Uint8Array([...parsed.authenticatorData, ...sha2562(parsed.clientDataJson)]);
    const sig = parsed.userSignature.slice(1, PASSKEY_SIGNATURE_SIZE + 1);
    return secp256r1.verify(sig, sha2562(payload), pk);
  }
};
PasskeyPublicKey.SIZE = PASSKEY_PUBLIC_KEY_SIZE;
function parseSerializedPasskeySignature(signature) {
  const bytes = typeof signature === "string" ? fromBase64(signature) : signature;
  if (bytes[0] !== SIGNATURE_SCHEME_TO_FLAG.Passkey) {
    throw new Error("Invalid signature scheme");
  }
  const dec = PasskeyAuthenticator.parse(bytes.slice(1));
  return {
    signatureScheme: "Passkey",
    serializedSignature: toBase64(bytes),
    signature: bytes,
    authenticatorData: dec.authenticatorData,
    clientDataJson: dec.clientDataJson,
    userSignature: new Uint8Array(dec.userSignature),
    publicKey: new Uint8Array(dec.userSignature.slice(1 + PASSKEY_SIGNATURE_SIZE))
  };
}

// node_modules/graphql/jsutils/devAssert.mjs
function devAssert(condition, message) {
  const booleanCondition = Boolean(condition);
  if (!booleanCondition) {
    throw new Error(message);
  }
}

// node_modules/graphql/language/ast.mjs
var QueryDocumentKeys = {
  Name: [],
  Document: ["definitions"],
  OperationDefinition: [
    "description",
    "name",
    "variableDefinitions",
    "directives",
    "selectionSet"
  ],
  VariableDefinition: [
    "description",
    "variable",
    "type",
    "defaultValue",
    "directives"
  ],
  Variable: ["name"],
  SelectionSet: ["selections"],
  Field: ["alias", "name", "arguments", "directives", "selectionSet"],
  Argument: ["name", "value"],
  FragmentSpread: ["name", "directives"],
  InlineFragment: ["typeCondition", "directives", "selectionSet"],
  FragmentDefinition: [
    "description",
    "name",
    // Note: fragment variable definitions are deprecated and will removed in v17.0.0
    "variableDefinitions",
    "typeCondition",
    "directives",
    "selectionSet"
  ],
  IntValue: [],
  FloatValue: [],
  StringValue: [],
  BooleanValue: [],
  NullValue: [],
  EnumValue: [],
  ListValue: ["values"],
  ObjectValue: ["fields"],
  ObjectField: ["name", "value"],
  Directive: ["name", "arguments"],
  NamedType: ["name"],
  ListType: ["type"],
  NonNullType: ["type"],
  SchemaDefinition: ["description", "directives", "operationTypes"],
  OperationTypeDefinition: ["type"],
  ScalarTypeDefinition: ["description", "name", "directives"],
  ObjectTypeDefinition: [
    "description",
    "name",
    "interfaces",
    "directives",
    "fields"
  ],
  FieldDefinition: ["description", "name", "arguments", "type", "directives"],
  InputValueDefinition: [
    "description",
    "name",
    "type",
    "defaultValue",
    "directives"
  ],
  InterfaceTypeDefinition: [
    "description",
    "name",
    "interfaces",
    "directives",
    "fields"
  ],
  UnionTypeDefinition: ["description", "name", "directives", "types"],
  EnumTypeDefinition: ["description", "name", "directives", "values"],
  EnumValueDefinition: ["description", "name", "directives"],
  InputObjectTypeDefinition: ["description", "name", "directives", "fields"],
  DirectiveDefinition: [
    "description",
    "name",
    "arguments",
    "directives",
    "locations"
  ],
  SchemaExtension: ["directives", "operationTypes"],
  DirectiveExtension: ["name", "directives"],
  ScalarTypeExtension: ["name", "directives"],
  ObjectTypeExtension: ["name", "interfaces", "directives", "fields"],
  InterfaceTypeExtension: ["name", "interfaces", "directives", "fields"],
  UnionTypeExtension: ["name", "directives", "types"],
  EnumTypeExtension: ["name", "directives", "values"],
  InputObjectTypeExtension: ["name", "directives", "fields"],
  TypeCoordinate: ["name"],
  MemberCoordinate: ["name", "memberName"],
  ArgumentCoordinate: ["name", "fieldName", "argumentName"],
  DirectiveCoordinate: ["name"],
  DirectiveArgumentCoordinate: ["name", "argumentName"]
};
var kindValues = new Set(Object.keys(QueryDocumentKeys));
function isNode(maybeNode) {
  const maybeKind = maybeNode === null || maybeNode === void 0 ? void 0 : maybeNode.kind;
  return typeof maybeKind === "string" && kindValues.has(maybeKind);
}
var OperationTypeNode;
(function(OperationTypeNode2) {
  OperationTypeNode2["QUERY"] = "query";
  OperationTypeNode2["MUTATION"] = "mutation";
  OperationTypeNode2["SUBSCRIPTION"] = "subscription";
})(OperationTypeNode || (OperationTypeNode = {}));

// node_modules/graphql/language/kinds.mjs
var Kind;
(function(Kind2) {
  Kind2["NAME"] = "Name";
  Kind2["DOCUMENT"] = "Document";
  Kind2["OPERATION_DEFINITION"] = "OperationDefinition";
  Kind2["VARIABLE_DEFINITION"] = "VariableDefinition";
  Kind2["SELECTION_SET"] = "SelectionSet";
  Kind2["FIELD"] = "Field";
  Kind2["ARGUMENT"] = "Argument";
  Kind2["FRAGMENT_SPREAD"] = "FragmentSpread";
  Kind2["INLINE_FRAGMENT"] = "InlineFragment";
  Kind2["FRAGMENT_DEFINITION"] = "FragmentDefinition";
  Kind2["VARIABLE"] = "Variable";
  Kind2["INT"] = "IntValue";
  Kind2["FLOAT"] = "FloatValue";
  Kind2["STRING"] = "StringValue";
  Kind2["BOOLEAN"] = "BooleanValue";
  Kind2["NULL"] = "NullValue";
  Kind2["ENUM"] = "EnumValue";
  Kind2["LIST"] = "ListValue";
  Kind2["OBJECT"] = "ObjectValue";
  Kind2["OBJECT_FIELD"] = "ObjectField";
  Kind2["DIRECTIVE"] = "Directive";
  Kind2["NAMED_TYPE"] = "NamedType";
  Kind2["LIST_TYPE"] = "ListType";
  Kind2["NON_NULL_TYPE"] = "NonNullType";
  Kind2["SCHEMA_DEFINITION"] = "SchemaDefinition";
  Kind2["OPERATION_TYPE_DEFINITION"] = "OperationTypeDefinition";
  Kind2["SCALAR_TYPE_DEFINITION"] = "ScalarTypeDefinition";
  Kind2["OBJECT_TYPE_DEFINITION"] = "ObjectTypeDefinition";
  Kind2["FIELD_DEFINITION"] = "FieldDefinition";
  Kind2["INPUT_VALUE_DEFINITION"] = "InputValueDefinition";
  Kind2["INTERFACE_TYPE_DEFINITION"] = "InterfaceTypeDefinition";
  Kind2["UNION_TYPE_DEFINITION"] = "UnionTypeDefinition";
  Kind2["ENUM_TYPE_DEFINITION"] = "EnumTypeDefinition";
  Kind2["ENUM_VALUE_DEFINITION"] = "EnumValueDefinition";
  Kind2["INPUT_OBJECT_TYPE_DEFINITION"] = "InputObjectTypeDefinition";
  Kind2["DIRECTIVE_DEFINITION"] = "DirectiveDefinition";
  Kind2["SCHEMA_EXTENSION"] = "SchemaExtension";
  Kind2["DIRECTIVE_EXTENSION"] = "DirectiveExtension";
  Kind2["SCALAR_TYPE_EXTENSION"] = "ScalarTypeExtension";
  Kind2["OBJECT_TYPE_EXTENSION"] = "ObjectTypeExtension";
  Kind2["INTERFACE_TYPE_EXTENSION"] = "InterfaceTypeExtension";
  Kind2["UNION_TYPE_EXTENSION"] = "UnionTypeExtension";
  Kind2["ENUM_TYPE_EXTENSION"] = "EnumTypeExtension";
  Kind2["INPUT_OBJECT_TYPE_EXTENSION"] = "InputObjectTypeExtension";
  Kind2["TYPE_COORDINATE"] = "TypeCoordinate";
  Kind2["MEMBER_COORDINATE"] = "MemberCoordinate";
  Kind2["ARGUMENT_COORDINATE"] = "ArgumentCoordinate";
  Kind2["DIRECTIVE_COORDINATE"] = "DirectiveCoordinate";
  Kind2["DIRECTIVE_ARGUMENT_COORDINATE"] = "DirectiveArgumentCoordinate";
})(Kind || (Kind = {}));

// node_modules/graphql/language/characterClasses.mjs
function isWhiteSpace(code) {
  return code === 9 || code === 32;
}

// node_modules/graphql/language/blockString.mjs
function printBlockString(value, options) {
  const escapedValue = value.replace(/"""/g, '\\"""');
  const lines = escapedValue.split(/\r\n|[\n\r]/g);
  const isSingleLine = lines.length === 1;
  const forceLeadingNewLine = lines.length > 1 && lines.slice(1).every((line) => line.length === 0 || isWhiteSpace(line.charCodeAt(0)));
  const hasTrailingTripleQuotes = escapedValue.endsWith('\\"""');
  const hasTrailingQuote = value.endsWith('"') && !hasTrailingTripleQuotes;
  const hasTrailingSlash = value.endsWith("\\");
  const forceTrailingNewline = hasTrailingQuote || hasTrailingSlash;
  const printAsMultipleLines = !(options !== null && options !== void 0 && options.minimize) && // add leading and trailing new lines only if it improves readability
  (!isSingleLine || value.length > 70 || forceTrailingNewline || forceLeadingNewLine || hasTrailingTripleQuotes);
  let result = "";
  const skipLeadingNewLine = isSingleLine && isWhiteSpace(value.charCodeAt(0));
  if (printAsMultipleLines && !skipLeadingNewLine || forceLeadingNewLine) {
    result += "\n";
  }
  result += escapedValue;
  if (printAsMultipleLines || forceTrailingNewline) {
    result += "\n";
  }
  return '"""' + result + '"""';
}

// node_modules/graphql/jsutils/inspect.mjs
var MAX_ARRAY_LENGTH = 10;
var MAX_RECURSIVE_DEPTH = 2;
function inspect(value) {
  return formatValue(value, []);
}
function formatValue(value, seenValues) {
  switch (typeof value) {
    case "string":
      return JSON.stringify(value);
    case "function":
      return value.name ? `[function ${value.name}]` : "[function]";
    case "object":
      return formatObjectValue(value, seenValues);
    default:
      return String(value);
  }
}
function formatObjectValue(value, previouslySeenValues) {
  if (value === null) {
    return "null";
  }
  if (previouslySeenValues.includes(value)) {
    return "[Circular]";
  }
  const seenValues = [...previouslySeenValues, value];
  if (isJSONable(value)) {
    const jsonValue = value.toJSON();
    if (jsonValue !== value) {
      return typeof jsonValue === "string" ? jsonValue : formatValue(jsonValue, seenValues);
    }
  } else if (Array.isArray(value)) {
    return formatArray(value, seenValues);
  }
  return formatObject(value, seenValues);
}
function isJSONable(value) {
  return typeof value.toJSON === "function";
}
function formatObject(object2, seenValues) {
  const entries = Object.entries(object2);
  if (entries.length === 0) {
    return "{}";
  }
  if (seenValues.length > MAX_RECURSIVE_DEPTH) {
    return "[" + getObjectTag(object2) + "]";
  }
  const properties = entries.map(
    ([key, value]) => key + ": " + formatValue(value, seenValues)
  );
  return "{ " + properties.join(", ") + " }";
}
function formatArray(array2, seenValues) {
  if (array2.length === 0) {
    return "[]";
  }
  if (seenValues.length > MAX_RECURSIVE_DEPTH) {
    return "[Array]";
  }
  const len = Math.min(MAX_ARRAY_LENGTH, array2.length);
  const remaining = array2.length - len;
  const items = [];
  for (let i = 0; i < len; ++i) {
    items.push(formatValue(array2[i], seenValues));
  }
  if (remaining === 1) {
    items.push("... 1 more item");
  } else if (remaining > 1) {
    items.push(`... ${remaining} more items`);
  }
  return "[" + items.join(", ") + "]";
}
function getObjectTag(object2) {
  const tag = Object.prototype.toString.call(object2).replace(/^\[object /, "").replace(/]$/, "");
  if (tag === "Object" && typeof object2.constructor === "function") {
    const name = object2.constructor.name;
    if (typeof name === "string" && name !== "") {
      return name;
    }
  }
  return tag;
}

// node_modules/graphql/language/printString.mjs
function printString(str) {
  return `"${str.replace(escapedRegExp, escapedReplacer)}"`;
}
var escapedRegExp = /[\x00-\x1f\x22\x5c\x7f-\x9f]/g;
function escapedReplacer(str) {
  return escapeSequences[str.charCodeAt(0)];
}
var escapeSequences = [
  "\\u0000",
  "\\u0001",
  "\\u0002",
  "\\u0003",
  "\\u0004",
  "\\u0005",
  "\\u0006",
  "\\u0007",
  "\\b",
  "\\t",
  "\\n",
  "\\u000B",
  "\\f",
  "\\r",
  "\\u000E",
  "\\u000F",
  "\\u0010",
  "\\u0011",
  "\\u0012",
  "\\u0013",
  "\\u0014",
  "\\u0015",
  "\\u0016",
  "\\u0017",
  "\\u0018",
  "\\u0019",
  "\\u001A",
  "\\u001B",
  "\\u001C",
  "\\u001D",
  "\\u001E",
  "\\u001F",
  "",
  "",
  '\\"',
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  // 2F
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  // 3F
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  // 4F
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "\\\\",
  "",
  "",
  "",
  // 5F
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  // 6F
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "",
  "\\u007F",
  "\\u0080",
  "\\u0081",
  "\\u0082",
  "\\u0083",
  "\\u0084",
  "\\u0085",
  "\\u0086",
  "\\u0087",
  "\\u0088",
  "\\u0089",
  "\\u008A",
  "\\u008B",
  "\\u008C",
  "\\u008D",
  "\\u008E",
  "\\u008F",
  "\\u0090",
  "\\u0091",
  "\\u0092",
  "\\u0093",
  "\\u0094",
  "\\u0095",
  "\\u0096",
  "\\u0097",
  "\\u0098",
  "\\u0099",
  "\\u009A",
  "\\u009B",
  "\\u009C",
  "\\u009D",
  "\\u009E",
  "\\u009F"
];

// node_modules/graphql/language/visitor.mjs
var BREAK = Object.freeze({});
function visit(root, visitor, visitorKeys = QueryDocumentKeys) {
  const enterLeaveMap = /* @__PURE__ */ new Map();
  for (const kind of Object.values(Kind)) {
    enterLeaveMap.set(kind, getEnterLeaveForKind(visitor, kind));
  }
  let stack = void 0;
  let inArray = Array.isArray(root);
  let keys = [root];
  let index = -1;
  let edits = [];
  let node = root;
  let key = void 0;
  let parent = void 0;
  const path = [];
  const ancestors = [];
  do {
    index++;
    const isLeaving = index === keys.length;
    const isEdited = isLeaving && edits.length !== 0;
    if (isLeaving) {
      key = ancestors.length === 0 ? void 0 : path[path.length - 1];
      node = parent;
      parent = ancestors.pop();
      if (isEdited) {
        if (inArray) {
          node = node.slice();
          let editOffset = 0;
          for (const [editKey, editValue] of edits) {
            const arrayKey = editKey - editOffset;
            if (editValue === null) {
              node.splice(arrayKey, 1);
              editOffset++;
            } else {
              node[arrayKey] = editValue;
            }
          }
        } else {
          node = { ...node };
          for (const [editKey, editValue] of edits) {
            node[editKey] = editValue;
          }
        }
      }
      index = stack.index;
      keys = stack.keys;
      edits = stack.edits;
      inArray = stack.inArray;
      stack = stack.prev;
    } else if (parent) {
      key = inArray ? index : keys[index];
      node = parent[key];
      if (node === null || node === void 0) {
        continue;
      }
      path.push(key);
    }
    let result;
    if (!Array.isArray(node)) {
      var _enterLeaveMap$get, _enterLeaveMap$get2;
      isNode(node) || devAssert(false, `Invalid AST Node: ${inspect(node)}.`);
      const visitFn = isLeaving ? (_enterLeaveMap$get = enterLeaveMap.get(node.kind)) === null || _enterLeaveMap$get === void 0 ? void 0 : _enterLeaveMap$get.leave : (_enterLeaveMap$get2 = enterLeaveMap.get(node.kind)) === null || _enterLeaveMap$get2 === void 0 ? void 0 : _enterLeaveMap$get2.enter;
      result = visitFn === null || visitFn === void 0 ? void 0 : visitFn.call(visitor, node, key, parent, path, ancestors);
      if (result === BREAK) {
        break;
      }
      if (result === false) {
        if (!isLeaving) {
          path.pop();
          continue;
        }
      } else if (result !== void 0) {
        edits.push([key, result]);
        if (!isLeaving) {
          if (isNode(result)) {
            node = result;
          } else {
            path.pop();
            continue;
          }
        }
      }
    }
    if (result === void 0 && isEdited) {
      edits.push([key, node]);
    }
    if (isLeaving) {
      path.pop();
    } else {
      var _node$kind;
      stack = {
        inArray,
        index,
        keys,
        edits,
        prev: stack
      };
      inArray = Array.isArray(node);
      keys = inArray ? node : (_node$kind = visitorKeys[node.kind]) !== null && _node$kind !== void 0 ? _node$kind : [];
      index = -1;
      edits = [];
      if (parent) {
        ancestors.push(parent);
      }
      parent = node;
    }
  } while (stack !== void 0);
  if (edits.length !== 0) {
    return edits[edits.length - 1][1];
  }
  return root;
}
function getEnterLeaveForKind(visitor, kind) {
  const kindVisitor = visitor[kind];
  if (typeof kindVisitor === "object") {
    return kindVisitor;
  } else if (typeof kindVisitor === "function") {
    return {
      enter: kindVisitor,
      leave: void 0
    };
  }
  return {
    enter: visitor.enter,
    leave: visitor.leave
  };
}

// node_modules/graphql/language/printer.mjs
function print(ast) {
  return visit(ast, printDocASTReducer);
}
var MAX_LINE_LENGTH = 80;
var printDocASTReducer = {
  Name: {
    leave: (node) => node.value
  },
  Variable: {
    leave: (node) => "$" + node.name
  },
  // Document
  Document: {
    leave: (node) => join2(node.definitions, "\n\n")
  },
  OperationDefinition: {
    leave(node) {
      const varDefs = hasMultilineItems(node.variableDefinitions) ? wrap("(\n", join2(node.variableDefinitions, "\n"), "\n)") : wrap("(", join2(node.variableDefinitions, ", "), ")");
      const prefix = wrap("", node.description, "\n") + join2(
        [
          node.operation,
          join2([node.name, varDefs]),
          join2(node.directives, " ")
        ],
        " "
      );
      return (prefix === "query" ? "" : prefix + " ") + node.selectionSet;
    }
  },
  VariableDefinition: {
    leave: ({ variable, type, defaultValue, directives, description }) => wrap("", description, "\n") + variable + ": " + type + wrap(" = ", defaultValue) + wrap(" ", join2(directives, " "))
  },
  SelectionSet: {
    leave: ({ selections }) => block(selections)
  },
  Field: {
    leave({ alias, name, arguments: args, directives, selectionSet }) {
      const prefix = wrap("", alias, ": ") + name;
      let argsLine = prefix + wrap("(", join2(args, ", "), ")");
      if (argsLine.length > MAX_LINE_LENGTH) {
        argsLine = prefix + wrap("(\n", indent(join2(args, "\n")), "\n)");
      }
      return join2([argsLine, join2(directives, " "), selectionSet], " ");
    }
  },
  Argument: {
    leave: ({ name, value }) => name + ": " + value
  },
  // Fragments
  FragmentSpread: {
    leave: ({ name, directives }) => "..." + name + wrap(" ", join2(directives, " "))
  },
  InlineFragment: {
    leave: ({ typeCondition, directives, selectionSet }) => join2(
      [
        "...",
        wrap("on ", typeCondition),
        join2(directives, " "),
        selectionSet
      ],
      " "
    )
  },
  FragmentDefinition: {
    leave: ({
      name,
      typeCondition,
      variableDefinitions,
      directives,
      selectionSet,
      description
    }) => wrap("", description, "\n") + // Note: fragment variable definitions are experimental and may be changed
    // or removed in the future.
    `fragment ${name}${wrap("(", join2(variableDefinitions, ", "), ")")} on ${typeCondition} ${wrap("", join2(directives, " "), " ")}` + selectionSet
  },
  // Value
  IntValue: {
    leave: ({ value }) => value
  },
  FloatValue: {
    leave: ({ value }) => value
  },
  StringValue: {
    leave: ({ value, block: isBlockString }) => isBlockString ? printBlockString(value) : printString(value)
  },
  BooleanValue: {
    leave: ({ value }) => value ? "true" : "false"
  },
  NullValue: {
    leave: () => "null"
  },
  EnumValue: {
    leave: ({ value }) => value
  },
  ListValue: {
    leave: ({ values }) => "[" + join2(values, ", ") + "]"
  },
  ObjectValue: {
    leave: ({ fields }) => "{" + join2(fields, ", ") + "}"
  },
  ObjectField: {
    leave: ({ name, value }) => name + ": " + value
  },
  // Directive
  Directive: {
    leave: ({ name, arguments: args }) => "@" + name + wrap("(", join2(args, ", "), ")")
  },
  // Type
  NamedType: {
    leave: ({ name }) => name
  },
  ListType: {
    leave: ({ type }) => "[" + type + "]"
  },
  NonNullType: {
    leave: ({ type }) => type + "!"
  },
  // Type System Definitions
  SchemaDefinition: {
    leave: ({ description, directives, operationTypes }) => wrap("", description, "\n") + join2(["schema", join2(directives, " "), block(operationTypes)], " ")
  },
  OperationTypeDefinition: {
    leave: ({ operation, type }) => operation + ": " + type
  },
  ScalarTypeDefinition: {
    leave: ({ description, name, directives }) => wrap("", description, "\n") + join2(["scalar", name, join2(directives, " ")], " ")
  },
  ObjectTypeDefinition: {
    leave: ({ description, name, interfaces, directives, fields }) => wrap("", description, "\n") + join2(
      [
        "type",
        name,
        wrap("implements ", join2(interfaces, " & ")),
        join2(directives, " "),
        block(fields)
      ],
      " "
    )
  },
  FieldDefinition: {
    leave: ({ description, name, arguments: args, type, directives }) => wrap("", description, "\n") + name + (hasMultilineItems(args) ? wrap("(\n", indent(join2(args, "\n")), "\n)") : wrap("(", join2(args, ", "), ")")) + ": " + type + wrap(" ", join2(directives, " "))
  },
  InputValueDefinition: {
    leave: ({ description, name, type, defaultValue, directives }) => wrap("", description, "\n") + join2(
      [name + ": " + type, wrap("= ", defaultValue), join2(directives, " ")],
      " "
    )
  },
  InterfaceTypeDefinition: {
    leave: ({ description, name, interfaces, directives, fields }) => wrap("", description, "\n") + join2(
      [
        "interface",
        name,
        wrap("implements ", join2(interfaces, " & ")),
        join2(directives, " "),
        block(fields)
      ],
      " "
    )
  },
  UnionTypeDefinition: {
    leave: ({ description, name, directives, types }) => wrap("", description, "\n") + join2(
      ["union", name, join2(directives, " "), wrap("= ", join2(types, " | "))],
      " "
    )
  },
  EnumTypeDefinition: {
    leave: ({ description, name, directives, values }) => wrap("", description, "\n") + join2(["enum", name, join2(directives, " "), block(values)], " ")
  },
  EnumValueDefinition: {
    leave: ({ description, name, directives }) => wrap("", description, "\n") + join2([name, join2(directives, " ")], " ")
  },
  InputObjectTypeDefinition: {
    leave: ({ description, name, directives, fields }) => wrap("", description, "\n") + join2(["input", name, join2(directives, " "), block(fields)], " ")
  },
  DirectiveDefinition: {
    leave: ({
      description,
      name,
      arguments: args,
      directives,
      repeatable,
      locations
    }) => wrap("", description, "\n") + "directive @" + name + (hasMultilineItems(args) ? wrap("(\n", indent(join2(args, "\n")), "\n)") : wrap("(", join2(args, ", "), ")")) + wrap(" ", join2(directives, " ")) + (repeatable ? " repeatable" : "") + " on " + join2(locations, " | ")
  },
  SchemaExtension: {
    leave: ({ directives, operationTypes }) => join2(
      ["extend schema", join2(directives, " "), block(operationTypes)],
      " "
    )
  },
  ScalarTypeExtension: {
    leave: ({ name, directives }) => join2(["extend scalar", name, join2(directives, " ")], " ")
  },
  ObjectTypeExtension: {
    leave: ({ name, interfaces, directives, fields }) => join2(
      [
        "extend type",
        name,
        wrap("implements ", join2(interfaces, " & ")),
        join2(directives, " "),
        block(fields)
      ],
      " "
    )
  },
  InterfaceTypeExtension: {
    leave: ({ name, interfaces, directives, fields }) => join2(
      [
        "extend interface",
        name,
        wrap("implements ", join2(interfaces, " & ")),
        join2(directives, " "),
        block(fields)
      ],
      " "
    )
  },
  UnionTypeExtension: {
    leave: ({ name, directives, types }) => join2(
      [
        "extend union",
        name,
        join2(directives, " "),
        wrap("= ", join2(types, " | "))
      ],
      " "
    )
  },
  EnumTypeExtension: {
    leave: ({ name, directives, values }) => join2(["extend enum", name, join2(directives, " "), block(values)], " ")
  },
  InputObjectTypeExtension: {
    leave: ({ name, directives, fields }) => join2(["extend input", name, join2(directives, " "), block(fields)], " ")
  },
  DirectiveExtension: {
    leave: ({ name, directives }) => join2(["extend directive @" + name, join2(directives, " ")], " ")
  },
  // Schema Coordinates
  TypeCoordinate: {
    leave: ({ name }) => name
  },
  MemberCoordinate: {
    leave: ({ name, memberName }) => join2([name, wrap(".", memberName)])
  },
  ArgumentCoordinate: {
    leave: ({ name, fieldName, argumentName }) => join2([name, wrap(".", fieldName), wrap("(", argumentName, ":)")])
  },
  DirectiveCoordinate: {
    leave: ({ name }) => join2(["@", name])
  },
  DirectiveArgumentCoordinate: {
    leave: ({ name, argumentName }) => join2(["@", name, wrap("(", argumentName, ":)")])
  }
};
function join2(maybeArray, separator = "") {
  var _maybeArray$filter$jo;
  return (_maybeArray$filter$jo = maybeArray === null || maybeArray === void 0 ? void 0 : maybeArray.filter((x) => x).join(separator)) !== null && _maybeArray$filter$jo !== void 0 ? _maybeArray$filter$jo : "";
}
function block(array2) {
  return wrap("{\n", indent(join2(array2, "\n")), "\n}");
}
function wrap(start, maybeString, end = "") {
  return maybeString != null && maybeString !== "" ? start + maybeString + end : "";
}
function indent(str) {
  return wrap("  ", str.replace(/\n/g, "\n  "));
}
function hasMultilineItems(maybeArray) {
  var _maybeArray$some;
  return (_maybeArray$some = maybeArray === null || maybeArray === void 0 ? void 0 : maybeArray.some((str) => str.includes("\n"))) !== null && _maybeArray$some !== void 0 ? _maybeArray$some : false;
}

// node_modules/@mysten/sui/dist/esm/experimental/cache.js
var __typeError2 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck2 = (obj, member, msg) => member.has(obj) || __typeError2("Cannot " + msg);
var __privateGet2 = (obj, member, getter) => (__accessCheck2(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd2 = (obj, member, value) => member.has(obj) ? __typeError2("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet2 = (obj, member, value, setter) => (__accessCheck2(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var _prefix;
var _cache;
var _ClientCache = class _ClientCache2 {
  constructor({ prefix, cache } = {}) {
    __privateAdd2(this, _prefix);
    __privateAdd2(this, _cache);
    __privateSet2(this, _prefix, prefix ?? []);
    __privateSet2(this, _cache, cache ?? /* @__PURE__ */ new Map());
  }
  read(key, load) {
    const cacheKey = [__privateGet2(this, _prefix), ...key].join(":");
    if (__privateGet2(this, _cache).has(cacheKey)) {
      return __privateGet2(this, _cache).get(cacheKey);
    }
    const result = load();
    __privateGet2(this, _cache).set(cacheKey, result);
    if (typeof result === "object" && result !== null && "then" in result) {
      return Promise.resolve(result).then((v) => {
        __privateGet2(this, _cache).set(cacheKey, v);
        return v;
      }).catch((err) => {
        __privateGet2(this, _cache).delete(cacheKey);
        throw err;
      });
    }
    return result;
  }
  readSync(key, load) {
    const cacheKey = [__privateGet2(this, _prefix), ...key].join(":");
    if (__privateGet2(this, _cache).has(cacheKey)) {
      return __privateGet2(this, _cache).get(cacheKey);
    }
    const result = load();
    __privateGet2(this, _cache).set(cacheKey, result);
    return result;
  }
  clear(prefix) {
    const prefixKey = [...__privateGet2(this, _prefix), ...prefix ?? []].join(":");
    if (!prefixKey) {
      __privateGet2(this, _cache).clear();
      return;
    }
    for (const key of __privateGet2(this, _cache).keys()) {
      if (key.startsWith(prefixKey)) {
        __privateGet2(this, _cache).delete(key);
      }
    }
  }
  scope(prefix) {
    return new _ClientCache2({
      prefix: [...__privateGet2(this, _prefix), ...Array.isArray(prefix) ? prefix : [prefix]],
      cache: __privateGet2(this, _cache)
    });
  }
};
_prefix = /* @__PURE__ */ new WeakMap();
_cache = /* @__PURE__ */ new WeakMap();
var ClientCache = _ClientCache;

// node_modules/@mysten/sui/dist/esm/experimental/client.js
var Experimental_BaseClient = class {
  constructor({
    network,
    base,
    cache = base?.cache ?? new ClientCache()
  }) {
    this.network = network;
    this.base = base ?? this;
    this.cache = cache;
  }
  $extend(...registrations) {
    return Object.create(
      this,
      Object.fromEntries(
        registrations.map((registration) => {
          return [registration.name, { value: registration.register(this) }];
        })
      )
    );
  }
};

// node_modules/@mysten/sui/dist/esm/utils/dynamic-fields.js
function deriveDynamicFieldID(parentId, typeTag, key) {
  const address = suiBcs.Address.serialize(parentId).toBytes();
  const tag = suiBcs.TypeTag.serialize(typeTag).toBytes();
  const keyLength = suiBcs.u64().serialize(key.length).toBytes();
  const hash = blake2b2.create({
    dkLen: 32
  });
  hash.update(new Uint8Array([240]));
  hash.update(address);
  hash.update(keyLength);
  hash.update(key);
  hash.update(tag);
  return `0x${toHex(hash.digest().slice(0, 32))}`;
}

// node_modules/@mysten/sui/dist/esm/version.js
var PACKAGE_VERSION = "1.45.2";
var TARGETED_RPC_VERSION = "1.62.0";

// node_modules/@mysten/sui/dist/esm/experimental/mvr.js
var __typeError3 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck3 = (obj, member, msg) => member.has(obj) || __typeError3("Cannot " + msg);
var __privateGet3 = (obj, member, getter) => (__accessCheck3(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd3 = (obj, member, value) => member.has(obj) ? __typeError3("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet3 = (obj, member, value, setter) => (__accessCheck3(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateMethod = (obj, member, method) => (__accessCheck3(obj, member, "access private method"), method);
var _cache2;
var _url;
var _pageSize;
var _overrides;
var _MvrClient_instances;
var mvrPackageDataLoader_get;
var mvrTypeDataLoader_get;
var resolvePackages_fn;
var resolveTypes_fn;
var fetch_fn;
var NAME_SEPARATOR2 = "/";
var MVR_API_HEADER = {
  "Mvr-Source": `@mysten/sui@${PACKAGE_VERSION}`
};
var MvrClient = class {
  constructor({ cache, url, pageSize = 50, overrides }) {
    __privateAdd3(this, _MvrClient_instances);
    __privateAdd3(this, _cache2);
    __privateAdd3(this, _url);
    __privateAdd3(this, _pageSize);
    __privateAdd3(this, _overrides);
    __privateSet3(this, _cache2, cache);
    __privateSet3(this, _url, url);
    __privateSet3(this, _pageSize, pageSize);
    __privateSet3(this, _overrides, {
      packages: overrides?.packages,
      types: overrides?.types
    });
    validateOverrides(__privateGet3(this, _overrides));
  }
  async resolvePackage({
    package: name
  }) {
    if (!hasMvrName(name)) {
      return {
        package: name
      };
    }
    const resolved = await __privateGet3(this, _MvrClient_instances, mvrPackageDataLoader_get).load(name);
    return {
      package: resolved
    };
  }
  async resolveType({
    type
  }) {
    if (!hasMvrName(type)) {
      return {
        type
      };
    }
    const mvrTypes = [...extractMvrTypes(type)];
    const resolvedTypes = await __privateGet3(this, _MvrClient_instances, mvrTypeDataLoader_get).loadMany(mvrTypes);
    const typeMap = {};
    for (let i = 0; i < mvrTypes.length; i++) {
      const resolvedType = resolvedTypes[i];
      if (resolvedType instanceof Error) {
        throw resolvedType;
      }
      typeMap[mvrTypes[i]] = resolvedType;
    }
    return {
      type: replaceMvrNames(type, typeMap)
    };
  }
  async resolve({
    types = [],
    packages = []
  }) {
    const mvrTypes = /* @__PURE__ */ new Set();
    for (const type of types ?? []) {
      extractMvrTypes(type, mvrTypes);
    }
    const typesArray = [...mvrTypes];
    const [resolvedTypes, resolvedPackages] = await Promise.all([
      typesArray.length > 0 ? __privateGet3(this, _MvrClient_instances, mvrTypeDataLoader_get).loadMany(typesArray) : [],
      packages.length > 0 ? __privateGet3(this, _MvrClient_instances, mvrPackageDataLoader_get).loadMany(packages) : []
    ]);
    const typeMap = {
      ...__privateGet3(this, _overrides)?.types
    };
    for (const [i, type] of typesArray.entries()) {
      const resolvedType = resolvedTypes[i];
      if (resolvedType instanceof Error) {
        throw resolvedType;
      }
      typeMap[type] = resolvedType;
    }
    const replacedTypes = {};
    for (const type of types ?? []) {
      const resolvedType = replaceMvrNames(type, typeMap);
      replacedTypes[type] = {
        type: resolvedType
      };
    }
    const replacedPackages = {};
    for (const [i, pkg] of (packages ?? []).entries()) {
      const resolvedPkg = __privateGet3(this, _overrides)?.packages?.[pkg] ?? resolvedPackages[i];
      if (resolvedPkg instanceof Error) {
        throw resolvedPkg;
      }
      replacedPackages[pkg] = {
        package: resolvedPkg
      };
    }
    return {
      types: replacedTypes,
      packages: replacedPackages
    };
  }
};
_cache2 = /* @__PURE__ */ new WeakMap();
_url = /* @__PURE__ */ new WeakMap();
_pageSize = /* @__PURE__ */ new WeakMap();
_overrides = /* @__PURE__ */ new WeakMap();
_MvrClient_instances = /* @__PURE__ */ new WeakSet();
mvrPackageDataLoader_get = function() {
  return __privateGet3(this, _cache2).readSync(["#mvrPackageDataLoader", __privateGet3(this, _url) ?? ""], () => {
    const loader = new DataLoader(async (packages) => {
      if (!__privateGet3(this, _url)) {
        throw new Error(
          `MVR Api URL is not set for the current client (resolving ${packages.join(", ")})`
        );
      }
      const resolved = await __privateMethod(this, _MvrClient_instances, resolvePackages_fn).call(this, packages);
      return packages.map(
        (pkg) => resolved[pkg] ?? new Error(`Failed to resolve package: ${pkg}`)
      );
    });
    const overrides = __privateGet3(this, _overrides)?.packages;
    if (overrides) {
      for (const [pkg, id] of Object.entries(overrides)) {
        loader.prime(pkg, id);
      }
    }
    return loader;
  });
};
mvrTypeDataLoader_get = function() {
  return __privateGet3(this, _cache2).readSync(["#mvrTypeDataLoader", __privateGet3(this, _url) ?? ""], () => {
    const loader = new DataLoader(async (types) => {
      if (!__privateGet3(this, _url)) {
        throw new Error(
          `MVR Api URL is not set for the current client (resolving ${types.join(", ")})`
        );
      }
      const resolved = await __privateMethod(this, _MvrClient_instances, resolveTypes_fn).call(this, types);
      return types.map((type) => resolved[type] ?? new Error(`Failed to resolve type: ${type}`));
    });
    const overrides = __privateGet3(this, _overrides)?.types;
    if (overrides) {
      for (const [type, id] of Object.entries(overrides)) {
        loader.prime(type, id);
      }
    }
    return loader;
  });
};
resolvePackages_fn = async function(packages) {
  if (packages.length === 0) return {};
  const batches = chunk(packages, __privateGet3(this, _pageSize));
  const results = {};
  await Promise.all(
    batches.map(async (batch) => {
      const data = await __privateMethod(this, _MvrClient_instances, fetch_fn).call(this, "/v1/resolution/bulk", {
        names: batch
      });
      if (!data?.resolution) return;
      for (const pkg of Object.keys(data?.resolution)) {
        const pkgData = data.resolution[pkg]?.package_id;
        if (!pkgData) continue;
        results[pkg] = pkgData;
      }
    })
  );
  return results;
};
resolveTypes_fn = async function(types) {
  if (types.length === 0) return {};
  const batches = chunk(types, __privateGet3(this, _pageSize));
  const results = {};
  await Promise.all(
    batches.map(async (batch) => {
      const data = await __privateMethod(this, _MvrClient_instances, fetch_fn).call(this, "/v1/struct-definition/bulk", {
        types: batch
      });
      if (!data?.resolution) return;
      for (const type of Object.keys(data?.resolution)) {
        const typeData = data.resolution[type]?.type_tag;
        if (!typeData) continue;
        results[type] = typeData;
      }
    })
  );
  return results;
};
fetch_fn = async function(url, body) {
  if (!__privateGet3(this, _url)) {
    throw new Error("MVR Api URL is not set for the current client");
  }
  const response = await fetch(`${__privateGet3(this, _url)}${url}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...MVR_API_HEADER
    },
    body: JSON.stringify(body)
  });
  if (!response.ok) {
    const errorBody = await response.json().catch(() => ({}));
    throw new Error(`Failed to resolve types: ${errorBody?.message}`);
  }
  return response.json();
};
function validateOverrides(overrides) {
  if (overrides?.packages) {
    for (const [pkg, id] of Object.entries(overrides.packages)) {
      if (!isValidNamedPackage(pkg)) {
        throw new Error(`Invalid package name: ${pkg}`);
      }
      if (!isValidSuiAddress(normalizeSuiAddress(id))) {
        throw new Error(`Invalid package ID: ${id}`);
      }
    }
  }
  if (overrides?.types) {
    for (const [type, val] of Object.entries(overrides.types)) {
      if (parseStructTag(type).typeParams.length > 0) {
        throw new Error(
          "Type overrides must be first-level only. If you want to supply generic types, just pass each type individually."
        );
      }
      const parsedValue = parseStructTag(val);
      if (!isValidSuiAddress(parsedValue.address)) {
        throw new Error(`Invalid type: ${val}`);
      }
    }
  }
}
function extractMvrTypes(type, types = /* @__PURE__ */ new Set()) {
  if (typeof type === "string" && !hasMvrName(type)) return types;
  const tag = isStructTag(type) ? type : parseStructTag(type);
  if (hasMvrName(tag.address)) types.add(`${tag.address}::${tag.module}::${tag.name}`);
  for (const param of tag.typeParams) {
    extractMvrTypes(param, types);
  }
  return types;
}
function replaceMvrNames(tag, typeCache) {
  const type = isStructTag(tag) ? tag : parseStructTag(tag);
  const typeTag = `${type.address}::${type.module}::${type.name}`;
  const cacheHit = typeCache[typeTag];
  return normalizeStructTag({
    ...type,
    address: cacheHit ? cacheHit.split("::")[0] : type.address,
    typeParams: type.typeParams.map((param) => replaceMvrNames(param, typeCache))
  });
}
function hasMvrName(nameOrType) {
  return nameOrType.includes(NAME_SEPARATOR2) || nameOrType.includes("@") || nameOrType.includes(".sui");
}
function isStructTag(type) {
  return typeof type === "object" && "address" in type && "module" in type && "name" in type && "typeParams" in type;
}
function findNamesInTransaction(builder) {
  const packages = /* @__PURE__ */ new Set();
  const types = /* @__PURE__ */ new Set();
  for (const command of builder.commands) {
    switch (command.$kind) {
      case "MakeMoveVec":
        if (command.MakeMoveVec.type) {
          getNamesFromTypeList([command.MakeMoveVec.type]).forEach((type) => {
            types.add(type);
          });
        }
        break;
      case "MoveCall":
        const moveCall = command.MoveCall;
        const pkg = moveCall.package.split("::")[0];
        if (hasMvrName(pkg)) {
          if (!isValidNamedPackage(pkg)) throw new Error(`Invalid package name: ${pkg}`);
          packages.add(pkg);
        }
        getNamesFromTypeList(moveCall.typeArguments ?? []).forEach((type) => {
          types.add(type);
        });
        break;
      default:
        break;
    }
  }
  return {
    packages: [...packages],
    types: [...types]
  };
}
function replaceNames(builder, resolved) {
  for (const command of builder.commands) {
    if (command.MakeMoveVec?.type) {
      if (!hasMvrName(command.MakeMoveVec.type)) continue;
      if (!resolved.types[command.MakeMoveVec.type])
        throw new Error(`No resolution found for type: ${command.MakeMoveVec.type}`);
      command.MakeMoveVec.type = resolved.types[command.MakeMoveVec.type].type;
    }
    const tx = command.MoveCall;
    if (!tx) continue;
    const nameParts = tx.package.split("::");
    const name = nameParts[0];
    if (hasMvrName(name) && !resolved.packages[name])
      throw new Error(`No address found for package: ${name}`);
    if (hasMvrName(name)) {
      nameParts[0] = resolved.packages[name].package;
      tx.package = nameParts.join("::");
    }
    const types = tx.typeArguments;
    if (!types) continue;
    for (let i = 0; i < types.length; i++) {
      if (!hasMvrName(types[i])) continue;
      if (!resolved.types[types[i]]) throw new Error(`No resolution found for type: ${types[i]}`);
      types[i] = resolved.types[types[i]].type;
    }
    tx.typeArguments = types;
  }
}
function getNamesFromTypeList(types) {
  const names = /* @__PURE__ */ new Set();
  for (const type of types) {
    if (hasMvrName(type)) {
      if (!isValidNamedType(type)) throw new Error(`Invalid type with names: ${type}`);
      names.add(type);
    }
  }
  return names;
}

// node_modules/@mysten/sui/dist/esm/experimental/core.js
var DEFAULT_MVR_URLS = {
  mainnet: "https://mainnet.mvr.mystenlabs.com",
  testnet: "https://testnet.mvr.mystenlabs.com"
};
var Experimental_CoreClient = class extends Experimental_BaseClient {
  constructor(options) {
    super(options);
    this.core = this;
    this.mvr = new MvrClient({
      cache: this.cache.scope("core.mvr"),
      url: options.mvr?.url ?? DEFAULT_MVR_URLS[this.network],
      pageSize: options.mvr?.pageSize,
      overrides: options.mvr?.overrides
    });
  }
  async getObject(options) {
    const { objectId } = options;
    const {
      objects: [result]
    } = await this.getObjects({ objectIds: [objectId], signal: options.signal });
    if (result instanceof Error) {
      throw result;
    }
    return { object: result };
  }
  async getDynamicField(options) {
    const normalizedNameType = TypeTagSerializer.parseFromStr(
      (await this.core.mvr.resolveType({
        type: options.name.type
      })).type
    );
    const fieldId = deriveDynamicFieldID(options.parentId, normalizedNameType, options.name.bcs);
    const {
      objects: [fieldObject]
    } = await this.getObjects({
      objectIds: [fieldId],
      signal: options.signal
    });
    if (fieldObject instanceof Error) {
      throw fieldObject;
    }
    const fieldType = parseStructTag(fieldObject.type);
    const content = await fieldObject.content;
    return {
      dynamicField: {
        id: fieldObject.id,
        digest: fieldObject.digest,
        version: fieldObject.version,
        type: fieldObject.type,
        previousTransaction: fieldObject.previousTransaction,
        name: {
          type: typeof fieldType.typeParams[0] === "string" ? fieldType.typeParams[0] : normalizeStructTag(fieldType.typeParams[0]),
          bcs: options.name.bcs
        },
        value: {
          type: typeof fieldType.typeParams[1] === "string" ? fieldType.typeParams[1] : normalizeStructTag(fieldType.typeParams[1]),
          bcs: content.slice(SUI_ADDRESS_LENGTH + options.name.bcs.length)
        }
      }
    };
  }
  async waitForTransaction({
    signal,
    timeout = 60 * 1e3,
    ...input
  }) {
    const abortSignal = signal ? AbortSignal.any([AbortSignal.timeout(timeout), signal]) : AbortSignal.timeout(timeout);
    const abortPromise = new Promise((_, reject) => {
      abortSignal.addEventListener("abort", () => reject(abortSignal.reason));
    });
    abortPromise.catch(() => {
    });
    while (true) {
      abortSignal.throwIfAborted();
      try {
        return await this.getTransaction({
          ...input,
          signal: abortSignal
        });
      } catch {
        await Promise.race([new Promise((resolve) => setTimeout(resolve, 2e3)), abortPromise]);
      }
    }
  }
};

// node_modules/valibot/dist/index.mjs
var store$4;
var DEFAULT_CONFIG = {
  lang: void 0,
  message: void 0,
  abortEarly: void 0,
  abortPipeEarly: void 0
};
// @__NO_SIDE_EFFECTS__
function getGlobalConfig(config$1) {
  if (!config$1 && !store$4) return DEFAULT_CONFIG;
  return {
    lang: config$1?.lang ?? store$4?.lang,
    message: config$1?.message,
    abortEarly: config$1?.abortEarly ?? store$4?.abortEarly,
    abortPipeEarly: config$1?.abortPipeEarly ?? store$4?.abortPipeEarly
  };
}
var store$3;
// @__NO_SIDE_EFFECTS__
function getGlobalMessage(lang) {
  return store$3?.get(lang);
}
var store$2;
// @__NO_SIDE_EFFECTS__
function getSchemaMessage(lang) {
  return store$2?.get(lang);
}
var store$1;
// @__NO_SIDE_EFFECTS__
function getSpecificMessage(reference, lang) {
  return store$1?.get(reference)?.get(lang);
}
// @__NO_SIDE_EFFECTS__
function _stringify(input) {
  const type = typeof input;
  if (type === "string") return `"${input}"`;
  if (type === "number" || type === "bigint" || type === "boolean") return `${input}`;
  if (type === "object" || type === "function") return (input && Object.getPrototypeOf(input)?.constructor?.name) ?? "null";
  return type;
}
function _addIssue(context, label, dataset, config$1, other) {
  const input = other && "input" in other ? other.input : dataset.value;
  const expected = other?.expected ?? context.expects ?? null;
  const received = other?.received ?? /* @__PURE__ */ _stringify(input);
  const issue = {
    kind: context.kind,
    type: context.type,
    input,
    expected,
    received,
    message: `Invalid ${label}: ${expected ? `Expected ${expected} but r` : "R"}eceived ${received}`,
    requirement: context.requirement,
    path: other?.path,
    issues: other?.issues,
    lang: config$1.lang,
    abortEarly: config$1.abortEarly,
    abortPipeEarly: config$1.abortPipeEarly
  };
  const isSchema = context.kind === "schema";
  const message$1 = other?.message ?? context.message ?? /* @__PURE__ */ getSpecificMessage(context.reference, issue.lang) ?? (isSchema ? /* @__PURE__ */ getSchemaMessage(issue.lang) : null) ?? config$1.message ?? /* @__PURE__ */ getGlobalMessage(issue.lang);
  if (message$1 !== void 0) issue.message = typeof message$1 === "function" ? message$1(issue) : message$1;
  if (isSchema) dataset.typed = false;
  if (dataset.issues) dataset.issues.push(issue);
  else dataset.issues = [issue];
}
var _standardCache = /* @__PURE__ */ new WeakMap();
// @__NO_SIDE_EFFECTS__
function _getStandardProps(context) {
  let cached = _standardCache.get(context);
  if (!cached) {
    cached = {
      version: 1,
      vendor: "valibot",
      validate(value$1) {
        return context["~run"]({ value: value$1 }, /* @__PURE__ */ getGlobalConfig());
      }
    };
    _standardCache.set(context, cached);
  }
  return cached;
}
// @__NO_SIDE_EFFECTS__
function _isValidObjectKey(object$1, key) {
  return Object.prototype.hasOwnProperty.call(object$1, key) && key !== "__proto__" && key !== "prototype" && key !== "constructor";
}
// @__NO_SIDE_EFFECTS__
function _joinExpects(values$1, separator) {
  const list = [...new Set(values$1)];
  if (list.length > 1) return `(${list.join(` ${separator} `)})`;
  return list[0] ?? "never";
}
var ValiError = class extends Error {
  /**
  * Creates a Valibot error with useful information.
  *
  * @param issues The error issues.
  */
  constructor(issues) {
    super(issues[0].message);
    this.name = "ValiError";
    this.issues = issues;
  }
};
// @__NO_SIDE_EFFECTS__
function check(requirement, message$1) {
  return {
    kind: "validation",
    type: "check",
    reference: check,
    async: false,
    expects: null,
    requirement,
    message: message$1,
    "~run"(dataset, config$1) {
      if (dataset.typed && !this.requirement(dataset.value)) _addIssue(this, "input", dataset, config$1);
      return dataset;
    }
  };
}
// @__NO_SIDE_EFFECTS__
function integer(message$1) {
  return {
    kind: "validation",
    type: "integer",
    reference: integer,
    async: false,
    expects: null,
    requirement: Number.isInteger,
    message: message$1,
    "~run"(dataset, config$1) {
      if (dataset.typed && !this.requirement(dataset.value)) _addIssue(this, "integer", dataset, config$1);
      return dataset;
    }
  };
}
// @__NO_SIDE_EFFECTS__
function transform(operation) {
  return {
    kind: "transformation",
    type: "transform",
    reference: transform,
    async: false,
    operation,
    "~run"(dataset) {
      dataset.value = this.operation(dataset.value);
      return dataset;
    }
  };
}
var ABORT_EARLY_CONFIG = { abortEarly: true };
// @__NO_SIDE_EFFECTS__
function getFallback(schema, dataset, config$1) {
  return typeof schema.fallback === "function" ? schema.fallback(dataset, config$1) : schema.fallback;
}
// @__NO_SIDE_EFFECTS__
function getDefault(schema, dataset, config$1) {
  return typeof schema.default === "function" ? schema.default(dataset, config$1) : schema.default;
}
// @__NO_SIDE_EFFECTS__
function is(schema, input) {
  return !schema["~run"]({ value: input }, ABORT_EARLY_CONFIG).issues;
}
// @__NO_SIDE_EFFECTS__
function array(item, message$1) {
  return {
    kind: "schema",
    type: "array",
    reference: array,
    expects: "Array",
    async: false,
    item,
    message: message$1,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      const input = dataset.value;
      if (Array.isArray(input)) {
        dataset.typed = true;
        dataset.value = [];
        for (let key = 0; key < input.length; key++) {
          const value$1 = input[key];
          const itemDataset = this.item["~run"]({ value: value$1 }, config$1);
          if (itemDataset.issues) {
            const pathItem = {
              type: "array",
              origin: "value",
              input,
              key,
              value: value$1
            };
            for (const issue of itemDataset.issues) {
              if (issue.path) issue.path.unshift(pathItem);
              else issue.path = [pathItem];
              dataset.issues?.push(issue);
            }
            if (!dataset.issues) dataset.issues = itemDataset.issues;
            if (config$1.abortEarly) {
              dataset.typed = false;
              break;
            }
          }
          if (!itemDataset.typed) dataset.typed = false;
          dataset.value.push(itemDataset.value);
        }
      } else _addIssue(this, "type", dataset, config$1);
      return dataset;
    }
  };
}
// @__NO_SIDE_EFFECTS__
function bigint(message$1) {
  return {
    kind: "schema",
    type: "bigint",
    reference: bigint,
    expects: "bigint",
    async: false,
    message: message$1,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      if (typeof dataset.value === "bigint") dataset.typed = true;
      else _addIssue(this, "type", dataset, config$1);
      return dataset;
    }
  };
}
// @__NO_SIDE_EFFECTS__
function boolean(message$1) {
  return {
    kind: "schema",
    type: "boolean",
    reference: boolean,
    expects: "boolean",
    async: false,
    message: message$1,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      if (typeof dataset.value === "boolean") dataset.typed = true;
      else _addIssue(this, "type", dataset, config$1);
      return dataset;
    }
  };
}
// @__NO_SIDE_EFFECTS__
function lazy(getter) {
  return {
    kind: "schema",
    type: "lazy",
    reference: lazy,
    expects: "unknown",
    async: false,
    getter,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      return this.getter(dataset.value)["~run"](dataset, config$1);
    }
  };
}
// @__NO_SIDE_EFFECTS__
function literal(literal_, message$1) {
  return {
    kind: "schema",
    type: "literal",
    reference: literal,
    expects: /* @__PURE__ */ _stringify(literal_),
    async: false,
    literal: literal_,
    message: message$1,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      if (dataset.value === this.literal) dataset.typed = true;
      else _addIssue(this, "type", dataset, config$1);
      return dataset;
    }
  };
}
// @__NO_SIDE_EFFECTS__
function nullable(wrapped, default_) {
  return {
    kind: "schema",
    type: "nullable",
    reference: nullable,
    expects: `(${wrapped.expects} | null)`,
    async: false,
    wrapped,
    default: default_,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      if (dataset.value === null) {
        if (this.default !== void 0) dataset.value = /* @__PURE__ */ getDefault(this, dataset, config$1);
        if (dataset.value === null) {
          dataset.typed = true;
          return dataset;
        }
      }
      return this.wrapped["~run"](dataset, config$1);
    }
  };
}
// @__NO_SIDE_EFFECTS__
function nullish(wrapped, default_) {
  return {
    kind: "schema",
    type: "nullish",
    reference: nullish,
    expects: `(${wrapped.expects} | null | undefined)`,
    async: false,
    wrapped,
    default: default_,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      if (dataset.value === null || dataset.value === void 0) {
        if (this.default !== void 0) dataset.value = /* @__PURE__ */ getDefault(this, dataset, config$1);
        if (dataset.value === null || dataset.value === void 0) {
          dataset.typed = true;
          return dataset;
        }
      }
      return this.wrapped["~run"](dataset, config$1);
    }
  };
}
// @__NO_SIDE_EFFECTS__
function number(message$1) {
  return {
    kind: "schema",
    type: "number",
    reference: number,
    expects: "number",
    async: false,
    message: message$1,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      if (typeof dataset.value === "number" && !isNaN(dataset.value)) dataset.typed = true;
      else _addIssue(this, "type", dataset, config$1);
      return dataset;
    }
  };
}
// @__NO_SIDE_EFFECTS__
function object(entries$1, message$1) {
  return {
    kind: "schema",
    type: "object",
    reference: object,
    expects: "Object",
    async: false,
    entries: entries$1,
    message: message$1,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      const input = dataset.value;
      if (input && typeof input === "object") {
        dataset.typed = true;
        dataset.value = {};
        for (const key in this.entries) {
          const valueSchema = this.entries[key];
          if (key in input || (valueSchema.type === "exact_optional" || valueSchema.type === "optional" || valueSchema.type === "nullish") && valueSchema.default !== void 0) {
            const value$1 = key in input ? input[key] : /* @__PURE__ */ getDefault(valueSchema);
            const valueDataset = valueSchema["~run"]({ value: value$1 }, config$1);
            if (valueDataset.issues) {
              const pathItem = {
                type: "object",
                origin: "value",
                input,
                key,
                value: value$1
              };
              for (const issue of valueDataset.issues) {
                if (issue.path) issue.path.unshift(pathItem);
                else issue.path = [pathItem];
                dataset.issues?.push(issue);
              }
              if (!dataset.issues) dataset.issues = valueDataset.issues;
              if (config$1.abortEarly) {
                dataset.typed = false;
                break;
              }
            }
            if (!valueDataset.typed) dataset.typed = false;
            dataset.value[key] = valueDataset.value;
          } else if (valueSchema.fallback !== void 0) dataset.value[key] = /* @__PURE__ */ getFallback(valueSchema);
          else if (valueSchema.type !== "exact_optional" && valueSchema.type !== "optional" && valueSchema.type !== "nullish") {
            _addIssue(this, "key", dataset, config$1, {
              input: void 0,
              expected: `"${key}"`,
              path: [{
                type: "object",
                origin: "key",
                input,
                key,
                value: input[key]
              }]
            });
            if (config$1.abortEarly) break;
          }
        }
      } else _addIssue(this, "type", dataset, config$1);
      return dataset;
    }
  };
}
// @__NO_SIDE_EFFECTS__
function optional(wrapped, default_) {
  return {
    kind: "schema",
    type: "optional",
    reference: optional,
    expects: `(${wrapped.expects} | undefined)`,
    async: false,
    wrapped,
    default: default_,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      if (dataset.value === void 0) {
        if (this.default !== void 0) dataset.value = /* @__PURE__ */ getDefault(this, dataset, config$1);
        if (dataset.value === void 0) {
          dataset.typed = true;
          return dataset;
        }
      }
      return this.wrapped["~run"](dataset, config$1);
    }
  };
}
// @__NO_SIDE_EFFECTS__
function record(key, value$1, message$1) {
  return {
    kind: "schema",
    type: "record",
    reference: record,
    expects: "Object",
    async: false,
    key,
    value: value$1,
    message: message$1,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      const input = dataset.value;
      if (input && typeof input === "object") {
        dataset.typed = true;
        dataset.value = {};
        for (const entryKey in input) if (/* @__PURE__ */ _isValidObjectKey(input, entryKey)) {
          const entryValue = input[entryKey];
          const keyDataset = this.key["~run"]({ value: entryKey }, config$1);
          if (keyDataset.issues) {
            const pathItem = {
              type: "object",
              origin: "key",
              input,
              key: entryKey,
              value: entryValue
            };
            for (const issue of keyDataset.issues) {
              issue.path = [pathItem];
              dataset.issues?.push(issue);
            }
            if (!dataset.issues) dataset.issues = keyDataset.issues;
            if (config$1.abortEarly) {
              dataset.typed = false;
              break;
            }
          }
          const valueDataset = this.value["~run"]({ value: entryValue }, config$1);
          if (valueDataset.issues) {
            const pathItem = {
              type: "object",
              origin: "value",
              input,
              key: entryKey,
              value: entryValue
            };
            for (const issue of valueDataset.issues) {
              if (issue.path) issue.path.unshift(pathItem);
              else issue.path = [pathItem];
              dataset.issues?.push(issue);
            }
            if (!dataset.issues) dataset.issues = valueDataset.issues;
            if (config$1.abortEarly) {
              dataset.typed = false;
              break;
            }
          }
          if (!keyDataset.typed || !valueDataset.typed) dataset.typed = false;
          if (keyDataset.typed) dataset.value[keyDataset.value] = valueDataset.value;
        }
      } else _addIssue(this, "type", dataset, config$1);
      return dataset;
    }
  };
}
// @__NO_SIDE_EFFECTS__
function string(message$1) {
  return {
    kind: "schema",
    type: "string",
    reference: string,
    expects: "string",
    async: false,
    message: message$1,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      if (typeof dataset.value === "string") dataset.typed = true;
      else _addIssue(this, "type", dataset, config$1);
      return dataset;
    }
  };
}
// @__NO_SIDE_EFFECTS__
function tuple(items, message$1) {
  return {
    kind: "schema",
    type: "tuple",
    reference: tuple,
    expects: "Array",
    async: false,
    items,
    message: message$1,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      const input = dataset.value;
      if (Array.isArray(input)) {
        dataset.typed = true;
        dataset.value = [];
        for (let key = 0; key < this.items.length; key++) {
          const value$1 = input[key];
          const itemDataset = this.items[key]["~run"]({ value: value$1 }, config$1);
          if (itemDataset.issues) {
            const pathItem = {
              type: "array",
              origin: "value",
              input,
              key,
              value: value$1
            };
            for (const issue of itemDataset.issues) {
              if (issue.path) issue.path.unshift(pathItem);
              else issue.path = [pathItem];
              dataset.issues?.push(issue);
            }
            if (!dataset.issues) dataset.issues = itemDataset.issues;
            if (config$1.abortEarly) {
              dataset.typed = false;
              break;
            }
          }
          if (!itemDataset.typed) dataset.typed = false;
          dataset.value.push(itemDataset.value);
        }
      } else _addIssue(this, "type", dataset, config$1);
      return dataset;
    }
  };
}
// @__NO_SIDE_EFFECTS__
function _subIssues(datasets) {
  let issues;
  if (datasets) for (const dataset of datasets) if (issues) for (const issue of dataset.issues) issues.push(issue);
  else issues = dataset.issues;
  return issues;
}
// @__NO_SIDE_EFFECTS__
function union(options, message$1) {
  return {
    kind: "schema",
    type: "union",
    reference: union,
    expects: /* @__PURE__ */ _joinExpects(options.map((option2) => option2.expects), "|"),
    async: false,
    options,
    message: message$1,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      let validDataset;
      let typedDatasets;
      let untypedDatasets;
      for (const schema of this.options) {
        const optionDataset = schema["~run"]({ value: dataset.value }, config$1);
        if (optionDataset.typed) if (optionDataset.issues) if (typedDatasets) typedDatasets.push(optionDataset);
        else typedDatasets = [optionDataset];
        else {
          validDataset = optionDataset;
          break;
        }
        else if (untypedDatasets) untypedDatasets.push(optionDataset);
        else untypedDatasets = [optionDataset];
      }
      if (validDataset) return validDataset;
      if (typedDatasets) {
        if (typedDatasets.length === 1) return typedDatasets[0];
        _addIssue(this, "type", dataset, config$1, { issues: /* @__PURE__ */ _subIssues(typedDatasets) });
        dataset.typed = true;
      } else if (untypedDatasets?.length === 1) return untypedDatasets[0];
      else _addIssue(this, "type", dataset, config$1, { issues: /* @__PURE__ */ _subIssues(untypedDatasets) });
      return dataset;
    }
  };
}
// @__NO_SIDE_EFFECTS__
function unknown() {
  return {
    kind: "schema",
    type: "unknown",
    reference: unknown,
    expects: "unknown",
    async: false,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset) {
      dataset.typed = true;
      return dataset;
    }
  };
}
function parse2(schema, input, config$1) {
  const dataset = schema["~run"]({ value: input }, /* @__PURE__ */ getGlobalConfig(config$1));
  if (dataset.issues) throw new ValiError(dataset.issues);
  return dataset.value;
}
// @__NO_SIDE_EFFECTS__
function pipe(...pipe$1) {
  return {
    ...pipe$1[0],
    pipe: pipe$1,
    get "~standard"() {
      return /* @__PURE__ */ _getStandardProps(this);
    },
    "~run"(dataset, config$1) {
      for (const item of pipe$1) if (item.kind !== "metadata") {
        if (dataset.issues && (item.kind === "schema" || item.kind === "transformation")) {
          dataset.typed = false;
          break;
        }
        if (!dataset.issues || !config$1.abortEarly && !config$1.abortPipeEarly) dataset = item["~run"](dataset, config$1);
      }
      return dataset;
    }
  };
}

// node_modules/@mysten/sui/dist/esm/transactions/data/internal.js
function safeEnum(options) {
  const unionOptions = Object.entries(options).map(([key, value]) => object({ [key]: value }));
  return pipe(
    union(unionOptions),
    transform(
      (value) => ({
        ...value,
        $kind: Object.keys(value)[0]
      })
    )
  );
}
var SuiAddress = pipe(
  string(),
  transform((value) => normalizeSuiAddress(value)),
  check(isValidSuiAddress)
);
var ObjectID = SuiAddress;
var BCSBytes = string();
var JsonU64 = pipe(
  union([string(), pipe(number(), integer())]),
  check((val) => {
    try {
      BigInt(val);
      return BigInt(val) >= 0 && BigInt(val) <= 18446744073709551615n;
    } catch {
      return false;
    }
  }, "Invalid u64")
);
var ObjectRefSchema = object({
  objectId: SuiAddress,
  version: JsonU64,
  digest: string()
});
var ArgumentSchema = pipe(
  union([
    object({ GasCoin: literal(true) }),
    object({ Input: pipe(number(), integer()), type: optional(literal("pure")) }),
    object({ Input: pipe(number(), integer()), type: optional(literal("object")) }),
    object({ Result: pipe(number(), integer()) }),
    object({ NestedResult: tuple([pipe(number(), integer()), pipe(number(), integer())]) })
  ]),
  transform((value) => ({
    ...value,
    $kind: Object.keys(value)[0]
  }))
  // Defined manually to add `type?: 'pure' | 'object'` to Input
);
var GasDataSchema = object({
  budget: nullable(JsonU64),
  price: nullable(JsonU64),
  owner: nullable(SuiAddress),
  payment: nullable(array(ObjectRefSchema))
});
var StructTagSchema = object({
  address: string(),
  module: string(),
  name: string(),
  // type_params in rust, should be updated to use camelCase
  typeParams: array(string())
});
var OpenMoveTypeSignatureBodySchema = union([
  literal("address"),
  literal("bool"),
  literal("u8"),
  literal("u16"),
  literal("u32"),
  literal("u64"),
  literal("u128"),
  literal("u256"),
  object({ vector: lazy(() => OpenMoveTypeSignatureBodySchema) }),
  object({
    datatype: object({
      package: string(),
      module: string(),
      type: string(),
      typeParameters: array(lazy(() => OpenMoveTypeSignatureBodySchema))
    })
  }),
  object({ typeParameter: pipe(number(), integer()) })
]);
var OpenMoveTypeSignatureSchema = object({
  ref: nullable(union([literal("&"), literal("&mut")])),
  body: OpenMoveTypeSignatureBodySchema
});
var ProgrammableMoveCallSchema = object({
  package: ObjectID,
  module: string(),
  function: string(),
  // snake case in rust
  typeArguments: array(string()),
  arguments: array(ArgumentSchema),
  _argumentTypes: optional(nullable(array(OpenMoveTypeSignatureSchema)))
});
var $Intent = object({
  name: string(),
  inputs: record(string(), union([ArgumentSchema, array(ArgumentSchema)])),
  data: record(string(), unknown())
});
var CommandSchema = safeEnum({
  MoveCall: ProgrammableMoveCallSchema,
  TransferObjects: object({
    objects: array(ArgumentSchema),
    address: ArgumentSchema
  }),
  SplitCoins: object({
    coin: ArgumentSchema,
    amounts: array(ArgumentSchema)
  }),
  MergeCoins: object({
    destination: ArgumentSchema,
    sources: array(ArgumentSchema)
  }),
  Publish: object({
    modules: array(BCSBytes),
    dependencies: array(ObjectID)
  }),
  MakeMoveVec: object({
    type: nullable(string()),
    elements: array(ArgumentSchema)
  }),
  Upgrade: object({
    modules: array(BCSBytes),
    dependencies: array(ObjectID),
    package: ObjectID,
    ticket: ArgumentSchema
  }),
  $Intent
});
var ObjectArgSchema = safeEnum({
  ImmOrOwnedObject: ObjectRefSchema,
  SharedObject: object({
    objectId: ObjectID,
    // snake case in rust
    initialSharedVersion: JsonU64,
    mutable: boolean()
  }),
  Receiving: ObjectRefSchema
});
var CallArgSchema = safeEnum({
  Object: ObjectArgSchema,
  Pure: object({
    bytes: BCSBytes
  }),
  UnresolvedPure: object({
    value: unknown()
  }),
  UnresolvedObject: object({
    objectId: ObjectID,
    version: optional(nullable(JsonU64)),
    digest: optional(nullable(string())),
    initialSharedVersion: optional(nullable(JsonU64)),
    mutable: optional(nullable(boolean()))
  })
});
var NormalizedCallArg = safeEnum({
  Object: ObjectArgSchema,
  Pure: object({
    bytes: BCSBytes
  })
});
var TransactionExpiration2 = safeEnum({
  None: literal(true),
  Epoch: JsonU64
});
var TransactionDataSchema = object({
  version: literal(2),
  sender: nullish(SuiAddress),
  expiration: nullish(TransactionExpiration2),
  gasData: GasDataSchema,
  inputs: array(CallArgSchema),
  commands: array(CommandSchema)
});

// node_modules/@mysten/sui/dist/esm/transactions/data/v1.js
var ObjectRef = object({
  digest: string(),
  objectId: string(),
  version: union([pipe(number(), integer()), string(), bigint()])
});
var ObjectArg2 = safeEnum({
  ImmOrOwned: ObjectRef,
  Shared: object({
    objectId: ObjectID,
    initialSharedVersion: JsonU64,
    mutable: boolean()
  }),
  Receiving: ObjectRef
});
var NormalizedCallArg2 = safeEnum({
  Object: ObjectArg2,
  Pure: array(pipe(number(), integer()))
});
var TransactionInput = union([
  object({
    kind: literal("Input"),
    index: pipe(number(), integer()),
    value: unknown(),
    type: optional(literal("object"))
  }),
  object({
    kind: literal("Input"),
    index: pipe(number(), integer()),
    value: unknown(),
    type: literal("pure")
  })
]);
var TransactionExpiration3 = union([
  object({ Epoch: pipe(number(), integer()) }),
  object({ None: nullable(literal(true)) })
]);
var StringEncodedBigint = pipe(
  union([number(), string(), bigint()]),
  check((val) => {
    if (!["string", "number", "bigint"].includes(typeof val)) return false;
    try {
      BigInt(val);
      return true;
    } catch {
      return false;
    }
  })
);
var TypeTag2 = union([
  object({ bool: nullable(literal(true)) }),
  object({ u8: nullable(literal(true)) }),
  object({ u64: nullable(literal(true)) }),
  object({ u128: nullable(literal(true)) }),
  object({ address: nullable(literal(true)) }),
  object({ signer: nullable(literal(true)) }),
  object({ vector: lazy(() => TypeTag2) }),
  object({ struct: lazy(() => StructTag2) }),
  object({ u16: nullable(literal(true)) }),
  object({ u32: nullable(literal(true)) }),
  object({ u256: nullable(literal(true)) })
]);
var StructTag2 = object({
  address: string(),
  module: string(),
  name: string(),
  typeParams: array(TypeTag2)
});
var GasConfig = object({
  budget: optional(StringEncodedBigint),
  price: optional(StringEncodedBigint),
  payment: optional(array(ObjectRef)),
  owner: optional(string())
});
var TransactionArgumentTypes = [
  TransactionInput,
  object({ kind: literal("GasCoin") }),
  object({ kind: literal("Result"), index: pipe(number(), integer()) }),
  object({
    kind: literal("NestedResult"),
    index: pipe(number(), integer()),
    resultIndex: pipe(number(), integer())
  })
];
var TransactionArgument = union([...TransactionArgumentTypes]);
var MoveCallTransaction = object({
  kind: literal("MoveCall"),
  target: pipe(
    string(),
    check((target) => target.split("::").length === 3)
  ),
  typeArguments: array(string()),
  arguments: array(TransactionArgument)
});
var TransferObjectsTransaction = object({
  kind: literal("TransferObjects"),
  objects: array(TransactionArgument),
  address: TransactionArgument
});
var SplitCoinsTransaction = object({
  kind: literal("SplitCoins"),
  coin: TransactionArgument,
  amounts: array(TransactionArgument)
});
var MergeCoinsTransaction = object({
  kind: literal("MergeCoins"),
  destination: TransactionArgument,
  sources: array(TransactionArgument)
});
var MakeMoveVecTransaction = object({
  kind: literal("MakeMoveVec"),
  type: union([object({ Some: TypeTag2 }), object({ None: nullable(literal(true)) })]),
  objects: array(TransactionArgument)
});
var PublishTransaction = object({
  kind: literal("Publish"),
  modules: array(array(pipe(number(), integer()))),
  dependencies: array(string())
});
var UpgradeTransaction = object({
  kind: literal("Upgrade"),
  modules: array(array(pipe(number(), integer()))),
  dependencies: array(string()),
  packageId: string(),
  ticket: TransactionArgument
});
var TransactionTypes = [
  MoveCallTransaction,
  TransferObjectsTransaction,
  SplitCoinsTransaction,
  MergeCoinsTransaction,
  PublishTransaction,
  UpgradeTransaction,
  MakeMoveVecTransaction
];
var TransactionType = union([...TransactionTypes]);
var SerializedTransactionDataV1 = object({
  version: literal(1),
  sender: optional(string()),
  expiration: nullish(TransactionExpiration3),
  gasConfig: GasConfig,
  inputs: array(TransactionInput),
  transactions: array(TransactionType)
});
function serializeV1TransactionData(transactionData) {
  const inputs = transactionData.inputs.map(
    (input, index) => {
      if (input.Object) {
        return {
          kind: "Input",
          index,
          value: {
            Object: input.Object.ImmOrOwnedObject ? {
              ImmOrOwned: input.Object.ImmOrOwnedObject
            } : input.Object.Receiving ? {
              Receiving: {
                digest: input.Object.Receiving.digest,
                version: input.Object.Receiving.version,
                objectId: input.Object.Receiving.objectId
              }
            } : {
              Shared: {
                mutable: input.Object.SharedObject.mutable,
                initialSharedVersion: input.Object.SharedObject.initialSharedVersion,
                objectId: input.Object.SharedObject.objectId
              }
            }
          },
          type: "object"
        };
      }
      if (input.Pure) {
        return {
          kind: "Input",
          index,
          value: {
            Pure: Array.from(fromBase64(input.Pure.bytes))
          },
          type: "pure"
        };
      }
      if (input.UnresolvedPure) {
        return {
          kind: "Input",
          type: "pure",
          index,
          value: input.UnresolvedPure.value
        };
      }
      if (input.UnresolvedObject) {
        return {
          kind: "Input",
          type: "object",
          index,
          value: input.UnresolvedObject.objectId
        };
      }
      throw new Error("Invalid input");
    }
  );
  return {
    version: 1,
    sender: transactionData.sender ?? void 0,
    expiration: transactionData.expiration?.$kind === "Epoch" ? { Epoch: Number(transactionData.expiration.Epoch) } : transactionData.expiration ? { None: true } : null,
    gasConfig: {
      owner: transactionData.gasData.owner ?? void 0,
      budget: transactionData.gasData.budget ?? void 0,
      price: transactionData.gasData.price ?? void 0,
      payment: transactionData.gasData.payment ?? void 0
    },
    inputs,
    transactions: transactionData.commands.map((command) => {
      if (command.MakeMoveVec) {
        return {
          kind: "MakeMoveVec",
          type: command.MakeMoveVec.type === null ? { None: true } : { Some: TypeTagSerializer.parseFromStr(command.MakeMoveVec.type) },
          objects: command.MakeMoveVec.elements.map(
            (arg) => convertTransactionArgument(arg, inputs)
          )
        };
      }
      if (command.MergeCoins) {
        return {
          kind: "MergeCoins",
          destination: convertTransactionArgument(command.MergeCoins.destination, inputs),
          sources: command.MergeCoins.sources.map((arg) => convertTransactionArgument(arg, inputs))
        };
      }
      if (command.MoveCall) {
        return {
          kind: "MoveCall",
          target: `${command.MoveCall.package}::${command.MoveCall.module}::${command.MoveCall.function}`,
          typeArguments: command.MoveCall.typeArguments,
          arguments: command.MoveCall.arguments.map(
            (arg) => convertTransactionArgument(arg, inputs)
          )
        };
      }
      if (command.Publish) {
        return {
          kind: "Publish",
          modules: command.Publish.modules.map((mod2) => Array.from(fromBase64(mod2))),
          dependencies: command.Publish.dependencies
        };
      }
      if (command.SplitCoins) {
        return {
          kind: "SplitCoins",
          coin: convertTransactionArgument(command.SplitCoins.coin, inputs),
          amounts: command.SplitCoins.amounts.map((arg) => convertTransactionArgument(arg, inputs))
        };
      }
      if (command.TransferObjects) {
        return {
          kind: "TransferObjects",
          objects: command.TransferObjects.objects.map(
            (arg) => convertTransactionArgument(arg, inputs)
          ),
          address: convertTransactionArgument(command.TransferObjects.address, inputs)
        };
      }
      if (command.Upgrade) {
        return {
          kind: "Upgrade",
          modules: command.Upgrade.modules.map((mod2) => Array.from(fromBase64(mod2))),
          dependencies: command.Upgrade.dependencies,
          packageId: command.Upgrade.package,
          ticket: convertTransactionArgument(command.Upgrade.ticket, inputs)
        };
      }
      throw new Error(`Unknown transaction ${Object.keys(command)}`);
    })
  };
}
function convertTransactionArgument(arg, inputs) {
  if (arg.$kind === "GasCoin") {
    return { kind: "GasCoin" };
  }
  if (arg.$kind === "Result") {
    return { kind: "Result", index: arg.Result };
  }
  if (arg.$kind === "NestedResult") {
    return { kind: "NestedResult", index: arg.NestedResult[0], resultIndex: arg.NestedResult[1] };
  }
  if (arg.$kind === "Input") {
    return inputs[arg.Input];
  }
  throw new Error(`Invalid argument ${Object.keys(arg)}`);
}
function transactionDataFromV1(data) {
  return parse2(TransactionDataSchema, {
    version: 2,
    sender: data.sender ?? null,
    expiration: data.expiration ? "Epoch" in data.expiration ? { Epoch: data.expiration.Epoch } : { None: true } : null,
    gasData: {
      owner: data.gasConfig.owner ?? null,
      budget: data.gasConfig.budget?.toString() ?? null,
      price: data.gasConfig.price?.toString() ?? null,
      payment: data.gasConfig.payment?.map((ref) => ({
        digest: ref.digest,
        objectId: ref.objectId,
        version: ref.version.toString()
      })) ?? null
    },
    inputs: data.inputs.map((input) => {
      if (input.kind === "Input") {
        if (is(NormalizedCallArg2, input.value)) {
          const value = parse2(NormalizedCallArg2, input.value);
          if (value.Object) {
            if (value.Object.ImmOrOwned) {
              return {
                Object: {
                  ImmOrOwnedObject: {
                    objectId: value.Object.ImmOrOwned.objectId,
                    version: String(value.Object.ImmOrOwned.version),
                    digest: value.Object.ImmOrOwned.digest
                  }
                }
              };
            }
            if (value.Object.Shared) {
              return {
                Object: {
                  SharedObject: {
                    mutable: value.Object.Shared.mutable ?? null,
                    initialSharedVersion: value.Object.Shared.initialSharedVersion,
                    objectId: value.Object.Shared.objectId
                  }
                }
              };
            }
            if (value.Object.Receiving) {
              return {
                Object: {
                  Receiving: {
                    digest: value.Object.Receiving.digest,
                    version: String(value.Object.Receiving.version),
                    objectId: value.Object.Receiving.objectId
                  }
                }
              };
            }
            throw new Error("Invalid object input");
          }
          return {
            Pure: {
              bytes: toBase64(new Uint8Array(value.Pure))
            }
          };
        }
        if (input.type === "object") {
          return {
            UnresolvedObject: {
              objectId: input.value
            }
          };
        }
        return {
          UnresolvedPure: {
            value: input.value
          }
        };
      }
      throw new Error("Invalid input");
    }),
    commands: data.transactions.map((transaction) => {
      switch (transaction.kind) {
        case "MakeMoveVec":
          return {
            MakeMoveVec: {
              type: "Some" in transaction.type ? TypeTagSerializer.tagToString(transaction.type.Some) : null,
              elements: transaction.objects.map((arg) => parseV1TransactionArgument(arg))
            }
          };
        case "MergeCoins": {
          return {
            MergeCoins: {
              destination: parseV1TransactionArgument(transaction.destination),
              sources: transaction.sources.map((arg) => parseV1TransactionArgument(arg))
            }
          };
        }
        case "MoveCall": {
          const [pkg, mod2, fn] = transaction.target.split("::");
          return {
            MoveCall: {
              package: pkg,
              module: mod2,
              function: fn,
              typeArguments: transaction.typeArguments,
              arguments: transaction.arguments.map((arg) => parseV1TransactionArgument(arg))
            }
          };
        }
        case "Publish": {
          return {
            Publish: {
              modules: transaction.modules.map((mod2) => toBase64(Uint8Array.from(mod2))),
              dependencies: transaction.dependencies
            }
          };
        }
        case "SplitCoins": {
          return {
            SplitCoins: {
              coin: parseV1TransactionArgument(transaction.coin),
              amounts: transaction.amounts.map((arg) => parseV1TransactionArgument(arg))
            }
          };
        }
        case "TransferObjects": {
          return {
            TransferObjects: {
              objects: transaction.objects.map((arg) => parseV1TransactionArgument(arg)),
              address: parseV1TransactionArgument(transaction.address)
            }
          };
        }
        case "Upgrade": {
          return {
            Upgrade: {
              modules: transaction.modules.map((mod2) => toBase64(Uint8Array.from(mod2))),
              dependencies: transaction.dependencies,
              package: transaction.packageId,
              ticket: parseV1TransactionArgument(transaction.ticket)
            }
          };
        }
      }
      throw new Error(`Unknown transaction ${Object.keys(transaction)}`);
    })
  });
}
function parseV1TransactionArgument(arg) {
  switch (arg.kind) {
    case "GasCoin": {
      return { GasCoin: true };
    }
    case "Result":
      return { Result: arg.index };
    case "NestedResult": {
      return { NestedResult: [arg.index, arg.resultIndex] };
    }
    case "Input": {
      return { Input: arg.index };
    }
  }
}

// node_modules/@mysten/sui/dist/esm/transactions/hash.js
function hashTypedData(typeTag, data) {
  const typeTagBytes = Array.from(`${typeTag}::`).map((e) => e.charCodeAt(0));
  const dataWithTag = new Uint8Array(typeTagBytes.length + data.length);
  dataWithTag.set(typeTagBytes);
  dataWithTag.set(data, typeTagBytes.length);
  return blake2b2(dataWithTag, { dkLen: 32 });
}

// node_modules/@mysten/sui/dist/esm/transactions/utils.js
function getIdFromCallArg(arg) {
  if (typeof arg === "string") {
    return normalizeSuiAddress(arg);
  }
  if (arg.Object) {
    if (arg.Object.ImmOrOwnedObject) {
      return normalizeSuiAddress(arg.Object.ImmOrOwnedObject.objectId);
    }
    if (arg.Object.Receiving) {
      return normalizeSuiAddress(arg.Object.Receiving.objectId);
    }
    return normalizeSuiAddress(arg.Object.SharedObject.objectId);
  }
  if (arg.UnresolvedObject) {
    return normalizeSuiAddress(arg.UnresolvedObject.objectId);
  }
  return void 0;
}
function isArgument(value) {
  return is(ArgumentSchema, value);
}
function remapCommandArguments(command, inputMapping, commandMapping) {
  const remapArg = (arg) => {
    switch (arg.$kind) {
      case "Input": {
        const newInputIndex = inputMapping.get(arg.Input);
        if (newInputIndex === void 0) {
          throw new Error(`Input ${arg.Input} not found in input mapping`);
        }
        return { ...arg, Input: newInputIndex };
      }
      case "Result": {
        const newCommandIndex = commandMapping.get(arg.Result);
        if (newCommandIndex !== void 0) {
          return { ...arg, Result: newCommandIndex };
        }
        return arg;
      }
      case "NestedResult": {
        const newCommandIndex = commandMapping.get(arg.NestedResult[0]);
        if (newCommandIndex !== void 0) {
          return { ...arg, NestedResult: [newCommandIndex, arg.NestedResult[1]] };
        }
        return arg;
      }
      default:
        return arg;
    }
  };
  switch (command.$kind) {
    case "MoveCall":
      command.MoveCall.arguments = command.MoveCall.arguments.map(remapArg);
      break;
    case "TransferObjects":
      command.TransferObjects.objects = command.TransferObjects.objects.map(remapArg);
      command.TransferObjects.address = remapArg(command.TransferObjects.address);
      break;
    case "SplitCoins":
      command.SplitCoins.coin = remapArg(command.SplitCoins.coin);
      command.SplitCoins.amounts = command.SplitCoins.amounts.map(remapArg);
      break;
    case "MergeCoins":
      command.MergeCoins.destination = remapArg(command.MergeCoins.destination);
      command.MergeCoins.sources = command.MergeCoins.sources.map(remapArg);
      break;
    case "MakeMoveVec":
      command.MakeMoveVec.elements = command.MakeMoveVec.elements.map(remapArg);
      break;
    case "Upgrade":
      command.Upgrade.ticket = remapArg(command.Upgrade.ticket);
      break;
    case "$Intent": {
      const inputs = command.$Intent.inputs;
      command.$Intent.inputs = {};
      for (const [key, value] of Object.entries(inputs)) {
        command.$Intent.inputs[key] = Array.isArray(value) ? value.map(remapArg) : remapArg(value);
      }
      break;
    }
    case "Publish":
      break;
  }
}

// node_modules/@mysten/sui/dist/esm/transactions/TransactionData.js
function prepareSuiAddress(address) {
  return normalizeSuiAddress(address).replace("0x", "");
}
var TransactionDataBuilder = class _TransactionDataBuilder {
  constructor(clone) {
    this.version = 2;
    this.sender = clone?.sender ?? null;
    this.expiration = clone?.expiration ?? null;
    this.inputs = clone?.inputs ?? [];
    this.commands = clone?.commands ?? [];
    this.gasData = clone?.gasData ?? {
      budget: null,
      price: null,
      owner: null,
      payment: null
    };
  }
  static fromKindBytes(bytes) {
    const kind = suiBcs.TransactionKind.parse(bytes);
    const programmableTx = kind.ProgrammableTransaction;
    if (!programmableTx) {
      throw new Error("Unable to deserialize from bytes.");
    }
    return _TransactionDataBuilder.restore({
      version: 2,
      sender: null,
      expiration: null,
      gasData: {
        budget: null,
        owner: null,
        payment: null,
        price: null
      },
      inputs: programmableTx.inputs,
      commands: programmableTx.commands
    });
  }
  static fromBytes(bytes) {
    const rawData = suiBcs.TransactionData.parse(bytes);
    const data = rawData?.V1;
    const programmableTx = data.kind.ProgrammableTransaction;
    if (!data || !programmableTx) {
      throw new Error("Unable to deserialize from bytes.");
    }
    return _TransactionDataBuilder.restore({
      version: 2,
      sender: data.sender,
      expiration: data.expiration,
      gasData: data.gasData,
      inputs: programmableTx.inputs,
      commands: programmableTx.commands
    });
  }
  static restore(data) {
    if (data.version === 2) {
      return new _TransactionDataBuilder(parse2(TransactionDataSchema, data));
    } else {
      return new _TransactionDataBuilder(parse2(TransactionDataSchema, transactionDataFromV1(data)));
    }
  }
  /**
   * Generate transaction digest.
   *
   * @param bytes BCS serialized transaction data
   * @returns transaction digest.
   */
  static getDigestFromBytes(bytes) {
    const hash = hashTypedData("TransactionData", bytes);
    return toBase58(hash);
  }
  // @deprecated use gasData instead
  get gasConfig() {
    return this.gasData;
  }
  // @deprecated use gasData instead
  set gasConfig(value) {
    this.gasData = value;
  }
  build({
    maxSizeBytes = Infinity,
    overrides,
    onlyTransactionKind
  } = {}) {
    const inputs = this.inputs;
    const commands = this.commands;
    const kind = {
      ProgrammableTransaction: {
        inputs,
        commands
      }
    };
    if (onlyTransactionKind) {
      return suiBcs.TransactionKind.serialize(kind, { maxSize: maxSizeBytes }).toBytes();
    }
    const expiration = overrides?.expiration ?? this.expiration;
    const sender = overrides?.sender ?? this.sender;
    const gasData = { ...this.gasData, ...overrides?.gasConfig, ...overrides?.gasData };
    if (!sender) {
      throw new Error("Missing transaction sender");
    }
    if (!gasData.budget) {
      throw new Error("Missing gas budget");
    }
    if (!gasData.payment) {
      throw new Error("Missing gas payment");
    }
    if (!gasData.price) {
      throw new Error("Missing gas price");
    }
    const transactionData = {
      sender: prepareSuiAddress(sender),
      expiration: expiration ? expiration : { None: true },
      gasData: {
        payment: gasData.payment,
        owner: prepareSuiAddress(this.gasData.owner ?? sender),
        price: BigInt(gasData.price),
        budget: BigInt(gasData.budget)
      },
      kind: {
        ProgrammableTransaction: {
          inputs,
          commands
        }
      }
    };
    return suiBcs.TransactionData.serialize(
      { V1: transactionData },
      { maxSize: maxSizeBytes }
    ).toBytes();
  }
  addInput(type, arg) {
    const index = this.inputs.length;
    this.inputs.push(arg);
    return { Input: index, type, $kind: "Input" };
  }
  getInputUses(index, fn) {
    this.mapArguments((arg, command) => {
      if (arg.$kind === "Input" && arg.Input === index) {
        fn(arg, command);
      }
      return arg;
    });
  }
  mapCommandArguments(index, fn) {
    const command = this.commands[index];
    switch (command.$kind) {
      case "MoveCall":
        command.MoveCall.arguments = command.MoveCall.arguments.map(
          (arg) => fn(arg, command, index)
        );
        break;
      case "TransferObjects":
        command.TransferObjects.objects = command.TransferObjects.objects.map(
          (arg) => fn(arg, command, index)
        );
        command.TransferObjects.address = fn(command.TransferObjects.address, command, index);
        break;
      case "SplitCoins":
        command.SplitCoins.coin = fn(command.SplitCoins.coin, command, index);
        command.SplitCoins.amounts = command.SplitCoins.amounts.map(
          (arg) => fn(arg, command, index)
        );
        break;
      case "MergeCoins":
        command.MergeCoins.destination = fn(command.MergeCoins.destination, command, index);
        command.MergeCoins.sources = command.MergeCoins.sources.map(
          (arg) => fn(arg, command, index)
        );
        break;
      case "MakeMoveVec":
        command.MakeMoveVec.elements = command.MakeMoveVec.elements.map(
          (arg) => fn(arg, command, index)
        );
        break;
      case "Upgrade":
        command.Upgrade.ticket = fn(command.Upgrade.ticket, command, index);
        break;
      case "$Intent":
        const inputs = command.$Intent.inputs;
        command.$Intent.inputs = {};
        for (const [key, value] of Object.entries(inputs)) {
          command.$Intent.inputs[key] = Array.isArray(value) ? value.map((arg) => fn(arg, command, index)) : fn(value, command, index);
        }
        break;
      case "Publish":
        break;
      default:
        throw new Error(`Unexpected transaction kind: ${command.$kind}`);
    }
  }
  mapArguments(fn) {
    for (const commandIndex of this.commands.keys()) {
      this.mapCommandArguments(commandIndex, fn);
    }
  }
  replaceCommand(index, replacement, resultIndex = index) {
    if (!Array.isArray(replacement)) {
      this.commands[index] = replacement;
      return;
    }
    const sizeDiff = replacement.length - 1;
    this.commands.splice(index, 1, ...structuredClone(replacement));
    this.mapArguments((arg, _command, commandIndex) => {
      if (commandIndex < index + replacement.length) {
        return arg;
      }
      if (typeof resultIndex !== "number") {
        if (arg.$kind === "Result" && arg.Result === index || arg.$kind === "NestedResult" && arg.NestedResult[0] === index) {
          if (!("NestedResult" in arg) || arg.NestedResult[1] === 0) {
            return parse2(ArgumentSchema, structuredClone(resultIndex));
          } else {
            throw new Error(
              `Cannot replace command ${index} with a specific result type: NestedResult[${index}, ${arg.NestedResult[1]}] references a nested element that cannot be mapped to the replacement result`
            );
          }
        }
      }
      switch (arg.$kind) {
        case "Result":
          if (arg.Result === index && typeof resultIndex === "number") {
            arg.Result = resultIndex;
          }
          if (arg.Result > index) {
            arg.Result += sizeDiff;
          }
          break;
        case "NestedResult":
          if (arg.NestedResult[0] === index && typeof resultIndex === "number") {
            return {
              $kind: "NestedResult",
              NestedResult: [resultIndex, arg.NestedResult[1]]
            };
          }
          if (arg.NestedResult[0] > index) {
            arg.NestedResult[0] += sizeDiff;
          }
          break;
      }
      return arg;
    });
  }
  replaceCommandWithTransaction(index, otherTransaction, result) {
    if (result.$kind !== "Result" && result.$kind !== "NestedResult") {
      throw new Error("Result must be of kind Result or NestedResult");
    }
    this.insertTransaction(index, otherTransaction);
    this.replaceCommand(
      index + otherTransaction.commands.length,
      [],
      "Result" in result ? { NestedResult: [result.Result + index, 0] } : {
        NestedResult: [
          result.NestedResult[0] + index,
          result.NestedResult[1]
        ]
      }
    );
  }
  insertTransaction(atCommandIndex, otherTransaction) {
    const inputMapping = /* @__PURE__ */ new Map();
    const commandMapping = /* @__PURE__ */ new Map();
    for (let i = 0; i < otherTransaction.inputs.length; i++) {
      const otherInput = otherTransaction.inputs[i];
      const id = getIdFromCallArg(otherInput);
      let existingIndex = -1;
      if (id !== void 0) {
        existingIndex = this.inputs.findIndex((input) => getIdFromCallArg(input) === id);
        if (existingIndex !== -1 && this.inputs[existingIndex].Object?.SharedObject && otherInput.Object?.SharedObject) {
          this.inputs[existingIndex].Object.SharedObject.mutable = this.inputs[existingIndex].Object.SharedObject.mutable || otherInput.Object.SharedObject.mutable;
        }
      }
      if (existingIndex !== -1) {
        inputMapping.set(i, existingIndex);
      } else {
        const newIndex = this.inputs.length;
        this.inputs.push(otherInput);
        inputMapping.set(i, newIndex);
      }
    }
    for (let i = 0; i < otherTransaction.commands.length; i++) {
      commandMapping.set(i, atCommandIndex + i);
    }
    const remappedCommands = [];
    for (let i = 0; i < otherTransaction.commands.length; i++) {
      const command = structuredClone(otherTransaction.commands[i]);
      remapCommandArguments(command, inputMapping, commandMapping);
      remappedCommands.push(command);
    }
    this.commands.splice(atCommandIndex, 0, ...remappedCommands);
    const sizeDiff = remappedCommands.length;
    if (sizeDiff > 0) {
      this.mapArguments((arg, _command, commandIndex) => {
        if (commandIndex >= atCommandIndex && commandIndex < atCommandIndex + remappedCommands.length) {
          return arg;
        }
        switch (arg.$kind) {
          case "Result":
            if (arg.Result >= atCommandIndex) {
              arg.Result += sizeDiff;
            }
            break;
          case "NestedResult":
            if (arg.NestedResult[0] >= atCommandIndex) {
              arg.NestedResult[0] += sizeDiff;
            }
            break;
        }
        return arg;
      });
    }
  }
  getDigest() {
    const bytes = this.build({ onlyTransactionKind: false });
    return _TransactionDataBuilder.getDigestFromBytes(bytes);
  }
  snapshot() {
    return parse2(TransactionDataSchema, this);
  }
  shallowClone() {
    return new _TransactionDataBuilder({
      version: this.version,
      sender: this.sender,
      expiration: this.expiration,
      gasData: {
        ...this.gasData
      },
      inputs: [...this.inputs],
      commands: [...this.commands]
    });
  }
  applyResolvedData(resolved) {
    if (!this.sender) {
      this.sender = resolved.sender ?? null;
    }
    if (!this.expiration) {
      this.expiration = resolved.expiration ?? null;
    }
    if (!this.gasData.budget) {
      this.gasData.budget = resolved.gasData.budget;
    }
    if (!this.gasData.owner) {
      this.gasData.owner = resolved.gasData.owner ?? null;
    }
    if (!this.gasData.payment) {
      this.gasData.payment = resolved.gasData.payment;
    }
    if (!this.gasData.price) {
      this.gasData.price = resolved.gasData.price;
    }
    for (let i = 0; i < this.inputs.length; i++) {
      const input = this.inputs[i];
      const resolvedInput = resolved.inputs[i];
      switch (input.$kind) {
        case "UnresolvedPure":
          if (resolvedInput.$kind !== "Pure") {
            throw new Error(
              `Expected input at index ${i} to resolve to a Pure argument, but got ${JSON.stringify(
                resolvedInput
              )}`
            );
          }
          this.inputs[i] = resolvedInput;
          break;
        case "UnresolvedObject":
          if (resolvedInput.$kind !== "Object") {
            throw new Error(
              `Expected input at index ${i} to resolve to an Object argument, but got ${JSON.stringify(
                resolvedInput
              )}`
            );
          }
          if (resolvedInput.Object.$kind === "ImmOrOwnedObject" || resolvedInput.Object.$kind === "Receiving") {
            const original = input.UnresolvedObject;
            const resolved2 = resolvedInput.Object.ImmOrOwnedObject ?? resolvedInput.Object.Receiving;
            if (normalizeSuiAddress(original.objectId) !== normalizeSuiAddress(resolved2.objectId) || original.version != null && original.version !== resolved2.version || original.digest != null && original.digest !== resolved2.digest || // Objects with shared object properties should not resolve to owned objects
            original.mutable != null || original.initialSharedVersion != null) {
              throw new Error(
                `Input at index ${i} did not match unresolved object. ${JSON.stringify(original)} is not compatible with ${JSON.stringify(resolved2)}`
              );
            }
          } else if (resolvedInput.Object.$kind === "SharedObject") {
            const original = input.UnresolvedObject;
            const resolved2 = resolvedInput.Object.SharedObject;
            if (normalizeSuiAddress(original.objectId) !== normalizeSuiAddress(resolved2.objectId) || original.initialSharedVersion != null && original.initialSharedVersion !== resolved2.initialSharedVersion || original.mutable != null && original.mutable !== resolved2.mutable || // Objects with owned object properties should not resolve to shared objects
            original.version != null || original.digest != null) {
              throw new Error(
                `Input at index ${i} did not match unresolved object. ${JSON.stringify(original)} is not compatible with ${JSON.stringify(resolved2)}`
              );
            }
          } else {
            throw new Error(
              `Input at index ${i} resolved to an unexpected Object kind: ${JSON.stringify(
                resolvedInput.Object
              )}`
            );
          }
          this.inputs[i] = resolvedInput;
          break;
      }
    }
  }
};

// node_modules/@mysten/sui/dist/esm/experimental/transports/utils.js
function parseTransactionBcs(bytes) {
  return {
    ...TransactionDataBuilder.fromBytes(bytes).snapshot(),
    bcs: bytes
  };
}
function parseTransactionEffectsBcs(effects) {
  const parsed = suiBcs.TransactionEffects.parse(effects);
  switch (parsed.$kind) {
    case "V1":
      return parseTransactionEffectsV1({ bytes: effects, effects: parsed.V1 });
    case "V2":
      return parseTransactionEffectsV2({ bytes: effects, effects: parsed.V2 });
    default:
      throw new Error(
        `Unknown transaction effects version: ${parsed.$kind}`
      );
  }
}
function parseTransactionEffectsV1(_) {
  throw new Error("V1 effects are not supported yet");
}
function parseTransactionEffectsV2({
  bytes,
  effects
}) {
  const changedObjects = effects.changedObjects.map(
    ([id, change]) => {
      return {
        id,
        inputState: change.inputState.$kind === "Exist" ? "Exists" : "DoesNotExist",
        inputVersion: change.inputState.Exist?.[0][0] ?? null,
        inputDigest: change.inputState.Exist?.[0][1] ?? null,
        inputOwner: change.inputState.Exist?.[1] ?? null,
        outputState: change.outputState.$kind === "NotExist" ? "DoesNotExist" : change.outputState.$kind,
        outputVersion: change.outputState.$kind === "PackageWrite" ? change.outputState.PackageWrite?.[0] : change.outputState.ObjectWrite ? effects.lamportVersion : null,
        outputDigest: change.outputState.$kind === "PackageWrite" ? change.outputState.PackageWrite?.[1] : change.outputState.ObjectWrite?.[0] ?? null,
        outputOwner: change.outputState.ObjectWrite ? change.outputState.ObjectWrite[1] : null,
        idOperation: change.idOperation.$kind
      };
    }
  );
  return {
    bcs: bytes,
    digest: effects.transactionDigest,
    version: 2,
    status: effects.status.$kind === "Success" ? {
      success: true,
      error: null
    } : {
      success: false,
      // TODO: add command
      error: effects.status.Failed.error.$kind
    },
    gasUsed: effects.gasUsed,
    transactionDigest: effects.transactionDigest,
    gasObject: effects.gasObjectIndex === null ? null : changedObjects[effects.gasObjectIndex] ?? null,
    eventsDigest: effects.eventsDigest,
    dependencies: effects.dependencies,
    lamportVersion: effects.lamportVersion,
    changedObjects,
    unchangedConsensusObjects: effects.unchangedSharedObjects.map(
      ([objectId, object2]) => {
        return {
          kind: object2.$kind === "MutateDeleted" ? "MutateConsensusStreamEnded" : object2.$kind === "ReadDeleted" ? "ReadConsensusStreamEnded" : object2.$kind,
          objectId,
          version: object2.$kind === "ReadOnlyRoot" ? object2.ReadOnlyRoot[0] : object2[object2.$kind],
          digest: object2.$kind === "ReadOnlyRoot" ? object2.ReadOnlyRoot[1] : null
        };
      }
    ),
    auxiliaryDataDigest: effects.auxDataDigest
  };
}

// node_modules/@mysten/sui/dist/esm/graphql/generated/queries.js
var ZkLoginIntentScope = /* @__PURE__ */ ((ZkLoginIntentScope2) => {
  ZkLoginIntentScope2["PersonalMessage"] = "PERSONAL_MESSAGE";
  ZkLoginIntentScope2["TransactionData"] = "TRANSACTION_DATA";
  return ZkLoginIntentScope2;
})(ZkLoginIntentScope || {});
var TypedDocumentString = class extends String {
  constructor(value, __meta__) {
    super(value);
    this.value = value;
    this.__meta__ = __meta__;
  }
  toString() {
    return this.value;
  }
};
var Object_Owner_FieldsFragmentDoc = new TypedDocumentString(`
    fragment OBJECT_OWNER_FIELDS on Owner {
  __typename
  ... on AddressOwner {
    address {
      address
    }
  }
  ... on ObjectOwner {
    address {
      address
    }
  }
  ... on Shared {
    initialSharedVersion
  }
  ... on ConsensusAddressOwner {
    startVersion
    address {
      address
    }
  }
}
    `, { "fragmentName": "OBJECT_OWNER_FIELDS" });
var Object_FieldsFragmentDoc = new TypedDocumentString(`
    fragment OBJECT_FIELDS on Object {
  address
  digest
  version
  asMoveObject {
    contents {
      bcs
      type {
        repr
      }
    }
  }
  owner {
    ...OBJECT_OWNER_FIELDS
  }
  previousTransaction {
    digest
  }
}
    fragment OBJECT_OWNER_FIELDS on Owner {
  __typename
  ... on AddressOwner {
    address {
      address
    }
  }
  ... on ObjectOwner {
    address {
      address
    }
  }
  ... on Shared {
    initialSharedVersion
  }
  ... on ConsensusAddressOwner {
    startVersion
    address {
      address
    }
  }
}`, { "fragmentName": "OBJECT_FIELDS" });
var Move_Object_FieldsFragmentDoc = new TypedDocumentString(`
    fragment MOVE_OBJECT_FIELDS on MoveObject {
  address
  digest
  version
  contents {
    bcs
    type {
      repr
    }
  }
  owner {
    ...OBJECT_OWNER_FIELDS
  }
  previousTransaction {
    digest
  }
}
    fragment OBJECT_OWNER_FIELDS on Owner {
  __typename
  ... on AddressOwner {
    address {
      address
    }
  }
  ... on ObjectOwner {
    address {
      address
    }
  }
  ... on Shared {
    initialSharedVersion
  }
  ... on ConsensusAddressOwner {
    startVersion
    address {
      address
    }
  }
}`, { "fragmentName": "MOVE_OBJECT_FIELDS" });
var Transaction_FieldsFragmentDoc = new TypedDocumentString(`
    fragment TRANSACTION_FIELDS on Transaction {
  digest
  transactionBcs
  signatures {
    signatureBytes
  }
  effects {
    effectsBcs
    epoch {
      epochId
    }
    unchangedConsensusObjects {
      nodes {
        __typename
        ... on ConsensusObjectRead {
          object {
            asMoveObject {
              address
              contents {
                type {
                  repr
                }
              }
            }
          }
        }
      }
    }
    objectChanges {
      nodes {
        address
        inputState {
          version
          asMoveObject {
            address
            contents {
              type {
                repr
              }
            }
          }
        }
        outputState {
          asMoveObject {
            address
            contents {
              type {
                repr
              }
            }
          }
        }
      }
    }
    balanceChanges(first: 50) {
      pageInfo {
        hasNextPage
      }
      nodes {
        owner {
          address
        }
        coinType {
          repr
        }
        amount
      }
    }
  }
}
    `, { "fragmentName": "TRANSACTION_FIELDS" });
var GetAllBalancesDocument = new TypedDocumentString(`
    query getAllBalances($owner: SuiAddress!, $limit: Int, $cursor: String) {
  address(address: $owner) {
    balances(first: $limit, after: $cursor) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        coinType {
          repr
        }
        totalBalance
      }
    }
  }
}
    `);
var GetBalanceDocument = new TypedDocumentString(`
    query getBalance($owner: SuiAddress!, $coinType: String! = "0x2::sui::SUI") {
  address(address: $owner) {
    balance(coinType: $coinType) {
      coinType {
        repr
      }
      totalBalance
    }
  }
}
    `);
var GetCoinsDocument = new TypedDocumentString(`
    query getCoins($owner: SuiAddress!, $first: Int, $cursor: String, $type: String! = "0x2::coin::Coin<0x2::sui::SUI>") {
  address(address: $owner) {
    address
    objects(first: $first, after: $cursor, filter: {type: $type}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        owner {
          ...OBJECT_OWNER_FIELDS
        }
        contents {
          bcs
          json
          type {
            repr
          }
        }
        address
        version
        digest
        previousTransaction {
          digest
        }
      }
    }
  }
}
    fragment OBJECT_OWNER_FIELDS on Owner {
  __typename
  ... on AddressOwner {
    address {
      address
    }
  }
  ... on ObjectOwner {
    address {
      address
    }
  }
  ... on Shared {
    initialSharedVersion
  }
  ... on ConsensusAddressOwner {
    startVersion
    address {
      address
    }
  }
}`);
var GetDynamicFieldsDocument = new TypedDocumentString(`
    query getDynamicFields($parentId: SuiAddress!, $first: Int, $cursor: String) {
  address(address: $parentId) {
    dynamicFields(first: $first, after: $cursor) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        name {
          bcs
          type {
            repr
          }
        }
        value {
          __typename
          ... on MoveValue {
            type {
              repr
            }
          }
          ... on MoveObject {
            contents {
              type {
                repr
              }
            }
          }
        }
      }
    }
  }
}
    `);
var GetMoveFunctionDocument = new TypedDocumentString(`
    query getMoveFunction($package: SuiAddress!, $module: String!, $function: String!) {
  package(address: $package) {
    module(name: $module) {
      function(name: $function) {
        name
        visibility
        isEntry
        typeParameters {
          constraints
        }
        parameters {
          signature
        }
        return {
          signature
        }
      }
    }
  }
}
    `);
var GetReferenceGasPriceDocument = new TypedDocumentString(`
    query getReferenceGasPrice {
  epoch {
    referenceGasPrice
  }
}
    `);
var DefaultSuinsNameDocument = new TypedDocumentString(`
    query defaultSuinsName($address: SuiAddress!) {
  address(address: $address) {
    defaultSuinsName
  }
}
    `);
var GetOwnedObjectsDocument = new TypedDocumentString(`
    query getOwnedObjects($owner: SuiAddress!, $limit: Int, $cursor: String, $filter: ObjectFilter) {
  address(address: $owner) {
    objects(first: $limit, after: $cursor, filter: $filter) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        ...MOVE_OBJECT_FIELDS
      }
    }
  }
}
    fragment MOVE_OBJECT_FIELDS on MoveObject {
  address
  digest
  version
  contents {
    bcs
    type {
      repr
    }
  }
  owner {
    ...OBJECT_OWNER_FIELDS
  }
  previousTransaction {
    digest
  }
}
fragment OBJECT_OWNER_FIELDS on Owner {
  __typename
  ... on AddressOwner {
    address {
      address
    }
  }
  ... on ObjectOwner {
    address {
      address
    }
  }
  ... on Shared {
    initialSharedVersion
  }
  ... on ConsensusAddressOwner {
    startVersion
    address {
      address
    }
  }
}`);
var MultiGetObjectsDocument = new TypedDocumentString(`
    query multiGetObjects($objectKeys: [ObjectKey!]!) {
  multiGetObjects(keys: $objectKeys) {
    ...OBJECT_FIELDS
  }
}
    fragment OBJECT_FIELDS on Object {
  address
  digest
  version
  asMoveObject {
    contents {
      bcs
      type {
        repr
      }
    }
  }
  owner {
    ...OBJECT_OWNER_FIELDS
  }
  previousTransaction {
    digest
  }
}
fragment OBJECT_OWNER_FIELDS on Owner {
  __typename
  ... on AddressOwner {
    address {
      address
    }
  }
  ... on ObjectOwner {
    address {
      address
    }
  }
  ... on Shared {
    initialSharedVersion
  }
  ... on ConsensusAddressOwner {
    startVersion
    address {
      address
    }
  }
}`);
var SimulateTransactionDocument = new TypedDocumentString(`
    query simulateTransaction($transaction: JSON!) {
  simulateTransaction(transaction: $transaction) {
    error
    effects {
      transaction {
        ...TRANSACTION_FIELDS
      }
    }
  }
}
    fragment TRANSACTION_FIELDS on Transaction {
  digest
  transactionBcs
  signatures {
    signatureBytes
  }
  effects {
    effectsBcs
    epoch {
      epochId
    }
    unchangedConsensusObjects {
      nodes {
        __typename
        ... on ConsensusObjectRead {
          object {
            asMoveObject {
              address
              contents {
                type {
                  repr
                }
              }
            }
          }
        }
      }
    }
    objectChanges {
      nodes {
        address
        inputState {
          version
          asMoveObject {
            address
            contents {
              type {
                repr
              }
            }
          }
        }
        outputState {
          asMoveObject {
            address
            contents {
              type {
                repr
              }
            }
          }
        }
      }
    }
    balanceChanges(first: 50) {
      pageInfo {
        hasNextPage
      }
      nodes {
        owner {
          address
        }
        coinType {
          repr
        }
        amount
      }
    }
  }
}`);
var ExecuteTransactionDocument = new TypedDocumentString(`
    mutation executeTransaction($transactionDataBcs: Base64!, $signatures: [Base64!]!) {
  executeTransaction(
    transactionDataBcs: $transactionDataBcs
    signatures: $signatures
  ) {
    errors
    effects {
      transaction {
        ...TRANSACTION_FIELDS
      }
    }
  }
}
    fragment TRANSACTION_FIELDS on Transaction {
  digest
  transactionBcs
  signatures {
    signatureBytes
  }
  effects {
    effectsBcs
    epoch {
      epochId
    }
    unchangedConsensusObjects {
      nodes {
        __typename
        ... on ConsensusObjectRead {
          object {
            asMoveObject {
              address
              contents {
                type {
                  repr
                }
              }
            }
          }
        }
      }
    }
    objectChanges {
      nodes {
        address
        inputState {
          version
          asMoveObject {
            address
            contents {
              type {
                repr
              }
            }
          }
        }
        outputState {
          asMoveObject {
            address
            contents {
              type {
                repr
              }
            }
          }
        }
      }
    }
    balanceChanges(first: 50) {
      pageInfo {
        hasNextPage
      }
      nodes {
        owner {
          address
        }
        coinType {
          repr
        }
        amount
      }
    }
  }
}`);
var GetTransactionBlockDocument = new TypedDocumentString(`
    query getTransactionBlock($digest: String!) {
  transaction(digest: $digest) {
    ...TRANSACTION_FIELDS
  }
}
    fragment TRANSACTION_FIELDS on Transaction {
  digest
  transactionBcs
  signatures {
    signatureBytes
  }
  effects {
    effectsBcs
    epoch {
      epochId
    }
    unchangedConsensusObjects {
      nodes {
        __typename
        ... on ConsensusObjectRead {
          object {
            asMoveObject {
              address
              contents {
                type {
                  repr
                }
              }
            }
          }
        }
      }
    }
    objectChanges {
      nodes {
        address
        inputState {
          version
          asMoveObject {
            address
            contents {
              type {
                repr
              }
            }
          }
        }
        outputState {
          asMoveObject {
            address
            contents {
              type {
                repr
              }
            }
          }
        }
      }
    }
    balanceChanges(first: 50) {
      pageInfo {
        hasNextPage
      }
      nodes {
        owner {
          address
        }
        coinType {
          repr
        }
        amount
      }
    }
  }
}`);
var VerifyZkLoginSignatureDocument = new TypedDocumentString(`
    query verifyZkLoginSignature($bytes: Base64!, $signature: Base64!, $intentScope: ZkLoginIntentScope!, $author: SuiAddress!) {
  verifyZkLoginSignature(
    bytes: $bytes
    signature: $signature
    intentScope: $intentScope
    author: $author
  ) {
    success
    error
  }
}
    `);

// node_modules/@mysten/sui/dist/esm/experimental/errors.js
var SuiClientError = class extends Error {
};
var ObjectError = class _ObjectError extends SuiClientError {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
  static fromResponse(response, objectId) {
    switch (response.code) {
      case "notExists":
        return new _ObjectError(response.code, `Object ${response.object_id} does not exist`);
      case "dynamicFieldNotFound":
        return new _ObjectError(
          response.code,
          `Dynamic field not found for object ${response.parent_object_id}`
        );
      case "deleted":
        return new _ObjectError(response.code, `Object ${response.object_id} has been deleted`);
      case "displayError":
        return new _ObjectError(response.code, `Display error: ${response.error}`);
      case "unknown":
      default:
        return new _ObjectError(
          response.code,
          `Unknown error while loading object${objectId ? ` ${objectId}` : ""}`
        );
    }
  }
};

// node_modules/@mysten/sui/dist/esm/graphql/core.js
var __typeError4 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck4 = (obj, member, msg) => member.has(obj) || __typeError4("Cannot " + msg);
var __privateGet4 = (obj, member, getter) => (__accessCheck4(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd4 = (obj, member, value) => member.has(obj) ? __typeError4("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet4 = (obj, member, value, setter) => (__accessCheck4(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateMethod2 = (obj, member, method) => (__accessCheck4(obj, member, "access private method"), method);
var _graphqlClient;
var _GraphQLCoreClient_instances;
var graphqlQuery_fn;
var GraphQLCoreClient = class extends Experimental_CoreClient {
  constructor({
    graphqlClient,
    mvr
  }) {
    super({ network: graphqlClient.network, base: graphqlClient, mvr });
    __privateAdd4(this, _GraphQLCoreClient_instances);
    __privateAdd4(this, _graphqlClient);
    __privateSet4(this, _graphqlClient, graphqlClient);
  }
  async getObjects(options) {
    const batches = chunk(options.objectIds, 50);
    const results = [];
    for (const batch of batches) {
      const page = await __privateMethod2(this, _GraphQLCoreClient_instances, graphqlQuery_fn).call(this, {
        query: MultiGetObjectsDocument,
        variables: {
          objectKeys: batch.map((address) => ({ address }))
        }
      }, (result) => result.multiGetObjects);
      results.push(
        ...batch.map((id) => normalizeSuiAddress(id)).map(
          (id) => page.find((obj) => obj?.address === id) ?? new ObjectError("notFound", `Object ${id} not found`)
        ).map((obj) => {
          if (obj instanceof ObjectError) {
            return obj;
          }
          return {
            id: obj.address,
            version: obj.version?.toString(),
            digest: obj.digest,
            owner: mapOwner(obj.owner),
            type: obj.asMoveObject?.contents?.type?.repr,
            content: Promise.resolve(
              obj.asMoveObject?.contents?.bcs ? fromBase64(obj.asMoveObject.contents.bcs) : new Uint8Array()
            ),
            previousTransaction: obj.previousTransaction?.digest ?? null
          };
        })
      );
    }
    return {
      objects: results
    };
  }
  async getOwnedObjects(options) {
    const objects = await __privateMethod2(this, _GraphQLCoreClient_instances, graphqlQuery_fn).call(this, {
      query: GetOwnedObjectsDocument,
      variables: {
        owner: options.address,
        limit: options.limit,
        cursor: options.cursor,
        filter: options.type ? { type: (await this.mvr.resolveType({ type: options.type })).type } : void 0
      }
    }, (result) => result.address?.objects);
    return {
      objects: objects.nodes.map((obj) => ({
        id: obj.address,
        version: obj.version?.toString(),
        digest: obj.digest,
        owner: mapOwner(obj.owner),
        type: obj.contents?.type?.repr,
        content: Promise.resolve(
          obj.contents?.bcs ? fromBase64(obj.contents.bcs) : new Uint8Array()
        ),
        previousTransaction: obj.previousTransaction?.digest ?? null
      })),
      hasNextPage: objects.pageInfo.hasNextPage,
      cursor: objects.pageInfo.endCursor ?? null
    };
  }
  async getCoins(options) {
    const coins = await __privateMethod2(this, _GraphQLCoreClient_instances, graphqlQuery_fn).call(this, {
      query: GetCoinsDocument,
      variables: {
        owner: options.address,
        cursor: options.cursor,
        first: options.limit,
        type: `0x2::coin::Coin<${(await this.mvr.resolveType({ type: options.coinType })).type}>`
      }
    }, (result) => result.address?.objects);
    return {
      cursor: coins.pageInfo.endCursor ?? null,
      hasNextPage: coins.pageInfo.hasNextPage,
      objects: coins.nodes.map((coin) => ({
        id: coin.address,
        version: coin.version?.toString(),
        digest: coin.digest,
        owner: mapOwner(coin.owner),
        type: coin.contents?.type?.repr,
        balance: coin.contents?.json?.balance,
        content: Promise.resolve(
          coin.contents?.bcs ? fromBase64(coin.contents.bcs) : new Uint8Array()
        ),
        previousTransaction: coin.previousTransaction?.digest ?? null
      }))
    };
  }
  async getBalance(options) {
    const result = await __privateMethod2(this, _GraphQLCoreClient_instances, graphqlQuery_fn).call(this, {
      query: GetBalanceDocument,
      variables: {
        owner: options.address,
        type: (await this.mvr.resolveType({ type: options.coinType })).type
      }
    }, (result2) => result2.address?.balance);
    return {
      balance: {
        coinType: result.coinType?.repr,
        balance: result.totalBalance
      }
    };
  }
  async getAllBalances(options) {
    const balances = await __privateMethod2(this, _GraphQLCoreClient_instances, graphqlQuery_fn).call(this, {
      query: GetAllBalancesDocument,
      variables: { owner: options.address }
    }, (result) => result.address?.balances);
    return {
      cursor: balances.pageInfo.endCursor ?? null,
      hasNextPage: balances.pageInfo.hasNextPage,
      balances: balances.nodes.map((balance) => ({
        coinType: balance.coinType?.repr,
        balance: balance.totalBalance
      }))
    };
  }
  async getTransaction(options) {
    const result = await __privateMethod2(this, _GraphQLCoreClient_instances, graphqlQuery_fn).call(this, {
      query: GetTransactionBlockDocument,
      variables: { digest: options.digest }
    }, (result2) => result2.transaction);
    return {
      transaction: parseTransaction(result)
    };
  }
  async executeTransaction(options) {
    const result = await __privateMethod2(this, _GraphQLCoreClient_instances, graphqlQuery_fn).call(this, {
      query: ExecuteTransactionDocument,
      variables: {
        transactionDataBcs: toBase64(options.transaction),
        signatures: options.signatures
      }
    }, (result2) => result2.executeTransaction);
    if (result.errors) {
      if (result.errors.length === 1) {
        throw new Error(result.errors[0]);
      }
      throw new AggregateError(result.errors.map((error) => new Error(error)));
    }
    return {
      transaction: parseTransaction(result.effects?.transaction)
    };
  }
  async dryRunTransaction(options) {
    const result = await __privateMethod2(this, _GraphQLCoreClient_instances, graphqlQuery_fn).call(this, {
      query: SimulateTransactionDocument,
      variables: {
        transaction: {
          bcs: {
            value: toBase64(options.transaction)
          }
        }
      }
    }, (result2) => result2.simulateTransaction);
    if (result.error) {
      throw new Error(result.error);
    }
    return {
      transaction: parseTransaction(result.effects?.transaction)
    };
  }
  async getReferenceGasPrice() {
    const result = await __privateMethod2(this, _GraphQLCoreClient_instances, graphqlQuery_fn).call(this, {
      query: GetReferenceGasPriceDocument
    }, (result2) => result2.epoch?.referenceGasPrice);
    return {
      referenceGasPrice: result
    };
  }
  async getDynamicFields(options) {
    const result = await __privateMethod2(this, _GraphQLCoreClient_instances, graphqlQuery_fn).call(this, {
      query: GetDynamicFieldsDocument,
      variables: { parentId: options.parentId }
    }, (result2) => result2.address?.dynamicFields);
    return {
      dynamicFields: result.nodes.map((dynamicField) => {
        const valueType = dynamicField.value?.__typename === "MoveObject" ? dynamicField.value.contents?.type?.repr : dynamicField.value?.type?.repr;
        return {
          id: deriveDynamicFieldID(
            options.parentId,
            dynamicField.name?.type?.repr,
            fromBase64(dynamicField.name?.bcs)
          ),
          type: normalizeStructTag(
            dynamicField.value?.__typename === "MoveObject" ? `0x2::dynamic_field::Field<0x2::dynamic_object_field::Wrapper<${dynamicField.name?.type?.repr}>,0x2::object::ID>` : `0x2::dynamic_field::Field<${dynamicField.name?.type?.repr},${valueType}>`
          ),
          name: {
            type: dynamicField.name?.type?.repr,
            bcs: fromBase64(dynamicField.name?.bcs)
          },
          valueType
        };
      }),
      cursor: result.pageInfo.endCursor ?? null,
      hasNextPage: result.pageInfo.hasNextPage
    };
  }
  async verifyZkLoginSignature(options) {
    const intentScope = options.intentScope === "TransactionData" ? ZkLoginIntentScope.TransactionData : ZkLoginIntentScope.PersonalMessage;
    const result = await __privateMethod2(this, _GraphQLCoreClient_instances, graphqlQuery_fn).call(this, {
      query: VerifyZkLoginSignatureDocument,
      variables: {
        bytes: options.bytes,
        signature: options.signature,
        intentScope,
        author: options.author
      }
    }, (result2) => result2.verifyZkLoginSignature);
    return {
      success: result.success ?? false,
      errors: result.error ? [result.error] : []
    };
  }
  async defaultNameServiceName(options) {
    const name = await __privateMethod2(this, _GraphQLCoreClient_instances, graphqlQuery_fn).call(this, {
      query: DefaultSuinsNameDocument,
      signal: options.signal,
      variables: {
        address: options.address
      }
    }, (result) => result.address?.defaultSuinsName ?? null);
    return {
      data: { name }
    };
  }
  async getMoveFunction(options) {
    const moveFunction = await __privateMethod2(this, _GraphQLCoreClient_instances, graphqlQuery_fn).call(this, {
      query: GetMoveFunctionDocument,
      variables: {
        package: (await this.mvr.resolvePackage({ package: options.packageId })).package,
        module: options.moduleName,
        function: options.name
      }
    }, (result) => result.package?.module?.function);
    let visibility = "unknown";
    switch (moveFunction.visibility) {
      case "PUBLIC":
        visibility = "public";
        break;
      case "PRIVATE":
        visibility = "private";
        break;
      case "FRIEND":
        visibility = "friend";
        break;
    }
    return {
      function: {
        packageId: normalizeSuiAddress(options.packageId),
        moduleName: options.moduleName,
        name: moveFunction.name,
        visibility,
        isEntry: moveFunction.isEntry ?? false,
        typeParameters: moveFunction.typeParameters?.map(({ constraints }) => ({
          isPhantom: false,
          constraints: constraints.map((constraint) => {
            switch (constraint) {
              case "COPY":
                return "copy";
              case "DROP":
                return "drop";
              case "STORE":
                return "store";
              case "KEY":
                return "key";
              default:
                return "unknown";
            }
          }) ?? []
        })) ?? [],
        parameters: moveFunction.parameters?.map((param) => parseNormalizedSuiMoveType(param.signature)) ?? [],
        returns: moveFunction.return?.map(({ signature }) => parseNormalizedSuiMoveType(signature)) ?? []
      }
    };
  }
  resolveTransactionPlugin() {
    throw new Error("GraphQL client does not support transaction resolution yet");
  }
};
_graphqlClient = /* @__PURE__ */ new WeakMap();
_GraphQLCoreClient_instances = /* @__PURE__ */ new WeakSet();
graphqlQuery_fn = async function(options, getData) {
  const { data, errors } = await __privateGet4(this, _graphqlClient).query(options);
  handleGraphQLErrors(errors);
  const extractedData = data && (getData ? getData(data) : data);
  if (extractedData == null) {
    throw new Error("Missing response data");
  }
  return extractedData;
};
function handleGraphQLErrors(errors) {
  if (!errors || errors.length === 0) return;
  const errorInstances = errors.map((error) => new GraphQLResponseError(error));
  if (errorInstances.length === 1) {
    throw errorInstances[0];
  }
  throw new AggregateError(errorInstances);
}
var GraphQLResponseError = class extends Error {
  constructor(error) {
    super(error.message);
    this.locations = error.locations;
  }
};
function mapOwner(owner) {
  switch (owner.__typename) {
    case "AddressOwner":
      return { $kind: "AddressOwner", AddressOwner: owner.address?.address };
    case "ConsensusAddressOwner":
      return {
        $kind: "ConsensusAddressOwner",
        ConsensusAddressOwner: {
          owner: owner?.address?.address,
          startVersion: String(owner.startVersion)
        }
      };
    case "ObjectOwner":
      return { $kind: "ObjectOwner", ObjectOwner: owner.address?.address };
    case "Immutable":
      return { $kind: "Immutable", Immutable: true };
    case "Shared":
      return {
        $kind: "Shared",
        Shared: { initialSharedVersion: String(owner.initialSharedVersion) }
      };
  }
}
function parseTransaction(transaction) {
  const objectTypes = {};
  transaction.effects?.unchangedConsensusObjects?.nodes.forEach((node) => {
    if (node.__typename === "ConsensusObjectRead") {
      const type = node.object?.asMoveObject?.contents?.type?.repr;
      const address = node.object?.asMoveObject?.address;
      if (type && address) {
        objectTypes[address] = type;
      }
    }
  });
  transaction.effects?.objectChanges?.nodes.forEach((node) => {
    const address = node.address;
    const type = node.inputState?.asMoveObject?.contents?.type?.repr ?? node.outputState?.asMoveObject?.contents?.type?.repr;
    if (address && type) {
      objectTypes[address] = type;
    }
  });
  if (transaction.effects?.balanceChanges?.pageInfo.hasNextPage) {
    throw new Error("Pagination for balance changes is not supported");
  }
  return {
    digest: transaction.digest,
    effects: parseTransactionEffectsBcs(fromBase64(transaction.effects?.effectsBcs)),
    epoch: transaction.effects?.epoch?.epochId?.toString() ?? null,
    objectTypes: Promise.resolve(objectTypes),
    transaction: parseTransactionBcs(fromBase64(transaction.transactionBcs)),
    signatures: transaction.signatures.map((sig) => sig.signatureBytes),
    balanceChanges: transaction.effects?.balanceChanges?.nodes.map((change) => ({
      coinType: change?.coinType?.repr,
      address: change.owner?.address,
      amount: change.amount
    })) ?? []
    // events: transaction.events?.pageInfo.hasNextPage
  };
}
function parseNormalizedSuiMoveType(type) {
  let reference = null;
  if (type.ref === "&") {
    reference = "immutable";
  } else if (type.ref === "&mut") {
    reference = "mutable";
  }
  return {
    reference,
    body: parseNormalizedSuiMoveTypeBody(type.body)
  };
}
function parseNormalizedSuiMoveTypeBody(type) {
  switch (type) {
    case "address":
      return { $kind: "address" };
    case "bool":
      return { $kind: "bool" };
    case "u8":
      return { $kind: "u8" };
    case "u16":
      return { $kind: "u16" };
    case "u32":
      return { $kind: "u32" };
    case "u64":
      return { $kind: "u64" };
    case "u128":
      return { $kind: "u128" };
    case "u256":
      return { $kind: "u256" };
  }
  if (typeof type === "string") {
    throw new Error(`Unknown type: ${type}`);
  }
  if ("vector" in type) {
    return {
      $kind: "vector",
      vector: parseNormalizedSuiMoveTypeBody(type.vector)
    };
  }
  if ("datatype" in type) {
    return {
      $kind: "datatype",
      datatype: {
        typeName: `${normalizeSuiAddress(type.datatype.package)}::${type.datatype.module}::${type.datatype.type}`,
        typeParameters: type.datatype.typeParameters.map((t) => parseNormalizedSuiMoveTypeBody(t))
      }
    };
  }
  if ("typeParameter" in type) {
    return {
      $kind: "typeParameter",
      index: type.typeParameter
    };
  }
  throw new Error(`Unknown type: ${JSON.stringify(type)}`);
}

// node_modules/@mysten/sui/dist/esm/graphql/client.js
var __typeError5 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck5 = (obj, member, msg) => member.has(obj) || __typeError5("Cannot " + msg);
var __privateGet5 = (obj, member, getter) => (__accessCheck5(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd5 = (obj, member, value) => member.has(obj) ? __typeError5("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet5 = (obj, member, value, setter) => (__accessCheck5(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var _url2;
var _queries;
var _headers;
var _fetch;
var SuiGraphQLRequestError = class extends Error {
};
var SuiGraphQLClient = class extends Experimental_BaseClient {
  constructor({
    url,
    fetch: fetchFn = fetch,
    headers = {},
    queries = {},
    network = "unknown",
    mvr
  }) {
    super({
      network
    });
    __privateAdd5(this, _url2);
    __privateAdd5(this, _queries);
    __privateAdd5(this, _headers);
    __privateAdd5(this, _fetch);
    __privateSet5(this, _url2, url);
    __privateSet5(this, _queries, queries);
    __privateSet5(this, _headers, headers);
    __privateSet5(this, _fetch, (...args) => fetchFn(...args));
    this.core = new GraphQLCoreClient({
      graphqlClient: this,
      mvr
    });
  }
  async query(options) {
    const res = await __privateGet5(this, _fetch).call(this, __privateGet5(this, _url2), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...__privateGet5(this, _headers)
      },
      body: JSON.stringify({
        query: typeof options.query === "string" || options.query instanceof String ? String(options.query) : print(options.query),
        variables: options.variables,
        extensions: options.extensions,
        operationName: options.operationName
      }),
      signal: options.signal
    });
    if (!res.ok) {
      throw new SuiGraphQLRequestError(`GraphQL request failed: ${res.statusText} (${res.status})`);
    }
    return await res.json();
  }
  async execute(query, options) {
    return this.query({
      ...options,
      query: __privateGet5(this, _queries)[query]
    });
  }
};
_url2 = /* @__PURE__ */ new WeakMap();
_queries = /* @__PURE__ */ new WeakMap();
_headers = /* @__PURE__ */ new WeakMap();
_fetch = /* @__PURE__ */ new WeakMap();

// node_modules/@mysten/sui/dist/esm/zklogin/utils.js
function findFirstNonZeroIndex(bytes) {
  for (let i = 0; i < bytes.length; i++) {
    if (bytes[i] !== 0) {
      return i;
    }
  }
  return -1;
}
function toPaddedBigEndianBytes(num, width) {
  const hex = num.toString(16);
  return hexToBytes(hex.padStart(width * 2, "0").slice(-width * 2));
}
function toBigEndianBytes(num, width) {
  const bytes = toPaddedBigEndianBytes(num, width);
  const firstNonZeroIndex = findFirstNonZeroIndex(bytes);
  if (firstNonZeroIndex === -1) {
    return new Uint8Array([0]);
  }
  return bytes.slice(firstNonZeroIndex);
}
function normalizeZkLoginIssuer(iss) {
  if (iss === "accounts.google.com") {
    return "https://accounts.google.com";
  }
  return iss;
}

// node_modules/@mysten/sui/dist/esm/zklogin/jwt-utils.js
function base64UrlCharTo6Bits(base64UrlChar) {
  if (base64UrlChar.length !== 1) {
    throw new Error("Invalid base64Url character: " + base64UrlChar);
  }
  const base64UrlCharacterSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  const index = base64UrlCharacterSet.indexOf(base64UrlChar);
  if (index === -1) {
    throw new Error("Invalid base64Url character: " + base64UrlChar);
  }
  const binaryString = index.toString(2).padStart(6, "0");
  const bits = Array.from(binaryString).map(Number);
  return bits;
}
function base64UrlStringToBitVector(base64UrlString) {
  let bitVector = [];
  for (let i = 0; i < base64UrlString.length; i++) {
    const base64UrlChar = base64UrlString.charAt(i);
    const bits = base64UrlCharTo6Bits(base64UrlChar);
    bitVector = bitVector.concat(bits);
  }
  return bitVector;
}
function decodeBase64URL(s, i) {
  if (s.length < 2) {
    throw new Error(`Input (s = ${s}) is not tightly packed because s.length < 2`);
  }
  let bits = base64UrlStringToBitVector(s);
  const firstCharOffset = i % 4;
  if (firstCharOffset === 0) {
  } else if (firstCharOffset === 1) {
    bits = bits.slice(2);
  } else if (firstCharOffset === 2) {
    bits = bits.slice(4);
  } else {
    throw new Error(`Input (s = ${s}) is not tightly packed because i%4 = 3 (i = ${i}))`);
  }
  const lastCharOffset = (i + s.length - 1) % 4;
  if (lastCharOffset === 3) {
  } else if (lastCharOffset === 2) {
    bits = bits.slice(0, bits.length - 2);
  } else if (lastCharOffset === 1) {
    bits = bits.slice(0, bits.length - 4);
  } else {
    throw new Error(
      `Input (s = ${s}) is not tightly packed because (i + s.length - 1)%4 = 0 (i = ${i}))`
    );
  }
  if (bits.length % 8 !== 0) {
    throw new Error(`We should never reach here...`);
  }
  const bytes = new Uint8Array(Math.floor(bits.length / 8));
  let currentByteIndex = 0;
  for (let i2 = 0; i2 < bits.length; i2 += 8) {
    const bitChunk = bits.slice(i2, i2 + 8);
    const byte = parseInt(bitChunk.join(""), 2);
    bytes[currentByteIndex++] = byte;
  }
  return new TextDecoder().decode(bytes);
}
function verifyExtendedClaim(claim) {
  if (!(claim.slice(-1) === "}" || claim.slice(-1) === ",")) {
    throw new Error("Invalid claim");
  }
  const json = JSON.parse("{" + claim.slice(0, -1) + "}");
  if (Object.keys(json).length !== 1) {
    throw new Error("Invalid claim");
  }
  const key = Object.keys(json)[0];
  return [key, json[key]];
}
function extractClaimValue(claim, claimName) {
  const extendedClaim = decodeBase64URL(claim.value, claim.indexMod4);
  const [name, value] = verifyExtendedClaim(extendedClaim);
  if (name !== claimName) {
    throw new Error(`Invalid field name: found ${name} expected ${claimName}`);
  }
  return value;
}

// node_modules/@mysten/sui/dist/esm/zklogin/bcs.js
var zkLoginSignature = bcs.struct("ZkLoginSignature", {
  inputs: bcs.struct("ZkLoginSignatureInputs", {
    proofPoints: bcs.struct("ZkLoginSignatureInputsProofPoints", {
      a: bcs.vector(bcs.string()),
      b: bcs.vector(bcs.vector(bcs.string())),
      c: bcs.vector(bcs.string())
    }),
    issBase64Details: bcs.struct("ZkLoginSignatureInputsClaim", {
      value: bcs.string(),
      indexMod4: bcs.u8()
    }),
    headerBase64: bcs.string(),
    addressSeed: bcs.string()
  }),
  maxEpoch: bcs.u64(),
  userSignature: bcs.byteVector()
});

// node_modules/@mysten/sui/dist/esm/zklogin/signature.js
function parseZkLoginSignature(signature) {
  return zkLoginSignature.parse(typeof signature === "string" ? fromBase64(signature) : signature);
}

// node_modules/@mysten/sui/dist/esm/zklogin/publickey.js
var __typeError6 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck6 = (obj, member, msg) => member.has(obj) || __typeError6("Cannot " + msg);
var __privateGet6 = (obj, member, getter) => (__accessCheck6(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd6 = (obj, member, value) => member.has(obj) ? __typeError6("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet6 = (obj, member, value, setter) => (__accessCheck6(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateMethod3 = (obj, member, method) => (__accessCheck6(obj, member, "access private method"), method);
var _data;
var _client;
var _legacyAddress;
var _ZkLoginPublicIdentifier_instances;
var toLegacyAddress_fn;
var _ZkLoginPublicIdentifier = class _ZkLoginPublicIdentifier2 extends PublicKey2 {
  /**
   * Create a new ZkLoginPublicIdentifier object
   * @param value zkLogin public identifier as buffer or base-64 encoded string
   */
  constructor(value, { client } = {}) {
    super();
    __privateAdd6(this, _ZkLoginPublicIdentifier_instances);
    __privateAdd6(this, _data);
    __privateAdd6(this, _client);
    __privateAdd6(this, _legacyAddress);
    __privateSet6(this, _client, client);
    if (typeof value === "string") {
      __privateSet6(this, _data, fromBase64(value));
    } else if (value instanceof Uint8Array) {
      __privateSet6(this, _data, value);
    } else {
      __privateSet6(this, _data, Uint8Array.from(value));
    }
    __privateSet6(this, _legacyAddress, __privateGet6(this, _data).length !== __privateGet6(this, _data)[0] + 1 + 32);
    if (__privateGet6(this, _legacyAddress)) {
      __privateSet6(this, _data, normalizeZkLoginPublicKeyBytes(__privateGet6(this, _data)));
    }
  }
  static fromBytes(bytes, {
    client,
    address,
    legacyAddress
  } = {}) {
    let publicKey;
    if (legacyAddress === true) {
      publicKey = new _ZkLoginPublicIdentifier2(normalizeZkLoginPublicKeyBytes(bytes, true), {
        client
      });
    } else if (legacyAddress === false) {
      publicKey = new _ZkLoginPublicIdentifier2(normalizeZkLoginPublicKeyBytes(bytes, false), {
        client
      });
    } else if (address) {
      publicKey = new _ZkLoginPublicIdentifier2(normalizeZkLoginPublicKeyBytes(bytes, false), {
        client
      });
      if (publicKey.toSuiAddress() !== address) {
        publicKey = new _ZkLoginPublicIdentifier2(normalizeZkLoginPublicKeyBytes(bytes, true), {
          client
        });
      }
    } else {
      publicKey = new _ZkLoginPublicIdentifier2(bytes, {
        client
      });
    }
    if (address && publicKey.toSuiAddress() !== address) {
      throw new Error("Public key bytes do not match the provided address");
    }
    return publicKey;
  }
  static fromProof(address, proof) {
    const { issBase64Details, addressSeed } = proof;
    const iss = extractClaimValue(issBase64Details, "iss");
    const legacyPublicKey = toZkLoginPublicIdentifier(BigInt(addressSeed), iss, {
      legacyAddress: true
    });
    if (legacyPublicKey.toSuiAddress() === address) {
      return legacyPublicKey;
    }
    const publicKey = toZkLoginPublicIdentifier(BigInt(addressSeed), iss, {
      legacyAddress: false
    });
    if (publicKey.toSuiAddress() !== address) {
      throw new Error("Proof does not match address");
    }
    return publicKey;
  }
  /**
   * Checks if two zkLogin public identifiers are equal
   */
  equals(publicKey) {
    return super.equals(publicKey);
  }
  toSuiAddress() {
    if (__privateGet6(this, _legacyAddress)) {
      return __privateMethod3(this, _ZkLoginPublicIdentifier_instances, toLegacyAddress_fn).call(this);
    }
    return super.toSuiAddress();
  }
  /**
   * Return the byte array representation of the zkLogin public identifier
   */
  toRawBytes() {
    return __privateGet6(this, _data);
  }
  /**
   * Return the Sui address associated with this ZkLogin public identifier
   */
  flag() {
    return SIGNATURE_SCHEME_TO_FLAG["ZkLogin"];
  }
  /**
   * Verifies that the signature is valid for for the provided message
   */
  async verify(_message, _signature) {
    throw Error("does not support");
  }
  /**
   * Verifies that the signature is valid for for the provided PersonalMessage
   */
  verifyPersonalMessage(message, signature) {
    const parsedSignature = parseSerializedZkLoginSignature(signature);
    const address = new _ZkLoginPublicIdentifier2(parsedSignature.publicKey).toSuiAddress();
    return graphqlVerifyZkLoginSignature({
      address,
      bytes: toBase64(message),
      signature: parsedSignature.serializedSignature,
      intentScope: "PersonalMessage",
      client: __privateGet6(this, _client)
    });
  }
  /**
   * Verifies that the signature is valid for for the provided Transaction
   */
  verifyTransaction(transaction, signature) {
    const parsedSignature = parseSerializedZkLoginSignature(signature);
    const address = new _ZkLoginPublicIdentifier2(parsedSignature.publicKey).toSuiAddress();
    return graphqlVerifyZkLoginSignature({
      address,
      bytes: toBase64(transaction),
      signature: parsedSignature.serializedSignature,
      intentScope: "TransactionData",
      client: __privateGet6(this, _client)
    });
  }
  /**
   * Verifies that the public key is associated with the provided address
   */
  verifyAddress(address) {
    return address === super.toSuiAddress() || address === __privateMethod3(this, _ZkLoginPublicIdentifier_instances, toLegacyAddress_fn).call(this);
  }
};
_data = /* @__PURE__ */ new WeakMap();
_client = /* @__PURE__ */ new WeakMap();
_legacyAddress = /* @__PURE__ */ new WeakMap();
_ZkLoginPublicIdentifier_instances = /* @__PURE__ */ new WeakSet();
toLegacyAddress_fn = function() {
  const legacyBytes = normalizeZkLoginPublicKeyBytes(__privateGet6(this, _data), true);
  const addressBytes = new Uint8Array(legacyBytes.length + 1);
  addressBytes[0] = this.flag();
  addressBytes.set(legacyBytes, 1);
  return normalizeSuiAddress(
    bytesToHex(blake2b2(addressBytes, { dkLen: 32 })).slice(0, SUI_ADDRESS_LENGTH * 2)
  );
};
var ZkLoginPublicIdentifier = _ZkLoginPublicIdentifier;
function toZkLoginPublicIdentifier(addressSeed, iss, options) {
  const addressSeedBytesBigEndian = options?.legacyAddress ? toBigEndianBytes(addressSeed, 32) : toPaddedBigEndianBytes(addressSeed, 32);
  const issBytes = new TextEncoder().encode(normalizeZkLoginIssuer(iss));
  const tmp = new Uint8Array(1 + issBytes.length + addressSeedBytesBigEndian.length);
  tmp.set([issBytes.length], 0);
  tmp.set(issBytes, 1);
  tmp.set(addressSeedBytesBigEndian, 1 + issBytes.length);
  return new ZkLoginPublicIdentifier(tmp, options);
}
function normalizeZkLoginPublicKeyBytes(bytes, legacyAddress = false) {
  const issByteLength = bytes[0] + 1;
  const addressSeed = BigInt(`0x${toHex(bytes.slice(issByteLength))}`);
  const seedBytes = legacyAddress ? toBigEndianBytes(addressSeed, 32) : toPaddedBigEndianBytes(addressSeed, 32);
  const data = new Uint8Array(issByteLength + seedBytes.length);
  data.set(bytes.slice(0, issByteLength), 0);
  data.set(seedBytes, issByteLength);
  return data;
}
async function graphqlVerifyZkLoginSignature({
  address,
  bytes,
  signature,
  intentScope,
  client = new SuiGraphQLClient({
    url: "https://graphql.mainnet.sui.io/graphql"
  })
}) {
  const resp = await client.core.verifyZkLoginSignature({
    bytes,
    signature,
    intentScope,
    author: address
  });
  return resp.success === true && resp.errors.length === 0;
}
function parseSerializedZkLoginSignature(signature) {
  const bytes = typeof signature === "string" ? fromBase64(signature) : signature;
  if (bytes[0] !== SIGNATURE_SCHEME_TO_FLAG.ZkLogin) {
    throw new Error("Invalid signature scheme");
  }
  const signatureBytes = bytes.slice(1);
  const { inputs, maxEpoch, userSignature } = parseZkLoginSignature(signatureBytes);
  const { issBase64Details, addressSeed } = inputs;
  const iss = extractClaimValue(issBase64Details, "iss");
  const publicIdentifer = toZkLoginPublicIdentifier(BigInt(addressSeed), iss);
  return {
    serializedSignature: toBase64(bytes),
    signatureScheme: "ZkLogin",
    zkLogin: {
      inputs,
      maxEpoch,
      userSignature,
      iss,
      addressSeed: BigInt(addressSeed)
    },
    signature: bytes,
    publicKey: publicIdentifer.toRawBytes()
  };
}

// node_modules/@mysten/sui/dist/esm/cryptography/signature.js
function toSerializedSignature({
  signature,
  signatureScheme,
  publicKey
}) {
  if (!publicKey) {
    throw new Error("`publicKey` is required");
  }
  const pubKeyBytes = publicKey.toRawBytes();
  const serializedSignature = new Uint8Array(1 + signature.length + pubKeyBytes.length);
  serializedSignature.set([SIGNATURE_SCHEME_TO_FLAG[signatureScheme]]);
  serializedSignature.set(signature, 1);
  serializedSignature.set(pubKeyBytes, 1 + signature.length);
  return toBase64(serializedSignature);
}
function parseSerializedSignature(serializedSignature) {
  const bytes = fromBase64(serializedSignature);
  const signatureScheme = SIGNATURE_FLAG_TO_SCHEME[bytes[0]];
  switch (signatureScheme) {
    case "Passkey":
      return parseSerializedPasskeySignature(serializedSignature);
    case "MultiSig":
      const multisig = suiBcs.MultiSig.parse(bytes.slice(1));
      return {
        serializedSignature,
        signatureScheme,
        multisig,
        bytes,
        signature: void 0
      };
    case "ZkLogin":
      return parseSerializedZkLoginSignature(serializedSignature);
    case "ED25519":
    case "Secp256k1":
    case "Secp256r1":
      return parseSerializedKeypairSignature(serializedSignature);
    default:
      throw new Error("Unsupported signature scheme");
  }
}

// node_modules/@noble/hashes/esm/pbkdf2.js
function pbkdf2Init(hash, _password, _salt, _opts) {
  ahash(hash);
  const opts = checkOpts({ dkLen: 32, asyncTick: 10 }, _opts);
  const { c, dkLen, asyncTick } = opts;
  anumber2(c);
  anumber2(dkLen);
  anumber2(asyncTick);
  if (c < 1)
    throw new Error("iterations (c) should be >= 1");
  const password = kdfInputToBytes(_password);
  const salt = kdfInputToBytes(_salt);
  const DK = new Uint8Array(dkLen);
  const PRF = hmac.create(hash, password);
  const PRFSalt = PRF._cloneInto().update(salt);
  return { c, dkLen, asyncTick, DK, PRF, PRFSalt };
}
function pbkdf2Output(PRF, PRFSalt, DK, prfW, u) {
  PRF.destroy();
  PRFSalt.destroy();
  if (prfW)
    prfW.destroy();
  clean(u);
  return DK;
}
function pbkdf2(hash, password, salt, opts) {
  const { c, dkLen, DK, PRF, PRFSalt } = pbkdf2Init(hash, password, salt, opts);
  let prfW;
  const arr = new Uint8Array(4);
  const view = createView(arr);
  const u = new Uint8Array(PRF.outputLen);
  for (let ti = 1, pos = 0; pos < dkLen; ti++, pos += PRF.outputLen) {
    const Ti = DK.subarray(pos, pos + PRF.outputLen);
    view.setInt32(0, ti, false);
    (prfW = PRFSalt._cloneInto(prfW)).update(arr).digestInto(u);
    Ti.set(u.subarray(0, Ti.length));
    for (let ui = 1; ui < c; ui++) {
      PRF._cloneInto(prfW).update(u).digestInto(u);
      for (let i = 0; i < Ti.length; i++)
        Ti[i] ^= u[i];
    }
  }
  return pbkdf2Output(PRF, PRFSalt, DK, prfW, u);
}

// node_modules/@scure/bip39/esm/index.js
function nfkd(str) {
  if (typeof str !== "string")
    throw new TypeError("invalid mnemonic type: " + typeof str);
  return str.normalize("NFKD");
}
function normalize(str) {
  const norm = nfkd(str);
  const words = norm.split(" ");
  if (![12, 15, 18, 21, 24].includes(words.length))
    throw new Error("Invalid mnemonic");
  return { nfkd: norm, words };
}
var psalt = (passphrase) => nfkd("mnemonic" + passphrase);
function mnemonicToSeedSync(mnemonic, passphrase = "") {
  return pbkdf2(sha512, normalize(mnemonic).nfkd, psalt(passphrase), { c: 2048, dkLen: 64 });
}

// node_modules/@mysten/sui/dist/esm/cryptography/mnemonics.js
function isValidHardenedPath(path) {
  if (!new RegExp("^m\\/44'\\/784'\\/[0-9]+'\\/[0-9]+'\\/[0-9]+'+$").test(path)) {
    return false;
  }
  return true;
}
function isValidBIP32Path(path) {
  if (!new RegExp("^m\\/(54|74)'\\/784'\\/[0-9]+'\\/[0-9]+\\/[0-9]+$").test(path)) {
    return false;
  }
  return true;
}
function mnemonicToSeed(mnemonics) {
  return mnemonicToSeedSync(mnemonics, "");
}
function mnemonicToSeedHex(mnemonics) {
  return toHex(mnemonicToSeed(mnemonics));
}

// node_modules/@mysten/sui/dist/esm/cryptography/keypair.js
var PRIVATE_KEY_SIZE = 32;
var SUI_PRIVATE_KEY_PREFIX = "suiprivkey";
var Signer = class {
  /**
   * Sign messages with a specific intent. By combining the message bytes with the intent before hashing and signing,
   * it ensures that a signed message is tied to a specific purpose and domain separator is provided
   */
  async signWithIntent(bytes, intent) {
    const intentMessage = messageWithIntent(intent, bytes);
    const digest = blake2b2(intentMessage, { dkLen: 32 });
    const signature = toSerializedSignature({
      signature: await this.sign(digest),
      signatureScheme: this.getKeyScheme(),
      publicKey: this.getPublicKey()
    });
    return {
      signature,
      bytes: toBase64(bytes)
    };
  }
  /**
   * Signs provided transaction by calling `signWithIntent()` with a `TransactionData` provided as intent scope
   */
  async signTransaction(bytes) {
    return this.signWithIntent(bytes, "TransactionData");
  }
  /**
   * Signs provided personal message by calling `signWithIntent()` with a `PersonalMessage` provided as intent scope
   */
  async signPersonalMessage(bytes) {
    const { signature } = await this.signWithIntent(
      bcs.byteVector().serialize(bytes).toBytes(),
      "PersonalMessage"
    );
    return {
      bytes: toBase64(bytes),
      signature
    };
  }
  async signAndExecuteTransaction({
    transaction,
    client
  }) {
    const bytes = await transaction.build({ client });
    const { signature } = await this.signTransaction(bytes);
    const response = await client.core.executeTransaction({
      transaction: bytes,
      signatures: [signature]
    });
    return response.transaction;
  }
  toSuiAddress() {
    return this.getPublicKey().toSuiAddress();
  }
};
var Keypair = class extends Signer {
};
function decodeSuiPrivateKey(value) {
  const { prefix, words } = bech32.decode(value);
  if (prefix !== SUI_PRIVATE_KEY_PREFIX) {
    throw new Error("invalid private key prefix");
  }
  const extendedSecretKey = new Uint8Array(bech32.fromWords(words));
  const secretKey = extendedSecretKey.slice(1);
  const signatureScheme = SIGNATURE_FLAG_TO_SCHEME[extendedSecretKey[0]];
  return {
    scheme: signatureScheme,
    schema: signatureScheme,
    secretKey
  };
}
function encodeSuiPrivateKey(bytes, scheme) {
  if (bytes.length !== PRIVATE_KEY_SIZE) {
    throw new Error("Invalid bytes length");
  }
  const flag = SIGNATURE_SCHEME_TO_FLAG[scheme];
  const privKeyBytes = new Uint8Array(bytes.length + 1);
  privKeyBytes.set([flag]);
  privKeyBytes.set(bytes, 1);
  return bech32.encode(SUI_PRIVATE_KEY_PREFIX, bech32.toWords(privKeyBytes));
}

// node_modules/@mysten/sui/dist/esm/jsonRpc/errors.js
var CODE_TO_ERROR_TYPE = {
  "-32700": "ParseError",
  "-32701": "OversizedRequest",
  "-32702": "OversizedResponse",
  "-32600": "InvalidRequest",
  "-32601": "MethodNotFound",
  "-32602": "InvalidParams",
  "-32603": "InternalError",
  "-32604": "ServerBusy",
  "-32000": "CallExecutionFailed",
  "-32001": "UnknownError",
  "-32003": "SubscriptionClosed",
  "-32004": "SubscriptionClosedWithError",
  "-32005": "BatchesNotSupported",
  "-32006": "TooManySubscriptions",
  "-32050": "TransientError",
  "-32002": "TransactionExecutionClientError"
};
var SuiHTTPTransportError = class extends Error {
};
var JsonRpcError = class extends SuiHTTPTransportError {
  constructor(message, code) {
    super(message);
    this.code = code;
    this.type = CODE_TO_ERROR_TYPE[code] ?? "ServerError";
  }
};
var SuiHTTPStatusError = class extends SuiHTTPTransportError {
  constructor(message, status, statusText) {
    super(message);
    this.status = status;
    this.statusText = statusText;
  }
};

// node_modules/@mysten/sui/dist/esm/jsonRpc/rpc-websocket-client.js
var __typeError7 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck7 = (obj, member, msg) => member.has(obj) || __typeError7("Cannot " + msg);
var __privateGet7 = (obj, member, getter) => (__accessCheck7(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd7 = (obj, member, value) => member.has(obj) ? __typeError7("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet7 = (obj, member, value, setter) => (__accessCheck7(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateMethod4 = (obj, member, method) => (__accessCheck7(obj, member, "access private method"), method);
var __privateWrapper = (obj, member, setter, getter) => ({
  set _(value) {
    __privateSet7(obj, member, value, setter);
  },
  get _() {
    return __privateGet7(obj, member, getter);
  }
});
var _requestId;
var _disconnects;
var _webSocket;
var _connectionPromise;
var _subscriptions;
var _pendingRequests;
var _WebsocketClient_instances;
var setupWebSocket_fn;
var reconnect_fn;
function getWebsocketUrl(httpUrl) {
  const url = new URL(httpUrl);
  url.protocol = url.protocol.replace("http", "ws");
  return url.toString();
}
var DEFAULT_CLIENT_OPTIONS = {
  // We fudge the typing because we also check for undefined in the constructor:
  WebSocketConstructor: typeof WebSocket !== "undefined" ? WebSocket : void 0,
  callTimeout: 3e4,
  reconnectTimeout: 3e3,
  maxReconnects: 5
};
var WebsocketClient = class {
  constructor(endpoint, options = {}) {
    __privateAdd7(this, _WebsocketClient_instances);
    __privateAdd7(this, _requestId, 0);
    __privateAdd7(this, _disconnects, 0);
    __privateAdd7(this, _webSocket, null);
    __privateAdd7(this, _connectionPromise, null);
    __privateAdd7(this, _subscriptions, /* @__PURE__ */ new Set());
    __privateAdd7(this, _pendingRequests, /* @__PURE__ */ new Map());
    this.endpoint = endpoint;
    this.options = { ...DEFAULT_CLIENT_OPTIONS, ...options };
    if (!this.options.WebSocketConstructor) {
      throw new Error("Missing WebSocket constructor");
    }
    if (this.endpoint.startsWith("http")) {
      this.endpoint = getWebsocketUrl(this.endpoint);
    }
  }
  async makeRequest(method, params, signal) {
    const webSocket = await __privateMethod4(this, _WebsocketClient_instances, setupWebSocket_fn).call(this);
    return new Promise((resolve, reject) => {
      __privateSet7(this, _requestId, __privateGet7(this, _requestId) + 1);
      __privateGet7(this, _pendingRequests).set(__privateGet7(this, _requestId), {
        resolve,
        reject,
        timeout: setTimeout(() => {
          __privateGet7(this, _pendingRequests).delete(__privateGet7(this, _requestId));
          reject(new Error(`Request timeout: ${method}`));
        }, this.options.callTimeout)
      });
      signal?.addEventListener("abort", () => {
        __privateGet7(this, _pendingRequests).delete(__privateGet7(this, _requestId));
        reject(signal.reason);
      });
      webSocket.send(JSON.stringify({ jsonrpc: "2.0", id: __privateGet7(this, _requestId), method, params }));
    }).then(({ error, result }) => {
      if (error) {
        throw new JsonRpcError(error.message, error.code);
      }
      return result;
    });
  }
  async subscribe(input) {
    const subscription = new RpcSubscription(input);
    __privateGet7(this, _subscriptions).add(subscription);
    await subscription.subscribe(this);
    return () => subscription.unsubscribe(this);
  }
};
_requestId = /* @__PURE__ */ new WeakMap();
_disconnects = /* @__PURE__ */ new WeakMap();
_webSocket = /* @__PURE__ */ new WeakMap();
_connectionPromise = /* @__PURE__ */ new WeakMap();
_subscriptions = /* @__PURE__ */ new WeakMap();
_pendingRequests = /* @__PURE__ */ new WeakMap();
_WebsocketClient_instances = /* @__PURE__ */ new WeakSet();
setupWebSocket_fn = function() {
  if (__privateGet7(this, _connectionPromise)) {
    return __privateGet7(this, _connectionPromise);
  }
  __privateSet7(this, _connectionPromise, new Promise((resolve) => {
    __privateGet7(this, _webSocket)?.close();
    __privateSet7(this, _webSocket, new this.options.WebSocketConstructor(this.endpoint));
    __privateGet7(this, _webSocket).addEventListener("open", () => {
      __privateSet7(this, _disconnects, 0);
      resolve(__privateGet7(this, _webSocket));
    });
    __privateGet7(this, _webSocket).addEventListener("close", () => {
      __privateWrapper(this, _disconnects)._++;
      if (__privateGet7(this, _disconnects) <= this.options.maxReconnects) {
        setTimeout(() => {
          __privateMethod4(this, _WebsocketClient_instances, reconnect_fn).call(this);
        }, this.options.reconnectTimeout);
      }
    });
    __privateGet7(this, _webSocket).addEventListener("message", ({ data }) => {
      let json;
      try {
        json = JSON.parse(data);
      } catch (error) {
        console.error(new Error(`Failed to parse RPC message: ${data}`, { cause: error }));
        return;
      }
      if ("id" in json && json.id != null && __privateGet7(this, _pendingRequests).has(json.id)) {
        const { resolve: resolve2, timeout } = __privateGet7(this, _pendingRequests).get(json.id);
        clearTimeout(timeout);
        resolve2(json);
      } else if ("params" in json) {
        const { params } = json;
        __privateGet7(this, _subscriptions).forEach((subscription) => {
          if (subscription.subscriptionId === params.subscription) {
            if (params.subscription === subscription.subscriptionId) {
              subscription.onMessage(params.result);
            }
          }
        });
      }
    });
  }));
  return __privateGet7(this, _connectionPromise);
};
reconnect_fn = async function() {
  __privateGet7(this, _webSocket)?.close();
  __privateSet7(this, _connectionPromise, null);
  return Promise.allSettled(
    [...__privateGet7(this, _subscriptions)].map((subscription) => subscription.subscribe(this))
  );
};
var RpcSubscription = class {
  constructor(input) {
    this.subscriptionId = null;
    this.subscribed = false;
    this.input = input;
  }
  onMessage(message) {
    if (this.subscribed) {
      this.input.onMessage(message);
    }
  }
  async unsubscribe(client) {
    const { subscriptionId } = this;
    this.subscribed = false;
    if (subscriptionId == null) return false;
    this.subscriptionId = null;
    return client.makeRequest(this.input.unsubscribe, [subscriptionId]);
  }
  async subscribe(client) {
    this.subscriptionId = null;
    this.subscribed = true;
    const newSubscriptionId = await client.makeRequest(
      this.input.method,
      this.input.params,
      this.input.signal
    );
    if (this.subscribed) {
      this.subscriptionId = newSubscriptionId;
    }
  }
};

// node_modules/@mysten/sui/dist/esm/jsonRpc/http-transport.js
var __typeError8 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck8 = (obj, member, msg) => member.has(obj) || __typeError8("Cannot " + msg);
var __privateGet8 = (obj, member, getter) => (__accessCheck8(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd8 = (obj, member, value) => member.has(obj) ? __typeError8("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet8 = (obj, member, value, setter) => (__accessCheck8(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateMethod5 = (obj, member, method) => (__accessCheck8(obj, member, "access private method"), method);
var _requestId2;
var _options;
var _websocketClient;
var _JsonRpcHTTPTransport_instances;
var getWebsocketClient_fn;
var JsonRpcHTTPTransport = class {
  constructor(options) {
    __privateAdd8(this, _JsonRpcHTTPTransport_instances);
    __privateAdd8(this, _requestId2, 0);
    __privateAdd8(this, _options);
    __privateAdd8(this, _websocketClient);
    __privateSet8(this, _options, options);
  }
  fetch(input, init) {
    const fetchFn = __privateGet8(this, _options).fetch ?? fetch;
    if (!fetchFn) {
      throw new Error(
        "The current environment does not support fetch, you can provide a fetch implementation in the options for SuiHTTPTransport."
      );
    }
    return fetchFn(input, init);
  }
  async request(input) {
    __privateSet8(this, _requestId2, __privateGet8(this, _requestId2) + 1);
    const res = await this.fetch(__privateGet8(this, _options).rpc?.url ?? __privateGet8(this, _options).url, {
      method: "POST",
      signal: input.signal,
      headers: {
        "Content-Type": "application/json",
        "Client-Sdk-Type": "typescript",
        "Client-Sdk-Version": PACKAGE_VERSION,
        "Client-Target-Api-Version": TARGETED_RPC_VERSION,
        "Client-Request-Method": input.method,
        ...__privateGet8(this, _options).rpc?.headers
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: __privateGet8(this, _requestId2),
        method: input.method,
        params: input.params
      })
    });
    if (!res.ok) {
      throw new SuiHTTPStatusError(
        `Unexpected status code: ${res.status}`,
        res.status,
        res.statusText
      );
    }
    const data = await res.json();
    if ("error" in data && data.error != null) {
      throw new JsonRpcError(data.error.message, data.error.code);
    }
    return data.result;
  }
  async subscribe(input) {
    const unsubscribe = await __privateMethod5(this, _JsonRpcHTTPTransport_instances, getWebsocketClient_fn).call(this).subscribe(input);
    if (input.signal) {
      input.signal.throwIfAborted();
      input.signal.addEventListener("abort", () => {
        unsubscribe();
      });
    }
    return async () => !!await unsubscribe();
  }
};
_requestId2 = /* @__PURE__ */ new WeakMap();
_options = /* @__PURE__ */ new WeakMap();
_websocketClient = /* @__PURE__ */ new WeakMap();
_JsonRpcHTTPTransport_instances = /* @__PURE__ */ new WeakSet();
getWebsocketClient_fn = function() {
  if (!__privateGet8(this, _websocketClient)) {
    const WebSocketConstructor = __privateGet8(this, _options).WebSocketConstructor ?? WebSocket;
    if (!WebSocketConstructor) {
      throw new Error(
        "The current environment does not support WebSocket, you can provide a WebSocketConstructor in the options for SuiHTTPTransport."
      );
    }
    __privateSet8(this, _websocketClient, new WebsocketClient(
      __privateGet8(this, _options).websocket?.url ?? __privateGet8(this, _options).url,
      {
        WebSocketConstructor,
        ...__privateGet8(this, _options).websocket
      }
    ));
  }
  return __privateGet8(this, _websocketClient);
};

// node_modules/@mysten/sui/dist/esm/client/network.js
function getFullnodeUrl(network) {
  switch (network) {
    case "mainnet":
      return "https://fullnode.mainnet.sui.io:443";
    case "testnet":
      return "https://fullnode.testnet.sui.io:443";
    case "devnet":
      return "https://fullnode.devnet.sui.io:443";
    case "localnet":
      return "http://127.0.0.1:9000";
    default:
      throw new Error(`Unknown network: ${network}`);
  }
}

// node_modules/@mysten/sui/dist/esm/transactions/Commands.js
var Commands = {
  MoveCall(input) {
    const [pkg, mod2 = "", fn = ""] = "target" in input ? input.target.split("::") : [input.package, input.module, input.function];
    return {
      $kind: "MoveCall",
      MoveCall: {
        package: pkg,
        module: mod2,
        function: fn,
        typeArguments: input.typeArguments ?? [],
        arguments: input.arguments ?? []
      }
    };
  },
  TransferObjects(objects, address) {
    return {
      $kind: "TransferObjects",
      TransferObjects: {
        objects: objects.map((o) => parse2(ArgumentSchema, o)),
        address: parse2(ArgumentSchema, address)
      }
    };
  },
  SplitCoins(coin, amounts) {
    return {
      $kind: "SplitCoins",
      SplitCoins: {
        coin: parse2(ArgumentSchema, coin),
        amounts: amounts.map((o) => parse2(ArgumentSchema, o))
      }
    };
  },
  MergeCoins(destination, sources) {
    return {
      $kind: "MergeCoins",
      MergeCoins: {
        destination: parse2(ArgumentSchema, destination),
        sources: sources.map((o) => parse2(ArgumentSchema, o))
      }
    };
  },
  Publish({
    modules,
    dependencies
  }) {
    return {
      $kind: "Publish",
      Publish: {
        modules: modules.map(
          (module2) => typeof module2 === "string" ? module2 : toBase64(new Uint8Array(module2))
        ),
        dependencies: dependencies.map((dep) => normalizeSuiObjectId(dep))
      }
    };
  },
  Upgrade({
    modules,
    dependencies,
    package: packageId,
    ticket
  }) {
    return {
      $kind: "Upgrade",
      Upgrade: {
        modules: modules.map(
          (module2) => typeof module2 === "string" ? module2 : toBase64(new Uint8Array(module2))
        ),
        dependencies: dependencies.map((dep) => normalizeSuiObjectId(dep)),
        package: packageId,
        ticket: parse2(ArgumentSchema, ticket)
      }
    };
  },
  MakeMoveVec({
    type,
    elements
  }) {
    return {
      $kind: "MakeMoveVec",
      MakeMoveVec: {
        type: type ?? null,
        elements: elements.map((o) => parse2(ArgumentSchema, o))
      }
    };
  },
  Intent({
    name,
    inputs = {},
    data = {}
  }) {
    return {
      $kind: "$Intent",
      $Intent: {
        name,
        inputs: Object.fromEntries(
          Object.entries(inputs).map(([key, value]) => [
            key,
            Array.isArray(value) ? value.map((o) => parse2(ArgumentSchema, o)) : parse2(ArgumentSchema, value)
          ])
        ),
        data
      }
    };
  }
};

// node_modules/@mysten/sui/dist/esm/transactions/data/v2.js
function enumUnion(options) {
  return union(
    Object.entries(options).map(([key, value]) => object({ [key]: value }))
  );
}
var Argument2 = enumUnion({
  GasCoin: literal(true),
  Input: pipe(number(), integer()),
  Result: pipe(number(), integer()),
  NestedResult: tuple([pipe(number(), integer()), pipe(number(), integer())])
});
var GasData2 = object({
  budget: nullable(JsonU64),
  price: nullable(JsonU64),
  owner: nullable(SuiAddress),
  payment: nullable(array(ObjectRefSchema))
});
var ProgrammableMoveCall2 = object({
  package: ObjectID,
  module: string(),
  function: string(),
  // snake case in rust
  typeArguments: array(string()),
  arguments: array(Argument2)
});
var $Intent2 = object({
  name: string(),
  inputs: record(string(), union([Argument2, array(Argument2)])),
  data: record(string(), unknown())
});
var Command2 = enumUnion({
  MoveCall: ProgrammableMoveCall2,
  TransferObjects: object({
    objects: array(Argument2),
    address: Argument2
  }),
  SplitCoins: object({
    coin: Argument2,
    amounts: array(Argument2)
  }),
  MergeCoins: object({
    destination: Argument2,
    sources: array(Argument2)
  }),
  Publish: object({
    modules: array(BCSBytes),
    dependencies: array(ObjectID)
  }),
  MakeMoveVec: object({
    type: nullable(string()),
    elements: array(Argument2)
  }),
  Upgrade: object({
    modules: array(BCSBytes),
    dependencies: array(ObjectID),
    package: ObjectID,
    ticket: Argument2
  }),
  $Intent: $Intent2
});
var ObjectArg3 = enumUnion({
  ImmOrOwnedObject: ObjectRefSchema,
  SharedObject: object({
    objectId: ObjectID,
    // snake case in rust
    initialSharedVersion: JsonU64,
    mutable: boolean()
  }),
  Receiving: ObjectRefSchema
});
var CallArg2 = enumUnion({
  Object: ObjectArg3,
  Pure: object({
    bytes: BCSBytes
  }),
  UnresolvedPure: object({
    value: unknown()
  }),
  UnresolvedObject: object({
    objectId: ObjectID,
    version: optional(nullable(JsonU64)),
    digest: optional(nullable(string())),
    initialSharedVersion: optional(nullable(JsonU64)),
    mutable: optional(nullable(boolean()))
  })
});
var TransactionExpiration4 = enumUnion({
  None: literal(true),
  Epoch: JsonU64
});
var SerializedTransactionDataV2Schema = object({
  version: literal(2),
  sender: nullish(SuiAddress),
  expiration: nullish(TransactionExpiration4),
  gasData: GasData2,
  inputs: array(CallArg2),
  commands: array(Command2),
  digest: optional(nullable(string()))
});

// node_modules/@mysten/sui/dist/esm/transactions/Inputs.js
function Pure(data) {
  return {
    $kind: "Pure",
    Pure: {
      bytes: data instanceof Uint8Array ? toBase64(data) : data.toBase64()
    }
  };
}
var Inputs = {
  Pure,
  ObjectRef({ objectId, digest, version }) {
    return {
      $kind: "Object",
      Object: {
        $kind: "ImmOrOwnedObject",
        ImmOrOwnedObject: {
          digest,
          version,
          objectId: normalizeSuiAddress(objectId)
        }
      }
    };
  },
  SharedObjectRef({
    objectId,
    mutable,
    initialSharedVersion
  }) {
    return {
      $kind: "Object",
      Object: {
        $kind: "SharedObject",
        SharedObject: {
          mutable,
          initialSharedVersion,
          objectId: normalizeSuiAddress(objectId)
        }
      }
    };
  },
  ReceivingRef({ objectId, digest, version }) {
    return {
      $kind: "Object",
      Object: {
        $kind: "Receiving",
        Receiving: {
          digest,
          version,
          objectId: normalizeSuiAddress(objectId)
        }
      }
    };
  }
};

// node_modules/@mysten/sui/dist/esm/utils/constants.js
var MIST_PER_SUI = BigInt(1e9);
var MOVE_STDLIB_ADDRESS = "0x1";
var SUI_FRAMEWORK_ADDRESS = "0x2";
var SUI_CLOCK_OBJECT_ID = normalizeSuiObjectId("0x6");
var SUI_TYPE_ARG = `${SUI_FRAMEWORK_ADDRESS}::sui::SUI`;
var SUI_SYSTEM_STATE_OBJECT_ID = normalizeSuiObjectId("0x5");
var SUI_RANDOM_OBJECT_ID = normalizeSuiObjectId("0x8");

// node_modules/@mysten/sui/dist/esm/transactions/serializer.js
var OBJECT_MODULE_NAME = "object";
var ID_STRUCT_NAME = "ID";
var STD_ASCII_MODULE_NAME = "ascii";
var STD_ASCII_STRUCT_NAME = "String";
var STD_UTF8_MODULE_NAME = "string";
var STD_UTF8_STRUCT_NAME = "String";
var STD_OPTION_MODULE_NAME = "option";
var STD_OPTION_STRUCT_NAME = "Option";
function isTxContext(param) {
  const struct = typeof param.body === "object" && "datatype" in param.body ? param.body.datatype : null;
  return !!struct && normalizeSuiAddress(struct.package) === normalizeSuiAddress("0x2") && struct.module === "tx_context" && struct.type === "TxContext";
}
function getPureBcsSchema(typeSignature) {
  if (typeof typeSignature === "string") {
    switch (typeSignature) {
      case "address":
        return suiBcs.Address;
      case "bool":
        return suiBcs.Bool;
      case "u8":
        return suiBcs.U8;
      case "u16":
        return suiBcs.U16;
      case "u32":
        return suiBcs.U32;
      case "u64":
        return suiBcs.U64;
      case "u128":
        return suiBcs.U128;
      case "u256":
        return suiBcs.U256;
      default:
        throw new Error(`Unknown type signature ${typeSignature}`);
    }
  }
  if ("vector" in typeSignature) {
    if (typeSignature.vector === "u8") {
      return suiBcs.byteVector().transform({
        input: (val) => typeof val === "string" ? new TextEncoder().encode(val) : val,
        output: (val) => val
      });
    }
    const type = getPureBcsSchema(typeSignature.vector);
    return type ? suiBcs.vector(type) : null;
  }
  if ("datatype" in typeSignature) {
    const pkg = normalizeSuiAddress(typeSignature.datatype.package);
    if (pkg === normalizeSuiAddress(MOVE_STDLIB_ADDRESS)) {
      if (typeSignature.datatype.module === STD_ASCII_MODULE_NAME && typeSignature.datatype.type === STD_ASCII_STRUCT_NAME) {
        return suiBcs.String;
      }
      if (typeSignature.datatype.module === STD_UTF8_MODULE_NAME && typeSignature.datatype.type === STD_UTF8_STRUCT_NAME) {
        return suiBcs.String;
      }
      if (typeSignature.datatype.module === STD_OPTION_MODULE_NAME && typeSignature.datatype.type === STD_OPTION_STRUCT_NAME) {
        const type = getPureBcsSchema(typeSignature.datatype.typeParameters[0]);
        return type ? suiBcs.vector(type) : null;
      }
    }
    if (pkg === normalizeSuiAddress(SUI_FRAMEWORK_ADDRESS) && typeSignature.datatype.module === OBJECT_MODULE_NAME && typeSignature.datatype.type === ID_STRUCT_NAME) {
      return suiBcs.Address;
    }
  }
  return null;
}
function normalizedTypeToMoveTypeSignature(type) {
  if (typeof type === "object" && "Reference" in type) {
    return {
      ref: "&",
      body: normalizedTypeToMoveTypeSignatureBody(type.Reference)
    };
  }
  if (typeof type === "object" && "MutableReference" in type) {
    return {
      ref: "&mut",
      body: normalizedTypeToMoveTypeSignatureBody(type.MutableReference)
    };
  }
  return {
    ref: null,
    body: normalizedTypeToMoveTypeSignatureBody(type)
  };
}
function normalizedTypeToMoveTypeSignatureBody(type) {
  if (typeof type === "string") {
    switch (type) {
      case "Address":
        return "address";
      case "Bool":
        return "bool";
      case "U8":
        return "u8";
      case "U16":
        return "u16";
      case "U32":
        return "u32";
      case "U64":
        return "u64";
      case "U128":
        return "u128";
      case "U256":
        return "u256";
      default:
        throw new Error(`Unexpected type ${type}`);
    }
  }
  if ("Vector" in type) {
    return { vector: normalizedTypeToMoveTypeSignatureBody(type.Vector) };
  }
  if ("Struct" in type) {
    return {
      datatype: {
        package: type.Struct.address,
        module: type.Struct.module,
        type: type.Struct.name,
        typeParameters: type.Struct.typeArguments.map(normalizedTypeToMoveTypeSignatureBody)
      }
    };
  }
  if ("TypeParameter" in type) {
    return { typeParameter: type.TypeParameter };
  }
  throw new Error(`Unexpected type ${JSON.stringify(type)}`);
}

// node_modules/@mysten/sui/dist/esm/jsonRpc/json-rpc-resolver.js
var MAX_OBJECTS_PER_FETCH = 50;
var GAS_SAFE_OVERHEAD = 1000n;
var MAX_GAS = 5e10;
function jsonRpcClientResolveTransactionPlugin(client) {
  return async function resolveTransactionData(transactionData, options, next) {
    await normalizeInputs(transactionData, client);
    await resolveObjectReferences(transactionData, client);
    if (!options.onlyTransactionKind) {
      await setGasPrice(transactionData, client);
      await setGasBudget(transactionData, client);
      await setGasPayment(transactionData, client);
    }
    return await next();
  };
}
async function setGasPrice(transactionData, client) {
  if (!transactionData.gasConfig.price) {
    transactionData.gasConfig.price = String(await client.getReferenceGasPrice());
  }
}
async function setGasBudget(transactionData, client) {
  if (transactionData.gasConfig.budget) {
    return;
  }
  const dryRunResult = await client.dryRunTransactionBlock({
    transactionBlock: transactionData.build({
      overrides: {
        gasData: {
          budget: String(MAX_GAS),
          payment: []
        }
      }
    })
  });
  if (dryRunResult.effects.status.status !== "success") {
    throw new Error(
      `Dry run failed, could not automatically determine a budget: ${dryRunResult.effects.status.error}`,
      { cause: dryRunResult }
    );
  }
  const safeOverhead = GAS_SAFE_OVERHEAD * BigInt(transactionData.gasConfig.price || 1n);
  const baseComputationCostWithOverhead = BigInt(dryRunResult.effects.gasUsed.computationCost) + safeOverhead;
  const gasBudget = baseComputationCostWithOverhead + BigInt(dryRunResult.effects.gasUsed.storageCost) - BigInt(dryRunResult.effects.gasUsed.storageRebate);
  transactionData.gasConfig.budget = String(
    gasBudget > baseComputationCostWithOverhead ? gasBudget : baseComputationCostWithOverhead
  );
}
async function setGasPayment(transactionData, client) {
  if (!transactionData.gasConfig.payment) {
    const coins = await client.getCoins({
      owner: transactionData.gasConfig.owner || transactionData.sender,
      coinType: SUI_TYPE_ARG
    });
    const paymentCoins = coins.data.filter((coin) => {
      const matchingInput = transactionData.inputs.find((input) => {
        if (input.Object?.ImmOrOwnedObject) {
          return coin.coinObjectId === input.Object.ImmOrOwnedObject.objectId;
        }
        return false;
      });
      return !matchingInput;
    }).map((coin) => ({
      objectId: coin.coinObjectId,
      digest: coin.digest,
      version: coin.version
    }));
    if (!paymentCoins.length) {
      throw new Error("No valid gas coins found for the transaction.");
    }
    transactionData.gasConfig.payment = paymentCoins.map(
      (payment) => parse2(ObjectRefSchema, payment)
    );
  }
}
async function resolveObjectReferences(transactionData, client) {
  const objectsToResolve = transactionData.inputs.filter((input) => {
    return input.UnresolvedObject && !(input.UnresolvedObject.version || input.UnresolvedObject?.initialSharedVersion);
  });
  const dedupedIds = [
    ...new Set(
      objectsToResolve.map((input) => normalizeSuiObjectId(input.UnresolvedObject.objectId))
    )
  ];
  const objectChunks = dedupedIds.length ? chunk(dedupedIds, MAX_OBJECTS_PER_FETCH) : [];
  const resolved = (await Promise.all(
    objectChunks.map(
      (chunk2) => client.multiGetObjects({
        ids: chunk2,
        options: { showOwner: true }
      })
    )
  )).flat();
  const responsesById = new Map(
    dedupedIds.map((id, index) => {
      return [id, resolved[index]];
    })
  );
  const invalidObjects = Array.from(responsesById).filter(([_, obj]) => obj.error).map(([_, obj]) => JSON.stringify(obj.error));
  if (invalidObjects.length) {
    throw new Error(`The following input objects are invalid: ${invalidObjects.join(", ")}`);
  }
  const objects = resolved.map((object2) => {
    if (object2.error || !object2.data) {
      throw new Error(`Failed to fetch object: ${object2.error}`);
    }
    const owner = object2.data.owner;
    const initialSharedVersion = owner && typeof owner === "object" ? "Shared" in owner ? owner.Shared.initial_shared_version : "ConsensusAddressOwner" in owner ? owner.ConsensusAddressOwner.start_version : null : null;
    return {
      objectId: object2.data.objectId,
      digest: object2.data.digest,
      version: object2.data.version,
      initialSharedVersion
    };
  });
  const objectsById = new Map(
    dedupedIds.map((id, index) => {
      return [id, objects[index]];
    })
  );
  for (const [index, input] of transactionData.inputs.entries()) {
    if (!input.UnresolvedObject) {
      continue;
    }
    let updated;
    const id = normalizeSuiAddress(input.UnresolvedObject.objectId);
    const object2 = objectsById.get(id);
    if (input.UnresolvedObject.initialSharedVersion ?? object2?.initialSharedVersion) {
      updated = Inputs.SharedObjectRef({
        objectId: id,
        initialSharedVersion: input.UnresolvedObject.initialSharedVersion || object2?.initialSharedVersion,
        mutable: input.UnresolvedObject.mutable || isUsedAsMutable(transactionData, index)
      });
    } else if (isUsedAsReceiving(transactionData, index)) {
      updated = Inputs.ReceivingRef(
        {
          objectId: id,
          digest: input.UnresolvedObject.digest ?? object2?.digest,
          version: input.UnresolvedObject.version ?? object2?.version
        }
      );
    }
    transactionData.inputs[transactionData.inputs.indexOf(input)] = updated ?? Inputs.ObjectRef({
      objectId: id,
      digest: input.UnresolvedObject.digest ?? object2?.digest,
      version: input.UnresolvedObject.version ?? object2?.version
    });
  }
}
async function normalizeInputs(transactionData, client) {
  const { inputs, commands } = transactionData;
  const moveCallsToResolve = [];
  const moveFunctionsToResolve = /* @__PURE__ */ new Set();
  commands.forEach((command) => {
    if (command.MoveCall) {
      if (command.MoveCall._argumentTypes) {
        return;
      }
      const inputs2 = command.MoveCall.arguments.map((arg) => {
        if (arg.$kind === "Input") {
          return transactionData.inputs[arg.Input];
        }
        return null;
      });
      const needsResolution = inputs2.some(
        (input) => input?.UnresolvedPure || input?.UnresolvedObject && typeof input?.UnresolvedObject.mutable !== "boolean"
      );
      if (needsResolution) {
        const functionName = `${command.MoveCall.package}::${command.MoveCall.module}::${command.MoveCall.function}`;
        moveFunctionsToResolve.add(functionName);
        moveCallsToResolve.push(command.MoveCall);
      }
    }
  });
  const moveFunctionParameters = /* @__PURE__ */ new Map();
  if (moveFunctionsToResolve.size > 0) {
    await Promise.all(
      [...moveFunctionsToResolve].map(async (functionName) => {
        const [packageId, moduleId, functionId] = functionName.split("::");
        const def = await client.getNormalizedMoveFunction({
          package: packageId,
          module: moduleId,
          function: functionId
        });
        moveFunctionParameters.set(
          functionName,
          def.parameters.map((param) => normalizedTypeToMoveTypeSignature(param))
        );
      })
    );
  }
  if (moveCallsToResolve.length) {
    await Promise.all(
      moveCallsToResolve.map(async (moveCall) => {
        const parameters = moveFunctionParameters.get(
          `${moveCall.package}::${moveCall.module}::${moveCall.function}`
        );
        if (!parameters) {
          return;
        }
        const hasTxContext = parameters.length > 0 && isTxContext(parameters.at(-1));
        const params = hasTxContext ? parameters.slice(0, parameters.length - 1) : parameters;
        moveCall._argumentTypes = params;
      })
    );
  }
  commands.forEach((command) => {
    if (!command.MoveCall) {
      return;
    }
    const moveCall = command.MoveCall;
    const fnName = `${moveCall.package}::${moveCall.module}::${moveCall.function}`;
    const params = moveCall._argumentTypes;
    if (!params) {
      return;
    }
    if (params.length !== command.MoveCall.arguments.length) {
      throw new Error(`Incorrect number of arguments for ${fnName}`);
    }
    params.forEach((param, i) => {
      const arg = moveCall.arguments[i];
      if (arg.$kind !== "Input") return;
      const input = inputs[arg.Input];
      if (!input.UnresolvedPure && !input.UnresolvedObject) {
        return;
      }
      const inputValue = input.UnresolvedPure?.value ?? input.UnresolvedObject?.objectId;
      const schema = getPureBcsSchema(param.body);
      if (schema) {
        arg.type = "pure";
        inputs[inputs.indexOf(input)] = Inputs.Pure(schema.serialize(inputValue));
        return;
      }
      if (typeof inputValue !== "string") {
        throw new Error(
          `Expect the argument to be an object id string, got ${JSON.stringify(
            inputValue,
            null,
            2
          )}`
        );
      }
      arg.type = "object";
      const unresolvedObject = input.UnresolvedPure ? {
        $kind: "UnresolvedObject",
        UnresolvedObject: {
          objectId: inputValue
        }
      } : input;
      inputs[arg.Input] = unresolvedObject;
    });
  });
}
function isUsedAsMutable(transactionData, index) {
  let usedAsMutable = false;
  transactionData.getInputUses(index, (arg, tx) => {
    if (tx.MoveCall && tx.MoveCall._argumentTypes) {
      const argIndex = tx.MoveCall.arguments.indexOf(arg);
      usedAsMutable = tx.MoveCall._argumentTypes[argIndex].ref !== "&" || usedAsMutable;
    }
    if (tx.$kind === "MakeMoveVec" || tx.$kind === "MergeCoins" || tx.$kind === "SplitCoins" || tx.$kind === "TransferObjects") {
      usedAsMutable = true;
    }
  });
  return usedAsMutable;
}
function isUsedAsReceiving(transactionData, index) {
  let usedAsReceiving = false;
  transactionData.getInputUses(index, (arg, tx) => {
    if (tx.MoveCall && tx.MoveCall._argumentTypes) {
      const argIndex = tx.MoveCall.arguments.indexOf(arg);
      usedAsReceiving = isReceivingType(tx.MoveCall._argumentTypes[argIndex]) || usedAsReceiving;
    }
  });
  return usedAsReceiving;
}
function isReceivingType(type) {
  if (typeof type.body !== "object" || !("datatype" in type.body)) {
    return false;
  }
  return type.body.datatype.package === "0x2" && type.body.datatype.module === "transfer" && type.body.datatype.type === "Receiving";
}

// node_modules/@mysten/sui/dist/esm/transactions/resolve.js
function needsTransactionResolution(data, options) {
  if (data.inputs.some((input) => {
    return input.UnresolvedObject || input.UnresolvedPure;
  })) {
    return true;
  }
  if (!options.onlyTransactionKind) {
    if (!data.gasConfig.price || !data.gasConfig.budget || !data.gasConfig.payment) {
      return true;
    }
  }
  return false;
}
async function resolveTransactionPlugin(transactionData, options, next) {
  normalizeRawArguments(transactionData);
  if (!needsTransactionResolution(transactionData, options)) {
    await validate(transactionData);
    return next();
  }
  const client = getClient(options);
  const plugin = client.core?.resolveTransactionPlugin() ?? jsonRpcClientResolveTransactionPlugin(client);
  return plugin(transactionData, options, async () => {
    await validate(transactionData);
    await next();
  });
}
function validate(transactionData) {
  transactionData.inputs.forEach((input, index) => {
    if (input.$kind !== "Object" && input.$kind !== "Pure") {
      throw new Error(
        `Input at index ${index} has not been resolved.  Expected a Pure or Object input, but found ${JSON.stringify(
          input
        )}`
      );
    }
  });
}
function getClient(options) {
  if (!options.client) {
    throw new Error(
      `No sui client passed to Transaction#build, but transaction data was not sufficient to build offline.`
    );
  }
  return options.client;
}
function normalizeRawArguments(transactionData) {
  for (const command of transactionData.commands) {
    switch (command.$kind) {
      case "SplitCoins":
        command.SplitCoins.amounts.forEach((amount) => {
          normalizeRawArgument(amount, suiBcs.U64, transactionData);
        });
        break;
      case "TransferObjects":
        normalizeRawArgument(command.TransferObjects.address, suiBcs.Address, transactionData);
        break;
    }
  }
}
function normalizeRawArgument(arg, schema, transactionData) {
  if (arg.$kind !== "Input") {
    return;
  }
  const input = transactionData.inputs[arg.Input];
  if (input.$kind !== "UnresolvedPure") {
    return;
  }
  transactionData.inputs[arg.Input] = Inputs.Pure(schema.serialize(input.UnresolvedPure.value));
}

// node_modules/@mysten/sui/dist/esm/transactions/object.js
function createObjectMethods(makeObject) {
  function object2(value) {
    return makeObject(value);
  }
  object2.system = (options) => {
    const mutable = options?.mutable;
    if (mutable !== void 0) {
      return object2(
        Inputs.SharedObjectRef({
          objectId: "0x5",
          initialSharedVersion: 1,
          mutable
        })
      );
    }
    return object2({
      $kind: "UnresolvedObject",
      UnresolvedObject: {
        objectId: "0x5",
        initialSharedVersion: 1
      }
    });
  };
  object2.clock = () => object2(
    Inputs.SharedObjectRef({
      objectId: "0x6",
      initialSharedVersion: 1,
      mutable: false
    })
  );
  object2.random = () => object2({
    $kind: "UnresolvedObject",
    UnresolvedObject: {
      objectId: "0x8",
      mutable: false
    }
  });
  object2.denyList = (options) => {
    return object2({
      $kind: "UnresolvedObject",
      UnresolvedObject: {
        objectId: "0x403",
        mutable: options?.mutable
      }
    });
  };
  object2.option = ({ type, value }) => (tx) => tx.moveCall({
    typeArguments: [type],
    target: `0x1::option::${value === null ? "none" : "some"}`,
    arguments: value === null ? [] : [tx.object(value)]
  });
  return object2;
}

// node_modules/@mysten/sui/dist/esm/transactions/pure.js
function createPure(makePure) {
  function pure(typeOrSerializedValue, value) {
    if (typeof typeOrSerializedValue === "string") {
      return makePure(pureBcsSchemaFromTypeName(typeOrSerializedValue).serialize(value));
    }
    if (typeOrSerializedValue instanceof Uint8Array || isSerializedBcs(typeOrSerializedValue)) {
      return makePure(typeOrSerializedValue);
    }
    throw new Error("tx.pure must be called either a bcs type name, or a serialized bcs value");
  }
  pure.u8 = (value) => makePure(suiBcs.U8.serialize(value));
  pure.u16 = (value) => makePure(suiBcs.U16.serialize(value));
  pure.u32 = (value) => makePure(suiBcs.U32.serialize(value));
  pure.u64 = (value) => makePure(suiBcs.U64.serialize(value));
  pure.u128 = (value) => makePure(suiBcs.U128.serialize(value));
  pure.u256 = (value) => makePure(suiBcs.U256.serialize(value));
  pure.bool = (value) => makePure(suiBcs.Bool.serialize(value));
  pure.string = (value) => makePure(suiBcs.String.serialize(value));
  pure.address = (value) => makePure(suiBcs.Address.serialize(value));
  pure.id = pure.address;
  pure.vector = (type, value) => {
    return makePure(
      suiBcs.vector(pureBcsSchemaFromTypeName(type)).serialize(value)
    );
  };
  pure.option = (type, value) => {
    return makePure(suiBcs.option(pureBcsSchemaFromTypeName(type)).serialize(value));
  };
  return pure;
}

// node_modules/@mysten/sui/dist/esm/transactions/plugins/NamedPackagesPlugin.js
var cacheMap = /* @__PURE__ */ new WeakMap();
var namedPackagesPlugin = (options) => {
  let mvrClient;
  if (options) {
    const overrides = options.overrides ?? {
      packages: {},
      types: {}
    };
    if (!cacheMap.has(overrides)) {
      cacheMap.set(overrides, new ClientCache());
    }
    mvrClient = new MvrClient({
      cache: cacheMap.get(overrides),
      url: options.url,
      pageSize: options.pageSize,
      overrides
    });
  }
  return async (transactionData, buildOptions, next) => {
    const names = findNamesInTransaction(transactionData);
    if (names.types.length === 0 && names.packages.length === 0) {
      return next();
    }
    const resolved = await (mvrClient || getClient2(buildOptions).core.mvr).resolve({
      types: names.types,
      packages: names.packages
    });
    replaceNames(transactionData, resolved);
    await next();
  };
};
function getClient2(options) {
  if (!options.client) {
    throw new Error(
      `No sui client passed to Transaction#build, but transaction data was not sufficient to build offline.`
    );
  }
  return options.client;
}

// node_modules/@mysten/sui/dist/esm/transactions/Transaction.js
var __typeError9 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck9 = (obj, member, msg) => member.has(obj) || __typeError9("Cannot " + msg);
var __privateGet9 = (obj, member, getter) => (__accessCheck9(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd9 = (obj, member, value) => member.has(obj) ? __typeError9("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet9 = (obj, member, value, setter) => (__accessCheck9(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateMethod6 = (obj, member, method) => (__accessCheck9(obj, member, "access private method"), method);
var _serializationPlugins;
var _buildPlugins;
var _intentResolvers;
var _inputSection;
var _commandSection;
var _availableResults;
var _pendingPromises;
var _added;
var _data2;
var _Transaction_instances;
var fork_fn;
var addCommand_fn;
var addInput_fn;
var normalizeTransactionArgument_fn;
var resolveArgument_fn;
var prepareBuild_fn;
var runPlugins_fn;
var waitForPendingTasks_fn;
var sortCommandsAndInputs_fn;
function createTransactionResult(index, length = Infinity) {
  const baseResult = {
    $kind: "Result",
    get Result() {
      return typeof index === "function" ? index() : index;
    }
  };
  const nestedResults = [];
  const nestedResultFor = (resultIndex) => nestedResults[resultIndex] ?? (nestedResults[resultIndex] = {
    $kind: "NestedResult",
    get NestedResult() {
      return [typeof index === "function" ? index() : index, resultIndex];
    }
  });
  return new Proxy(baseResult, {
    set() {
      throw new Error(
        "The transaction result is a proxy, and does not support setting properties directly"
      );
    },
    // TODO: Instead of making this return a concrete argument, we should ideally
    // make it reference-based (so that this gets resolved at build-time), which
    // allows re-ordering transactions.
    get(target, property) {
      if (property in target) {
        return Reflect.get(target, property);
      }
      if (property === Symbol.iterator) {
        return function* () {
          let i = 0;
          while (i < length) {
            yield nestedResultFor(i);
            i++;
          }
        };
      }
      if (typeof property === "symbol") return;
      const resultIndex = parseInt(property, 10);
      if (Number.isNaN(resultIndex) || resultIndex < 0) return;
      return nestedResultFor(resultIndex);
    }
  });
}
var TRANSACTION_BRAND = Symbol.for("@mysten/transaction");
function isTransaction(obj) {
  return !!obj && typeof obj === "object" && obj[TRANSACTION_BRAND] === true;
}
var modulePluginRegistry = {
  buildPlugins: /* @__PURE__ */ new Map(),
  serializationPlugins: /* @__PURE__ */ new Map()
};
var TRANSACTION_REGISTRY_KEY = Symbol.for("@mysten/transaction/registry");
function getGlobalPluginRegistry() {
  try {
    const target = globalThis;
    if (!target[TRANSACTION_REGISTRY_KEY]) {
      target[TRANSACTION_REGISTRY_KEY] = modulePluginRegistry;
    }
    return target[TRANSACTION_REGISTRY_KEY];
  } catch {
    return modulePluginRegistry;
  }
}
var _Transaction = class _Transaction2 {
  constructor() {
    __privateAdd9(this, _Transaction_instances);
    __privateAdd9(this, _serializationPlugins);
    __privateAdd9(this, _buildPlugins);
    __privateAdd9(this, _intentResolvers, /* @__PURE__ */ new Map());
    __privateAdd9(this, _inputSection, []);
    __privateAdd9(this, _commandSection, []);
    __privateAdd9(this, _availableResults, /* @__PURE__ */ new Set());
    __privateAdd9(this, _pendingPromises, /* @__PURE__ */ new Set());
    __privateAdd9(this, _added, /* @__PURE__ */ new Map());
    __privateAdd9(this, _data2);
    this.object = createObjectMethods(
      (value) => {
        if (typeof value === "function") {
          return this.object(this.add(value));
        }
        if (typeof value === "object" && is(ArgumentSchema, value)) {
          return value;
        }
        const id = getIdFromCallArg(value);
        const inserted = __privateGet9(this, _data2).inputs.find((i) => id === getIdFromCallArg(i));
        if (inserted?.Object?.SharedObject && typeof value === "object" && value.Object?.SharedObject) {
          inserted.Object.SharedObject.mutable = inserted.Object.SharedObject.mutable || value.Object.SharedObject.mutable;
        }
        return inserted ? { $kind: "Input", Input: __privateGet9(this, _data2).inputs.indexOf(inserted), type: "object" } : __privateMethod6(this, _Transaction_instances, addInput_fn).call(this, "object", typeof value === "string" ? {
          $kind: "UnresolvedObject",
          UnresolvedObject: { objectId: normalizeSuiAddress(value) }
        } : value);
      }
    );
    const globalPlugins = getGlobalPluginRegistry();
    __privateSet9(this, _data2, new TransactionDataBuilder());
    __privateSet9(this, _buildPlugins, [...globalPlugins.buildPlugins.values()]);
    __privateSet9(this, _serializationPlugins, [...globalPlugins.serializationPlugins.values()]);
  }
  /**
   * Converts from a serialize transaction kind (built with `build({ onlyTransactionKind: true })`) to a `Transaction` class.
   * Supports either a byte array, or base64-encoded bytes.
   */
  static fromKind(serialized) {
    const tx = new _Transaction2();
    __privateSet9(tx, _data2, TransactionDataBuilder.fromKindBytes(
      typeof serialized === "string" ? fromBase64(serialized) : serialized
    ));
    __privateSet9(tx, _inputSection, __privateGet9(tx, _data2).inputs.slice());
    __privateSet9(tx, _commandSection, __privateGet9(tx, _data2).commands.slice());
    __privateSet9(tx, _availableResults, new Set(__privateGet9(tx, _commandSection).map((_, i) => i)));
    return tx;
  }
  /**
   * Converts from a serialized transaction format to a `Transaction` class.
   * There are two supported serialized formats:
   * - A string returned from `Transaction#serialize`. The serialized format must be compatible, or it will throw an error.
   * - A byte array (or base64-encoded bytes) containing BCS transaction data.
   */
  static from(transaction) {
    const newTransaction = new _Transaction2();
    if (isTransaction(transaction)) {
      __privateSet9(newTransaction, _data2, TransactionDataBuilder.restore(
        transaction.getData()
      ));
    } else if (typeof transaction !== "string" || !transaction.startsWith("{")) {
      __privateSet9(newTransaction, _data2, TransactionDataBuilder.fromBytes(
        typeof transaction === "string" ? fromBase64(transaction) : transaction
      ));
    } else {
      __privateSet9(newTransaction, _data2, TransactionDataBuilder.restore(JSON.parse(transaction)));
    }
    __privateSet9(newTransaction, _inputSection, __privateGet9(newTransaction, _data2).inputs.slice());
    __privateSet9(newTransaction, _commandSection, __privateGet9(newTransaction, _data2).commands.slice());
    __privateSet9(newTransaction, _availableResults, new Set(__privateGet9(newTransaction, _commandSection).map((_, i) => i)));
    return newTransaction;
  }
  static registerGlobalSerializationPlugin(stepOrStep, step) {
    getGlobalPluginRegistry().serializationPlugins.set(
      stepOrStep,
      step ?? stepOrStep
    );
  }
  static unregisterGlobalSerializationPlugin(name) {
    getGlobalPluginRegistry().serializationPlugins.delete(name);
  }
  static registerGlobalBuildPlugin(stepOrStep, step) {
    getGlobalPluginRegistry().buildPlugins.set(
      stepOrStep,
      step ?? stepOrStep
    );
  }
  static unregisterGlobalBuildPlugin(name) {
    getGlobalPluginRegistry().buildPlugins.delete(name);
  }
  addSerializationPlugin(step) {
    __privateGet9(this, _serializationPlugins).push(step);
  }
  addBuildPlugin(step) {
    __privateGet9(this, _buildPlugins).push(step);
  }
  addIntentResolver(intent, resolver) {
    if (__privateGet9(this, _intentResolvers).has(intent) && __privateGet9(this, _intentResolvers).get(intent) !== resolver) {
      throw new Error(`Intent resolver for ${intent} already exists`);
    }
    __privateGet9(this, _intentResolvers).set(intent, resolver);
  }
  setSender(sender) {
    __privateGet9(this, _data2).sender = sender;
  }
  /**
   * Sets the sender only if it has not already been set.
   * This is useful for sponsored transaction flows where the sender may not be the same as the signer address.
   */
  setSenderIfNotSet(sender) {
    if (!__privateGet9(this, _data2).sender) {
      __privateGet9(this, _data2).sender = sender;
    }
  }
  setExpiration(expiration) {
    __privateGet9(this, _data2).expiration = expiration ? parse2(TransactionExpiration2, expiration) : null;
  }
  setGasPrice(price) {
    __privateGet9(this, _data2).gasConfig.price = String(price);
  }
  setGasBudget(budget) {
    __privateGet9(this, _data2).gasConfig.budget = String(budget);
  }
  setGasBudgetIfNotSet(budget) {
    if (__privateGet9(this, _data2).gasData.budget == null) {
      __privateGet9(this, _data2).gasConfig.budget = String(budget);
    }
  }
  setGasOwner(owner) {
    __privateGet9(this, _data2).gasConfig.owner = owner;
  }
  setGasPayment(payments) {
    __privateGet9(this, _data2).gasConfig.payment = payments.map((payment) => parse2(ObjectRefSchema, payment));
  }
  /** @deprecated Use `getData()` instead. */
  get blockData() {
    return serializeV1TransactionData(__privateGet9(this, _data2).snapshot());
  }
  /** Get a snapshot of the transaction data, in JSON form: */
  getData() {
    return __privateGet9(this, _data2).snapshot();
  }
  // Used to brand transaction classes so that they can be identified, even between multiple copies
  // of the builder.
  get [TRANSACTION_BRAND]() {
    return true;
  }
  // Temporary workaround for the wallet interface accidentally serializing transactions via postMessage
  get pure() {
    Object.defineProperty(this, "pure", {
      enumerable: false,
      value: createPure((value) => {
        if (isSerializedBcs(value)) {
          return __privateMethod6(this, _Transaction_instances, addInput_fn).call(this, "pure", {
            $kind: "Pure",
            Pure: {
              bytes: value.toBase64()
            }
          });
        }
        return __privateMethod6(this, _Transaction_instances, addInput_fn).call(this, "pure", is(NormalizedCallArg, value) ? parse2(NormalizedCallArg, value) : value instanceof Uint8Array ? Inputs.Pure(value) : { $kind: "UnresolvedPure", UnresolvedPure: { value } });
      })
    });
    return this.pure;
  }
  /** Returns an argument for the gas coin, to be used in a transaction. */
  get gas() {
    return { $kind: "GasCoin", GasCoin: true };
  }
  /**
   * Add a new object input to the transaction using the fully-resolved object reference.
   * If you only have an object ID, use `builder.object(id)` instead.
   */
  objectRef(...args) {
    return this.object(Inputs.ObjectRef(...args));
  }
  /**
   * Add a new receiving input to the transaction using the fully-resolved object reference.
   * If you only have an object ID, use `builder.object(id)` instead.
   */
  receivingRef(...args) {
    return this.object(Inputs.ReceivingRef(...args));
  }
  /**
   * Add a new shared object input to the transaction using the fully-resolved shared object reference.
   * If you only have an object ID, use `builder.object(id)` instead.
   */
  sharedObjectRef(...args) {
    return this.object(Inputs.SharedObjectRef(...args));
  }
  add(command) {
    if (typeof command === "function") {
      if (__privateGet9(this, _added).has(command)) {
        return __privateGet9(this, _added).get(command);
      }
      const fork = __privateMethod6(this, _Transaction_instances, fork_fn).call(this);
      const result = command(fork);
      if (!(result && typeof result === "object" && "then" in result)) {
        __privateSet9(this, _availableResults, __privateGet9(fork, _availableResults));
        __privateGet9(this, _added).set(command, result);
        return result;
      }
      const placeholder = __privateMethod6(this, _Transaction_instances, addCommand_fn).call(this, {
        $kind: "$Intent",
        $Intent: {
          name: "AsyncTransactionThunk",
          inputs: {},
          data: {
            resultIndex: __privateGet9(this, _data2).commands.length,
            result: null
          }
        }
      });
      __privateGet9(this, _pendingPromises).add(
        Promise.resolve(result).then((result2) => {
          placeholder.$Intent.data.result = result2;
        })
      );
      const txResult = createTransactionResult(() => placeholder.$Intent.data.resultIndex);
      __privateGet9(this, _added).set(command, txResult);
      return txResult;
    } else {
      __privateMethod6(this, _Transaction_instances, addCommand_fn).call(this, command);
    }
    return createTransactionResult(__privateGet9(this, _data2).commands.length - 1);
  }
  // Method shorthands:
  splitCoins(coin, amounts) {
    const command = Commands.SplitCoins(
      typeof coin === "string" ? this.object(coin) : __privateMethod6(this, _Transaction_instances, resolveArgument_fn).call(this, coin),
      amounts.map(
        (amount) => typeof amount === "number" || typeof amount === "bigint" || typeof amount === "string" ? this.pure.u64(amount) : __privateMethod6(this, _Transaction_instances, normalizeTransactionArgument_fn).call(this, amount)
      )
    );
    __privateMethod6(this, _Transaction_instances, addCommand_fn).call(this, command);
    return createTransactionResult(__privateGet9(this, _data2).commands.length - 1, amounts.length);
  }
  mergeCoins(destination, sources) {
    return this.add(
      Commands.MergeCoins(
        this.object(destination),
        sources.map((src) => this.object(src))
      )
    );
  }
  publish({ modules, dependencies }) {
    return this.add(
      Commands.Publish({
        modules,
        dependencies
      })
    );
  }
  upgrade({
    modules,
    dependencies,
    package: packageId,
    ticket
  }) {
    return this.add(
      Commands.Upgrade({
        modules,
        dependencies,
        package: packageId,
        ticket: this.object(ticket)
      })
    );
  }
  moveCall({
    arguments: args,
    ...input
  }) {
    return this.add(
      Commands.MoveCall({
        ...input,
        arguments: args?.map((arg) => __privateMethod6(this, _Transaction_instances, normalizeTransactionArgument_fn).call(this, arg))
      })
    );
  }
  transferObjects(objects, address) {
    return this.add(
      Commands.TransferObjects(
        objects.map((obj) => this.object(obj)),
        typeof address === "string" ? this.pure.address(address) : __privateMethod6(this, _Transaction_instances, normalizeTransactionArgument_fn).call(this, address)
      )
    );
  }
  makeMoveVec({
    type,
    elements
  }) {
    return this.add(
      Commands.MakeMoveVec({
        type,
        elements: elements.map((obj) => this.object(obj))
      })
    );
  }
  /**
   * @deprecated Use toJSON instead.
   * For synchronous serialization, you can use `getData()`
   * */
  serialize() {
    return JSON.stringify(serializeV1TransactionData(__privateGet9(this, _data2).snapshot()));
  }
  async toJSON(options = {}) {
    await this.prepareForSerialization(options);
    const fullyResolved = this.isFullyResolved();
    return JSON.stringify(
      parse2(
        SerializedTransactionDataV2Schema,
        fullyResolved ? {
          ...__privateGet9(this, _data2).snapshot(),
          digest: __privateGet9(this, _data2).getDigest()
        } : __privateGet9(this, _data2).snapshot()
      ),
      (_key, value) => typeof value === "bigint" ? value.toString() : value,
      2
    );
  }
  /** Build the transaction to BCS bytes, and sign it with the provided keypair. */
  async sign(options) {
    const { signer, ...buildOptions } = options;
    const bytes = await this.build(buildOptions);
    return signer.signTransaction(bytes);
  }
  /**
   *  Ensures that:
   *  - All objects have been fully resolved to a specific version
   *  - All pure inputs have been serialized to bytes
   *  - All async thunks have been fully resolved
   *  - All transaction intents have been resolved
   * 	- The gas payment, budget, and price have been set
   *  - The transaction sender has been set
   *
   *  When true, the transaction will always be built to the same bytes and digest (unless the transaction is mutated)
   */
  isFullyResolved() {
    if (!__privateGet9(this, _data2).sender) {
      return false;
    }
    if (__privateGet9(this, _pendingPromises).size > 0) {
      return false;
    }
    if (__privateGet9(this, _data2).commands.some((cmd) => cmd.$Intent)) {
      return false;
    }
    if (needsTransactionResolution(__privateGet9(this, _data2), {})) {
      return false;
    }
    return true;
  }
  /** Build the transaction to BCS bytes. */
  async build(options = {}) {
    await this.prepareForSerialization(options);
    await __privateMethod6(this, _Transaction_instances, prepareBuild_fn).call(this, options);
    return __privateGet9(this, _data2).build({
      onlyTransactionKind: options.onlyTransactionKind
    });
  }
  /** Derive transaction digest */
  async getDigest(options = {}) {
    await this.prepareForSerialization(options);
    await __privateMethod6(this, _Transaction_instances, prepareBuild_fn).call(this, options);
    return __privateGet9(this, _data2).getDigest();
  }
  async prepareForSerialization(options) {
    await __privateMethod6(this, _Transaction_instances, waitForPendingTasks_fn).call(this);
    __privateMethod6(this, _Transaction_instances, sortCommandsAndInputs_fn).call(this);
    const intents = /* @__PURE__ */ new Set();
    for (const command of __privateGet9(this, _data2).commands) {
      if (command.$Intent) {
        intents.add(command.$Intent.name);
      }
    }
    const steps = [...__privateGet9(this, _serializationPlugins)];
    for (const intent of intents) {
      if (options.supportedIntents?.includes(intent)) {
        continue;
      }
      if (!__privateGet9(this, _intentResolvers).has(intent)) {
        throw new Error(`Missing intent resolver for ${intent}`);
      }
      steps.push(__privateGet9(this, _intentResolvers).get(intent));
    }
    steps.push(namedPackagesPlugin());
    await __privateMethod6(this, _Transaction_instances, runPlugins_fn).call(this, steps, options);
  }
};
_serializationPlugins = /* @__PURE__ */ new WeakMap();
_buildPlugins = /* @__PURE__ */ new WeakMap();
_intentResolvers = /* @__PURE__ */ new WeakMap();
_inputSection = /* @__PURE__ */ new WeakMap();
_commandSection = /* @__PURE__ */ new WeakMap();
_availableResults = /* @__PURE__ */ new WeakMap();
_pendingPromises = /* @__PURE__ */ new WeakMap();
_added = /* @__PURE__ */ new WeakMap();
_data2 = /* @__PURE__ */ new WeakMap();
_Transaction_instances = /* @__PURE__ */ new WeakSet();
fork_fn = function() {
  const fork = new _Transaction();
  __privateSet9(fork, _data2, __privateGet9(this, _data2));
  __privateSet9(fork, _serializationPlugins, __privateGet9(this, _serializationPlugins));
  __privateSet9(fork, _buildPlugins, __privateGet9(this, _buildPlugins));
  __privateSet9(fork, _intentResolvers, __privateGet9(this, _intentResolvers));
  __privateSet9(fork, _pendingPromises, __privateGet9(this, _pendingPromises));
  __privateSet9(fork, _availableResults, new Set(__privateGet9(this, _availableResults)));
  __privateSet9(fork, _added, __privateGet9(this, _added));
  __privateGet9(this, _inputSection).push(__privateGet9(fork, _inputSection));
  __privateGet9(this, _commandSection).push(__privateGet9(fork, _commandSection));
  return fork;
};
addCommand_fn = function(command) {
  const resultIndex = __privateGet9(this, _data2).commands.length;
  __privateGet9(this, _commandSection).push(command);
  __privateGet9(this, _availableResults).add(resultIndex);
  __privateGet9(this, _data2).commands.push(command);
  __privateGet9(this, _data2).mapCommandArguments(resultIndex, (arg) => {
    if (arg.$kind === "Result" && !__privateGet9(this, _availableResults).has(arg.Result)) {
      throw new Error(
        `Result { Result: ${arg.Result} } is not available to use in the current transaction`
      );
    }
    if (arg.$kind === "NestedResult" && !__privateGet9(this, _availableResults).has(arg.NestedResult[0])) {
      throw new Error(
        `Result { NestedResult: [${arg.NestedResult[0]}, ${arg.NestedResult[1]}] } is not available to use in the current transaction`
      );
    }
    if (arg.$kind === "Input" && arg.Input >= __privateGet9(this, _data2).inputs.length) {
      throw new Error(
        `Input { Input: ${arg.Input} } references an input that does not exist in the current transaction`
      );
    }
    return arg;
  });
  return command;
};
addInput_fn = function(type, input) {
  __privateGet9(this, _inputSection).push(input);
  return __privateGet9(this, _data2).addInput(type, input);
};
normalizeTransactionArgument_fn = function(arg) {
  if (isSerializedBcs(arg)) {
    return this.pure(arg);
  }
  return __privateMethod6(this, _Transaction_instances, resolveArgument_fn).call(this, arg);
};
resolveArgument_fn = function(arg) {
  if (typeof arg === "function") {
    const resolved = this.add(arg);
    if (typeof resolved === "function") {
      return __privateMethod6(this, _Transaction_instances, resolveArgument_fn).call(this, resolved);
    }
    return parse2(ArgumentSchema, resolved);
  }
  return parse2(ArgumentSchema, arg);
};
prepareBuild_fn = async function(options) {
  if (!options.onlyTransactionKind && !__privateGet9(this, _data2).sender) {
    throw new Error("Missing transaction sender");
  }
  await __privateMethod6(this, _Transaction_instances, runPlugins_fn).call(this, [...__privateGet9(this, _buildPlugins), resolveTransactionPlugin], options);
};
runPlugins_fn = async function(plugins, options) {
  try {
    const createNext = (i) => {
      if (i >= plugins.length) {
        return () => {
        };
      }
      const plugin = plugins[i];
      return async () => {
        const next = createNext(i + 1);
        let calledNext = false;
        let nextResolved = false;
        await plugin(__privateGet9(this, _data2), options, async () => {
          if (calledNext) {
            throw new Error(`next() was call multiple times in TransactionPlugin ${i}`);
          }
          calledNext = true;
          await next();
          nextResolved = true;
        });
        if (!calledNext) {
          throw new Error(`next() was not called in TransactionPlugin ${i}`);
        }
        if (!nextResolved) {
          throw new Error(`next() was not awaited in TransactionPlugin ${i}`);
        }
      };
    };
    await createNext(0)();
  } finally {
    __privateSet9(this, _inputSection, __privateGet9(this, _data2).inputs.slice());
    __privateSet9(this, _commandSection, __privateGet9(this, _data2).commands.slice());
    __privateSet9(this, _availableResults, new Set(__privateGet9(this, _commandSection).map((_, i) => i)));
  }
};
waitForPendingTasks_fn = async function() {
  while (__privateGet9(this, _pendingPromises).size > 0) {
    const newPromise = Promise.all(__privateGet9(this, _pendingPromises));
    __privateGet9(this, _pendingPromises).clear();
    __privateGet9(this, _pendingPromises).add(newPromise);
    await newPromise;
    __privateGet9(this, _pendingPromises).delete(newPromise);
  }
};
sortCommandsAndInputs_fn = function() {
  const unorderedCommands = __privateGet9(this, _data2).commands;
  const unorderedInputs = __privateGet9(this, _data2).inputs;
  const orderedCommands = __privateGet9(this, _commandSection).flat(Infinity);
  const orderedInputs = __privateGet9(this, _inputSection).flat(Infinity);
  if (orderedCommands.length !== unorderedCommands.length) {
    throw new Error("Unexpected number of commands found in transaction data");
  }
  if (orderedInputs.length !== unorderedInputs.length) {
    throw new Error("Unexpected number of inputs found in transaction data");
  }
  const filteredCommands = orderedCommands.filter(
    (cmd) => cmd.$Intent?.name !== "AsyncTransactionThunk"
  );
  __privateGet9(this, _data2).commands = filteredCommands;
  __privateGet9(this, _data2).inputs = orderedInputs;
  __privateSet9(this, _commandSection, filteredCommands);
  __privateSet9(this, _inputSection, orderedInputs);
  __privateSet9(this, _availableResults, new Set(filteredCommands.map((_, i) => i)));
  function getOriginalIndex(index) {
    const command = unorderedCommands[index];
    if (command.$Intent?.name === "AsyncTransactionThunk") {
      const result = command.$Intent.data.result;
      if (result == null) {
        throw new Error("AsyncTransactionThunk has not been resolved");
      }
      return getOriginalIndex(result.Result);
    }
    const updated = filteredCommands.indexOf(command);
    if (updated === -1) {
      throw new Error("Unable to find original index for command");
    }
    return updated;
  }
  __privateGet9(this, _data2).mapArguments((arg) => {
    if (arg.$kind === "Input") {
      const updated = orderedInputs.indexOf(unorderedInputs[arg.Input]);
      if (updated === -1) {
        throw new Error("Input has not been resolved");
      }
      return { ...arg, Input: updated };
    } else if (arg.$kind === "Result") {
      const updated = getOriginalIndex(arg.Result);
      return { ...arg, Result: updated };
    } else if (arg.$kind === "NestedResult") {
      const updated = getOriginalIndex(arg.NestedResult[0]);
      return { ...arg, NestedResult: [updated, arg.NestedResult[1]] };
    }
    return arg;
  });
  for (const [i, cmd] of unorderedCommands.entries()) {
    if (cmd.$Intent?.name === "AsyncTransactionThunk") {
      try {
        cmd.$Intent.data.resultIndex = getOriginalIndex(i);
      } catch {
      }
    }
  }
};
var Transaction = _Transaction;

// node_modules/@mysten/sui/dist/esm/jsonRpc/core.js
var __typeError10 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck10 = (obj, member, msg) => member.has(obj) || __typeError10("Cannot " + msg);
var __privateGet10 = (obj, member, getter) => (__accessCheck10(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd10 = (obj, member, value) => member.has(obj) ? __typeError10("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet10 = (obj, member, value, setter) => (__accessCheck10(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var _jsonRpcClient;
var JSONRpcCoreClient = class extends Experimental_CoreClient {
  constructor({
    jsonRpcClient,
    mvr
  }) {
    super({ network: jsonRpcClient.network, base: jsonRpcClient, mvr });
    __privateAdd10(this, _jsonRpcClient);
    __privateSet10(this, _jsonRpcClient, jsonRpcClient);
  }
  async getObjects(options) {
    const batches = chunk(options.objectIds, 50);
    const results = [];
    for (const batch of batches) {
      const objects = await __privateGet10(this, _jsonRpcClient).multiGetObjects({
        ids: batch,
        options: {
          showOwner: true,
          showType: true,
          showBcs: true,
          showPreviousTransaction: true
        },
        signal: options.signal
      });
      for (const [idx, object2] of objects.entries()) {
        if (object2.error) {
          results.push(ObjectError.fromResponse(object2.error, batch[idx]));
        } else {
          results.push(parseObject(object2.data));
        }
      }
    }
    return {
      objects: results
    };
  }
  async getOwnedObjects(options) {
    const objects = await __privateGet10(this, _jsonRpcClient).getOwnedObjects({
      owner: options.address,
      limit: options.limit,
      cursor: options.cursor,
      options: {
        showOwner: true,
        showType: true,
        showBcs: true,
        showPreviousTransaction: true
      },
      filter: options.type ? { StructType: options.type } : null,
      signal: options.signal
    });
    return {
      objects: objects.data.map((result) => {
        if (result.error) {
          throw ObjectError.fromResponse(result.error);
        }
        return parseObject(result.data);
      }),
      hasNextPage: objects.hasNextPage,
      cursor: objects.nextCursor ?? null
    };
  }
  async getCoins(options) {
    const coins = await __privateGet10(this, _jsonRpcClient).getCoins({
      owner: options.address,
      coinType: options.coinType,
      limit: options.limit,
      cursor: options.cursor,
      signal: options.signal
    });
    return {
      objects: coins.data.map((coin) => {
        return {
          id: coin.coinObjectId,
          version: coin.version,
          digest: coin.digest,
          balance: coin.balance,
          type: `0x2::coin::Coin<${coin.coinType}>`,
          content: Promise.resolve(
            Coin.serialize({
              id: coin.coinObjectId,
              balance: {
                value: coin.balance
              }
            }).toBytes()
          ),
          owner: {
            $kind: "ObjectOwner",
            ObjectOwner: options.address
          },
          previousTransaction: coin.previousTransaction
        };
      }),
      hasNextPage: coins.hasNextPage,
      cursor: coins.nextCursor ?? null
    };
  }
  async getBalance(options) {
    const balance = await __privateGet10(this, _jsonRpcClient).getBalance({
      owner: options.address,
      coinType: options.coinType,
      signal: options.signal
    });
    return {
      balance: {
        coinType: balance.coinType,
        balance: balance.totalBalance
      }
    };
  }
  async getAllBalances(options) {
    const balances = await __privateGet10(this, _jsonRpcClient).getAllBalances({
      owner: options.address,
      signal: options.signal
    });
    return {
      balances: balances.map((balance) => ({
        coinType: balance.coinType,
        balance: balance.totalBalance
      })),
      hasNextPage: false,
      cursor: null
    };
  }
  async getTransaction(options) {
    const transaction = await __privateGet10(this, _jsonRpcClient).getTransactionBlock({
      digest: options.digest,
      options: {
        showRawInput: true,
        showObjectChanges: true,
        showRawEffects: true,
        showEvents: true,
        showEffects: true,
        showBalanceChanges: true
      },
      signal: options.signal
    });
    return {
      transaction: parseTransaction2(transaction)
    };
  }
  async executeTransaction(options) {
    const transaction = await __privateGet10(this, _jsonRpcClient).executeTransactionBlock({
      transactionBlock: options.transaction,
      signature: options.signatures,
      options: {
        showRawEffects: true,
        showEvents: true,
        showObjectChanges: true,
        showRawInput: true,
        showEffects: true,
        showBalanceChanges: true
      },
      signal: options.signal
    });
    return {
      transaction: parseTransaction2(transaction)
    };
  }
  async dryRunTransaction(options) {
    const tx = Transaction.from(options.transaction);
    const result = await __privateGet10(this, _jsonRpcClient).dryRunTransactionBlock({
      transactionBlock: options.transaction,
      signal: options.signal
    });
    const { effects, objectTypes } = parseTransactionEffectsJson({
      effects: result.effects,
      objectChanges: result.objectChanges
    });
    return {
      transaction: {
        digest: await tx.getDigest(),
        epoch: null,
        effects,
        objectTypes: Promise.resolve(objectTypes),
        signatures: [],
        transaction: parseTransactionBcs(options.transaction),
        balanceChanges: result.balanceChanges.map((change) => ({
          coinType: change.coinType,
          address: parseOwnerAddress(change.owner),
          amount: change.amount
        }))
      }
    };
  }
  async getReferenceGasPrice(options) {
    const referenceGasPrice = await __privateGet10(this, _jsonRpcClient).getReferenceGasPrice({
      signal: options?.signal
    });
    return {
      referenceGasPrice: String(referenceGasPrice)
    };
  }
  async getDynamicFields(options) {
    const dynamicFields = await __privateGet10(this, _jsonRpcClient).getDynamicFields({
      parentId: options.parentId,
      limit: options.limit,
      cursor: options.cursor
    });
    return {
      dynamicFields: dynamicFields.data.map((dynamicField) => {
        return {
          id: dynamicField.objectId,
          type: dynamicField.objectType,
          name: {
            type: dynamicField.name.type,
            bcs: fromBase64(dynamicField.bcsName)
          }
        };
      }),
      hasNextPage: dynamicFields.hasNextPage,
      cursor: dynamicFields.nextCursor
    };
  }
  async verifyZkLoginSignature(options) {
    const result = await __privateGet10(this, _jsonRpcClient).verifyZkLoginSignature({
      bytes: options.bytes,
      signature: options.signature,
      intentScope: options.intentScope,
      author: options.author
    });
    return {
      success: result.success,
      errors: result.errors
    };
  }
  async defaultNameServiceName(options) {
    const name = (await __privateGet10(this, _jsonRpcClient).resolveNameServiceNames(options)).data[0];
    return {
      data: {
        name
      }
    };
  }
  resolveTransactionPlugin() {
    return jsonRpcClientResolveTransactionPlugin(__privateGet10(this, _jsonRpcClient));
  }
  async getMoveFunction(options) {
    const result = await __privateGet10(this, _jsonRpcClient).getNormalizedMoveFunction({
      package: (await this.mvr.resolvePackage({ package: options.packageId })).package,
      module: options.moduleName,
      function: options.name
    });
    return {
      function: {
        packageId: normalizeSuiAddress(options.packageId),
        moduleName: options.moduleName,
        name: options.name,
        visibility: parseVisibility(result.visibility),
        isEntry: result.isEntry,
        typeParameters: result.typeParameters.map((abilities) => ({
          isPhantom: false,
          constraints: parseAbilities(abilities)
        })),
        parameters: result.parameters.map((param) => parseNormalizedSuiMoveType2(param)),
        returns: result.return.map((ret) => parseNormalizedSuiMoveType2(ret))
      }
    };
  }
};
_jsonRpcClient = /* @__PURE__ */ new WeakMap();
function parseObject(object2) {
  return {
    id: object2.objectId,
    version: object2.version,
    digest: object2.digest,
    type: object2.type,
    content: Promise.resolve(
      object2.bcs?.dataType === "moveObject" ? fromBase64(object2.bcs.bcsBytes) : new Uint8Array()
    ),
    owner: parseOwner(object2.owner),
    previousTransaction: object2.previousTransaction ?? null
  };
}
function parseOwner(owner) {
  if (owner === "Immutable") {
    return {
      $kind: "Immutable",
      Immutable: true
    };
  }
  if ("ConsensusAddressOwner" in owner) {
    return {
      $kind: "ConsensusAddressOwner",
      ConsensusAddressOwner: {
        owner: owner.ConsensusAddressOwner.owner,
        startVersion: owner.ConsensusAddressOwner.start_version
      }
    };
  }
  if ("AddressOwner" in owner) {
    return {
      $kind: "AddressOwner",
      AddressOwner: owner.AddressOwner
    };
  }
  if ("ObjectOwner" in owner) {
    return {
      $kind: "ObjectOwner",
      ObjectOwner: owner.ObjectOwner
    };
  }
  if ("Shared" in owner) {
    return {
      $kind: "Shared",
      Shared: {
        initialSharedVersion: owner.Shared.initial_shared_version
      }
    };
  }
  throw new Error(`Unknown owner type: ${JSON.stringify(owner)}`);
}
function parseOwnerAddress(owner) {
  if (owner === "Immutable") {
    return null;
  }
  if ("ConsensusAddressOwner" in owner) {
    return owner.ConsensusAddressOwner.owner;
  }
  if ("AddressOwner" in owner) {
    return owner.AddressOwner;
  }
  if ("ObjectOwner" in owner) {
    return owner.ObjectOwner;
  }
  if ("Shared" in owner) {
    return null;
  }
  throw new Error(`Unknown owner type: ${JSON.stringify(owner)}`);
}
function parseTransaction2(transaction) {
  const parsedTx = suiBcs.SenderSignedData.parse(fromBase64(transaction.rawTransaction))[0];
  const objectTypes = {};
  transaction.objectChanges?.forEach((change) => {
    if (change.type !== "published") {
      objectTypes[change.objectId] = change.objectType;
    }
  });
  const bytes = suiBcs.TransactionData.serialize(parsedTx.intentMessage.value).toBytes();
  const data = TransactionDataBuilder.restore({
    version: 2,
    sender: parsedTx.intentMessage.value.V1.sender,
    expiration: parsedTx.intentMessage.value.V1.expiration,
    gasData: parsedTx.intentMessage.value.V1.gasData,
    inputs: parsedTx.intentMessage.value.V1.kind.ProgrammableTransaction.inputs,
    commands: parsedTx.intentMessage.value.V1.kind.ProgrammableTransaction.commands
  });
  return {
    digest: transaction.digest,
    epoch: transaction.effects?.executedEpoch ?? null,
    effects: parseTransactionEffectsBcs(new Uint8Array(transaction.rawEffects)),
    objectTypes: Promise.resolve(objectTypes),
    transaction: {
      ...data,
      bcs: bytes
    },
    signatures: parsedTx.txSignatures,
    balanceChanges: transaction.balanceChanges?.map((change) => ({
      coinType: change.coinType,
      address: parseOwnerAddress(change.owner),
      amount: change.amount
    })) ?? []
  };
}
function parseTransactionEffectsJson({
  bytes,
  effects,
  objectChanges
}) {
  const changedObjects = [];
  const unchangedConsensusObjects = [];
  const objectTypes = {};
  objectChanges?.forEach((change) => {
    switch (change.type) {
      case "published":
        changedObjects.push({
          id: change.packageId,
          inputState: "DoesNotExist",
          inputVersion: null,
          inputDigest: null,
          inputOwner: null,
          outputState: "PackageWrite",
          outputVersion: change.version,
          outputDigest: change.digest,
          outputOwner: null,
          idOperation: "Created"
        });
        break;
      case "transferred":
        changedObjects.push({
          id: change.objectId,
          inputState: "Exists",
          inputVersion: change.version,
          inputDigest: change.digest,
          inputOwner: {
            $kind: "AddressOwner",
            AddressOwner: change.sender
          },
          outputState: "ObjectWrite",
          outputVersion: change.version,
          outputDigest: change.digest,
          outputOwner: parseOwner(change.recipient),
          idOperation: "None"
        });
        objectTypes[change.objectId] = change.objectType;
        break;
      case "mutated":
        changedObjects.push({
          id: change.objectId,
          inputState: "Exists",
          inputVersion: change.previousVersion,
          inputDigest: null,
          inputOwner: parseOwner(change.owner),
          outputState: "ObjectWrite",
          outputVersion: change.version,
          outputDigest: change.digest,
          outputOwner: parseOwner(change.owner),
          idOperation: "None"
        });
        objectTypes[change.objectId] = change.objectType;
        break;
      case "deleted":
        changedObjects.push({
          id: change.objectId,
          inputState: "Exists",
          inputVersion: change.version,
          inputDigest: effects.deleted?.find((d) => d.objectId === change.objectId)?.digest ?? null,
          inputOwner: null,
          outputState: "DoesNotExist",
          outputVersion: null,
          outputDigest: null,
          outputOwner: null,
          idOperation: "Deleted"
        });
        objectTypes[change.objectId] = change.objectType;
        break;
      case "wrapped":
        changedObjects.push({
          id: change.objectId,
          inputState: "Exists",
          inputVersion: change.version,
          inputDigest: null,
          inputOwner: {
            $kind: "AddressOwner",
            AddressOwner: change.sender
          },
          outputState: "ObjectWrite",
          outputVersion: change.version,
          outputDigest: effects.wrapped?.find((w) => w.objectId === change.objectId)?.digest ?? null,
          outputOwner: {
            $kind: "ObjectOwner",
            ObjectOwner: change.sender
          },
          idOperation: "None"
        });
        objectTypes[change.objectId] = change.objectType;
        break;
      case "created":
        changedObjects.push({
          id: change.objectId,
          inputState: "DoesNotExist",
          inputVersion: null,
          inputDigest: null,
          inputOwner: null,
          outputState: "ObjectWrite",
          outputVersion: change.version,
          outputDigest: change.digest,
          outputOwner: parseOwner(change.owner),
          idOperation: "Created"
        });
        objectTypes[change.objectId] = change.objectType;
        break;
    }
  });
  return {
    objectTypes,
    effects: {
      bcs: bytes ?? null,
      digest: effects.transactionDigest,
      version: 2,
      status: effects.status.status === "success" ? { success: true, error: null } : { success: false, error: effects.status.error },
      gasUsed: effects.gasUsed,
      transactionDigest: effects.transactionDigest,
      gasObject: {
        id: effects.gasObject?.reference.objectId,
        inputState: "Exists",
        inputVersion: null,
        inputDigest: null,
        inputOwner: null,
        outputState: "ObjectWrite",
        outputVersion: effects.gasObject.reference.version,
        outputDigest: effects.gasObject.reference.digest,
        outputOwner: parseOwner(effects.gasObject.owner),
        idOperation: "None"
      },
      eventsDigest: effects.eventsDigest ?? null,
      dependencies: effects.dependencies ?? [],
      lamportVersion: effects.gasObject.reference.version,
      changedObjects,
      unchangedConsensusObjects,
      auxiliaryDataDigest: null
    }
  };
}
var Balance = suiBcs.struct("Balance", {
  value: suiBcs.u64()
});
var Coin = suiBcs.struct("Coin", {
  id: suiBcs.Address,
  balance: Balance
});
function parseNormalizedSuiMoveType2(type) {
  if (typeof type !== "string") {
    if ("Reference" in type) {
      return {
        reference: "immutable",
        body: parseNormalizedSuiMoveTypeBody2(type.Reference)
      };
    }
    if ("MutableReference" in type) {
      return {
        reference: "mutable",
        body: parseNormalizedSuiMoveTypeBody2(type.MutableReference)
      };
    }
  }
  return {
    reference: null,
    body: parseNormalizedSuiMoveTypeBody2(type)
  };
}
function parseNormalizedSuiMoveTypeBody2(type) {
  switch (type) {
    case "Address":
      return { $kind: "address" };
    case "Bool":
      return { $kind: "bool" };
    case "U8":
      return { $kind: "u8" };
    case "U16":
      return { $kind: "u16" };
    case "U32":
      return { $kind: "u32" };
    case "U64":
      return { $kind: "u64" };
    case "U128":
      return { $kind: "u128" };
    case "U256":
      return { $kind: "u256" };
  }
  if (typeof type === "string") {
    throw new Error(`Unknown type: ${type}`);
  }
  if ("Vector" in type) {
    return {
      $kind: "vector",
      vector: parseNormalizedSuiMoveTypeBody2(type.Vector)
    };
  }
  if ("Struct" in type) {
    return {
      $kind: "datatype",
      datatype: {
        typeName: `${normalizeSuiAddress(type.Struct.address)}::${type.Struct.module}::${type.Struct.name}`,
        typeParameters: type.Struct.typeArguments.map((t) => parseNormalizedSuiMoveTypeBody2(t))
      }
    };
  }
  if ("TypeParameter" in type) {
    return {
      $kind: "typeParameter",
      index: type.TypeParameter
    };
  }
  throw new Error(`Unknown type: ${JSON.stringify(type)}`);
}
function parseAbilities(abilitySet) {
  return abilitySet.abilities.map((ability) => {
    switch (ability) {
      case "Copy":
        return "copy";
      case "Drop":
        return "drop";
      case "Store":
        return "store";
      case "Key":
        return "key";
      default:
        return "unknown";
    }
  });
}
function parseVisibility(visibility) {
  switch (visibility) {
    case "Public":
      return "public";
    case "Private":
      return "private";
    case "Friend":
      return "friend";
    default:
      return "unknown";
  }
}

// node_modules/@mysten/sui/dist/esm/jsonRpc/client.js
var SUI_CLIENT_BRAND = Symbol.for("@mysten/SuiClient");
var SuiJsonRpcClient = class extends Experimental_BaseClient {
  /**
   * Establish a connection to a Sui RPC endpoint
   *
   * @param options configuration options for the API Client
   */
  constructor(options) {
    super({ network: options.network ?? "unknown" });
    this.jsonRpc = this;
    this.transport = options.transport ?? new JsonRpcHTTPTransport({ url: options.url });
    this.core = new JSONRpcCoreClient({
      jsonRpcClient: this,
      mvr: options.mvr
    });
  }
  get [SUI_CLIENT_BRAND]() {
    return true;
  }
  async getRpcApiVersion({ signal } = {}) {
    const resp = await this.transport.request({
      method: "rpc.discover",
      params: [],
      signal
    });
    return resp.info.version;
  }
  /**
   * Get all Coin<`coin_type`> objects owned by an address.
   */
  async getCoins({
    coinType,
    owner,
    cursor,
    limit,
    signal
  }) {
    if (!owner || !isValidSuiAddress(normalizeSuiAddress(owner))) {
      throw new Error("Invalid Sui address");
    }
    if (coinType && hasMvrName(coinType)) {
      coinType = (await this.core.mvr.resolveType({
        type: coinType
      })).type;
    }
    return await this.transport.request({
      method: "suix_getCoins",
      params: [owner, coinType, cursor, limit],
      signal
    });
  }
  /**
   * Get all Coin objects owned by an address.
   */
  async getAllCoins(input) {
    if (!input.owner || !isValidSuiAddress(normalizeSuiAddress(input.owner))) {
      throw new Error("Invalid Sui address");
    }
    return await this.transport.request({
      method: "suix_getAllCoins",
      params: [input.owner, input.cursor, input.limit],
      signal: input.signal
    });
  }
  /**
   * Get the total coin balance for one coin type, owned by the address owner.
   */
  async getBalance({ owner, coinType, signal }) {
    if (!owner || !isValidSuiAddress(normalizeSuiAddress(owner))) {
      throw new Error("Invalid Sui address");
    }
    if (coinType && hasMvrName(coinType)) {
      coinType = (await this.core.mvr.resolveType({
        type: coinType
      })).type;
    }
    return await this.transport.request({
      method: "suix_getBalance",
      params: [owner, coinType],
      signal
    });
  }
  /**
   * Get the total coin balance for all coin types, owned by the address owner.
   */
  async getAllBalances(input) {
    if (!input.owner || !isValidSuiAddress(normalizeSuiAddress(input.owner))) {
      throw new Error("Invalid Sui address");
    }
    return await this.transport.request({
      method: "suix_getAllBalances",
      params: [input.owner],
      signal: input.signal
    });
  }
  /**
   * Fetch CoinMetadata for a given coin type
   */
  async getCoinMetadata({ coinType, signal }) {
    if (coinType && hasMvrName(coinType)) {
      coinType = (await this.core.mvr.resolveType({
        type: coinType
      })).type;
    }
    return await this.transport.request({
      method: "suix_getCoinMetadata",
      params: [coinType],
      signal
    });
  }
  /**
   *  Fetch total supply for a coin
   */
  async getTotalSupply({ coinType, signal }) {
    if (coinType && hasMvrName(coinType)) {
      coinType = (await this.core.mvr.resolveType({
        type: coinType
      })).type;
    }
    return await this.transport.request({
      method: "suix_getTotalSupply",
      params: [coinType],
      signal
    });
  }
  /**
   * Invoke any RPC method
   * @param method the method to be invoked
   * @param args the arguments to be passed to the RPC request
   */
  async call(method, params, { signal } = {}) {
    return await this.transport.request({ method, params, signal });
  }
  /**
   * Get Move function argument types like read, write and full access
   */
  async getMoveFunctionArgTypes({
    package: pkg,
    module: module2,
    function: fn,
    signal
  }) {
    if (pkg && isValidNamedPackage(pkg)) {
      pkg = (await this.core.mvr.resolvePackage({
        package: pkg
      })).package;
    }
    return await this.transport.request({
      method: "sui_getMoveFunctionArgTypes",
      params: [pkg, module2, fn],
      signal
    });
  }
  /**
   * Get a map from module name to
   * structured representations of Move modules
   */
  async getNormalizedMoveModulesByPackage({
    package: pkg,
    signal
  }) {
    if (pkg && isValidNamedPackage(pkg)) {
      pkg = (await this.core.mvr.resolvePackage({
        package: pkg
      })).package;
    }
    return await this.transport.request({
      method: "sui_getNormalizedMoveModulesByPackage",
      params: [pkg],
      signal
    });
  }
  /**
   * Get a structured representation of Move module
   */
  async getNormalizedMoveModule({
    package: pkg,
    module: module2,
    signal
  }) {
    if (pkg && isValidNamedPackage(pkg)) {
      pkg = (await this.core.mvr.resolvePackage({
        package: pkg
      })).package;
    }
    return await this.transport.request({
      method: "sui_getNormalizedMoveModule",
      params: [pkg, module2],
      signal
    });
  }
  /**
   * Get a structured representation of Move function
   */
  async getNormalizedMoveFunction({
    package: pkg,
    module: module2,
    function: fn,
    signal
  }) {
    if (pkg && isValidNamedPackage(pkg)) {
      pkg = (await this.core.mvr.resolvePackage({
        package: pkg
      })).package;
    }
    return await this.transport.request({
      method: "sui_getNormalizedMoveFunction",
      params: [pkg, module2, fn],
      signal
    });
  }
  /**
   * Get a structured representation of Move struct
   */
  async getNormalizedMoveStruct({
    package: pkg,
    module: module2,
    struct,
    signal
  }) {
    if (pkg && isValidNamedPackage(pkg)) {
      pkg = (await this.core.mvr.resolvePackage({
        package: pkg
      })).package;
    }
    return await this.transport.request({
      method: "sui_getNormalizedMoveStruct",
      params: [pkg, module2, struct],
      signal
    });
  }
  /**
   * Get all objects owned by an address
   */
  async getOwnedObjects(input) {
    if (!input.owner || !isValidSuiAddress(normalizeSuiAddress(input.owner))) {
      throw new Error("Invalid Sui address");
    }
    const filter = input.filter ? {
      ...input.filter
    } : void 0;
    if (filter && "MoveModule" in filter && isValidNamedPackage(filter.MoveModule.package)) {
      filter.MoveModule = {
        module: filter.MoveModule.module,
        package: (await this.core.mvr.resolvePackage({
          package: filter.MoveModule.package
        })).package
      };
    } else if (filter && "StructType" in filter && hasMvrName(filter.StructType)) {
      filter.StructType = (await this.core.mvr.resolveType({
        type: filter.StructType
      })).type;
    }
    return await this.transport.request({
      method: "suix_getOwnedObjects",
      params: [
        input.owner,
        {
          filter,
          options: input.options
        },
        input.cursor,
        input.limit
      ],
      signal: input.signal
    });
  }
  /**
   * Get details about an object
   */
  async getObject(input) {
    if (!input.id || !isValidSuiObjectId(normalizeSuiObjectId(input.id))) {
      throw new Error("Invalid Sui Object id");
    }
    return await this.transport.request({
      method: "sui_getObject",
      params: [input.id, input.options],
      signal: input.signal
    });
  }
  async tryGetPastObject(input) {
    return await this.transport.request({
      method: "sui_tryGetPastObject",
      params: [input.id, input.version, input.options],
      signal: input.signal
    });
  }
  /**
   * Batch get details about a list of objects. If any of the object ids are duplicates the call will fail
   */
  async multiGetObjects(input) {
    input.ids.forEach((id) => {
      if (!id || !isValidSuiObjectId(normalizeSuiObjectId(id))) {
        throw new Error(`Invalid Sui Object id ${id}`);
      }
    });
    const hasDuplicates = input.ids.length !== new Set(input.ids).size;
    if (hasDuplicates) {
      throw new Error(`Duplicate object ids in batch call ${input.ids}`);
    }
    return await this.transport.request({
      method: "sui_multiGetObjects",
      params: [input.ids, input.options],
      signal: input.signal
    });
  }
  /**
   * Get transaction blocks for a given query criteria
   */
  async queryTransactionBlocks({
    filter,
    options,
    cursor,
    limit,
    order,
    signal
  }) {
    if (filter && "MoveFunction" in filter && isValidNamedPackage(filter.MoveFunction.package)) {
      filter = {
        ...filter,
        MoveFunction: {
          package: (await this.core.mvr.resolvePackage({
            package: filter.MoveFunction.package
          })).package
        }
      };
    }
    return await this.transport.request({
      method: "suix_queryTransactionBlocks",
      params: [
        {
          filter,
          options
        },
        cursor,
        limit,
        (order || "descending") === "descending"
      ],
      signal
    });
  }
  async getTransactionBlock(input) {
    if (!isValidTransactionDigest(input.digest)) {
      throw new Error("Invalid Transaction digest");
    }
    return await this.transport.request({
      method: "sui_getTransactionBlock",
      params: [input.digest, input.options],
      signal: input.signal
    });
  }
  async multiGetTransactionBlocks(input) {
    input.digests.forEach((d) => {
      if (!isValidTransactionDigest(d)) {
        throw new Error(`Invalid Transaction digest ${d}`);
      }
    });
    const hasDuplicates = input.digests.length !== new Set(input.digests).size;
    if (hasDuplicates) {
      throw new Error(`Duplicate digests in batch call ${input.digests}`);
    }
    return await this.transport.request({
      method: "sui_multiGetTransactionBlocks",
      params: [input.digests, input.options],
      signal: input.signal
    });
  }
  async executeTransactionBlock({
    transactionBlock,
    signature,
    options,
    requestType,
    signal
  }) {
    const result = await this.transport.request({
      method: "sui_executeTransactionBlock",
      params: [
        typeof transactionBlock === "string" ? transactionBlock : toBase64(transactionBlock),
        Array.isArray(signature) ? signature : [signature],
        options
      ],
      signal
    });
    if (requestType === "WaitForLocalExecution") {
      try {
        await this.waitForTransaction({
          digest: result.digest
        });
      } catch {
      }
    }
    return result;
  }
  async signAndExecuteTransaction({
    transaction,
    signer,
    ...input
  }) {
    let transactionBytes;
    if (transaction instanceof Uint8Array) {
      transactionBytes = transaction;
    } else {
      transaction.setSenderIfNotSet(signer.toSuiAddress());
      transactionBytes = await transaction.build({ client: this });
    }
    const { signature, bytes } = await signer.signTransaction(transactionBytes);
    return this.executeTransactionBlock({
      transactionBlock: bytes,
      signature,
      ...input
    });
  }
  /**
   * Get total number of transactions
   */
  async getTotalTransactionBlocks({ signal } = {}) {
    const resp = await this.transport.request({
      method: "sui_getTotalTransactionBlocks",
      params: [],
      signal
    });
    return BigInt(resp);
  }
  /**
   * Getting the reference gas price for the network
   */
  async getReferenceGasPrice({ signal } = {}) {
    const resp = await this.transport.request({
      method: "suix_getReferenceGasPrice",
      params: [],
      signal
    });
    return BigInt(resp);
  }
  /**
   * Return the delegated stakes for an address
   */
  async getStakes(input) {
    if (!input.owner || !isValidSuiAddress(normalizeSuiAddress(input.owner))) {
      throw new Error("Invalid Sui address");
    }
    return await this.transport.request({
      method: "suix_getStakes",
      params: [input.owner],
      signal: input.signal
    });
  }
  /**
   * Return the delegated stakes queried by id.
   */
  async getStakesByIds(input) {
    input.stakedSuiIds.forEach((id) => {
      if (!id || !isValidSuiObjectId(normalizeSuiObjectId(id))) {
        throw new Error(`Invalid Sui Stake id ${id}`);
      }
    });
    return await this.transport.request({
      method: "suix_getStakesByIds",
      params: [input.stakedSuiIds],
      signal: input.signal
    });
  }
  /**
   * Return the latest system state content.
   */
  async getLatestSuiSystemState({
    signal
  } = {}) {
    return await this.transport.request({
      method: "suix_getLatestSuiSystemState",
      params: [],
      signal
    });
  }
  /**
   * Get events for a given query criteria
   */
  async queryEvents({
    query,
    cursor,
    limit,
    order,
    signal
  }) {
    if (query && "MoveEventType" in query && hasMvrName(query.MoveEventType)) {
      query = {
        ...query,
        MoveEventType: (await this.core.mvr.resolveType({
          type: query.MoveEventType
        })).type
      };
    }
    if (query && "MoveEventModule" in query && isValidNamedPackage(query.MoveEventModule.package)) {
      query = {
        ...query,
        MoveEventModule: {
          module: query.MoveEventModule.module,
          package: (await this.core.mvr.resolvePackage({
            package: query.MoveEventModule.package
          })).package
        }
      };
    }
    if ("MoveModule" in query && isValidNamedPackage(query.MoveModule.package)) {
      query = {
        ...query,
        MoveModule: {
          module: query.MoveModule.module,
          package: (await this.core.mvr.resolvePackage({
            package: query.MoveModule.package
          })).package
        }
      };
    }
    return await this.transport.request({
      method: "suix_queryEvents",
      params: [query, cursor, limit, (order || "descending") === "descending"],
      signal
    });
  }
  /**
   * Subscribe to get notifications whenever an event matching the filter occurs
   *
   * @deprecated
   */
  async subscribeEvent(input) {
    return this.transport.subscribe({
      method: "suix_subscribeEvent",
      unsubscribe: "suix_unsubscribeEvent",
      params: [input.filter],
      onMessage: input.onMessage,
      signal: input.signal
    });
  }
  /**
   * @deprecated
   */
  async subscribeTransaction(input) {
    return this.transport.subscribe({
      method: "suix_subscribeTransaction",
      unsubscribe: "suix_unsubscribeTransaction",
      params: [input.filter],
      onMessage: input.onMessage,
      signal: input.signal
    });
  }
  /**
   * Runs the transaction block in dev-inspect mode. Which allows for nearly any
   * transaction (or Move call) with any arguments. Detailed results are
   * provided, including both the transaction effects and any return values.
   */
  async devInspectTransactionBlock(input) {
    let devInspectTxBytes;
    if (isTransaction(input.transactionBlock)) {
      input.transactionBlock.setSenderIfNotSet(input.sender);
      devInspectTxBytes = toBase64(
        await input.transactionBlock.build({
          client: this,
          onlyTransactionKind: true
        })
      );
    } else if (typeof input.transactionBlock === "string") {
      devInspectTxBytes = input.transactionBlock;
    } else if (input.transactionBlock instanceof Uint8Array) {
      devInspectTxBytes = toBase64(input.transactionBlock);
    } else {
      throw new Error("Unknown transaction block format.");
    }
    input.signal?.throwIfAborted();
    return await this.transport.request({
      method: "sui_devInspectTransactionBlock",
      params: [input.sender, devInspectTxBytes, input.gasPrice?.toString(), input.epoch],
      signal: input.signal
    });
  }
  /**
   * Dry run a transaction block and return the result.
   */
  async dryRunTransactionBlock(input) {
    return await this.transport.request({
      method: "sui_dryRunTransactionBlock",
      params: [
        typeof input.transactionBlock === "string" ? input.transactionBlock : toBase64(input.transactionBlock)
      ]
    });
  }
  /**
   * Return the list of dynamic field objects owned by an object
   */
  async getDynamicFields(input) {
    if (!input.parentId || !isValidSuiObjectId(normalizeSuiObjectId(input.parentId))) {
      throw new Error("Invalid Sui Object id");
    }
    return await this.transport.request({
      method: "suix_getDynamicFields",
      params: [input.parentId, input.cursor, input.limit],
      signal: input.signal
    });
  }
  /**
   * Return the dynamic field object information for a specified object
   */
  async getDynamicFieldObject(input) {
    return await this.transport.request({
      method: "suix_getDynamicFieldObject",
      params: [input.parentId, input.name],
      signal: input.signal
    });
  }
  /**
   * Get the sequence number of the latest checkpoint that has been executed
   */
  async getLatestCheckpointSequenceNumber({
    signal
  } = {}) {
    const resp = await this.transport.request({
      method: "sui_getLatestCheckpointSequenceNumber",
      params: [],
      signal
    });
    return String(resp);
  }
  /**
   * Returns information about a given checkpoint
   */
  async getCheckpoint(input) {
    return await this.transport.request({
      method: "sui_getCheckpoint",
      params: [input.id],
      signal: input.signal
    });
  }
  /**
   * Returns historical checkpoints paginated
   */
  async getCheckpoints(input) {
    return await this.transport.request({
      method: "sui_getCheckpoints",
      params: [input.cursor, input?.limit, input.descendingOrder],
      signal: input.signal
    });
  }
  /**
   * Return the committee information for the asked epoch
   */
  async getCommitteeInfo(input) {
    return await this.transport.request({
      method: "suix_getCommitteeInfo",
      params: [input?.epoch],
      signal: input?.signal
    });
  }
  async getNetworkMetrics({ signal } = {}) {
    return await this.transport.request({
      method: "suix_getNetworkMetrics",
      params: [],
      signal
    });
  }
  async getAddressMetrics({ signal } = {}) {
    return await this.transport.request({
      method: "suix_getLatestAddressMetrics",
      params: [],
      signal
    });
  }
  async getEpochMetrics(input) {
    return await this.transport.request({
      method: "suix_getEpochMetrics",
      params: [input?.cursor, input?.limit, input?.descendingOrder],
      signal: input?.signal
    });
  }
  async getAllEpochAddressMetrics(input) {
    return await this.transport.request({
      method: "suix_getAllEpochAddressMetrics",
      params: [input?.descendingOrder],
      signal: input?.signal
    });
  }
  /**
   * Return the committee information for the asked epoch
   */
  async getEpochs(input) {
    return await this.transport.request({
      method: "suix_getEpochs",
      params: [input?.cursor, input?.limit, input?.descendingOrder],
      signal: input?.signal
    });
  }
  /**
   * Returns list of top move calls by usage
   */
  async getMoveCallMetrics({ signal } = {}) {
    return await this.transport.request({
      method: "suix_getMoveCallMetrics",
      params: [],
      signal
    });
  }
  /**
   * Return the committee information for the asked epoch
   */
  async getCurrentEpoch({ signal } = {}) {
    return await this.transport.request({
      method: "suix_getCurrentEpoch",
      params: [],
      signal
    });
  }
  /**
   * Return the Validators APYs
   */
  async getValidatorsApy({ signal } = {}) {
    return await this.transport.request({
      method: "suix_getValidatorsApy",
      params: [],
      signal
    });
  }
  // TODO: Migrate this to `sui_getChainIdentifier` once it is widely available.
  async getChainIdentifier({ signal } = {}) {
    const checkpoint = await this.getCheckpoint({ id: "0", signal });
    const bytes = fromBase58(checkpoint.digest);
    return toHex(bytes.slice(0, 4));
  }
  async resolveNameServiceAddress(input) {
    return await this.transport.request({
      method: "suix_resolveNameServiceAddress",
      params: [input.name],
      signal: input.signal
    });
  }
  async resolveNameServiceNames({
    format = "dot",
    ...input
  }) {
    const { nextCursor, hasNextPage, data } = await this.transport.request({
      method: "suix_resolveNameServiceNames",
      params: [input.address, input.cursor, input.limit],
      signal: input.signal
    });
    return {
      hasNextPage,
      nextCursor,
      data: data.map((name) => normalizeSuiNSName(name, format))
    };
  }
  async getProtocolConfig(input) {
    return await this.transport.request({
      method: "sui_getProtocolConfig",
      params: [input?.version],
      signal: input?.signal
    });
  }
  async verifyZkLoginSignature(input) {
    return await this.transport.request({
      method: "sui_verifyZkLoginSignature",
      params: [input.bytes, input.signature, input.intentScope, input.author],
      signal: input.signal
    });
  }
  /**
   * Wait for a transaction block result to be available over the API.
   * This can be used in conjunction with `executeTransactionBlock` to wait for the transaction to
   * be available via the API.
   * This currently polls the `getTransactionBlock` API to check for the transaction.
   */
  async waitForTransaction({
    signal,
    timeout = 60 * 1e3,
    pollInterval = 2 * 1e3,
    ...input
  }) {
    const timeoutSignal = AbortSignal.timeout(timeout);
    const timeoutPromise = new Promise((_, reject) => {
      timeoutSignal.addEventListener("abort", () => reject(timeoutSignal.reason));
    });
    timeoutPromise.catch(() => {
    });
    while (!timeoutSignal.aborted) {
      signal?.throwIfAborted();
      try {
        return await this.getTransactionBlock(input);
      } catch {
        await Promise.race([
          new Promise((resolve) => setTimeout(resolve, pollInterval)),
          timeoutPromise
        ]);
      }
    }
    timeoutSignal.throwIfAborted();
    throw new Error("Unexpected error while waiting for transaction block.");
  }
};

// node_modules/@noble/curves/esm/abstract/edwards.js
var _0n5 = BigInt(0);
var _1n5 = BigInt(1);
var _2n3 = BigInt(2);
var _8n2 = BigInt(8);
function isEdValidXY(Fp2, CURVE, x, y) {
  const x2 = Fp2.sqr(x);
  const y2 = Fp2.sqr(y);
  const left = Fp2.add(Fp2.mul(CURVE.a, x2), y2);
  const right = Fp2.add(Fp2.ONE, Fp2.mul(CURVE.d, Fp2.mul(x2, y2)));
  return Fp2.eql(left, right);
}
function edwards(CURVE, curveOpts = {}) {
  const { Fp: Fp2, Fn: Fn2 } = _createCurveFields("edwards", CURVE, curveOpts);
  const { h: cofactor, n: CURVE_ORDER } = CURVE;
  _validateObject(curveOpts, {}, { uvRatio: "function" });
  const MASK = _2n3 << BigInt(Fn2.BYTES * 8) - _1n5;
  const modP = (n) => Fp2.create(n);
  const uvRatio2 = curveOpts.uvRatio || ((u, v) => {
    try {
      return { isValid: true, value: Fp2.sqrt(Fp2.div(u, v)) };
    } catch (e) {
      return { isValid: false, value: _0n5 };
    }
  });
  if (!isEdValidXY(Fp2, CURVE, CURVE.Gx, CURVE.Gy))
    throw new Error("bad curve params: generator point");
  function acoord(title, n, banZero = false) {
    const min = banZero ? _1n5 : _0n5;
    aInRange("coordinate " + title, n, min, MASK);
    return n;
  }
  function aextpoint(other) {
    if (!(other instanceof Point2))
      throw new Error("ExtendedPoint expected");
  }
  const toAffineMemo = memoized((p, iz) => {
    const { X, Y, Z } = p;
    const is0 = p.is0();
    if (iz == null)
      iz = is0 ? _8n2 : Fp2.inv(Z);
    const x = modP(X * iz);
    const y = modP(Y * iz);
    const zz = Fp2.mul(Z, iz);
    if (is0)
      return { x: _0n5, y: _1n5 };
    if (zz !== _1n5)
      throw new Error("invZ was invalid");
    return { x, y };
  });
  const assertValidMemo = memoized((p) => {
    const { a, d } = CURVE;
    if (p.is0())
      throw new Error("bad point: ZERO");
    const { X, Y, Z, T } = p;
    const X2 = modP(X * X);
    const Y2 = modP(Y * Y);
    const Z2 = modP(Z * Z);
    const Z4 = modP(Z2 * Z2);
    const aX2 = modP(X2 * a);
    const left = modP(Z2 * modP(aX2 + Y2));
    const right = modP(Z4 + modP(d * modP(X2 * Y2)));
    if (left !== right)
      throw new Error("bad point: equation left != right (1)");
    const XY = modP(X * Y);
    const ZT = modP(Z * T);
    if (XY !== ZT)
      throw new Error("bad point: equation left != right (2)");
    return true;
  });
  class Point2 {
    constructor(X, Y, Z, T) {
      this.X = acoord("x", X);
      this.Y = acoord("y", Y);
      this.Z = acoord("z", Z, true);
      this.T = acoord("t", T);
      Object.freeze(this);
    }
    get x() {
      return this.toAffine().x;
    }
    get y() {
      return this.toAffine().y;
    }
    // TODO: remove
    get ex() {
      return this.X;
    }
    get ey() {
      return this.Y;
    }
    get ez() {
      return this.Z;
    }
    get et() {
      return this.T;
    }
    static normalizeZ(points) {
      return normalizeZ(Point2, points);
    }
    static msm(points, scalars) {
      return pippenger(Point2, Fn2, points, scalars);
    }
    _setWindowSize(windowSize) {
      this.precompute(windowSize);
    }
    static fromAffine(p) {
      if (p instanceof Point2)
        throw new Error("extended point not allowed");
      const { x, y } = p || {};
      acoord("x", x);
      acoord("y", y);
      return new Point2(x, y, _1n5, modP(x * y));
    }
    precompute(windowSize = 8, isLazy = true) {
      wnaf.createCache(this, windowSize);
      if (!isLazy)
        this.multiply(_2n3);
      return this;
    }
    // Useful in fromAffine() - not for fromBytes(), which always created valid points.
    assertValidity() {
      assertValidMemo(this);
    }
    // Compare one point to another.
    equals(other) {
      aextpoint(other);
      const { X: X1, Y: Y1, Z: Z1 } = this;
      const { X: X2, Y: Y2, Z: Z2 } = other;
      const X1Z2 = modP(X1 * Z2);
      const X2Z1 = modP(X2 * Z1);
      const Y1Z2 = modP(Y1 * Z2);
      const Y2Z1 = modP(Y2 * Z1);
      return X1Z2 === X2Z1 && Y1Z2 === Y2Z1;
    }
    is0() {
      return this.equals(Point2.ZERO);
    }
    negate() {
      return new Point2(modP(-this.X), this.Y, this.Z, modP(-this.T));
    }
    // Fast algo for doubling Extended Point.
    // https://hyperelliptic.org/EFD/g1p/auto-twisted-extended.html#doubling-dbl-2008-hwcd
    // Cost: 4M + 4S + 1*a + 6add + 1*2.
    double() {
      const { a } = CURVE;
      const { X: X1, Y: Y1, Z: Z1 } = this;
      const A = modP(X1 * X1);
      const B = modP(Y1 * Y1);
      const C = modP(_2n3 * modP(Z1 * Z1));
      const D = modP(a * A);
      const x1y1 = X1 + Y1;
      const E = modP(modP(x1y1 * x1y1) - A - B);
      const G = D + B;
      const F = G - C;
      const H = D - B;
      const X3 = modP(E * F);
      const Y3 = modP(G * H);
      const T3 = modP(E * H);
      const Z3 = modP(F * G);
      return new Point2(X3, Y3, Z3, T3);
    }
    // Fast algo for adding 2 Extended Points.
    // https://hyperelliptic.org/EFD/g1p/auto-twisted-extended.html#addition-add-2008-hwcd
    // Cost: 9M + 1*a + 1*d + 7add.
    add(other) {
      aextpoint(other);
      const { a, d } = CURVE;
      const { X: X1, Y: Y1, Z: Z1, T: T1 } = this;
      const { X: X2, Y: Y2, Z: Z2, T: T2 } = other;
      const A = modP(X1 * X2);
      const B = modP(Y1 * Y2);
      const C = modP(T1 * d * T2);
      const D = modP(Z1 * Z2);
      const E = modP((X1 + Y1) * (X2 + Y2) - A - B);
      const F = D - C;
      const G = D + C;
      const H = modP(B - a * A);
      const X3 = modP(E * F);
      const Y3 = modP(G * H);
      const T3 = modP(E * H);
      const Z3 = modP(F * G);
      return new Point2(X3, Y3, Z3, T3);
    }
    subtract(other) {
      return this.add(other.negate());
    }
    // Constant-time multiplication.
    multiply(scalar) {
      const n = scalar;
      aInRange("scalar", n, _1n5, CURVE_ORDER);
      const { p, f } = wnaf.cached(this, n, (p2) => normalizeZ(Point2, p2));
      return normalizeZ(Point2, [p, f])[0];
    }
    // Non-constant-time multiplication. Uses double-and-add algorithm.
    // It's faster, but should only be used when you don't care about
    // an exposed private key e.g. sig verification.
    // Does NOT allow scalars higher than CURVE.n.
    // Accepts optional accumulator to merge with multiply (important for sparse scalars)
    multiplyUnsafe(scalar, acc = Point2.ZERO) {
      const n = scalar;
      aInRange("scalar", n, _0n5, CURVE_ORDER);
      if (n === _0n5)
        return Point2.ZERO;
      if (this.is0() || n === _1n5)
        return this;
      return wnaf.unsafe(this, n, (p) => normalizeZ(Point2, p), acc);
    }
    // Checks if point is of small order.
    // If you add something to small order point, you will have "dirty"
    // point with torsion component.
    // Multiplies point by cofactor and checks if the result is 0.
    isSmallOrder() {
      return this.multiplyUnsafe(cofactor).is0();
    }
    // Multiplies point by curve order and checks if the result is 0.
    // Returns `false` is the point is dirty.
    isTorsionFree() {
      return wnaf.unsafe(this, CURVE_ORDER).is0();
    }
    // Converts Extended point to default (x, y) coordinates.
    // Can accept precomputed Z^-1 - for example, from invertBatch.
    toAffine(invertedZ) {
      return toAffineMemo(this, invertedZ);
    }
    clearCofactor() {
      if (cofactor === _1n5)
        return this;
      return this.multiplyUnsafe(cofactor);
    }
    static fromBytes(bytes, zip215 = false) {
      abytes(bytes);
      return Point2.fromHex(bytes, zip215);
    }
    // Converts hash string or Uint8Array to Point.
    // Uses algo from RFC8032 5.1.3.
    static fromHex(hex, zip215 = false) {
      const { d, a } = CURVE;
      const len = Fp2.BYTES;
      hex = ensureBytes("pointHex", hex, len);
      abool("zip215", zip215);
      const normed = hex.slice();
      const lastByte = hex[len - 1];
      normed[len - 1] = lastByte & ~128;
      const y = bytesToNumberLE(normed);
      const max = zip215 ? MASK : Fp2.ORDER;
      aInRange("pointHex.y", y, _0n5, max);
      const y2 = modP(y * y);
      const u = modP(y2 - _1n5);
      const v = modP(d * y2 - a);
      let { isValid, value: x } = uvRatio2(u, v);
      if (!isValid)
        throw new Error("Point.fromHex: invalid y coordinate");
      const isXOdd = (x & _1n5) === _1n5;
      const isLastByteOdd = (lastByte & 128) !== 0;
      if (!zip215 && x === _0n5 && isLastByteOdd)
        throw new Error("Point.fromHex: x=0 and x_0=1");
      if (isLastByteOdd !== isXOdd)
        x = modP(-x);
      return Point2.fromAffine({ x, y });
    }
    toBytes() {
      const { x, y } = this.toAffine();
      const bytes = numberToBytesLE(y, Fp2.BYTES);
      bytes[bytes.length - 1] |= x & _1n5 ? 128 : 0;
      return bytes;
    }
    /** @deprecated use `toBytes` */
    toRawBytes() {
      return this.toBytes();
    }
    toHex() {
      return bytesToHex(this.toBytes());
    }
    toString() {
      return `<Point ${this.is0() ? "ZERO" : this.toHex()}>`;
    }
  }
  Point2.BASE = new Point2(CURVE.Gx, CURVE.Gy, _1n5, modP(CURVE.Gx * CURVE.Gy));
  Point2.ZERO = new Point2(_0n5, _1n5, _1n5, _0n5);
  Point2.Fp = Fp2;
  Point2.Fn = Fn2;
  const wnaf = new wNAF(Point2, Fn2.BYTES * 8);
  return Point2;
}
var PrimeEdwardsPoint = class {
  constructor(ep) {
    this.ep = ep;
  }
  // Static methods that must be implemented by subclasses
  static fromBytes(_bytes2) {
    throw new Error("fromBytes must be implemented by subclass");
  }
  static fromHex(_hex) {
    throw new Error("fromHex must be implemented by subclass");
  }
  get x() {
    return this.toAffine().x;
  }
  get y() {
    return this.toAffine().y;
  }
  // Common implementations
  clearCofactor() {
    return this;
  }
  assertValidity() {
    this.ep.assertValidity();
  }
  toAffine(invertedZ) {
    return this.ep.toAffine(invertedZ);
  }
  /** @deprecated use `toBytes` */
  toRawBytes() {
    return this.toBytes();
  }
  toHex() {
    return bytesToHex(this.toBytes());
  }
  toString() {
    return this.toHex();
  }
  isTorsionFree() {
    return true;
  }
  isSmallOrder() {
    return false;
  }
  add(other) {
    this.assertSame(other);
    return this.init(this.ep.add(other.ep));
  }
  subtract(other) {
    this.assertSame(other);
    return this.init(this.ep.subtract(other.ep));
  }
  multiply(scalar) {
    return this.init(this.ep.multiply(scalar));
  }
  multiplyUnsafe(scalar) {
    return this.init(this.ep.multiplyUnsafe(scalar));
  }
  double() {
    return this.init(this.ep.double());
  }
  negate() {
    return this.init(this.ep.negate());
  }
  precompute(windowSize, isLazy) {
    return this.init(this.ep.precompute(windowSize, isLazy));
  }
};
function eddsa(Point2, cHash, eddsaOpts) {
  if (typeof cHash !== "function")
    throw new Error('"hash" function param is required');
  _validateObject(eddsaOpts, {}, {
    adjustScalarBytes: "function",
    randomBytes: "function",
    domain: "function",
    prehash: "function",
    mapToCurve: "function"
  });
  const { prehash } = eddsaOpts;
  const { BASE: G, Fp: Fp2, Fn: Fn2 } = Point2;
  const CURVE_ORDER = Fn2.ORDER;
  const randomBytes_ = eddsaOpts.randomBytes || randomBytes;
  const adjustScalarBytes2 = eddsaOpts.adjustScalarBytes || ((bytes) => bytes);
  const domain = eddsaOpts.domain || ((data, ctx, phflag) => {
    abool("phflag", phflag);
    if (ctx.length || phflag)
      throw new Error("Contexts/pre-hash are not supported");
    return data;
  });
  function modN(a) {
    return Fn2.create(a);
  }
  function modN_LE(hash) {
    return modN(bytesToNumberLE(hash));
  }
  function getPrivateScalar(key) {
    const len = Fp2.BYTES;
    key = ensureBytes("private key", key, len);
    const hashed = ensureBytes("hashed private key", cHash(key), 2 * len);
    const head = adjustScalarBytes2(hashed.slice(0, len));
    const prefix = hashed.slice(len, 2 * len);
    const scalar = modN_LE(head);
    return { head, prefix, scalar };
  }
  function getExtendedPublicKey(secretKey) {
    const { head, prefix, scalar } = getPrivateScalar(secretKey);
    const point = G.multiply(scalar);
    const pointBytes = point.toBytes();
    return { head, prefix, scalar, point, pointBytes };
  }
  function getPublicKey(secretKey) {
    return getExtendedPublicKey(secretKey).pointBytes;
  }
  function hashDomainToScalar(context = Uint8Array.of(), ...msgs) {
    const msg = concatBytes(...msgs);
    return modN_LE(cHash(domain(msg, ensureBytes("context", context), !!prehash)));
  }
  function sign(msg, secretKey, options = {}) {
    msg = ensureBytes("message", msg);
    if (prehash)
      msg = prehash(msg);
    const { prefix, scalar, pointBytes } = getExtendedPublicKey(secretKey);
    const r = hashDomainToScalar(options.context, prefix, msg);
    const R = G.multiply(r).toBytes();
    const k = hashDomainToScalar(options.context, R, pointBytes, msg);
    const s = modN(r + k * scalar);
    aInRange("signature.s", s, _0n5, CURVE_ORDER);
    const L = Fp2.BYTES;
    const res = concatBytes(R, numberToBytesLE(s, L));
    return ensureBytes("result", res, L * 2);
  }
  const verifyOpts = { zip215: true };
  function verify(sig, msg, publicKey, options = verifyOpts) {
    const { context, zip215 } = options;
    const len = Fp2.BYTES;
    sig = ensureBytes("signature", sig, 2 * len);
    msg = ensureBytes("message", msg);
    publicKey = ensureBytes("publicKey", publicKey, len);
    if (zip215 !== void 0)
      abool("zip215", zip215);
    if (prehash)
      msg = prehash(msg);
    const s = bytesToNumberLE(sig.slice(len, 2 * len));
    let A, R, SB;
    try {
      A = Point2.fromHex(publicKey, zip215);
      R = Point2.fromHex(sig.slice(0, len), zip215);
      SB = G.multiplyUnsafe(s);
    } catch (error) {
      return false;
    }
    if (!zip215 && A.isSmallOrder())
      return false;
    const k = hashDomainToScalar(context, R.toBytes(), A.toBytes(), msg);
    const RkA = R.add(A.multiplyUnsafe(k));
    return RkA.subtract(SB).clearCofactor().is0();
  }
  G.precompute(8);
  const size = Fp2.BYTES;
  const lengths = {
    secret: size,
    public: size,
    signature: 2 * size,
    seed: size
  };
  function randomSecretKey(seed = randomBytes_(lengths.seed)) {
    return seed;
  }
  const utils = {
    getExtendedPublicKey,
    /** ed25519 priv keys are uniform 32b. No need to check for modulo bias, like in secp256k1. */
    randomSecretKey,
    isValidSecretKey,
    isValidPublicKey,
    randomPrivateKey: randomSecretKey,
    /**
     * Converts ed public key to x public key. Uses formula:
     * - ed25519:
     *   - `(u, v) = ((1+y)/(1-y), sqrt(-486664)*u/x)`
     *   - `(x, y) = (sqrt(-486664)*u/v, (u-1)/(u+1))`
     * - ed448:
     *   - `(u, v) = ((y-1)/(y+1), sqrt(156324)*u/x)`
     *   - `(x, y) = (sqrt(156324)*u/v, (1+u)/(1-u))`
     *
     * There is NO `fromMontgomery`:
     * - There are 2 valid ed25519 points for every x25519, with flipped coordinate
     * - Sometimes there are 0 valid ed25519 points, because x25519 *additionally*
     *   accepts inputs on the quadratic twist, which can't be moved to ed25519
     */
    toMontgomery(publicKey) {
      const { y } = Point2.fromBytes(publicKey);
      const is25519 = size === 32;
      if (!is25519 && size !== 57)
        throw new Error("only defined for 25519 and 448");
      const u = is25519 ? Fp2.div(_1n5 + y, _1n5 - y) : Fp2.div(y - _1n5, y + _1n5);
      return Fp2.toBytes(u);
    },
    toMontgomeryPriv(privateKey) {
      abytes(privateKey, size);
      const hashed = cHash(privateKey.subarray(0, size));
      return adjustScalarBytes2(hashed).subarray(0, size);
    },
    /**
     * We're doing scalar multiplication (used in getPublicKey etc) with precomputed BASE_POINT
     * values. This slows down first getPublicKey() by milliseconds (see Speed section),
     * but allows to speed-up subsequent getPublicKey() calls up to 20x.
     * @param windowSize 2, 4, 8, 16
     */
    precompute(windowSize = 8, point = Point2.BASE) {
      return point.precompute(windowSize, false);
    }
  };
  function keygen(seed) {
    const secretKey = utils.randomSecretKey(seed);
    return { secretKey, publicKey: getPublicKey(secretKey) };
  }
  function isValidSecretKey(key) {
    try {
      return !!Fn2.fromBytes(key, false);
    } catch (error) {
      return false;
    }
  }
  function isValidPublicKey(key, zip215) {
    try {
      return !!Point2.fromBytes(key, zip215);
    } catch (error) {
      return false;
    }
  }
  return Object.freeze({
    keygen,
    getPublicKey,
    sign,
    verify,
    utils,
    Point: Point2,
    info: { type: "edwards", lengths }
  });
}
function _eddsa_legacy_opts_to_new(c) {
  const CURVE = {
    a: c.a,
    d: c.d,
    p: c.Fp.ORDER,
    n: c.n,
    h: c.h,
    Gx: c.Gx,
    Gy: c.Gy
  };
  const Fp2 = c.Fp;
  const Fn2 = Field(CURVE.n, c.nBitLength, true);
  const curveOpts = { Fp: Fp2, Fn: Fn2, uvRatio: c.uvRatio };
  const eddsaOpts = {
    randomBytes: c.randomBytes,
    adjustScalarBytes: c.adjustScalarBytes,
    domain: c.domain,
    prehash: c.prehash,
    mapToCurve: c.mapToCurve
  };
  return { CURVE, curveOpts, hash: c.hash, eddsaOpts };
}
function _eddsa_new_output_to_legacy(c, eddsa2) {
  const legacy = Object.assign({}, eddsa2, { ExtendedPoint: eddsa2.Point, CURVE: c });
  return legacy;
}
function twistedEdwards(c) {
  const { CURVE, curveOpts, hash, eddsaOpts } = _eddsa_legacy_opts_to_new(c);
  const Point2 = edwards(CURVE, curveOpts);
  const EDDSA = eddsa(Point2, hash, eddsaOpts);
  return _eddsa_new_output_to_legacy(c, EDDSA);
}

// node_modules/@noble/curves/esm/ed25519.js
var _0n6 = BigInt(0);
var _1n6 = BigInt(1);
var _2n4 = BigInt(2);
var _3n3 = BigInt(3);
var _5n2 = BigInt(5);
var _8n3 = BigInt(8);
var ed25519_CURVE = {
  p: BigInt("0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffed"),
  n: BigInt("0x1000000000000000000000000000000014def9dea2f79cd65812631a5cf5d3ed"),
  h: _8n3,
  a: BigInt("0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffec"),
  d: BigInt("0x52036cee2b6ffe738cc740797779e89800700a4d4141d8ab75eb4dca135978a3"),
  Gx: BigInt("0x216936d3cd6e53fec0a4e231fdd6dc5c692cc7609525a7b2c9562d608f25d51a"),
  Gy: BigInt("0x6666666666666666666666666666666666666666666666666666666666666658")
};
function ed25519_pow_2_252_3(x) {
  const _10n = BigInt(10), _20n = BigInt(20), _40n = BigInt(40), _80n = BigInt(80);
  const P = ed25519_CURVE.p;
  const x2 = x * x % P;
  const b2 = x2 * x % P;
  const b4 = pow2(b2, _2n4, P) * b2 % P;
  const b5 = pow2(b4, _1n6, P) * x % P;
  const b10 = pow2(b5, _5n2, P) * b5 % P;
  const b20 = pow2(b10, _10n, P) * b10 % P;
  const b40 = pow2(b20, _20n, P) * b20 % P;
  const b80 = pow2(b40, _40n, P) * b40 % P;
  const b160 = pow2(b80, _80n, P) * b80 % P;
  const b240 = pow2(b160, _80n, P) * b80 % P;
  const b250 = pow2(b240, _10n, P) * b10 % P;
  const pow_p_5_8 = pow2(b250, _2n4, P) * x % P;
  return { pow_p_5_8, b2 };
}
function adjustScalarBytes(bytes) {
  bytes[0] &= 248;
  bytes[31] &= 127;
  bytes[31] |= 64;
  return bytes;
}
var ED25519_SQRT_M1 = /* @__PURE__ */ BigInt("19681161376707505956807079304988542015446066515923890162744021073123829784752");
function uvRatio(u, v) {
  const P = ed25519_CURVE.p;
  const v3 = mod(v * v * v, P);
  const v7 = mod(v3 * v3 * v, P);
  const pow = ed25519_pow_2_252_3(u * v7).pow_p_5_8;
  let x = mod(u * v3 * pow, P);
  const vx2 = mod(v * x * x, P);
  const root1 = x;
  const root2 = mod(x * ED25519_SQRT_M1, P);
  const useRoot1 = vx2 === u;
  const useRoot2 = vx2 === mod(-u, P);
  const noRoot = vx2 === mod(-u * ED25519_SQRT_M1, P);
  if (useRoot1)
    x = root1;
  if (useRoot2 || noRoot)
    x = root2;
  if (isNegativeLE(x, P))
    x = mod(-x, P);
  return { isValid: useRoot1 || useRoot2, value: x };
}
var Fp = /* @__PURE__ */ (() => Field(ed25519_CURVE.p, { isLE: true }))();
var Fn = /* @__PURE__ */ (() => Field(ed25519_CURVE.n, { isLE: true }))();
var ed25519Defaults = /* @__PURE__ */ (() => ({
  ...ed25519_CURVE,
  Fp,
  hash: sha512,
  adjustScalarBytes,
  // dom2
  // Ratio of u to v. Allows us to combine inversion and square root. Uses algo from RFC8032 5.1.3.
  // Constant-time, u/√v
  uvRatio
}))();
var ed25519 = /* @__PURE__ */ (() => twistedEdwards(ed25519Defaults))();
var SQRT_M1 = ED25519_SQRT_M1;
var SQRT_AD_MINUS_ONE = /* @__PURE__ */ BigInt("25063068953384623474111414158702152701244531502492656460079210482610430750235");
var INVSQRT_A_MINUS_D = /* @__PURE__ */ BigInt("54469307008909316920995813868745141605393597292927456921205312896311721017578");
var ONE_MINUS_D_SQ = /* @__PURE__ */ BigInt("1159843021668779879193775521855586647937357759715417654439879720876111806838");
var D_MINUS_ONE_SQ = /* @__PURE__ */ BigInt("40440834346308536858101042469323190826248399146238708352240133220865137265952");
var invertSqrt = (number2) => uvRatio(_1n6, number2);
var MAX_255B = /* @__PURE__ */ BigInt("0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
var bytes255ToNumberLE = (bytes) => ed25519.CURVE.Fp.create(bytesToNumberLE(bytes) & MAX_255B);
function calcElligatorRistrettoMap(r0) {
  const { d } = ed25519.CURVE;
  const P = ed25519.CURVE.Fp.ORDER;
  const mod2 = ed25519.CURVE.Fp.create;
  const r = mod2(SQRT_M1 * r0 * r0);
  const Ns = mod2((r + _1n6) * ONE_MINUS_D_SQ);
  let c = BigInt(-1);
  const D = mod2((c - d * r) * mod2(r + d));
  let { isValid: Ns_D_is_sq, value: s } = uvRatio(Ns, D);
  let s_ = mod2(s * r0);
  if (!isNegativeLE(s_, P))
    s_ = mod2(-s_);
  if (!Ns_D_is_sq)
    s = s_;
  if (!Ns_D_is_sq)
    c = r;
  const Nt = mod2(c * (r - _1n6) * D_MINUS_ONE_SQ - D);
  const s2 = s * s;
  const W0 = mod2((s + s) * D);
  const W1 = mod2(Nt * SQRT_AD_MINUS_ONE);
  const W2 = mod2(_1n6 - s2);
  const W3 = mod2(_1n6 + s2);
  return new ed25519.Point(mod2(W0 * W3), mod2(W2 * W1), mod2(W1 * W3), mod2(W0 * W2));
}
function ristretto255_map(bytes) {
  abytes(bytes, 64);
  const r1 = bytes255ToNumberLE(bytes.subarray(0, 32));
  const R1 = calcElligatorRistrettoMap(r1);
  const r2 = bytes255ToNumberLE(bytes.subarray(32, 64));
  const R2 = calcElligatorRistrettoMap(r2);
  return new _RistrettoPoint(R1.add(R2));
}
var _RistrettoPoint = class __RistrettoPoint extends PrimeEdwardsPoint {
  constructor(ep) {
    super(ep);
  }
  static fromAffine(ap) {
    return new __RistrettoPoint(ed25519.Point.fromAffine(ap));
  }
  assertSame(other) {
    if (!(other instanceof __RistrettoPoint))
      throw new Error("RistrettoPoint expected");
  }
  init(ep) {
    return new __RistrettoPoint(ep);
  }
  /** @deprecated use `import { ristretto255_hasher } from '@noble/curves/ed25519.js';` */
  static hashToCurve(hex) {
    return ristretto255_map(ensureBytes("ristrettoHash", hex, 64));
  }
  static fromBytes(bytes) {
    abytes(bytes, 32);
    const { a, d } = ed25519.CURVE;
    const P = Fp.ORDER;
    const mod2 = Fp.create;
    const s = bytes255ToNumberLE(bytes);
    if (!equalBytes(numberToBytesLE(s, 32), bytes) || isNegativeLE(s, P))
      throw new Error("invalid ristretto255 encoding 1");
    const s2 = mod2(s * s);
    const u1 = mod2(_1n6 + a * s2);
    const u2 = mod2(_1n6 - a * s2);
    const u1_2 = mod2(u1 * u1);
    const u2_2 = mod2(u2 * u2);
    const v = mod2(a * d * u1_2 - u2_2);
    const { isValid, value: I } = invertSqrt(mod2(v * u2_2));
    const Dx = mod2(I * u2);
    const Dy = mod2(I * Dx * v);
    let x = mod2((s + s) * Dx);
    if (isNegativeLE(x, P))
      x = mod2(-x);
    const y = mod2(u1 * Dy);
    const t = mod2(x * y);
    if (!isValid || isNegativeLE(t, P) || y === _0n6)
      throw new Error("invalid ristretto255 encoding 2");
    return new __RistrettoPoint(new ed25519.Point(x, y, _1n6, t));
  }
  /**
   * Converts ristretto-encoded string to ristretto point.
   * Described in [RFC9496](https://www.rfc-editor.org/rfc/rfc9496#name-decode).
   * @param hex Ristretto-encoded 32 bytes. Not every 32-byte string is valid ristretto encoding
   */
  static fromHex(hex) {
    return __RistrettoPoint.fromBytes(ensureBytes("ristrettoHex", hex, 32));
  }
  static msm(points, scalars) {
    return pippenger(__RistrettoPoint, ed25519.Point.Fn, points, scalars);
  }
  /**
   * Encodes ristretto point to Uint8Array.
   * Described in [RFC9496](https://www.rfc-editor.org/rfc/rfc9496#name-encode).
   */
  toBytes() {
    let { X, Y, Z, T } = this.ep;
    const P = Fp.ORDER;
    const mod2 = Fp.create;
    const u1 = mod2(mod2(Z + Y) * mod2(Z - Y));
    const u2 = mod2(X * Y);
    const u2sq = mod2(u2 * u2);
    const { value: invsqrt } = invertSqrt(mod2(u1 * u2sq));
    const D1 = mod2(invsqrt * u1);
    const D2 = mod2(invsqrt * u2);
    const zInv = mod2(D1 * D2 * T);
    let D;
    if (isNegativeLE(T * zInv, P)) {
      let _x = mod2(Y * SQRT_M1);
      let _y = mod2(X * SQRT_M1);
      X = _x;
      Y = _y;
      D = mod2(D1 * INVSQRT_A_MINUS_D);
    } else {
      D = D2;
    }
    if (isNegativeLE(X * zInv, P))
      Y = mod2(-Y);
    let s = mod2((Z - Y) * D);
    if (isNegativeLE(s, P))
      s = mod2(-s);
    return numberToBytesLE(s, 32);
  }
  /**
   * Compares two Ristretto points.
   * Described in [RFC9496](https://www.rfc-editor.org/rfc/rfc9496#name-equals).
   */
  equals(other) {
    this.assertSame(other);
    const { X: X1, Y: Y1 } = this.ep;
    const { X: X2, Y: Y2 } = other.ep;
    const mod2 = Fp.create;
    const one = mod2(X1 * Y2) === mod2(Y1 * X2);
    const two = mod2(Y1 * Y2) === mod2(X1 * X2);
    return one || two;
  }
  is0() {
    return this.equals(__RistrettoPoint.ZERO);
  }
};
_RistrettoPoint.BASE = /* @__PURE__ */ (() => new _RistrettoPoint(ed25519.Point.BASE))();
_RistrettoPoint.ZERO = /* @__PURE__ */ (() => new _RistrettoPoint(ed25519.Point.ZERO))();
_RistrettoPoint.Fp = Fp;
_RistrettoPoint.Fn = Fn;

// node_modules/@noble/hashes/esm/sha512.js
var sha5122 = sha512;

// node_modules/@mysten/sui/dist/esm/keypairs/ed25519/ed25519-hd-key.js
var ED25519_CURVE = "ed25519 seed";
var HARDENED_OFFSET = 2147483648;
var pathRegex = new RegExp("^m(\\/[0-9]+')+$");
var replaceDerive = (val) => val.replace("'", "");
var getMasterKeyFromSeed = (seed) => {
  const h = hmac.create(sha5122, ED25519_CURVE);
  const I = h.update(fromHex(seed)).digest();
  const IL = I.slice(0, 32);
  const IR = I.slice(32);
  return {
    key: IL,
    chainCode: IR
  };
};
var CKDPriv = ({ key, chainCode }, index) => {
  const indexBuffer = new ArrayBuffer(4);
  const cv = new DataView(indexBuffer);
  cv.setUint32(0, index);
  const data = new Uint8Array(1 + key.length + indexBuffer.byteLength);
  data.set(new Uint8Array(1).fill(0));
  data.set(key, 1);
  data.set(new Uint8Array(indexBuffer, 0, indexBuffer.byteLength), key.length + 1);
  const I = hmac.create(sha5122, chainCode).update(data).digest();
  const IL = I.slice(0, 32);
  const IR = I.slice(32);
  return {
    key: IL,
    chainCode: IR
  };
};
var isValidPath = (path) => {
  if (!pathRegex.test(path)) {
    return false;
  }
  return !path.split("/").slice(1).map(replaceDerive).some(
    isNaN
    /* ts T_T*/
  );
};
var derivePath = (path, seed, offset = HARDENED_OFFSET) => {
  if (!isValidPath(path)) {
    throw new Error("Invalid derivation path");
  }
  const { key, chainCode } = getMasterKeyFromSeed(seed);
  const segments = path.split("/").slice(1).map(replaceDerive).map((el) => parseInt(el, 10));
  return segments.reduce((parentKeys, segment) => CKDPriv(parentKeys, segment + offset), {
    key,
    chainCode
  });
};

// node_modules/@mysten/sui/dist/esm/keypairs/ed25519/publickey.js
var PUBLIC_KEY_SIZE = 32;
var Ed25519PublicKey = class extends PublicKey2 {
  /**
   * Create a new Ed25519PublicKey object
   * @param value ed25519 public key as buffer or base-64 encoded string
   */
  constructor(value) {
    super();
    if (typeof value === "string") {
      this.data = fromBase64(value);
    } else if (value instanceof Uint8Array) {
      this.data = value;
    } else {
      this.data = Uint8Array.from(value);
    }
    if (this.data.length !== PUBLIC_KEY_SIZE) {
      throw new Error(
        `Invalid public key input. Expected ${PUBLIC_KEY_SIZE} bytes, got ${this.data.length}`
      );
    }
  }
  /**
   * Checks if two Ed25519 public keys are equal
   */
  equals(publicKey) {
    return super.equals(publicKey);
  }
  /**
   * Return the byte array representation of the Ed25519 public key
   */
  toRawBytes() {
    return this.data;
  }
  /**
   * Return the Sui address associated with this Ed25519 public key
   */
  flag() {
    return SIGNATURE_SCHEME_TO_FLAG["ED25519"];
  }
  /**
   * Verifies that the signature is valid for for the provided message
   */
  async verify(message, signature) {
    let bytes;
    if (typeof signature === "string") {
      const parsed = parseSerializedKeypairSignature(signature);
      if (parsed.signatureScheme !== "ED25519") {
        throw new Error("Invalid signature scheme");
      }
      if (!bytesEqual(this.toRawBytes(), parsed.publicKey)) {
        throw new Error("Signature does not match public key");
      }
      bytes = parsed.signature;
    } else {
      bytes = signature;
    }
    return ed25519.verify(bytes, message, this.toRawBytes());
  }
};
Ed25519PublicKey.SIZE = PUBLIC_KEY_SIZE;

// node_modules/@mysten/sui/dist/esm/keypairs/ed25519/keypair.js
var DEFAULT_ED25519_DERIVATION_PATH = "m/44'/784'/0'/0'/0'";
var Ed25519Keypair = class _Ed25519Keypair extends Keypair {
  /**
   * Create a new Ed25519 keypair instance.
   * Generate random keypair if no {@link Ed25519Keypair} is provided.
   *
   * @param keypair Ed25519 keypair
   */
  constructor(keypair) {
    super();
    if (keypair) {
      this.keypair = {
        publicKey: keypair.publicKey,
        secretKey: keypair.secretKey.slice(0, 32)
      };
    } else {
      const privateKey = ed25519.utils.randomPrivateKey();
      this.keypair = {
        publicKey: ed25519.getPublicKey(privateKey),
        secretKey: privateKey
      };
    }
  }
  /**
   * Get the key scheme of the keypair ED25519
   */
  getKeyScheme() {
    return "ED25519";
  }
  /**
   * Generate a new random Ed25519 keypair
   */
  static generate() {
    const secretKey = ed25519.utils.randomPrivateKey();
    return new _Ed25519Keypair({
      publicKey: ed25519.getPublicKey(secretKey),
      secretKey
    });
  }
  /**
   * Create a Ed25519 keypair from a raw secret key byte array, also known as seed.
   * This is NOT the private scalar which is result of hashing and bit clamping of
   * the raw secret key.
   *
   * @throws error if the provided secret key is invalid and validation is not skipped.
   *
   * @param secretKey secret key as a byte array or Bech32 secret key string
   * @param options: skip secret key validation
   */
  static fromSecretKey(secretKey, options) {
    if (typeof secretKey === "string") {
      const decoded = decodeSuiPrivateKey(secretKey);
      if (decoded.schema !== "ED25519") {
        throw new Error(`Expected a ED25519 keypair, got ${decoded.schema}`);
      }
      return this.fromSecretKey(decoded.secretKey, options);
    }
    const secretKeyLength = secretKey.length;
    if (secretKeyLength !== PRIVATE_KEY_SIZE) {
      throw new Error(
        `Wrong secretKey size. Expected ${PRIVATE_KEY_SIZE} bytes, got ${secretKeyLength}.`
      );
    }
    const keypair = {
      publicKey: ed25519.getPublicKey(secretKey),
      secretKey
    };
    if (!options || !options.skipValidation) {
      const encoder = new TextEncoder();
      const signData = encoder.encode("sui validation");
      const signature = ed25519.sign(signData, secretKey);
      if (!ed25519.verify(signature, signData, keypair.publicKey)) {
        throw new Error("provided secretKey is invalid");
      }
    }
    return new _Ed25519Keypair(keypair);
  }
  /**
   * The public key for this Ed25519 keypair
   */
  getPublicKey() {
    return new Ed25519PublicKey(this.keypair.publicKey);
  }
  /**
   * The Bech32 secret key string for this Ed25519 keypair
   */
  getSecretKey() {
    return encodeSuiPrivateKey(
      this.keypair.secretKey.slice(0, PRIVATE_KEY_SIZE),
      this.getKeyScheme()
    );
  }
  /**
   * Return the signature for the provided data using Ed25519.
   */
  async sign(data) {
    return ed25519.sign(data, this.keypair.secretKey);
  }
  /**
   * Derive Ed25519 keypair from mnemonics and path. The mnemonics must be normalized
   * and validated against the english wordlist.
   *
   * If path is none, it will default to m/44'/784'/0'/0'/0', otherwise the path must
   * be compliant to SLIP-0010 in form m/44'/784'/{account_index}'/{change_index}'/{address_index}'.
   */
  static deriveKeypair(mnemonics, path) {
    if (path == null) {
      path = DEFAULT_ED25519_DERIVATION_PATH;
    }
    if (!isValidHardenedPath(path)) {
      throw new Error("Invalid derivation path");
    }
    const { key } = derivePath(path, mnemonicToSeedHex(mnemonics));
    return _Ed25519Keypair.fromSecretKey(key);
  }
  /**
   * Derive Ed25519 keypair from mnemonicSeed and path.
   *
   * If path is none, it will default to m/44'/784'/0'/0'/0', otherwise the path must
   * be compliant to SLIP-0010 in form m/44'/784'/{account_index}'/{change_index}'/{address_index}'.
   */
  static deriveKeypairFromSeed(seedHex, path) {
    if (path == null) {
      path = DEFAULT_ED25519_DERIVATION_PATH;
    }
    if (!isValidHardenedPath(path)) {
      throw new Error("Invalid derivation path");
    }
    const { key } = derivePath(path, seedHex);
    return _Ed25519Keypair.fromSecretKey(key);
  }
};

// node_modules/@noble/curves/esm/secp256k1.js
var secp256k1_CURVE = {
  p: BigInt("0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f"),
  n: BigInt("0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141"),
  h: BigInt(1),
  a: BigInt(0),
  b: BigInt(7),
  Gx: BigInt("0x79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"),
  Gy: BigInt("0x483ada7726a3c4655da4fbfc0e1108a8fd17b448a68554199c47d08ffb10d4b8")
};
var secp256k1_ENDO = {
  beta: BigInt("0x7ae96a2b657c07106e64479eac3434e99cf0497512f58995c1396c28719501ee"),
  basises: [
    [BigInt("0x3086d221a7d46bcde86c90e49284eb15"), -BigInt("0xe4437ed6010e88286f547fa90abfe4c3")],
    [BigInt("0x114ca50f7a8e2f3f657c1108d9d44cfd8"), BigInt("0x3086d221a7d46bcde86c90e49284eb15")]
  ]
};
var _2n5 = /* @__PURE__ */ BigInt(2);
function sqrtMod(y) {
  const P = secp256k1_CURVE.p;
  const _3n4 = BigInt(3), _6n = BigInt(6), _11n = BigInt(11), _22n = BigInt(22);
  const _23n = BigInt(23), _44n = BigInt(44), _88n = BigInt(88);
  const b2 = y * y * y % P;
  const b3 = b2 * b2 * y % P;
  const b6 = pow2(b3, _3n4, P) * b3 % P;
  const b9 = pow2(b6, _3n4, P) * b3 % P;
  const b11 = pow2(b9, _2n5, P) * b2 % P;
  const b22 = pow2(b11, _11n, P) * b11 % P;
  const b44 = pow2(b22, _22n, P) * b22 % P;
  const b88 = pow2(b44, _44n, P) * b44 % P;
  const b176 = pow2(b88, _88n, P) * b88 % P;
  const b220 = pow2(b176, _44n, P) * b44 % P;
  const b223 = pow2(b220, _3n4, P) * b3 % P;
  const t1 = pow2(b223, _23n, P) * b22 % P;
  const t2 = pow2(t1, _6n, P) * b2 % P;
  const root = pow2(t2, _2n5, P);
  if (!Fpk1.eql(Fpk1.sqr(root), y))
    throw new Error("Cannot find square root");
  return root;
}
var Fpk1 = Field(secp256k1_CURVE.p, void 0, void 0, { sqrt: sqrtMod });
var secp256k1 = createCurve({ ...secp256k1_CURVE, Fp: Fpk1, lowS: true, endo: secp256k1_ENDO }, sha256);

// node_modules/@noble/hashes/esm/legacy.js
var Rho160 = /* @__PURE__ */ Uint8Array.from([
  7,
  4,
  13,
  1,
  10,
  6,
  15,
  3,
  12,
  0,
  9,
  5,
  2,
  14,
  11,
  8
]);
var Id160 = /* @__PURE__ */ (() => Uint8Array.from(new Array(16).fill(0).map((_, i) => i)))();
var Pi160 = /* @__PURE__ */ (() => Id160.map((i) => (9 * i + 5) % 16))();
var idxLR = /* @__PURE__ */ (() => {
  const L = [Id160];
  const R = [Pi160];
  const res = [L, R];
  for (let i = 0; i < 4; i++)
    for (let j of res)
      j.push(j[i].map((k) => Rho160[k]));
  return res;
})();
var idxL = /* @__PURE__ */ (() => idxLR[0])();
var idxR = /* @__PURE__ */ (() => idxLR[1])();
var shifts160 = /* @__PURE__ */ [
  [11, 14, 15, 12, 5, 8, 7, 9, 11, 13, 14, 15, 6, 7, 9, 8],
  [12, 13, 11, 15, 6, 9, 9, 7, 12, 15, 11, 13, 7, 8, 7, 7],
  [13, 15, 14, 11, 7, 7, 6, 8, 13, 14, 13, 12, 5, 5, 6, 9],
  [14, 11, 12, 14, 8, 6, 5, 5, 15, 12, 15, 14, 9, 9, 8, 6],
  [15, 12, 13, 13, 9, 5, 8, 6, 14, 11, 12, 11, 8, 6, 5, 5]
].map((i) => Uint8Array.from(i));
var shiftsL160 = /* @__PURE__ */ idxL.map((idx, i) => idx.map((j) => shifts160[i][j]));
var shiftsR160 = /* @__PURE__ */ idxR.map((idx, i) => idx.map((j) => shifts160[i][j]));
var Kl160 = /* @__PURE__ */ Uint32Array.from([
  0,
  1518500249,
  1859775393,
  2400959708,
  2840853838
]);
var Kr160 = /* @__PURE__ */ Uint32Array.from([
  1352829926,
  1548603684,
  1836072691,
  2053994217,
  0
]);
function ripemd_f(group, x, y, z) {
  if (group === 0)
    return x ^ y ^ z;
  if (group === 1)
    return x & y | ~x & z;
  if (group === 2)
    return (x | ~y) ^ z;
  if (group === 3)
    return x & z | y & ~z;
  return x ^ (y | ~z);
}
var BUF_160 = /* @__PURE__ */ new Uint32Array(16);
var RIPEMD160 = class extends HashMD {
  constructor() {
    super(64, 20, 8, true);
    this.h0 = 1732584193 | 0;
    this.h1 = 4023233417 | 0;
    this.h2 = 2562383102 | 0;
    this.h3 = 271733878 | 0;
    this.h4 = 3285377520 | 0;
  }
  get() {
    const { h0, h1, h2, h3, h4 } = this;
    return [h0, h1, h2, h3, h4];
  }
  set(h0, h1, h2, h3, h4) {
    this.h0 = h0 | 0;
    this.h1 = h1 | 0;
    this.h2 = h2 | 0;
    this.h3 = h3 | 0;
    this.h4 = h4 | 0;
  }
  process(view, offset) {
    for (let i = 0; i < 16; i++, offset += 4)
      BUF_160[i] = view.getUint32(offset, true);
    let al = this.h0 | 0, ar = al, bl = this.h1 | 0, br = bl, cl = this.h2 | 0, cr = cl, dl = this.h3 | 0, dr = dl, el = this.h4 | 0, er = el;
    for (let group = 0; group < 5; group++) {
      const rGroup = 4 - group;
      const hbl = Kl160[group], hbr = Kr160[group];
      const rl = idxL[group], rr = idxR[group];
      const sl = shiftsL160[group], sr = shiftsR160[group];
      for (let i = 0; i < 16; i++) {
        const tl = rotl(al + ripemd_f(group, bl, cl, dl) + BUF_160[rl[i]] + hbl, sl[i]) + el | 0;
        al = el, el = dl, dl = rotl(cl, 10) | 0, cl = bl, bl = tl;
      }
      for (let i = 0; i < 16; i++) {
        const tr = rotl(ar + ripemd_f(rGroup, br, cr, dr) + BUF_160[rr[i]] + hbr, sr[i]) + er | 0;
        ar = er, er = dr, dr = rotl(cr, 10) | 0, cr = br, br = tr;
      }
    }
    this.set(this.h1 + cl + dr | 0, this.h2 + dl + er | 0, this.h3 + el + ar | 0, this.h4 + al + br | 0, this.h0 + bl + cr | 0);
  }
  roundClean() {
    clean(BUF_160);
  }
  destroy() {
    this.destroyed = true;
    clean(this.buffer);
    this.set(0, 0, 0, 0, 0);
  }
};
var ripemd160 = /* @__PURE__ */ createHasher(() => new RIPEMD160());

// node_modules/@scure/bip32/lib/esm/index.js
var Point = secp256k1.ProjectivePoint;
var base58check = createBase58check(sha256);
function bytesToNumber(bytes) {
  abytes(bytes);
  const h = bytes.length === 0 ? "0" : bytesToHex(bytes);
  return BigInt("0x" + h);
}
function numberToBytes(num) {
  if (typeof num !== "bigint")
    throw new Error("bigint expected");
  return hexToBytes(num.toString(16).padStart(64, "0"));
}
var MASTER_SECRET = utf8ToBytes("Bitcoin seed");
var BITCOIN_VERSIONS = { private: 76066276, public: 76067358 };
var HARDENED_OFFSET2 = 2147483648;
var hash160 = (data) => ripemd160(sha256(data));
var fromU32 = (data) => createView(data).getUint32(0, false);
var toU32 = (n) => {
  if (!Number.isSafeInteger(n) || n < 0 || n > 2 ** 32 - 1) {
    throw new Error("invalid number, should be from 0 to 2**32-1, got " + n);
  }
  const buf = new Uint8Array(4);
  createView(buf).setUint32(0, n, false);
  return buf;
};
var HDKey = class _HDKey {
  get fingerprint() {
    if (!this.pubHash) {
      throw new Error("No publicKey set!");
    }
    return fromU32(this.pubHash);
  }
  get identifier() {
    return this.pubHash;
  }
  get pubKeyHash() {
    return this.pubHash;
  }
  get privateKey() {
    return this.privKeyBytes || null;
  }
  get publicKey() {
    return this.pubKey || null;
  }
  get privateExtendedKey() {
    const priv = this.privateKey;
    if (!priv) {
      throw new Error("No private key");
    }
    return base58check.encode(this.serialize(this.versions.private, concatBytes(new Uint8Array([0]), priv)));
  }
  get publicExtendedKey() {
    if (!this.pubKey) {
      throw new Error("No public key");
    }
    return base58check.encode(this.serialize(this.versions.public, this.pubKey));
  }
  static fromMasterSeed(seed, versions = BITCOIN_VERSIONS) {
    abytes(seed);
    if (8 * seed.length < 128 || 8 * seed.length > 512) {
      throw new Error("HDKey: seed length must be between 128 and 512 bits; 256 bits is advised, got " + seed.length);
    }
    const I = hmac(sha512, MASTER_SECRET, seed);
    return new _HDKey({
      versions,
      chainCode: I.slice(32),
      privateKey: I.slice(0, 32)
    });
  }
  static fromExtendedKey(base58key, versions = BITCOIN_VERSIONS) {
    const keyBuffer = base58check.decode(base58key);
    const keyView = createView(keyBuffer);
    const version = keyView.getUint32(0, false);
    const opt = {
      versions,
      depth: keyBuffer[4],
      parentFingerprint: keyView.getUint32(5, false),
      index: keyView.getUint32(9, false),
      chainCode: keyBuffer.slice(13, 45)
    };
    const key = keyBuffer.slice(45);
    const isPriv = key[0] === 0;
    if (version !== versions[isPriv ? "private" : "public"]) {
      throw new Error("Version mismatch");
    }
    if (isPriv) {
      return new _HDKey({ ...opt, privateKey: key.slice(1) });
    } else {
      return new _HDKey({ ...opt, publicKey: key });
    }
  }
  static fromJSON(json) {
    return _HDKey.fromExtendedKey(json.xpriv);
  }
  constructor(opt) {
    this.depth = 0;
    this.index = 0;
    this.chainCode = null;
    this.parentFingerprint = 0;
    if (!opt || typeof opt !== "object") {
      throw new Error("HDKey.constructor must not be called directly");
    }
    this.versions = opt.versions || BITCOIN_VERSIONS;
    this.depth = opt.depth || 0;
    this.chainCode = opt.chainCode || null;
    this.index = opt.index || 0;
    this.parentFingerprint = opt.parentFingerprint || 0;
    if (!this.depth) {
      if (this.parentFingerprint || this.index) {
        throw new Error("HDKey: zero depth with non-zero index/parent fingerprint");
      }
    }
    if (opt.publicKey && opt.privateKey) {
      throw new Error("HDKey: publicKey and privateKey at same time.");
    }
    if (opt.privateKey) {
      if (!secp256k1.utils.isValidPrivateKey(opt.privateKey)) {
        throw new Error("Invalid private key");
      }
      this.privKey = typeof opt.privateKey === "bigint" ? opt.privateKey : bytesToNumber(opt.privateKey);
      this.privKeyBytes = numberToBytes(this.privKey);
      this.pubKey = secp256k1.getPublicKey(opt.privateKey, true);
    } else if (opt.publicKey) {
      this.pubKey = Point.fromHex(opt.publicKey).toRawBytes(true);
    } else {
      throw new Error("HDKey: no public or private key provided");
    }
    this.pubHash = hash160(this.pubKey);
  }
  derive(path) {
    if (!/^[mM]'?/.test(path)) {
      throw new Error('Path must start with "m" or "M"');
    }
    if (/^[mM]'?$/.test(path)) {
      return this;
    }
    const parts = path.replace(/^[mM]'?\//, "").split("/");
    let child = this;
    for (const c of parts) {
      const m = /^(\d+)('?)$/.exec(c);
      const m1 = m && m[1];
      if (!m || m.length !== 3 || typeof m1 !== "string")
        throw new Error("invalid child index: " + c);
      let idx = +m1;
      if (!Number.isSafeInteger(idx) || idx >= HARDENED_OFFSET2) {
        throw new Error("Invalid index");
      }
      if (m[2] === "'") {
        idx += HARDENED_OFFSET2;
      }
      child = child.deriveChild(idx);
    }
    return child;
  }
  deriveChild(index) {
    if (!this.pubKey || !this.chainCode) {
      throw new Error("No publicKey or chainCode set");
    }
    let data = toU32(index);
    if (index >= HARDENED_OFFSET2) {
      const priv = this.privateKey;
      if (!priv) {
        throw new Error("Could not derive hardened child key");
      }
      data = concatBytes(new Uint8Array([0]), priv, data);
    } else {
      data = concatBytes(this.pubKey, data);
    }
    const I = hmac(sha512, this.chainCode, data);
    const childTweak = bytesToNumber(I.slice(0, 32));
    const chainCode = I.slice(32);
    if (!secp256k1.utils.isValidPrivateKey(childTweak)) {
      throw new Error("Tweak bigger than curve order");
    }
    const opt = {
      versions: this.versions,
      chainCode,
      depth: this.depth + 1,
      parentFingerprint: this.fingerprint,
      index
    };
    try {
      if (this.privateKey) {
        const added = mod(this.privKey + childTweak, secp256k1.CURVE.n);
        if (!secp256k1.utils.isValidPrivateKey(added)) {
          throw new Error("The tweak was out of range or the resulted private key is invalid");
        }
        opt.privateKey = added;
      } else {
        const added = Point.fromHex(this.pubKey).add(Point.fromPrivateKey(childTweak));
        if (added.equals(Point.ZERO)) {
          throw new Error("The tweak was equal to negative P, which made the result key invalid");
        }
        opt.publicKey = added.toRawBytes(true);
      }
      return new _HDKey(opt);
    } catch (err) {
      return this.deriveChild(index + 1);
    }
  }
  sign(hash) {
    if (!this.privateKey) {
      throw new Error("No privateKey set!");
    }
    abytes(hash, 32);
    return secp256k1.sign(hash, this.privKey).toCompactRawBytes();
  }
  verify(hash, signature) {
    abytes(hash, 32);
    abytes(signature, 64);
    if (!this.publicKey) {
      throw new Error("No publicKey set!");
    }
    let sig;
    try {
      sig = secp256k1.Signature.fromCompact(signature);
    } catch (error) {
      return false;
    }
    return secp256k1.verify(sig, hash, this.publicKey);
  }
  wipePrivateData() {
    this.privKey = void 0;
    if (this.privKeyBytes) {
      this.privKeyBytes.fill(0);
      this.privKeyBytes = void 0;
    }
    return this;
  }
  toJSON() {
    return {
      xpriv: this.privateExtendedKey,
      xpub: this.publicExtendedKey
    };
  }
  serialize(version, key) {
    if (!this.chainCode) {
      throw new Error("No chainCode set");
    }
    abytes(key, 33);
    return concatBytes(toU32(version), new Uint8Array([this.depth]), toU32(this.parentFingerprint), toU32(this.index), this.chainCode, key);
  }
};

// node_modules/@mysten/sui/dist/esm/keypairs/secp256k1/publickey.js
var SECP256K1_PUBLIC_KEY_SIZE = 33;
var Secp256k1PublicKey = class extends PublicKey2 {
  /**
   * Create a new Secp256k1PublicKey object
   * @param value secp256k1 public key as buffer or base-64 encoded string
   */
  constructor(value) {
    super();
    if (typeof value === "string") {
      this.data = fromBase64(value);
    } else if (value instanceof Uint8Array) {
      this.data = value;
    } else {
      this.data = Uint8Array.from(value);
    }
    if (this.data.length !== SECP256K1_PUBLIC_KEY_SIZE) {
      throw new Error(
        `Invalid public key input. Expected ${SECP256K1_PUBLIC_KEY_SIZE} bytes, got ${this.data.length}`
      );
    }
  }
  /**
   * Checks if two Secp256k1 public keys are equal
   */
  equals(publicKey) {
    return super.equals(publicKey);
  }
  /**
   * Return the byte array representation of the Secp256k1 public key
   */
  toRawBytes() {
    return this.data;
  }
  /**
   * Return the Sui address associated with this Secp256k1 public key
   */
  flag() {
    return SIGNATURE_SCHEME_TO_FLAG["Secp256k1"];
  }
  /**
   * Verifies that the signature is valid for for the provided message
   */
  async verify(message, signature) {
    let bytes;
    if (typeof signature === "string") {
      const parsed = parseSerializedKeypairSignature(signature);
      if (parsed.signatureScheme !== "Secp256k1") {
        throw new Error("Invalid signature scheme");
      }
      if (!bytesEqual(this.toRawBytes(), parsed.publicKey)) {
        throw new Error("Signature does not match public key");
      }
      bytes = parsed.signature;
    } else {
      bytes = signature;
    }
    return secp256k1.verify(
      secp256k1.Signature.fromCompact(bytes),
      sha2562(message),
      this.toRawBytes()
    );
  }
};
Secp256k1PublicKey.SIZE = SECP256K1_PUBLIC_KEY_SIZE;

// node_modules/@mysten/sui/dist/esm/keypairs/secp256k1/keypair.js
var DEFAULT_SECP256K1_DERIVATION_PATH = "m/54'/784'/0'/0/0";
var Secp256k1Keypair = class _Secp256k1Keypair extends Keypair {
  /**
   * Create a new keypair instance.
   * Generate random keypair if no {@link Secp256k1Keypair} is provided.
   *
   * @param keypair secp256k1 keypair
   */
  constructor(keypair) {
    super();
    if (keypair) {
      this.keypair = keypair;
    } else {
      const secretKey = secp256k1.utils.randomPrivateKey();
      const publicKey = secp256k1.getPublicKey(secretKey, true);
      this.keypair = { publicKey, secretKey };
    }
  }
  /**
   * Get the key scheme of the keypair Secp256k1
   */
  getKeyScheme() {
    return "Secp256k1";
  }
  /**
   * Generate a new random keypair
   */
  static generate() {
    return new _Secp256k1Keypair();
  }
  /**
   * Create a keypair from a raw secret key byte array.
   *
   * This method should only be used to recreate a keypair from a previously
   * generated secret key. Generating keypairs from a random seed should be done
   * with the {@link Keypair.fromSeed} method.
   *
   * @throws error if the provided secret key is invalid and validation is not skipped.
   *
   * @param secretKey secret key byte array  or Bech32 secret key string
   * @param options: skip secret key validation
   */
  static fromSecretKey(secretKey, options) {
    if (typeof secretKey === "string") {
      const decoded = decodeSuiPrivateKey(secretKey);
      if (decoded.schema !== "Secp256k1") {
        throw new Error(`Expected a Secp256k1 keypair, got ${decoded.schema}`);
      }
      return this.fromSecretKey(decoded.secretKey, options);
    }
    const publicKey = secp256k1.getPublicKey(secretKey, true);
    if (!options || !options.skipValidation) {
      const encoder = new TextEncoder();
      const signData = encoder.encode("sui validation");
      const msgHash = bytesToHex(blake2b2(signData, { dkLen: 32 }));
      const signature = secp256k1.sign(msgHash, secretKey);
      if (!secp256k1.verify(signature, msgHash, publicKey, { lowS: true })) {
        throw new Error("Provided secretKey is invalid");
      }
    }
    return new _Secp256k1Keypair({ publicKey, secretKey });
  }
  /**
   * Generate a keypair from a 32 byte seed.
   *
   * @param seed seed byte array
   */
  static fromSeed(seed) {
    const publicKey = secp256k1.getPublicKey(seed, true);
    return new _Secp256k1Keypair({ publicKey, secretKey: seed });
  }
  /**
   * The public key for this keypair
   */
  getPublicKey() {
    return new Secp256k1PublicKey(this.keypair.publicKey);
  }
  /**
   * The Bech32 secret key string for this Secp256k1 keypair
   */
  getSecretKey() {
    return encodeSuiPrivateKey(this.keypair.secretKey, this.getKeyScheme());
  }
  /**
   * Return the signature for the provided data.
   */
  async sign(data) {
    const msgHash = sha2562(data);
    const sig = secp256k1.sign(msgHash, this.keypair.secretKey, {
      lowS: true
    });
    return sig.toCompactRawBytes();
  }
  /**
   * Derive Secp256k1 keypair from mnemonics and path. The mnemonics must be normalized
   * and validated against the english wordlist.
   *
   * If path is none, it will default to m/54'/784'/0'/0/0, otherwise the path must
   * be compliant to BIP-32 in form m/54'/784'/{account_index}'/{change_index}/{address_index}.
   */
  static deriveKeypair(mnemonics, path) {
    if (path == null) {
      path = DEFAULT_SECP256K1_DERIVATION_PATH;
    }
    if (!isValidBIP32Path(path)) {
      throw new Error("Invalid derivation path");
    }
    const key = HDKey.fromMasterSeed(mnemonicToSeed(mnemonics)).derive(path);
    if (key.publicKey == null || key.privateKey == null) {
      throw new Error("Invalid key");
    }
    return new _Secp256k1Keypair({
      publicKey: key.publicKey,
      secretKey: key.privateKey
    });
  }
};

// node_modules/@mysten/sui/dist/esm/keypairs/secp256r1/publickey.js
var SECP256R1_PUBLIC_KEY_SIZE = 33;
var Secp256r1PublicKey = class extends PublicKey2 {
  /**
   * Create a new Secp256r1PublicKey object
   * @param value secp256r1 public key as buffer or base-64 encoded string
   */
  constructor(value) {
    super();
    if (typeof value === "string") {
      this.data = fromBase64(value);
    } else if (value instanceof Uint8Array) {
      this.data = value;
    } else {
      this.data = Uint8Array.from(value);
    }
    if (this.data.length !== SECP256R1_PUBLIC_KEY_SIZE) {
      throw new Error(
        `Invalid public key input. Expected ${SECP256R1_PUBLIC_KEY_SIZE} bytes, got ${this.data.length}`
      );
    }
  }
  /**
   * Checks if two Secp256r1 public keys are equal
   */
  equals(publicKey) {
    return super.equals(publicKey);
  }
  /**
   * Return the byte array representation of the Secp256r1 public key
   */
  toRawBytes() {
    return this.data;
  }
  /**
   * Return the Sui address associated with this Secp256r1 public key
   */
  flag() {
    return SIGNATURE_SCHEME_TO_FLAG["Secp256r1"];
  }
  /**
   * Verifies that the signature is valid for for the provided message
   */
  async verify(message, signature) {
    let bytes;
    if (typeof signature === "string") {
      const parsed = parseSerializedSignature(signature);
      if (parsed.signatureScheme !== "Secp256r1") {
        throw new Error("Invalid signature scheme");
      }
      if (!bytesEqual(this.toRawBytes(), parsed.publicKey)) {
        throw new Error("Signature does not match public key");
      }
      bytes = parsed.signature;
    } else {
      bytes = signature;
    }
    return secp256r1.verify(
      secp256r1.Signature.fromCompact(bytes),
      sha2562(message),
      this.toRawBytes()
    );
  }
};
Secp256r1PublicKey.SIZE = SECP256R1_PUBLIC_KEY_SIZE;

// node_modules/@mysten/sui/dist/esm/keypairs/secp256r1/keypair.js
var DEFAULT_SECP256R1_DERIVATION_PATH = "m/74'/784'/0'/0/0";
var Secp256r1Keypair = class _Secp256r1Keypair extends Keypair {
  /**
   * Create a new keypair instance.
   * Generate random keypair if no {@link Secp256r1Keypair} is provided.
   *
   * @param keypair Secp256r1 keypair
   */
  constructor(keypair) {
    super();
    if (keypair) {
      this.keypair = keypair;
    } else {
      const secretKey = secp256r1.utils.randomPrivateKey();
      const publicKey = secp256r1.getPublicKey(secretKey, true);
      this.keypair = { publicKey, secretKey };
    }
  }
  /**
   * Get the key scheme of the keypair Secp256r1
   */
  getKeyScheme() {
    return "Secp256r1";
  }
  /**
   * Generate a new random keypair
   */
  static generate() {
    return new _Secp256r1Keypair();
  }
  /**
   * Create a keypair from a raw secret key byte array.
   *
   * This method should only be used to recreate a keypair from a previously
   * generated secret key. Generating keypairs from a random seed should be done
   * with the {@link Keypair.fromSeed} method.
   *
   * @throws error if the provided secret key is invalid and validation is not skipped.
   *
   * @param secretKey secret key byte array or Bech32 secret key string
   * @param options: skip secret key validation
   */
  static fromSecretKey(secretKey, options) {
    if (typeof secretKey === "string") {
      const decoded = decodeSuiPrivateKey(secretKey);
      if (decoded.schema !== "Secp256r1") {
        throw new Error(`Expected a Secp256r1 keypair, got ${decoded.schema}`);
      }
      return this.fromSecretKey(decoded.secretKey, options);
    }
    const publicKey = secp256r1.getPublicKey(secretKey, true);
    if (!options || !options.skipValidation) {
      const encoder = new TextEncoder();
      const signData = encoder.encode("sui validation");
      const msgHash = bytesToHex(blake2b2(signData, { dkLen: 32 }));
      const signature = secp256r1.sign(msgHash, secretKey, { lowS: true });
      if (!secp256r1.verify(signature, msgHash, publicKey, { lowS: true })) {
        throw new Error("Provided secretKey is invalid");
      }
    }
    return new _Secp256r1Keypair({ publicKey, secretKey });
  }
  /**
   * Generate a keypair from a 32 byte seed.
   *
   * @param seed seed byte array
   */
  static fromSeed(seed) {
    const publicKey = secp256r1.getPublicKey(seed, true);
    return new _Secp256r1Keypair({ publicKey, secretKey: seed });
  }
  /**
   * The public key for this keypair
   */
  getPublicKey() {
    return new Secp256r1PublicKey(this.keypair.publicKey);
  }
  /**
   * The Bech32 secret key string for this Secp256r1 keypair
   */
  getSecretKey() {
    return encodeSuiPrivateKey(this.keypair.secretKey, this.getKeyScheme());
  }
  /**
   * Return the signature for the provided data.
   */
  async sign(data) {
    const msgHash = sha2562(data);
    const sig = secp256r1.sign(msgHash, this.keypair.secretKey, {
      lowS: true
    });
    return sig.toCompactRawBytes();
  }
  /**
   * Derive Secp256r1 keypair from mnemonics and path. The mnemonics must be normalized
   * and validated against the english wordlist.
   *
   * If path is none, it will default to m/74'/784'/0'/0/0, otherwise the path must
   * be compliant to BIP-32 in form m/74'/784'/{account_index}'/{change_index}/{address_index}.
   */
  static deriveKeypair(mnemonics, path) {
    if (path == null) {
      path = DEFAULT_SECP256R1_DERIVATION_PATH;
    }
    if (!isValidBIP32Path(path)) {
      throw new Error("Invalid derivation path");
    }
    const privateKey = HDKey.fromMasterSeed(mnemonicToSeed(mnemonics)).derive(path).privateKey;
    return _Secp256r1Keypair.fromSecretKey(privateKey);
  }
};

// node_modules/@mysten/sui/dist/esm/transactions/intents/CoinWithBalance.js
var COIN_WITH_BALANCE = "CoinWithBalance";
var SUI_TYPE = normalizeStructTag("0x2::sui::SUI");
function coinWithBalance({
  type = SUI_TYPE,
  balance,
  useGasCoin = true
}) {
  let coinResult = null;
  return (tx) => {
    if (coinResult) {
      return coinResult;
    }
    tx.addIntentResolver(COIN_WITH_BALANCE, resolveCoinBalance);
    const coinType = type === "gas" ? type : normalizeStructTag(type);
    coinResult = tx.add(
      Commands.Intent({
        name: COIN_WITH_BALANCE,
        inputs: {},
        data: {
          type: coinType === SUI_TYPE && useGasCoin ? "gas" : coinType,
          balance: BigInt(balance)
        }
      })
    );
    return coinResult;
  };
}
var CoinWithBalanceData = object({
  type: string(),
  balance: bigint()
});
async function resolveCoinBalance(transactionData, buildOptions, next) {
  const coinTypes = /* @__PURE__ */ new Set();
  const totalByType = /* @__PURE__ */ new Map();
  if (!transactionData.sender) {
    throw new Error("Sender must be set to resolve CoinWithBalance");
  }
  for (const command of transactionData.commands) {
    if (command.$kind === "$Intent" && command.$Intent.name === COIN_WITH_BALANCE) {
      const { type, balance } = parse2(CoinWithBalanceData, command.$Intent.data);
      if (type !== "gas" && balance > 0n) {
        coinTypes.add(type);
      }
      totalByType.set(type, (totalByType.get(type) ?? 0n) + balance);
    }
  }
  const usedIds = /* @__PURE__ */ new Set();
  for (const input of transactionData.inputs) {
    if (input.Object?.ImmOrOwnedObject) {
      usedIds.add(input.Object.ImmOrOwnedObject.objectId);
    }
    if (input.UnresolvedObject?.objectId) {
      usedIds.add(input.UnresolvedObject.objectId);
    }
  }
  const coinsByType = /* @__PURE__ */ new Map();
  const client = getSuiClient(buildOptions);
  await Promise.all(
    [...coinTypes].map(async (coinType) => {
      coinsByType.set(
        coinType,
        await getCoinsOfType({
          coinType,
          balance: totalByType.get(coinType),
          client,
          owner: transactionData.sender,
          usedIds
        })
      );
    })
  );
  const mergedCoins = /* @__PURE__ */ new Map();
  mergedCoins.set("gas", { $kind: "GasCoin", GasCoin: true });
  for (const [index, transaction] of transactionData.commands.entries()) {
    if (transaction.$kind !== "$Intent" || transaction.$Intent.name !== COIN_WITH_BALANCE) {
      continue;
    }
    const { type, balance } = transaction.$Intent.data;
    if (balance === 0n && type !== "gas") {
      transactionData.replaceCommand(
        index,
        Commands.MoveCall({ target: "0x2::coin::zero", typeArguments: [type] })
      );
      continue;
    }
    const commands = [];
    if (!mergedCoins.has(type)) {
      const [first, ...rest] = coinsByType.get(type).map(
        (coin) => transactionData.addInput(
          "object",
          Inputs.ObjectRef({
            objectId: coin.coinObjectId,
            digest: coin.digest,
            version: coin.version
          })
        )
      );
      if (rest.length > 0) {
        commands.push(Commands.MergeCoins(first, rest));
      }
      mergedCoins.set(type, first);
    }
    commands.push(
      Commands.SplitCoins(mergedCoins.get(type), [
        transactionData.addInput("pure", Inputs.Pure(suiBcs.u64().serialize(balance)))
      ])
    );
    transactionData.replaceCommand(index, commands);
    transactionData.mapArguments((arg) => {
      if (arg.$kind === "Result" && arg.Result === index) {
        return {
          $kind: "NestedResult",
          NestedResult: [index + commands.length - 1, 0]
        };
      }
      return arg;
    });
  }
  return next();
}
async function getCoinsOfType({
  coinType,
  balance,
  client,
  owner,
  usedIds
}) {
  let remainingBalance = balance;
  const coins = [];
  return loadMoreCoins();
  async function loadMoreCoins(cursor = null) {
    const { data, hasNextPage, nextCursor } = await client.getCoins({ owner, coinType, cursor });
    const sortedCoins = data.sort((a, b) => Number(BigInt(b.balance) - BigInt(a.balance)));
    for (const coin of sortedCoins) {
      if (usedIds.has(coin.coinObjectId)) {
        continue;
      }
      const coinBalance = BigInt(coin.balance);
      coins.push(coin);
      remainingBalance -= coinBalance;
      if (remainingBalance <= 0) {
        return coins;
      }
    }
    if (hasNextPage) {
      return loadMoreCoins(nextCursor);
    }
    throw new Error(`Not enough coins of type ${coinType} to satisfy requested balance`);
  }
}
function getSuiClient(options) {
  const client = getClient(options);
  if (!client.jsonRpc) {
    throw new Error(`CoinWithBalance intent currently only works with SuiClient`);
  }
  return client;
}

// node_modules/@mysten/walrus/dist/esm/constants.js
var TESTNET_WALRUS_PACKAGE_CONFIG = {
  systemObjectId: "0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af",
  stakingPoolId: "0xbe46180321c30aab2f8b3501e24048377287fa708018a5b7c2792b35fe339ee3",
  exchangeIds: [
    "0xf4d164ea2def5fe07dc573992a029e010dba09b1a8dcbc44c5c2e79567f39073",
    "0x19825121c52080bb1073662231cfea5c0e4d905fd13e95f21e9a018f2ef41862",
    "0x83b454e524c71f30803f4d6c302a86fb6a39e96cdfb873c2d1e93bc1c26a3bc5",
    "0x8d63209cf8589ce7aef8f262437163c67577ed09f3e636a9d8e0813843fb8bf1"
  ]
};
var MAINNET_WALRUS_PACKAGE_CONFIG = {
  systemObjectId: "0x2134d52768ea07e8c43570ef975eb3e4c27a39fa6396bef985b5abc58d03ddd2",
  stakingPoolId: "0x10b9d30c28448939ce6c4d6c6e0ffce4a7f8a4ada8248bdad09ef8b70e4a3904"
};
var statusLifecycleRank = {
  nonexistent: 0,
  deletable: 1,
  permanent: 2,
  invalid: 3
};

// node_modules/@mysten/walrus/dist/esm/contracts/utils/index.js
var MOVE_STDLIB_ADDRESS2 = normalizeSuiAddress("0x1");
var SUI_FRAMEWORK_ADDRESS2 = normalizeSuiAddress("0x2");
var SUI_SYSTEM_ADDRESS2 = normalizeSuiAddress("0x3");
function getPureBcsSchema2(typeTag) {
  const parsedTag = typeof typeTag === "string" ? TypeTagSerializer.parseFromStr(typeTag) : typeTag;
  if ("u8" in parsedTag) {
    return suiBcs.U8;
  } else if ("u16" in parsedTag) {
    return suiBcs.U16;
  } else if ("u32" in parsedTag) {
    return suiBcs.U32;
  } else if ("u64" in parsedTag) {
    return suiBcs.U64;
  } else if ("u128" in parsedTag) {
    return suiBcs.U128;
  } else if ("u256" in parsedTag) {
    return suiBcs.U256;
  } else if ("address" in parsedTag) {
    return suiBcs.Address;
  } else if ("bool" in parsedTag) {
    return suiBcs.Bool;
  } else if ("vector" in parsedTag) {
    const type = getPureBcsSchema2(parsedTag.vector);
    return type ? suiBcs.vector(type) : null;
  } else if ("struct" in parsedTag) {
    const structTag = parsedTag.struct;
    const pkg = normalizeSuiAddress(parsedTag.struct.address);
    if (pkg === MOVE_STDLIB_ADDRESS2) {
      if ((structTag.module === "ascii" || structTag.module === "string") && structTag.name === "String") {
        return suiBcs.String;
      }
      if (structTag.module === "option" && structTag.name === "Option") {
        const type = getPureBcsSchema2(structTag.typeParams[0]);
        return type ? suiBcs.option(type) : null;
      }
    }
    if (pkg === SUI_FRAMEWORK_ADDRESS2 && structTag.module === "Object" && structTag.name === "ID") {
      return suiBcs.Address;
    }
  }
  return null;
}
function normalizeMoveArguments(args, argTypes, parameterNames) {
  const argLen = Array.isArray(args) ? args.length : Object.keys(args).length;
  if (parameterNames && argLen !== parameterNames.length) {
    throw new Error(
      `Invalid number of arguments, expected ${parameterNames.length}, got ${argLen}`
    );
  }
  const normalizedArgs = [];
  let index = 0;
  for (const [i, argType] of argTypes.entries()) {
    if (argType === `${SUI_FRAMEWORK_ADDRESS2}::deny_list::DenyList`) {
      normalizedArgs.push((tx) => tx.object.denyList());
      continue;
    }
    if (argType === `${SUI_FRAMEWORK_ADDRESS2}::random::Random`) {
      normalizedArgs.push((tx) => tx.object.random());
      continue;
    }
    if (argType === `${SUI_FRAMEWORK_ADDRESS2}::clock::Clock`) {
      normalizedArgs.push((tx) => tx.object.clock());
      continue;
    }
    if (argType === `${SUI_SYSTEM_ADDRESS2}::sui_system::SuiSystemState`) {
      normalizedArgs.push((tx) => tx.object.system());
      continue;
    }
    let arg;
    if (Array.isArray(args)) {
      if (index >= args.length) {
        throw new Error(
          `Invalid number of arguments, expected at least ${index + 1}, got ${args.length}`
        );
      }
      arg = args[index];
    } else {
      if (!parameterNames) {
        throw new Error(`Expected arguments to be passed as an array`);
      }
      const name = parameterNames[index];
      arg = args[name];
      if (arg === void 0) {
        throw new Error(`Parameter ${name} is required`);
      }
    }
    index += 1;
    if (typeof arg === "function" || isArgument(arg)) {
      normalizedArgs.push(arg);
      continue;
    }
    const type = argTypes[i];
    const bcsType = getPureBcsSchema2(type);
    if (bcsType) {
      const bytes = bcsType.serialize(arg);
      normalizedArgs.push((tx) => tx.pure(bytes));
      continue;
    } else if (typeof arg === "string") {
      normalizedArgs.push((tx) => tx.object(arg));
      continue;
    }
    throw new Error(`Invalid argument ${stringify(arg)} for type ${type}`);
  }
  return normalizedArgs;
}
var MoveStruct = class extends BcsStruct {
};
var MoveEnum = class extends BcsEnum {
};
var MoveTuple = class extends BcsTuple {
};
function stringify(val) {
  if (typeof val === "object") {
    return JSON.stringify(val, (val2) => val2);
  }
  if (typeof val === "bigint") {
    return val.toString();
  }
  return val;
}

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/deps/sui/object.js
var $moduleName = "0x2::object";
var UID = new MoveStruct({
  name: `${$moduleName}::UID`,
  fields: {
    id: suiBcs.Address
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/storage_resource.js
var $moduleName2 = "@local-pkg/walrus::storage_resource";
var Storage = new MoveStruct({
  name: `${$moduleName2}::Storage`,
  fields: {
    id: UID,
    start_epoch: suiBcs.u32(),
    end_epoch: suiBcs.u32(),
    storage_size: suiBcs.u64()
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/blob.js
var $moduleName3 = "@local-pkg/walrus::blob";
var Blob2 = new MoveStruct({
  name: `${$moduleName3}::Blob`,
  fields: {
    id: UID,
    registered_epoch: suiBcs.u32(),
    blob_id: suiBcs.u256(),
    size: suiBcs.u64(),
    encoding_type: suiBcs.u8(),
    certified_epoch: suiBcs.option(suiBcs.u32()),
    storage: Storage,
    deletable: suiBcs.bool()
  }
});
var BlobIdDerivation = new MoveStruct({
  name: `${$moduleName3}::BlobIdDerivation`,
  fields: {
    encoding_type: suiBcs.u8(),
    size: suiBcs.u64(),
    root_hash: suiBcs.u256()
  }
});
function addMetadata(options) {
  const packageAddress = options.package ?? "@local-pkg/walrus";
  const argumentsTypes = [
    `${packageAddress}::blob::Blob`,
    `${packageAddress}::metadata::Metadata`
  ];
  const parameterNames = ["self", "metadata"];
  return (tx) => tx.moveCall({
    package: packageAddress,
    module: "blob",
    function: "add_metadata",
    arguments: normalizeMoveArguments(options.arguments, argumentsTypes, parameterNames)
  });
}
function insertOrUpdateMetadataPair(options) {
  const packageAddress = options.package ?? "@local-pkg/walrus";
  const argumentsTypes = [
    `${packageAddress}::blob::Blob`,
    "0x0000000000000000000000000000000000000000000000000000000000000001::string::String",
    "0x0000000000000000000000000000000000000000000000000000000000000001::string::String"
  ];
  const parameterNames = ["self", "key", "value"];
  return (tx) => tx.moveCall({
    package: packageAddress,
    module: "blob",
    function: "insert_or_update_metadata_pair",
    arguments: normalizeMoveArguments(options.arguments, argumentsTypes, parameterNames)
  });
}
function removeMetadataPair(options) {
  const packageAddress = options.package ?? "@local-pkg/walrus";
  const argumentsTypes = [
    `${packageAddress}::blob::Blob`,
    "0x0000000000000000000000000000000000000000000000000000000000000001::string::String"
  ];
  const parameterNames = ["self", "key"];
  return (tx) => tx.moveCall({
    package: packageAddress,
    module: "blob",
    function: "remove_metadata_pair",
    arguments: normalizeMoveArguments(options.arguments, argumentsTypes, parameterNames)
  });
}

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/deps/sui/vec_map.js
var $moduleName4 = "0x2::vec_map";
function Entry(...typeParameters) {
  return new MoveStruct({
    name: `${$moduleName4}::Entry<${typeParameters[0].name}, ${typeParameters[1].name}>`,
    fields: {
      key: typeParameters[0],
      value: typeParameters[1]
    }
  });
}
function VecMap(...typeParameters) {
  return new MoveStruct({
    name: `${$moduleName4}::VecMap<${typeParameters[0].name}, ${typeParameters[1].name}>`,
    fields: {
      contents: suiBcs.vector(Entry(typeParameters[0], typeParameters[1]))
    }
  });
}

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/metadata.js
var $moduleName5 = "@local-pkg/walrus::metadata";
var Metadata = new MoveStruct({
  name: `${$moduleName5}::Metadata`,
  fields: {
    metadata: VecMap(suiBcs.string(), suiBcs.string())
  }
});
function _new(options = {}) {
  const packageAddress = options.package ?? "@local-pkg/walrus";
  return (tx) => tx.moveCall({
    package: packageAddress,
    module: "metadata",
    function: "new"
  });
}

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/deps/sui/object_table.js
var $moduleName6 = "0x2::object_table";
var ObjectTable = new MoveStruct({
  name: `${$moduleName6}::ObjectTable`,
  fields: {
    /** the ID of this table */
    id: UID,
    /** the number of key-value pairs in the table */
    size: suiBcs.u64()
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/extended_field.js
var $moduleName7 = "@local-pkg/walrus::extended_field";
var ExtendedField = new MoveStruct({
  name: `${$moduleName7}::ExtendedField`,
  fields: {
    id: UID
  }
});
var Key = new MoveTuple({ name: `${$moduleName7}::Key`, fields: [suiBcs.bool()] });

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/committee.js
var $moduleName8 = "@local-pkg/walrus::committee";
var Committee = new MoveTuple({
  name: `${$moduleName8}::Committee`,
  fields: [VecMap(suiBcs.Address, suiBcs.vector(suiBcs.u16()))]
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/epoch_parameters.js
var $moduleName9 = "@local-pkg/walrus::epoch_parameters";
var EpochParams = new MoveStruct({
  name: `${$moduleName9}::EpochParams`,
  fields: {
    /** The storage capacity of the system. */
    total_capacity_size: suiBcs.u64(),
    /** The price per unit size of storage. */
    storage_price_per_unit_size: suiBcs.u64(),
    /** The write price per unit size. */
    write_price_per_unit_size: suiBcs.u64()
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/staking_inner.js
var $moduleName10 = "@local-pkg/walrus::staking_inner";
var EpochState = new MoveEnum({
  name: `${$moduleName10}::EpochState`,
  fields: {
    EpochChangeSync: suiBcs.u16(),
    EpochChangeDone: suiBcs.u64(),
    NextParamsSelected: suiBcs.u64()
  }
});
var StakingInnerV1 = new MoveStruct({
  name: `${$moduleName10}::StakingInnerV1`,
  fields: {
    /** The number of shards in the system. */
    n_shards: suiBcs.u16(),
    /** The duration of an epoch in ms. Does not affect the first (zero) epoch. */
    epoch_duration: suiBcs.u64(),
    /**
     * Special parameter, used only for the first epoch. The timestamp when the first
     * epoch can be started.
     */
    first_epoch_start: suiBcs.u64(),
    /**
     * Stored staking pools, each identified by a unique `ID` and contains the
     * `StakingPool` object. Uses `ObjectTable` to make the pool discovery easier by
     * avoiding wrapping.
     *
     * The key is the ID of the staking pool.
     */
    pools: ObjectTable,
    /**
     * The current epoch of the Walrus system. The epochs are not the same as the Sui
     * epochs, not to be mistaken with `ctx.epoch()`.
     */
    epoch: suiBcs.u32(),
    /** Stores the active set of storage nodes. Tracks the total amount of staked WAL. */
    active_set: ExtendedField,
    /** The next committee in the system. */
    next_committee: suiBcs.option(Committee),
    /** The current committee in the system. */
    committee: Committee,
    /** The previous committee in the system. */
    previous_committee: Committee,
    /** The next epoch parameters. */
    next_epoch_params: suiBcs.option(EpochParams),
    /** The state of the current epoch. */
    epoch_state: EpochState,
    /**
     * The public keys for the next epoch. The keys are stored in a sorted `VecMap`,
     * and mirror the order of the nodes in the `next_committee`. The value is set in
     * the `select_committee` function and consumed in the `next_bls_committee`
     * function.
     */
    next_epoch_public_keys: ExtendedField
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/deps/sui/group_ops.js
var $moduleName11 = "0x2::group_ops";
var Element = new MoveStruct({
  name: `${$moduleName11}::Element`,
  fields: {
    bytes: suiBcs.vector(suiBcs.u8())
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/event_blob.js
var $moduleName12 = "@local-pkg/walrus::event_blob";
var EventBlobAttestation = new MoveStruct({
  name: `${$moduleName12}::EventBlobAttestation`,
  fields: {
    checkpoint_sequence_num: suiBcs.u64(),
    epoch: suiBcs.u32()
  }
});
var EventBlob = new MoveStruct({
  name: `${$moduleName12}::EventBlob`,
  fields: {
    /** Blob id of the certified event blob. */
    blob_id: suiBcs.u256(),
    /** Ending sui checkpoint of the certified event blob. */
    ending_checkpoint_sequence_number: suiBcs.u64()
  }
});
var EventBlobCertificationState = new MoveStruct({
  name: `${$moduleName12}::EventBlobCertificationState`,
  fields: {
    /** Latest certified event blob. */
    latest_certified_blob: suiBcs.option(EventBlob),
    /** Current event blob being attested. */
    aggregate_weight_per_blob: VecMap(EventBlob, suiBcs.u16())
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/storage_node.js
var $moduleName13 = "@local-pkg/walrus::storage_node";
var StorageNodeInfo = new MoveStruct({
  name: `${$moduleName13}::StorageNodeInfo`,
  fields: {
    name: suiBcs.string(),
    node_id: suiBcs.Address,
    network_address: suiBcs.string(),
    public_key: Element,
    next_epoch_public_key: suiBcs.option(Element),
    network_public_key: suiBcs.vector(suiBcs.u8()),
    metadata: ExtendedField
  }
});
var StorageNodeCap = new MoveStruct({
  name: `${$moduleName13}::StorageNodeCap`,
  fields: {
    id: UID,
    node_id: suiBcs.Address,
    last_epoch_sync_done: suiBcs.u32(),
    last_event_blob_attestation: suiBcs.option(EventBlobAttestation),
    /** Stores the Merkle root of the deny list for the storage node. */
    deny_list_root: suiBcs.u256(),
    /** Stores the sequence number of the deny list for the storage node. */
    deny_list_sequence: suiBcs.u64(),
    /** Stores the size of the deny list for the storage node. */
    deny_list_size: suiBcs.u64()
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/pending_values.js
var $moduleName14 = "@local-pkg/walrus::pending_values";
var PendingValues = new MoveTuple({
  name: `${$moduleName14}::PendingValues`,
  fields: [VecMap(suiBcs.u32(), suiBcs.u64())]
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/deps/sui/table.js
var $moduleName15 = "0x2::table";
var Table = new MoveStruct({
  name: `${$moduleName15}::Table`,
  fields: {
    /** the ID of this table */
    id: UID,
    /** the number of key-value pairs in the table */
    size: suiBcs.u64()
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/deps/sui/balance.js
var $moduleName16 = "0x2::balance";
var Balance2 = new MoveStruct({
  name: `${$moduleName16}::Balance`,
  fields: {
    value: suiBcs.u64()
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/auth.js
var $moduleName17 = "@local-pkg/walrus::auth";
var Authenticated = new MoveEnum({
  name: `${$moduleName17}::Authenticated`,
  fields: {
    Sender: suiBcs.Address,
    Object: suiBcs.Address
  }
});
var Authorized = new MoveEnum({
  name: `${$moduleName17}::Authorized`,
  fields: {
    Address: suiBcs.Address,
    ObjectID: suiBcs.Address
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/deps/sui/bag.js
var $moduleName18 = "0x2::bag";
var Bag = new MoveStruct({
  name: `${$moduleName18}::Bag`,
  fields: {
    /** the ID of this bag */
    id: UID,
    /** the number of key-value pairs in the bag */
    size: suiBcs.u64()
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/staking_pool.js
var $moduleName19 = "@local-pkg/walrus::staking_pool";
var VotingParams = new MoveStruct({
  name: `${$moduleName19}::VotingParams`,
  fields: {
    /** Voting: storage price for the next epoch. */
    storage_price: suiBcs.u64(),
    /** Voting: write price for the next epoch. */
    write_price: suiBcs.u64(),
    /** Voting: node capacity for the next epoch. */
    node_capacity: suiBcs.u64()
  }
});
var PoolState = new MoveEnum({
  name: `${$moduleName19}::PoolState`,
  fields: {
    Active: null,
    Withdrawing: suiBcs.u32(),
    Withdrawn: null
  }
});
var StakingPool = new MoveStruct({
  name: `${$moduleName19}::StakingPool`,
  fields: {
    id: UID,
    /** The current state of the pool. */
    state: PoolState,
    /** Current epoch's pool parameters. */
    voting_params: VotingParams,
    /** The storage node info for the pool. */
    node_info: StorageNodeInfo,
    /**
     * The epoch when the pool is / will be activated. Serves information purposes
     * only, the checks are performed in the `state` property.
     */
    activation_epoch: suiBcs.u32(),
    /** Epoch when the pool was last updated. */
    latest_epoch: suiBcs.u32(),
    /** Currently staked WAL in the pool + rewards pool. */
    wal_balance: suiBcs.u64(),
    /** The total number of shares in the current epoch. */
    num_shares: suiBcs.u64(),
    /**
     * The amount of the shares that will be withdrawn in E+1 or E+2. We use this
     * amount to calculate the WAL withdrawal in the `process_pending_stake`.
     */
    pending_shares_withdraw: PendingValues,
    /**
     * The amount of the stake requested for withdrawal for a node that may part of the
     * next committee. Stores principals of not yet active stakes. In practice, those
     * tokens are staked for exactly one epoch.
     */
    pre_active_withdrawals: PendingValues,
    /**
     * The pending commission rate for the pool. Commission rate is applied in E+2, so
     * we store the value for the matching epoch and apply it in the `advance_epoch`
     * function.
     */
    pending_commission_rate: PendingValues,
    /** The commission rate for the pool, in basis points. */
    commission_rate: suiBcs.u16(),
    /**
     * Historical exchange rates for the pool. The key is the epoch when the exchange
     * rate was set, and the value is the exchange rate (the ratio of the amount of WAL
     * tokens for the pool shares).
     */
    exchange_rates: Table,
    /**
     * The amount of stake that will be added to the `wal_balance`. Can hold up to two
     * keys: E+1 and E+2, due to the differences in the activation epoch.
     *
     * ```
     * E+1 -> Balance
     * E+2 -> Balance
     * ```
     *
     * Single key is cleared in the `advance_epoch` function, leaving only the next
     * epoch's stake.
     */
    pending_stake: PendingValues,
    /** The rewards that the pool has received from being in the committee. */
    rewards_pool: Balance2,
    /** The commission that the pool has received from the rewards. */
    commission: Balance2,
    /** An Object or an address which can claim the commission. */
    commission_receiver: Authorized,
    /** An Object or address that can authorize governance actions, such as upgrades. */
    governance_authorized: Authorized,
    /** Reserved for future use and migrations. */
    extra_fields: Bag
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/staking.js
var $moduleName20 = "@local-pkg/walrus::staking";
var Staking = new MoveStruct({
  name: `${$moduleName20}::Staking`,
  fields: {
    id: UID,
    version: suiBcs.u64(),
    package_id: suiBcs.Address,
    new_package_id: suiBcs.option(suiBcs.Address)
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/bls_aggregate.js
var $moduleName21 = "@local-pkg/walrus::bls_aggregate";
var BlsCommitteeMember = new MoveStruct({
  name: `${$moduleName21}::BlsCommitteeMember`,
  fields: {
    public_key: Element,
    weight: suiBcs.u16(),
    node_id: suiBcs.Address
  }
});
var BlsCommittee = new MoveStruct({
  name: `${$moduleName21}::BlsCommittee`,
  fields: {
    /** A vector of committee members */
    members: suiBcs.vector(BlsCommitteeMember),
    /** The total number of shards held by the committee */
    n_shards: suiBcs.u16(),
    /** The epoch in which the committee is active. */
    epoch: suiBcs.u32(),
    /** The aggregation of public keys for all members of the committee */
    total_aggregated_key: Element
  }
});
var RequiredWeight = new MoveEnum({
  name: `${$moduleName21}::RequiredWeight`,
  fields: {
    /** Verify that the signers form a quorum. */
    Quorum: null,
    /** Verify that the signers include at least one correct node. */
    OneCorrectNode: null
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/storage_accounting.js
var $moduleName22 = "@local-pkg/walrus::storage_accounting";
var FutureAccounting = new MoveStruct({
  name: `${$moduleName22}::FutureAccounting`,
  fields: {
    epoch: suiBcs.u32(),
    /**
     * This field stores `used_capacity` for the epoch. Currently, impossible to rename
     * due to package upgrade limitations.
     */
    used_capacity: suiBcs.u64(),
    rewards_to_distribute: Balance2
  }
});
var FutureAccountingRingBuffer = new MoveStruct({
  name: `${$moduleName22}::FutureAccountingRingBuffer`,
  fields: {
    current_index: suiBcs.u32(),
    length: suiBcs.u32(),
    ring_buffer: suiBcs.vector(FutureAccounting)
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/system_state_inner.js
var $moduleName23 = "@local-pkg/walrus::system_state_inner";
var SystemStateInnerV1 = new MoveStruct({
  name: `${$moduleName23}::SystemStateInnerV1`,
  fields: {
    /** The current committee, with the current epoch. */
    committee: BlsCommittee,
    /**
     * Maximum capacity size for the current and future epochs. Changed by voting on
     * the epoch parameters.
     */
    total_capacity_size: suiBcs.u64(),
    /** Contains the used capacity size for the current epoch. */
    used_capacity_size: suiBcs.u64(),
    /** The price per unit size of storage. */
    storage_price_per_unit_size: suiBcs.u64(),
    /** The write price per unit size. */
    write_price_per_unit_size: suiBcs.u64(),
    /** Accounting ring buffer for future epochs. */
    future_accounting: FutureAccountingRingBuffer,
    /** Event blob certification state */
    event_blob_certification_state: EventBlobCertificationState,
    /**
     * Sizes of deny lists for storage nodes. Only current committee members can
     * register their updates in this map. Hence, we don't expect it to bloat.
     *
     * Max number of stored entries is ~6500. If there's any concern about the
     * performance of the map, it can be cleaned up as a side effect of the updates /
     * registrations.
     */
    deny_list_sizes: ExtendedField
  }
});

// node_modules/@mysten/walrus/dist/esm/contracts/walrus/system.js
var $moduleName24 = "@local-pkg/walrus::system";
var System = new MoveStruct({
  name: `${$moduleName24}::System`,
  fields: {
    id: UID,
    version: suiBcs.u64(),
    package_id: suiBcs.Address,
    new_package_id: suiBcs.option(suiBcs.Address)
  }
});
function reserveSpace(options) {
  const packageAddress = options.package ?? "@local-pkg/walrus";
  const argumentsTypes = [
    `${packageAddress}::system::System`,
    "u64",
    "u32",
    `0x0000000000000000000000000000000000000000000000000000000000000002::coin::Coin<${packageAddress}::wal::WAL>`
  ];
  const parameterNames = ["self", "storageAmount", "epochsAhead", "payment"];
  return (tx) => tx.moveCall({
    package: packageAddress,
    module: "system",
    function: "reserve_space",
    arguments: normalizeMoveArguments(options.arguments, argumentsTypes, parameterNames)
  });
}
function registerBlob(options) {
  const packageAddress = options.package ?? "@local-pkg/walrus";
  const argumentsTypes = [
    `${packageAddress}::system::System`,
    `${packageAddress}::storage_resource::Storage`,
    "u256",
    "u256",
    "u64",
    "u8",
    "bool",
    `0x0000000000000000000000000000000000000000000000000000000000000002::coin::Coin<${packageAddress}::wal::WAL>`
  ];
  const parameterNames = [
    "self",
    "storage",
    "blobId",
    "rootHash",
    "size",
    "encodingType",
    "deletable",
    "writePayment"
  ];
  return (tx) => tx.moveCall({
    package: packageAddress,
    module: "system",
    function: "register_blob",
    arguments: normalizeMoveArguments(options.arguments, argumentsTypes, parameterNames)
  });
}
function certifyBlob(options) {
  const packageAddress = options.package ?? "@local-pkg/walrus";
  const argumentsTypes = [
    `${packageAddress}::system::System`,
    `${packageAddress}::blob::Blob`,
    "vector<u8>",
    "vector<u8>",
    "vector<u8>"
  ];
  const parameterNames = ["self", "blob", "signature", "signersBitmap", "message"];
  return (tx) => tx.moveCall({
    package: packageAddress,
    module: "system",
    function: "certify_blob",
    arguments: normalizeMoveArguments(options.arguments, argumentsTypes, parameterNames)
  });
}
function deleteBlob(options) {
  const packageAddress = options.package ?? "@local-pkg/walrus";
  const argumentsTypes = [
    `${packageAddress}::system::System`,
    `${packageAddress}::blob::Blob`
  ];
  const parameterNames = ["self", "blob"];
  return (tx) => tx.moveCall({
    package: packageAddress,
    module: "system",
    function: "delete_blob",
    arguments: normalizeMoveArguments(options.arguments, argumentsTypes, parameterNames)
  });
}
function extendBlob(options) {
  const packageAddress = options.package ?? "@local-pkg/walrus";
  const argumentsTypes = [
    `${packageAddress}::system::System`,
    `${packageAddress}::blob::Blob`,
    "u32",
    `0x0000000000000000000000000000000000000000000000000000000000000002::coin::Coin<${packageAddress}::wal::WAL>`
  ];
  const parameterNames = ["self", "blob", "extendedEpochs", "payment"];
  return (tx) => tx.moveCall({
    package: packageAddress,
    module: "system",
    function: "extend_blob",
    arguments: normalizeMoveArguments(options.arguments, argumentsTypes, parameterNames)
  });
}

// node_modules/@mysten/walrus/dist/esm/error.js
var WalrusClientError = class extends Error {
};
var RetryableWalrusClientError = class extends WalrusClientError {
};
var NoBlobStatusReceivedError = class extends WalrusClientError {
};
var NoVerifiedBlobStatusReceivedError = class extends WalrusClientError {
};
var NoBlobMetadataReceivedError = class extends RetryableWalrusClientError {
};
var NotEnoughSliversReceivedError = class extends RetryableWalrusClientError {
};
var NotEnoughBlobConfirmationsError = class extends RetryableWalrusClientError {
};
var BehindCurrentEpochError = class extends RetryableWalrusClientError {
};
var BlobNotCertifiedError = class extends RetryableWalrusClientError {
};
var InconsistentBlobError = class extends WalrusClientError {
};
var BlobBlockedError = class extends Error {
};

// node_modules/@mysten/walrus/dist/esm/utils/bcs.js
var MerkleNode = suiBcs.enum("MerkleNode", {
  Empty: null,
  Digest: suiBcs.bytes(32)
});
var SliverPairMetadata = suiBcs.struct("SliverPairMetadata", {
  primary_hash: MerkleNode,
  secondary_hash: MerkleNode
});
var EncodingType = suiBcs.enum("EncodingType", {
  RedStuff: null,
  RS2: null
}).transform({
  input: (encodingType) => typeof encodingType === "string" ? { [encodingType]: null } : encodingType,
  output: (encodingType) => encodingType
});
var BlobMetadataV1 = suiBcs.struct("BlobMetadataV1", {
  encoding_type: EncodingType,
  unencoded_length: suiBcs.u64(),
  hashes: suiBcs.vector(SliverPairMetadata)
});
var BlobMetadata = suiBcs.enum("BlobMetadata", {
  V1: BlobMetadataV1
});
var BlobId = suiBcs.u256().transform({
  input: (blobId) => typeof blobId === "string" ? blobIdToInt(blobId) : blobId,
  output: (id) => blobIdFromInt(id)
});
function blobIdFromInt(blobId) {
  return suiBcs.u256().serialize(blobId).toBase64().replace(/=*$/, "").replaceAll("+", "-").replaceAll("/", "_");
}
function blobIdFromBytes(blobId) {
  return blobIdFromInt(suiBcs.u256().parse(blobId));
}
function blobIdToInt(blobId) {
  return BigInt(suiBcs.u256().fromBase64(blobId.replaceAll("-", "+").replaceAll("_", "/")));
}
var BlobMetadataWithId = suiBcs.struct("BlobMetadataWithId", {
  blobId: BlobId,
  metadata: BlobMetadata
});
var Symbols = suiBcs.struct("Symbols", {
  data: suiBcs.byteVector(),
  symbol_size: suiBcs.u16()
});
var SliverData = suiBcs.struct("SliverData", {
  symbols: Symbols,
  index: suiBcs.u16()
});
var Sliver = suiBcs.enum("Sliver", {
  Primary: SliverData,
  Secondary: SliverData
});
var SliverPair = suiBcs.struct("SliverPair", {
  primary: SliverData,
  secondary: SliverData
});
var IntentType = /* @__PURE__ */ ((IntentType2) => {
  IntentType2[IntentType2["PROOF_OF_POSSESSION_MSG"] = 0] = "PROOF_OF_POSSESSION_MSG";
  IntentType2[IntentType2["BLOB_CERT_MSG"] = 1] = "BLOB_CERT_MSG";
  IntentType2[IntentType2["INVALID_BLOB_ID_MSG"] = 2] = "INVALID_BLOB_ID_MSG";
  IntentType2[IntentType2["SYNC_SHARD_MSG"] = 3] = "SYNC_SHARD_MSG";
  return IntentType2;
})(IntentType || {});
var Intent2 = suiBcs.struct("Intent", {
  type: suiBcs.u8().transform({
    input: (type) => type,
    output: (type) => type
  }),
  version: suiBcs.u8(),
  appId: suiBcs.u8()
}).transform({
  input: (intent) => ({
    type: intent,
    version: 0,
    appId: 3
  }),
  output: (intent) => intent.type
});
function ProtocolMessage(messageContents) {
  return suiBcs.struct(`ProtocolMessage<${messageContents.name}>`, {
    intent: Intent2,
    epoch: suiBcs.u32(),
    messageContents
  });
}
var BlobPersistenceType = suiBcs.enum("BlobPersistenceType", {
  Permanent: null,
  Deletable: suiBcs.struct("Deletable", {
    objectId: suiBcs.Address
  })
});
var StorageConfirmationBody = suiBcs.struct("StorageConfirmationBody", {
  blobId: BlobId,
  blobType: BlobPersistenceType
});
var StorageConfirmation = ProtocolMessage(StorageConfirmationBody);
function Field2(...typeParameters) {
  return suiBcs.struct("Field", {
    id: suiBcs.Address,
    name: typeParameters[0],
    value: typeParameters[1]
  });
}
var QuiltPatchTags = suiBcs.map(suiBcs.string(), suiBcs.string()).transform({
  // tags is a BTreeMap, so we need to sort entries before serializing
  input: (tags) => new Map(
    [...tags instanceof Map ? tags : Object.entries(tags)].sort(
      ([a], [b]) => (
        // TODO: sorting for map keys should be moved into @mysten/bcs
        compareBcsBytes(suiBcs.string().serialize(a).toBytes(), suiBcs.string().serialize(b).toBytes())
      )
    )
  ),
  output: (tags) => Object.fromEntries(tags)
});
var QuiltPatchV1 = suiBcs.struct("QuiltPatchV1", {
  endIndex: suiBcs.u16(),
  identifier: suiBcs.string(),
  tags: QuiltPatchTags
});
function compareBcsBytes(a, b) {
  if (a.length !== b.length) {
    return a.length - b.length;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      return a[i] - b[i];
    }
  }
  return 0;
}
var QuiltIndexV1 = suiBcs.struct("QuiltIndexV1", {
  patches: suiBcs.vector(QuiltPatchV1)
});
var QuiltPatchId = suiBcs.struct("QuiltPatchId", {
  quiltId: BlobId,
  patchId: suiBcs.struct("InternalQuiltPatchId", {
    version: suiBcs.u8(),
    startIndex: suiBcs.u16(),
    endIndex: suiBcs.u16()
  })
});
var QuiltPatchBlobHeader = suiBcs.struct("QuiltPatchBlobHeader", {
  version: suiBcs.u8(),
  length: suiBcs.u32(),
  mask: suiBcs.u8()
});

// node_modules/@mysten/walrus/dist/esm/storage-node/error.js
var __typeError11 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck11 = (obj, member, msg) => member.has(obj) || __typeError11("Cannot " + msg);
var __privateAdd11 = (obj, member, value) => member.has(obj) ? __typeError11("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateMethod7 = (obj, member, method) => (__accessCheck11(obj, member, "access private method"), method);
var _StorageNodeAPIError_static;
var makeMessage_fn;
var StorageNodeError = class extends Error {
};
var _StorageNodeAPIError = class _StorageNodeAPIError2 extends StorageNodeError {
  constructor(status, error, message) {
    var _a;
    super(__privateMethod7(_a = _StorageNodeAPIError2, _StorageNodeAPIError_static, makeMessage_fn).call(_a, status, error, message));
    this.status = status;
    this.error = error;
  }
  static generate(status, errorResponse, message) {
    if (!status) {
      return new ConnectionError({ message });
    }
    if (status === 400) {
      return BadRequestError.generate(status, errorResponse, message);
    }
    if (status === 401) {
      return new AuthenticationError(status, errorResponse, message);
    }
    if (status === 403) {
      return new PermissionDeniedError(status, errorResponse, message);
    }
    if (status === 404) {
      return new NotFoundError(status, errorResponse, message);
    }
    if (status === 409) {
      return new ConflictError(status, errorResponse, message);
    }
    if (status === 422) {
      return new UnprocessableEntityError(status, errorResponse, message);
    }
    if (status === 429) {
      return new RateLimitError(status, errorResponse, message);
    }
    if (status === 451) {
      return new LegallyUnavailableError(status, errorResponse, message);
    }
    if (status >= 500) {
      return new InternalServerError(status, errorResponse, message);
    }
    return new _StorageNodeAPIError2(status, errorResponse, message);
  }
};
_StorageNodeAPIError_static = /* @__PURE__ */ new WeakSet();
makeMessage_fn = function(status, error, message) {
  function hasErrorMessage(error2) {
    return typeof error2?.error?.message === "string";
  }
  const inferredMessage = hasErrorMessage(error) ? error.error.message : message;
  const finalMessage = inferredMessage ? inferredMessage : JSON.stringify(error);
  if (status && finalMessage) {
    return `${status} ${finalMessage}`;
  } else if (finalMessage) {
    return finalMessage;
  } else if (status) {
    return `${status} status code (no body)`;
  }
  return "(no status code or body)";
};
__privateAdd11(_StorageNodeAPIError, _StorageNodeAPIError_static);
var StorageNodeAPIError = _StorageNodeAPIError;
var UserAbortError = class extends StorageNodeAPIError {
  constructor({ message } = {}) {
    super(void 0, void 0, message || "Request was aborted.");
  }
};
var ConnectionError = class extends StorageNodeAPIError {
  constructor({ message }) {
    super(void 0, void 0, message || "Connection error.");
  }
};
var ConnectionTimeoutError = class extends StorageNodeAPIError {
  constructor({ message } = {}) {
    super(void 0, void 0, message ?? "Request timed out.");
  }
};
var BadRequestError = class _BadRequestError extends StorageNodeAPIError {
  static generate(status, errorResponse, message) {
    if (errorResponse && typeof errorResponse === "object" && "error" in errorResponse) {
      const error = errorResponse.error;
      if (error.details?.[0]?.reason === "NOT_REGISTERED") {
        return new BlobNotRegisteredError(errorResponse, message);
      }
    }
    return new _BadRequestError(status, errorResponse, message);
  }
};
var BlobNotRegisteredError = class extends StorageNodeAPIError {
  constructor(error, message) {
    super(400, error, message);
  }
};
var AuthenticationError = class extends StorageNodeAPIError {
};
var PermissionDeniedError = class extends StorageNodeAPIError {
};
var NotFoundError = class extends StorageNodeAPIError {
};
var ConflictError = class extends StorageNodeAPIError {
};
var UnprocessableEntityError = class extends StorageNodeAPIError {
};
var RateLimitError = class extends StorageNodeAPIError {
};
var LegallyUnavailableError = class extends StorageNodeAPIError {
};
var InternalServerError = class extends StorageNodeAPIError {
};

// node_modules/@mysten/walrus/dist/esm/storage-node/utils.js
function mergeHeaders(...headers) {
  const mergedHeaders = new Headers();
  for (const header of headers) {
    if (!header || typeof header !== "object") {
      continue;
    }
    for (const [key, value] of Object.entries(header)) {
      if (value === null) {
        mergedHeaders.delete(key);
      } else if (Array.isArray(value)) {
        for (const v of value) {
          mergedHeaders.append(key, v);
        }
      } else if (value !== void 0) {
        mergedHeaders.set(key, value);
      }
    }
  }
  return mergedHeaders;
}

// node_modules/@mysten/walrus/dist/esm/storage-node/client.js
var __typeError12 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck12 = (obj, member, msg) => member.has(obj) || __typeError12("Cannot " + msg);
var __privateGet11 = (obj, member, getter) => (__accessCheck12(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd12 = (obj, member, value) => member.has(obj) ? __typeError12("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet11 = (obj, member, value, setter) => (__accessCheck12(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateMethod8 = (obj, member, method) => (__accessCheck12(obj, member, "access private method"), method);
var _fetch2;
var _timeout;
var _onError;
var _StorageNodeClient_instances;
var request_fn;
var StorageNodeClient = class {
  constructor({ fetch: overriddenFetch, timeout, onError } = {}) {
    __privateAdd12(this, _StorageNodeClient_instances);
    __privateAdd12(this, _fetch2);
    __privateAdd12(this, _timeout);
    __privateAdd12(this, _onError);
    __privateSet11(this, _fetch2, overriddenFetch ?? globalThis.fetch);
    __privateSet11(this, _timeout, timeout ?? 3e4);
    __privateSet11(this, _onError, onError);
  }
  /**
   * Gets the metadata associated with a Walrus blob.
   */
  async getBlobMetadata({ blobId }, options) {
    const response = await __privateMethod8(this, _StorageNodeClient_instances, request_fn).call(this, `/v1/blobs/${blobId}/metadata`, {
      ...options,
      headers: mergeHeaders({ Accept: "application/octet-stream" }, options.headers)
    });
    const bcsBytes = await response.arrayBuffer();
    return BlobMetadataWithId.parse(new Uint8Array(bcsBytes));
  }
  /**
   * Gets the status associated with a Walrus blob.
   */
  async getBlobStatus({ blobId }, options) {
    const response = await __privateMethod8(this, _StorageNodeClient_instances, request_fn).call(this, `/v1/blobs/${blobId}/status`, options);
    const json = await response.json();
    const blobStatus = json.success.data;
    if (blobStatus === "nonexistent") {
      return { type: "nonexistent" };
    }
    if ("invalid" in blobStatus) {
      return {
        type: "invalid",
        ...blobStatus.invalid
      };
    }
    if ("permanent" in blobStatus) {
      return {
        type: "permanent",
        ...blobStatus.permanent
      };
    }
    if ("deletable" in blobStatus) {
      return {
        type: "deletable",
        ...blobStatus.deletable
      };
    }
    throw new StorageNodeError(`Unknown blob status received: ${blobStatus}`);
  }
  /**
   * Stores the metadata associated with a registered Walrus blob at this storage
   * node. This is a pre-requisite for storing the encoded slivers of the blob. The
   * ID of the blob must first be registered on Sui, after which storing the metadata
   * becomes possible.
   *
   * This endpoint may return an error if the node has not yet received the
   * registration event from the chain.
   */
  async storeBlobMetadata({ blobId, metadata }, options) {
    const isBcsInput = typeof metadata === "object" && "V1" in metadata;
    const body = isBcsInput ? BlobMetadata.serialize(metadata).toBytes() : metadata;
    const response = await __privateMethod8(this, _StorageNodeClient_instances, request_fn).call(this, `/v1/blobs/${blobId}/metadata`, {
      ...options,
      method: "PUT",
      body,
      headers: mergeHeaders({ "Content-Type": "application/octet-stream" }, options.headers)
    });
    const json = await response.json();
    return json;
  }
  /**
   * Gets the primary or secondary sliver identified by the specified blob ID and
   * index. The index should represent a sliver that is assigned to be stored at one
   * of the shards managed by this storage node during this epoch.
   */
  async getSliver({ blobId, sliverPairIndex, sliverType }, options) {
    const response = await __privateMethod8(this, _StorageNodeClient_instances, request_fn).call(this, `/v1/blobs/${blobId}/slivers/${sliverPairIndex}/${sliverType}`, {
      ...options,
      headers: mergeHeaders({ Accept: "application/octet-stream" }, options.headers)
    });
    const bcsBytes = await response.arrayBuffer();
    return new Uint8Array(bcsBytes);
  }
  /**
   * Stores a primary or secondary blob sliver at the storage node.
   */
  async storeSliver({ blobId, sliverPairIndex, sliverType, sliver }, options) {
    const isBcsInput = typeof sliver === "object" && "symbols" in sliver;
    const body = isBcsInput ? SliverData.serialize(sliver).toBytes() : sliver;
    const response = await __privateMethod8(this, _StorageNodeClient_instances, request_fn).call(this, `/v1/blobs/${blobId}/slivers/${sliverPairIndex}/${sliverType}`, {
      ...options,
      method: "PUT",
      body,
      headers: mergeHeaders({ "Content-Type": "application/octet-stream" }, options.headers)
    });
    const json = await response.json();
    return json;
  }
  /**
   * Gets a signed storage confirmation from this storage node, indicating that all shards
   * assigned to this storage node for the current epoch have stored their respective slivers.
   */
  async getDeletableBlobConfirmation({ blobId, objectId }, options) {
    const response = await __privateMethod8(this, _StorageNodeClient_instances, request_fn).call(this, `/v1/blobs/${blobId}/confirmation/deletable/${objectId}`, options);
    const json = await response.json();
    return json;
  }
  /**
   * Gets a signed storage confirmation from this storage node, indicating that all shards
   * assigned to this storage node for the current epoch have stored their respective slivers.
   */
  async getPermanentBlobConfirmation({ blobId }, options) {
    const response = await __privateMethod8(this, _StorageNodeClient_instances, request_fn).call(this, `/v1/blobs/${blobId}/confirmation/permanent`, options);
    const json = await response.json();
    return json;
  }
};
_fetch2 = /* @__PURE__ */ new WeakMap();
_timeout = /* @__PURE__ */ new WeakMap();
_onError = /* @__PURE__ */ new WeakMap();
_StorageNodeClient_instances = /* @__PURE__ */ new WeakSet();
request_fn = async function(path, options) {
  var _a, _b, _c;
  const { nodeUrl, signal, timeout, ...init } = options;
  if (signal?.aborted) {
    throw new UserAbortError();
  }
  const timeoutSignal = AbortSignal.timeout(timeout ?? __privateGet11(this, _timeout));
  let response;
  try {
    const fetch2 = __privateGet11(this, _fetch2);
    response = await fetch2(`${nodeUrl}${path}`, {
      ...init,
      signal: signal ? AbortSignal.any([timeoutSignal, signal]) : timeoutSignal
    });
  } catch (error) {
    if (signal?.aborted) {
      throw new UserAbortError();
    }
    if (error instanceof Error && error.name === "AbortError") {
      const error2 = new ConnectionTimeoutError();
      (_a = __privateGet11(this, _onError)) == null ? void 0 : _a.call(this, error2);
      throw error2;
    }
    (_b = __privateGet11(this, _onError)) == null ? void 0 : _b.call(this, error);
    throw error;
  }
  if (!response.ok) {
    const errorText = await response.text().catch((reason) => reason);
    const errorJSON = safeParseJSON(errorText);
    const errorMessage = errorJSON ? void 0 : errorText;
    const error = StorageNodeAPIError.generate(response.status, errorJSON, errorMessage);
    (_c = __privateGet11(this, _onError)) == null ? void 0 : _c.call(this, error);
    throw error;
  }
  return response;
};
function safeParseJSON(value) {
  try {
    return JSON.parse(value);
  } catch {
    return void 0;
  }
}

// node_modules/@mysten/walrus/dist/esm/utils/index.js
var DIGEST_LEN = 32;
var BLOB_ID_LEN = 32;
var REQUIRED_ALIGNMENT_BY_ENCODING_TYPE = {
  RS2: 2,
  RedStuff: 2
};
var MAX_SYMBOL_SIZE_BY_ENCODING_TYPE = {
  RS2: 2 ** 16 - 1,
  RedStuff: 2 ** 16 - 1
};
function encodedBlobLength(unencodedLength, nShards, encodingType = "RS2") {
  const sliverSize = encodedSliverSize(unencodedLength, nShards, encodingType);
  const metadata = nShards * DIGEST_LEN * 2 + BLOB_ID_LEN;
  return nShards * metadata + sliverSize;
}
function encodedSliverSize(unencodedLength, nShards, encodingType = "RS2") {
  const { primarySymbols, secondarySymbols } = getSourceSymbols(nShards, encodingType);
  let symbolSize = Math.floor((Math.max(unencodedLength, 1) - 1) / (primarySymbols * secondarySymbols)) + 1;
  if (encodingType === "RS2" && symbolSize % 2 === 1) {
    symbolSize = symbolSize + 1;
  }
  const singleShardSize = (primarySymbols + secondarySymbols) * symbolSize;
  return singleShardSize * nShards;
}
function getSourceSymbols(nShards, encodingType = "RS2") {
  const safetyLimit = decodingSafetyLimit(nShards, encodingType);
  const maxFaulty = getMaxFaultyNodes(nShards);
  const minCorrect = nShards - maxFaulty;
  return {
    primarySymbols: minCorrect - maxFaulty - safetyLimit,
    secondarySymbols: minCorrect - safetyLimit
  };
}
function isQuorum(size, nShards) {
  const maxFaulty = getMaxFaultyNodes(nShards);
  return size > 2 * maxFaulty;
}
function isAboveValidity(size, nShards) {
  const maxFaulty = getMaxFaultyNodes(nShards);
  return size > maxFaulty;
}
function getMaxFaultyNodes(nShards) {
  return Math.floor((nShards - 1) / 3);
}
function decodingSafetyLimit(nShards, encodingType) {
  switch (encodingType) {
    case "RedStuff":
      return Math.min(5, Math.floor(getMaxFaultyNodes(nShards) / 5));
    case "RS2":
      return 0;
    default:
      throw new Error(`Encountered unknown encoding type of ${encodingType}`);
  }
}
var BYTES_PER_UNIT_SIZE = 1024 * 1024;
function storageUnitsFromSize(size) {
  return Math.ceil(size / BYTES_PER_UNIT_SIZE);
}
function rotationOffset(bytes, modulus) {
  return bytes.reduce((acc, byte) => (acc * 256 + byte) % modulus, 0);
}
function toShardIndex(sliverPairIndex, blobId, numShards) {
  const offset = rotationOffset(BlobId.serialize(blobId).toBytes(), numShards);
  return (sliverPairIndex + offset) % numShards;
}
function sliverPairIndexFromSecondarySliverIndex(sliverIndex, numShards) {
  return numShards - sliverIndex - 1;
}
function toPairIndex(shardIndex, blobId, numShards) {
  const offset = rotationOffset(BlobId.serialize(blobId).toBytes(), numShards);
  return (numShards + shardIndex - offset) % numShards;
}
function signersToBitmap(signers, committeeSize) {
  const bitmapSize = Math.ceil(committeeSize / 8);
  const bitmap = new Uint8Array(bitmapSize);
  for (const signer of signers) {
    const byteIndex = Math.floor(signer / 8);
    const bitIndex = signer % 8;
    bitmap[byteIndex] |= 1 << bitIndex;
  }
  return bitmap;
}
function getShardIndicesByNodeId(committee) {
  const shardIndicesByNodeId = /* @__PURE__ */ new Map();
  for (const node of committee[0].contents) {
    if (!shardIndicesByNodeId.has(node.key)) {
      shardIndicesByNodeId.set(node.key, []);
    }
    shardIndicesByNodeId.get(node.key).push(...node.value);
  }
  return shardIndicesByNodeId;
}
function urlSafeBase64(bytes) {
  return toBase64(bytes).replace(/=*$/, "").replaceAll("+", "-").replaceAll("/", "_");
}
function fromUrlSafeBase64(base64) {
  return fromBase64(base64.replaceAll("-", "+").replaceAll("_", "/"));
}
function getSizes(blobSize, numShards) {
  const encodedBlobSize = encodedSliverSize(blobSize, numShards);
  const { primarySymbols, secondarySymbols } = getSourceSymbols(numShards);
  const totalSymbols = (primarySymbols + secondarySymbols) * numShards;
  if (encodedBlobSize % totalSymbols !== 0) {
    throw new Error("encoded blob size should be divisible by total symbols");
  }
  const symbolSize = encodedBlobSize / totalSymbols;
  if (encodedBlobSize % totalSymbols !== 0) {
    throw new Error("blob length should be divisible by total symbols");
  }
  const rowSize = symbolSize * secondarySymbols;
  const columnSize = symbolSize * primarySymbols;
  return {
    symbolSize,
    rowSize,
    columnSize,
    blobSize
  };
}

// node_modules/@mysten/walrus/dist/esm/utils/object-loader.js
var import_dataloader2 = __toESM(require_dataloader(), 1);
var __typeError13 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck13 = (obj, member, msg) => member.has(obj) || __typeError13("Cannot " + msg);
var __privateGet12 = (obj, member, getter) => (__accessCheck13(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd13 = (obj, member, value) => member.has(obj) ? __typeError13("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var _dynamicFieldCache;
var SuiObjectDataLoader = class extends import_dataloader2.default {
  constructor(suiClient) {
    super(async (ids) => {
      const { objects } = await suiClient.core.getObjects({
        objectIds: ids
      });
      return objects;
    });
    __privateAdd13(this, _dynamicFieldCache, /* @__PURE__ */ new Map());
  }
  async load(id, schema) {
    const data = await super.load(id);
    if (schema) {
      return schema.parse(await data.content);
    }
    return data;
  }
  async loadMany(ids, schema) {
    const data = await super.loadMany(ids);
    if (!schema) {
      return data;
    }
    return Promise.all(
      data.map(async (d) => {
        if (d instanceof Error) {
          return d;
        }
        return schema.parse(await d.content);
      })
    );
  }
  async loadManyOrThrow(ids, schema) {
    const data = await this.loadMany(ids, schema);
    for (const d of data) {
      if (d instanceof Error) {
        throw d;
      }
    }
    return data;
  }
  clearAll() {
    __privateGet12(this, _dynamicFieldCache).clear();
    return super.clearAll();
  }
  clear(key) {
    __privateGet12(this, _dynamicFieldCache).delete(key);
    return super.clear(key);
  }
  async loadFieldObject(parent, name, type) {
    const schema = pureBcsSchemaFromTypeName(name.type);
    const id = deriveDynamicFieldID(parent, "u64", schema.serialize(name.value).toBytes());
    return (await this.load(id, Field2(schema, type))).value;
  }
};
_dynamicFieldCache = /* @__PURE__ */ new WeakMap();

// node_modules/@mysten/walrus/dist/esm/utils/randomness.js
function weightedShuffle(arr) {
  return arr.map(({ value, weight }) => ({
    value,
    weight: Math.pow(Math.random(), 1 / weight)
  })).sort((a, b) => b.weight - a.weight).map((item) => item.value);
}
function shuffle(arr) {
  const result = [...arr];
  for (let i = result.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
}

// node_modules/@mysten/walrus-wasm/web/walrus_wasm.js
var import_meta = {};
var wasm;
var cachedUint8ArrayMemory0 = null;
function getUint8ArrayMemory0() {
  if (cachedUint8ArrayMemory0 === null || cachedUint8ArrayMemory0.byteLength === 0) {
    cachedUint8ArrayMemory0 = new Uint8Array(wasm.memory.buffer);
  }
  return cachedUint8ArrayMemory0;
}
var cachedTextDecoder = new TextDecoder("utf-8", { ignoreBOM: true, fatal: true });
cachedTextDecoder.decode();
var MAX_SAFARI_DECODE_BYTES = 2146435072;
var numBytesDecoded = 0;
function decodeText(ptr, len) {
  numBytesDecoded += len;
  if (numBytesDecoded >= MAX_SAFARI_DECODE_BYTES) {
    cachedTextDecoder = new TextDecoder("utf-8", { ignoreBOM: true, fatal: true });
    cachedTextDecoder.decode();
    numBytesDecoded = len;
  }
  return cachedTextDecoder.decode(getUint8ArrayMemory0().subarray(ptr, ptr + len));
}
function getStringFromWasm0(ptr, len) {
  ptr = ptr >>> 0;
  return decodeText(ptr, len);
}
var WASM_VECTOR_LEN = 0;
var cachedTextEncoder = new TextEncoder();
if (!("encodeInto" in cachedTextEncoder)) {
  cachedTextEncoder.encodeInto = function(arg, view) {
    const buf = cachedTextEncoder.encode(arg);
    view.set(buf);
    return {
      read: arg.length,
      written: buf.length
    };
  };
}
function passStringToWasm0(arg, malloc, realloc) {
  if (realloc === void 0) {
    const buf = cachedTextEncoder.encode(arg);
    const ptr2 = malloc(buf.length, 1) >>> 0;
    getUint8ArrayMemory0().subarray(ptr2, ptr2 + buf.length).set(buf);
    WASM_VECTOR_LEN = buf.length;
    return ptr2;
  }
  let len = arg.length;
  let ptr = malloc(len, 1) >>> 0;
  const mem = getUint8ArrayMemory0();
  let offset = 0;
  for (; offset < len; offset++) {
    const code = arg.charCodeAt(offset);
    if (code > 127) break;
    mem[ptr + offset] = code;
  }
  if (offset !== len) {
    if (offset !== 0) {
      arg = arg.slice(offset);
    }
    ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
    const view = getUint8ArrayMemory0().subarray(ptr + offset, ptr + len);
    const ret = cachedTextEncoder.encodeInto(arg, view);
    offset += ret.written;
    ptr = realloc(ptr, len, offset, 1) >>> 0;
  }
  WASM_VECTOR_LEN = offset;
  return ptr;
}
var cachedDataViewMemory0 = null;
function getDataViewMemory0() {
  if (cachedDataViewMemory0 === null || cachedDataViewMemory0.buffer.detached === true || cachedDataViewMemory0.buffer.detached === void 0 && cachedDataViewMemory0.buffer !== wasm.memory.buffer) {
    cachedDataViewMemory0 = new DataView(wasm.memory.buffer);
  }
  return cachedDataViewMemory0;
}
function isLikeNone(x) {
  return x === void 0 || x === null;
}
function debugString(val) {
  const type = typeof val;
  if (type == "number" || type == "boolean" || val == null) {
    return `${val}`;
  }
  if (type == "string") {
    return `"${val}"`;
  }
  if (type == "symbol") {
    const description = val.description;
    if (description == null) {
      return "Symbol";
    } else {
      return `Symbol(${description})`;
    }
  }
  if (type == "function") {
    const name = val.name;
    if (typeof name == "string" && name.length > 0) {
      return `Function(${name})`;
    } else {
      return "Function";
    }
  }
  if (Array.isArray(val)) {
    const length = val.length;
    let debug = "[";
    if (length > 0) {
      debug += debugString(val[0]);
    }
    for (let i = 1; i < length; i++) {
      debug += ", " + debugString(val[i]);
    }
    debug += "]";
    return debug;
  }
  const builtInMatches = /\[object ([^\]]+)\]/.exec(toString.call(val));
  let className;
  if (builtInMatches && builtInMatches.length > 1) {
    className = builtInMatches[1];
  } else {
    return toString.call(val);
  }
  if (className == "Object") {
    try {
      return "Object(" + JSON.stringify(val) + ")";
    } catch (_) {
      return "Object";
    }
  }
  if (val instanceof Error) {
    return `${val.name}: ${val.message}
${val.stack}`;
  }
  return className;
}
function addToExternrefTable0(obj) {
  const idx = wasm.__externref_table_alloc();
  wasm.__wbindgen_externrefs.set(idx, obj);
  return idx;
}
function handleError(f, args) {
  try {
    return f.apply(this, args);
  } catch (e) {
    const idx = addToExternrefTable0(e);
    wasm.__wbindgen_exn_store(idx);
  }
}
function getArrayU8FromWasm0(ptr, len) {
  ptr = ptr >>> 0;
  return getUint8ArrayMemory0().subarray(ptr / 1, ptr / 1 + len);
}
function takeFromExternrefTable0(idx) {
  const value = wasm.__wbindgen_externrefs.get(idx);
  wasm.__externref_table_dealloc(idx);
  return value;
}
function passArrayJsValueToWasm0(array2, malloc) {
  const ptr = malloc(array2.length * 4, 4) >>> 0;
  for (let i = 0; i < array2.length; i++) {
    const add2 = addToExternrefTable0(array2[i]);
    getDataViewMemory0().setUint32(ptr + 4 * i, add2, true);
  }
  WASM_VECTOR_LEN = array2.length;
  return ptr;
}
function bls12381_min_pk_aggregate(signatures) {
  const ret = wasm.bls12381_min_pk_aggregate(signatures);
  if (ret[3]) {
    throw takeFromExternrefTable0(ret[2]);
  }
  var v1 = getArrayU8FromWasm0(ret[0], ret[1]).slice();
  wasm.__wbindgen_free(ret[0], ret[1] * 1, 1);
  return v1;
}
function passArray8ToWasm0(arg, malloc) {
  const ptr = malloc(arg.length * 1, 1) >>> 0;
  getUint8ArrayMemory0().set(arg, ptr / 1);
  WASM_VECTOR_LEN = arg.length;
  return ptr;
}
function bls12381_min_pk_verify(signature, public_key, msg) {
  const ptr0 = passArray8ToWasm0(signature, wasm.__wbindgen_malloc);
  const len0 = WASM_VECTOR_LEN;
  const ptr1 = passArray8ToWasm0(public_key, wasm.__wbindgen_malloc);
  const len1 = WASM_VECTOR_LEN;
  const ptr2 = passArray8ToWasm0(msg, wasm.__wbindgen_malloc);
  const len2 = WASM_VECTOR_LEN;
  const ret = wasm.bls12381_min_pk_verify(ptr0, len0, ptr1, len1, ptr2, len2);
  if (ret[2]) {
    throw takeFromExternrefTable0(ret[1]);
  }
  return ret[0] !== 0;
}
var BlobEncoderFinalization = typeof FinalizationRegistry === "undefined" ? { register: () => {
}, unregister: () => {
} } : new FinalizationRegistry((ptr) => wasm.__wbg_blobencoder_free(ptr >>> 0, 1));
var BlobEncoder = class {
  __destroy_into_raw() {
    const ptr = this.__wbg_ptr;
    this.__wbg_ptr = 0;
    BlobEncoderFinalization.unregister(this);
    return ptr;
  }
  free() {
    const ptr = this.__destroy_into_raw();
    wasm.__wbg_blobencoder_free(ptr, 0);
  }
  /**
   * Compute metadata for data without encoding it.
   * Returns only the essential fields needed for blob registration:
   * (blob_id, root_hash, unencoded_length, encoding_type)
   *
   * This avoids serializing all 2k sliver hashes across the JS/WASM boundary.
   * @param {Uint8Array} data
   * @returns {any}
   */
  compute_metadata(data) {
    const ret = wasm.blobencoder_compute_metadata(this.__wbg_ptr, data);
    if (ret[2]) {
      throw takeFromExternrefTable0(ret[1]);
    }
    return takeFromExternrefTable0(ret[0]);
  }
  /**
   * @param {number} n_shards
   */
  constructor(n_shards) {
    const ret = wasm.blobencoder_new(n_shards);
    if (ret[2]) {
      throw takeFromExternrefTable0(ret[1]);
    }
    this.__wbg_ptr = ret[0] >>> 0;
    BlobEncoderFinalization.register(this, this.__wbg_ptr, this);
    return this;
  }
  /**
   * Decode blob from BCS-encoded SliverData buffers.
   *
   * Arguments:
   * - blob_id: The blob identifier
   * - blob_size: The original unencoded blob size in bytes
   * - bcs_buffers: Vec<Uint8Array>, each containing BCS-encoded SliverData<Primary>
   * - output_buffer: Uint8Array to write decoded data into (must be exactly blob_size bytes)
   * @param {any} blob_id
   * @param {bigint} blob_size
   * @param {Uint8Array[]} bcs_buffers
   * @param {Uint8Array} output_buffer
   */
  decode(blob_id, blob_size, bcs_buffers, output_buffer) {
    const ptr0 = passArrayJsValueToWasm0(bcs_buffers, wasm.__wbindgen_malloc);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.blobencoder_decode(this.__wbg_ptr, blob_id, blob_size, ptr0, len0, output_buffer);
    if (ret[1]) {
      throw takeFromExternrefTable0(ret[0]);
    }
  }
  /**
   * Encode data and write BCS-encoded SliverData directly into pre-allocated buffers.
   *
   * Arguments:
   * - data: Input data to encode
   * - primary_buffers: Array of Uint8Array buffers (one per shard) for primary slivers
   * - secondary_buffers: Array of Uint8Array buffers (one per shard) for secondary slivers
   *
   * Each buffer will be written with BCS-encoded SliverData.
   *
   * Returns: JsValue with (metadata, root_hash)
   * @param {Uint8Array} data
   * @param {Array<any>} primary_buffers
   * @param {Array<any>} secondary_buffers
   * @returns {any}
   */
  encode(data, primary_buffers, secondary_buffers) {
    const ret = wasm.blobencoder_encode(this.__wbg_ptr, data, primary_buffers, secondary_buffers);
    if (ret[2]) {
      throw takeFromExternrefTable0(ret[1]);
    }
    return takeFromExternrefTable0(ret[0]);
  }
};
if (Symbol.dispose) BlobEncoder.prototype[Symbol.dispose] = BlobEncoder.prototype.free;
var EXPECTED_RESPONSE_TYPES = /* @__PURE__ */ new Set(["basic", "cors", "default"]);
async function __wbg_load(module2, imports) {
  if (typeof Response === "function" && module2 instanceof Response) {
    if (typeof WebAssembly.instantiateStreaming === "function") {
      try {
        return await WebAssembly.instantiateStreaming(module2, imports);
      } catch (e) {
        const validResponse = module2.ok && EXPECTED_RESPONSE_TYPES.has(module2.type);
        if (validResponse && module2.headers.get("Content-Type") !== "application/wasm") {
          console.warn("`WebAssembly.instantiateStreaming` failed because your server does not serve Wasm with `application/wasm` MIME type. Falling back to `WebAssembly.instantiate` which is slower. Original error:\n", e);
        } else {
          throw e;
        }
      }
    }
    const bytes = await module2.arrayBuffer();
    return await WebAssembly.instantiate(bytes, imports);
  } else {
    const instance = await WebAssembly.instantiate(module2, imports);
    if (instance instanceof WebAssembly.Instance) {
      return { instance, module: module2 };
    } else {
      return instance;
    }
  }
}
function __wbg_get_imports() {
  const imports = {};
  imports.wbg = {};
  imports.wbg.__wbg_Error_e83987f665cf5504 = function(arg0, arg1) {
    const ret = Error(getStringFromWasm0(arg0, arg1));
    return ret;
  };
  imports.wbg.__wbg_String_fed4d24b68977888 = function(arg0, arg1) {
    const ret = String(arg1);
    const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len1 = WASM_VECTOR_LEN;
    getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
    getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
  };
  imports.wbg.__wbg___wbindgen_boolean_get_6d5a1ee65bab5f68 = function(arg0) {
    const v = arg0;
    const ret = typeof v === "boolean" ? v : void 0;
    return isLikeNone(ret) ? 16777215 : ret ? 1 : 0;
  };
  imports.wbg.__wbg___wbindgen_debug_string_df47ffb5e35e6763 = function(arg0, arg1) {
    const ret = debugString(arg1);
    const ptr1 = passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    const len1 = WASM_VECTOR_LEN;
    getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
    getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
  };
  imports.wbg.__wbg___wbindgen_is_function_ee8a6c5833c90377 = function(arg0) {
    const ret = typeof arg0 === "function";
    return ret;
  };
  imports.wbg.__wbg___wbindgen_is_object_c818261d21f283a4 = function(arg0) {
    const val = arg0;
    const ret = typeof val === "object" && val !== null;
    return ret;
  };
  imports.wbg.__wbg___wbindgen_jsval_loose_eq_b664b38a2f582147 = function(arg0, arg1) {
    const ret = arg0 == arg1;
    return ret;
  };
  imports.wbg.__wbg___wbindgen_number_get_a20bf9b85341449d = function(arg0, arg1) {
    const obj = arg1;
    const ret = typeof obj === "number" ? obj : void 0;
    getDataViewMemory0().setFloat64(arg0 + 8 * 1, isLikeNone(ret) ? 0 : ret, true);
    getDataViewMemory0().setInt32(arg0 + 4 * 0, !isLikeNone(ret), true);
  };
  imports.wbg.__wbg___wbindgen_string_get_e4f06c90489ad01b = function(arg0, arg1) {
    const obj = arg1;
    const ret = typeof obj === "string" ? obj : void 0;
    var ptr1 = isLikeNone(ret) ? 0 : passStringToWasm0(ret, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
    var len1 = WASM_VECTOR_LEN;
    getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
    getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
  };
  imports.wbg.__wbg___wbindgen_throw_b855445ff6a94295 = function(arg0, arg1) {
    throw new Error(getStringFromWasm0(arg0, arg1));
  };
  imports.wbg.__wbg_call_e762c39fa8ea36bf = function() {
    return handleError(function(arg0, arg1) {
      const ret = arg0.call(arg1);
      return ret;
    }, arguments);
  };
  imports.wbg.__wbg_done_2042aa2670fb1db1 = function(arg0) {
    const ret = arg0.done;
    return ret;
  };
  imports.wbg.__wbg_get_7bed016f185add81 = function(arg0, arg1) {
    const ret = arg0[arg1 >>> 0];
    return ret;
  };
  imports.wbg.__wbg_get_efcb449f58ec27c2 = function() {
    return handleError(function(arg0, arg1) {
      const ret = Reflect.get(arg0, arg1);
      return ret;
    }, arguments);
  };
  imports.wbg.__wbg_instanceof_ArrayBuffer_70beb1189ca63b38 = function(arg0) {
    let result;
    try {
      result = arg0 instanceof ArrayBuffer;
    } catch (_) {
      result = false;
    }
    const ret = result;
    return ret;
  };
  imports.wbg.__wbg_instanceof_Uint8Array_20c8e73002f7af98 = function(arg0) {
    let result;
    try {
      result = arg0 instanceof Uint8Array;
    } catch (_) {
      result = false;
    }
    const ret = result;
    return ret;
  };
  imports.wbg.__wbg_isArray_96e0af9891d0945d = function(arg0) {
    const ret = Array.isArray(arg0);
    return ret;
  };
  imports.wbg.__wbg_isSafeInteger_d216eda7911dde36 = function(arg0) {
    const ret = Number.isSafeInteger(arg0);
    return ret;
  };
  imports.wbg.__wbg_iterator_e5822695327a3c39 = function() {
    const ret = Symbol.iterator;
    return ret;
  };
  imports.wbg.__wbg_length_69bca3cb64fc8748 = function(arg0) {
    const ret = arg0.length;
    return ret;
  };
  imports.wbg.__wbg_length_cdd215e10d9dd507 = function(arg0) {
    const ret = arg0.length;
    return ret;
  };
  imports.wbg.__wbg_new_1acc0b6eea89d040 = function() {
    const ret = new Object();
    return ret;
  };
  imports.wbg.__wbg_new_5a79be3ab53b8aa5 = function(arg0) {
    const ret = new Uint8Array(arg0);
    return ret;
  };
  imports.wbg.__wbg_new_e17d9f43105b08be = function() {
    const ret = new Array();
    return ret;
  };
  imports.wbg.__wbg_next_020810e0ae8ebcb0 = function() {
    return handleError(function(arg0) {
      const ret = arg0.next();
      return ret;
    }, arguments);
  };
  imports.wbg.__wbg_next_2c826fe5dfec6b6a = function(arg0) {
    const ret = arg0.next;
    return ret;
  };
  imports.wbg.__wbg_prototypesetcall_2a6620b6922694b2 = function(arg0, arg1, arg2) {
    Uint8Array.prototype.set.call(getArrayU8FromWasm0(arg0, arg1), arg2);
  };
  imports.wbg.__wbg_set_3fda3bac07393de4 = function(arg0, arg1, arg2) {
    arg0[arg1] = arg2;
  };
  imports.wbg.__wbg_set_9e6516df7b7d0f19 = function(arg0, arg1, arg2) {
    arg0.set(getArrayU8FromWasm0(arg1, arg2));
  };
  imports.wbg.__wbg_set_c213c871859d6500 = function(arg0, arg1, arg2) {
    arg0[arg1 >>> 0] = arg2;
  };
  imports.wbg.__wbg_value_692627309814bb8c = function(arg0) {
    const ret = arg0.value;
    return ret;
  };
  imports.wbg.__wbindgen_cast_2241b6af4c4b2941 = function(arg0, arg1) {
    const ret = getStringFromWasm0(arg0, arg1);
    return ret;
  };
  imports.wbg.__wbindgen_cast_4625c577ab2ec9ee = function(arg0) {
    const ret = BigInt.asUintN(64, arg0);
    return ret;
  };
  imports.wbg.__wbindgen_cast_d6cd19b81560fd6e = function(arg0) {
    const ret = arg0;
    return ret;
  };
  imports.wbg.__wbindgen_init_externref_table = function() {
    const table = wasm.__wbindgen_externrefs;
    const offset = table.grow(4);
    table.set(0, void 0);
    table.set(offset + 0, void 0);
    table.set(offset + 1, null);
    table.set(offset + 2, true);
    table.set(offset + 3, false);
    ;
  };
  return imports;
}
function __wbg_finalize_init(instance, module2) {
  wasm = instance.exports;
  __wbg_init.__wbindgen_wasm_module = module2;
  cachedDataViewMemory0 = null;
  cachedUint8ArrayMemory0 = null;
  wasm.__wbindgen_start();
  return wasm;
}
async function __wbg_init(module_or_path) {
  if (wasm !== void 0) return wasm;
  if (typeof module_or_path !== "undefined") {
    if (Object.getPrototypeOf(module_or_path) === Object.prototype) {
      ({ module_or_path } = module_or_path);
    } else {
      console.warn("using deprecated parameters for the initialization function; pass a single object instead");
    }
  }
  if (typeof module_or_path === "undefined") {
    module_or_path = new URL("walrus_wasm_bg.wasm", import_meta.url);
  }
  const imports = __wbg_get_imports();
  if (typeof module_or_path === "string" || typeof Request === "function" && module_or_path instanceof Request || typeof URL === "function" && module_or_path instanceof URL) {
    module_or_path = fetch(module_or_path);
  }
  const { instance, module: module2 } = await __wbg_load(await module_or_path, imports);
  return __wbg_finalize_init(instance, module2);
}
var walrus_wasm_default = __wbg_init;

// node_modules/@mysten/walrus/dist/esm/wasm.js
async function getWasmBindings(url) {
  await walrus_wasm_default({ module_or_path: url });
  function encodeBlob(nShards, bytes, encodingType = "RS2") {
    const encoder = new BlobEncoder(nShards);
    if (encodingType !== "RS2") {
      throw new Error(`Unsupported encoding type: ${encodingType}`);
    }
    const bufferSizes = computeBcsBufferSizes(bytes.length, nShards);
    const primaryBuffers = Array.from({ length: nShards }).map(
      () => new Uint8Array(bufferSizes.primary)
    );
    const secondaryBuffers = Array.from({ length: nShards }).map(
      () => new Uint8Array(bufferSizes.secondary)
    );
    const [metadata, rootHash] = encoder.encode(bytes, primaryBuffers, secondaryBuffers);
    return {
      primarySlivers: primaryBuffers,
      secondarySlivers: secondaryBuffers,
      blobId: blobIdFromBytes(new Uint8Array(metadata.blob_id)),
      metadata: metadata.metadata,
      rootHash: new Uint8Array(rootHash.Digest)
    };
  }
  function combineSignatures(confirmations, signerIndices) {
    const signature = bls12381_min_pk_aggregate(
      confirmations.map((confirmation) => fromBase64(confirmation.signature))
    );
    return {
      signers: signerIndices,
      serializedMessage: fromBase64(confirmations[0].serializedMessage),
      signature
    };
  }
  function decodePrimarySlivers(blobId, nShards, size, slivers, encodingType = "RS2") {
    const encoder = new BlobEncoder(nShards);
    if (encodingType !== "RS2") {
      throw new Error(`Unsupported encoding type: ${encodingType}`);
    }
    const blobSize = BigInt(size);
    const outputBuffer = new Uint8Array(Number(blobSize));
    encoder.decode(BlobId.serialize(blobId).toBytes(), blobSize, slivers, outputBuffer);
    return outputBuffer;
  }
  function getVerifySignature() {
    return (confirmation, publicKey) => bls12381_min_pk_verify(
      fromBase64(confirmation.signature),
      publicKey,
      fromBase64(confirmation.serializedMessage)
    );
  }
  function computeMetadata(nShards, bytes, encodingType = "RS2") {
    const encoder = new BlobEncoder(nShards);
    const [blobId, rootHash, unencodedLength, encType] = encoder.compute_metadata(bytes);
    if (encodingType !== "RS2") {
      throw new Error(`Unsupported encoding type: ${encodingType}`);
    }
    return {
      blobId: blobIdFromBytes(new Uint8Array(blobId)),
      rootHash: new Uint8Array(rootHash.Digest),
      unencodedLength: BigInt(unencodedLength),
      encodingType: encType
    };
  }
  return {
    encodeBlob,
    combineSignatures,
    decodePrimarySlivers,
    getVerifySignature,
    computeMetadata
  };
}
function uleb128Size(value) {
  let size = 1;
  value >>= 7;
  while (value !== 0) {
    size++;
    value >>= 7;
  }
  return size;
}
function computeBcsBufferSize(dataLength) {
  const ulebSize = uleb128Size(dataLength);
  return ulebSize + dataLength + 2 + 2;
}
function computeBcsBufferSizes(blobSize, nShards) {
  const { primarySymbols, secondarySymbols } = getSourceSymbols(nShards);
  let symbolSize = Math.floor((Math.max(blobSize, 1) - 1) / (primarySymbols * secondarySymbols)) + 1;
  if (symbolSize % 2 === 1) {
    symbolSize = symbolSize + 1;
  }
  const primarySliverSize = secondarySymbols * symbolSize;
  const secondarySliverSize = primarySymbols * symbolSize;
  const primaryBcsSize = computeBcsBufferSize(primarySliverSize);
  const secondaryBcsSize = computeBcsBufferSize(secondarySliverSize);
  return {
    nShards,
    primary: primaryBcsSize,
    secondary: secondaryBcsSize
  };
}

// node_modules/@mysten/walrus/dist/esm/upload-relay/client.js
var __typeError14 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck14 = (obj, member, msg) => member.has(obj) || __typeError14("Cannot " + msg);
var __privateGet13 = (obj, member, getter) => (__accessCheck14(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd14 = (obj, member, value) => member.has(obj) ? __typeError14("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet12 = (obj, member, value, setter) => (__accessCheck14(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateMethod9 = (obj, member, method) => (__accessCheck14(obj, member, "access private method"), method);
var _fetch3;
var _timeout2;
var _onError2;
var _UploadRelayClient_instances;
var request_fn2;
var UploadRelayClient = class {
  constructor({ host, fetch: overriddenFetch, timeout, onError }) {
    __privateAdd14(this, _UploadRelayClient_instances);
    __privateAdd14(this, _fetch3);
    __privateAdd14(this, _timeout2);
    __privateAdd14(this, _onError2);
    this.host = host;
    __privateSet12(this, _fetch3, overriddenFetch ?? globalThis.fetch);
    __privateSet12(this, _timeout2, timeout ?? 3e4);
    __privateSet12(this, _onError2, onError);
  }
  async tipConfig() {
    const response = await __privateMethod9(this, _UploadRelayClient_instances, request_fn2).call(this, {
      method: "GET",
      path: "/v1/tip-config"
    });
    const data = await response.json();
    if (typeof data === "string") {
      return null;
    }
    if ("const" in data.send_tip.kind) {
      return {
        address: data.send_tip.address,
        kind: {
          const: data.send_tip.kind.const
        }
      };
    }
    return {
      address: data.send_tip.address,
      kind: {
        linear: {
          base: data.send_tip.kind.linear.base,
          perEncodedKib: data.send_tip.kind.linear.encoded_size_mul_per_kib
        }
      }
    };
  }
  async writeBlob({
    blobId,
    nonce,
    txDigest,
    blob,
    deletable,
    blobObjectId,
    requiresTip,
    encodingType,
    ...options
  }) {
    const query = new URLSearchParams({
      blob_id: blobId
    });
    if (requiresTip) {
      query.set("nonce", urlSafeBase64(nonce));
      query.set("tx_id", txDigest);
    }
    if (deletable) {
      query.set("deletable_blob_object", blobObjectId);
    }
    if (encodingType) {
      query.set("encoding_type", encodingType);
    }
    const response = await __privateMethod9(this, _UploadRelayClient_instances, request_fn2).call(this, {
      method: "POST",
      path: `/v1/blob-upload-relay?${query.toString()}`,
      body: blob,
      ...options
    });
    const data = await response.json();
    return {
      blobId,
      certificate: {
        signers: data.confirmation_certificate.signers,
        serializedMessage: new Uint8Array(data.confirmation_certificate.serialized_message),
        signature: fromUrlSafeBase64(data.confirmation_certificate.signature)
      }
    };
  }
};
_fetch3 = /* @__PURE__ */ new WeakMap();
_timeout2 = /* @__PURE__ */ new WeakMap();
_onError2 = /* @__PURE__ */ new WeakMap();
_UploadRelayClient_instances = /* @__PURE__ */ new WeakSet();
request_fn2 = async function(options) {
  var _a, _b, _c;
  const { signal, timeout, ...init } = options;
  if (signal?.aborted) {
    throw new UserAbortError();
  }
  const timeoutSignal = AbortSignal.timeout(timeout ?? __privateGet13(this, _timeout2));
  let response;
  try {
    const fetch2 = __privateGet13(this, _fetch3);
    response = await fetch2(`${this.host}${options.path}`, {
      ...init,
      signal: signal ? AbortSignal.any([timeoutSignal, signal]) : timeoutSignal
    });
  } catch (error) {
    if (signal?.aborted) {
      throw new UserAbortError();
    }
    if (error instanceof Error && error.name === "AbortError") {
      const error2 = new ConnectionTimeoutError();
      (_a = __privateGet13(this, _onError2)) == null ? void 0 : _a.call(this, error2);
      throw error2;
    }
    (_b = __privateGet13(this, _onError2)) == null ? void 0 : _b.call(this, error);
    throw error;
  }
  if (!response.ok) {
    const errorText = await response.text().catch((reason) => reason);
    const errorJSON = safeParseJSON2(errorText);
    const errorMessage = errorJSON ? void 0 : errorText;
    const error = StorageNodeAPIError.generate(response.status, errorJSON, errorMessage);
    (_c = __privateGet13(this, _onError2)) == null ? void 0 : _c.call(this, error);
    throw error;
  }
  return response;
};
function safeParseJSON2(value) {
  try {
    return JSON.parse(value);
  } catch {
    return void 0;
  }
}

// node_modules/@mysten/walrus/dist/esm/utils/quilts.js
var QUILT_INDEX_SIZE_BYTES_LENGTH = 4;
var QUILT_VERSION_BYTES_LENGTH = 1;
var QUILT_INDEX_PREFIX_SIZE = QUILT_VERSION_BYTES_LENGTH + QUILT_INDEX_SIZE_BYTES_LENGTH;
var QUILT_PATCH_BLOB_HEADER_SIZE = 1 + 4 + 1;
var BLOB_IDENTIFIER_SIZE_BYTES_LENGTH = 2;
var TAGS_SIZE_BYTES_LENGTH = 2;
var MAX_BLOB_IDENTIFIER_BYTES_LENGTH = (1 << 8 * BLOB_IDENTIFIER_SIZE_BYTES_LENGTH) - 1;
var MAX_NUM_SLIVERS_FOR_QUILT_INDEX = 10;
var HAS_TAGS_FLAG = 1 << 0;
function computeSymbolSize(blobsSizes, nColumns, nRows, maxNumColumnsForQuiltIndex, encodingType = "RS2") {
  if (blobsSizes.length > nColumns) {
    throw new Error("Too many blobs, the number of blobs must be less than the number of columns");
  }
  if (blobsSizes.length === 0) {
    throw new Error("No blobs provided");
  }
  let minVal = Math.max(
    blobsSizes.reduce((acc, size) => acc + size, 0) / (nColumns * nRows),
    blobsSizes[0] / (nRows * maxNumColumnsForQuiltIndex),
    Math.ceil(QUILT_INDEX_PREFIX_SIZE / nRows)
  );
  let maxVal = Math.ceil(Math.max(...blobsSizes) / (nColumns / blobsSizes.length) * nRows);
  while (minVal < maxVal) {
    const mid = Math.floor((minVal + maxVal) / 2);
    if (canBlobsFitIntoMatrix(blobsSizes, nColumns, mid * nRows)) {
      maxVal = mid;
    } else {
      minVal = mid + 1;
    }
  }
  const symbolSize = Math.ceil(minVal / REQUIRED_ALIGNMENT_BY_ENCODING_TYPE[encodingType]) * REQUIRED_ALIGNMENT_BY_ENCODING_TYPE[encodingType];
  if (!canBlobsFitIntoMatrix(blobsSizes, nColumns, symbolSize * nRows)) {
    throw new Error("Quilt oversize");
  }
  if (symbolSize > MAX_SYMBOL_SIZE_BY_ENCODING_TYPE[encodingType]) {
    throw new Error(
      `Quilt oversize: the resulting symbol size ${symbolSize} is larger than the maximum symbol size ${MAX_SYMBOL_SIZE_BY_ENCODING_TYPE[encodingType]}; remove some blobs`
    );
  }
  return symbolSize;
}
function canBlobsFitIntoMatrix(blobsSizes, nColumns, columnSize) {
  return blobsSizes.reduce((acc, size) => acc + Math.ceil(size / columnSize), 0) <= nColumns;
}
function parseQuiltPatchId(id) {
  return QuiltPatchId.parse(fromUrlSafeBase64(id));
}
function encodeQuiltPatchId(id) {
  return urlSafeBase64(QuiltPatchId.serialize(id).toBytes());
}
function parseWalrusId(id) {
  const bytes = fromUrlSafeBase64(id);
  if (bytes.length === 32) {
    return {
      kind: "blob",
      id
    };
  }
  return {
    kind: "quiltPatch",
    id: parseQuiltPatchId(id)
  };
}
function encodeQuilt({ blobs, numShards, encodingType }) {
  const { primarySymbols: nRows, secondarySymbols: nCols } = getSourceSymbols(
    numShards,
    encodingType
  );
  const sortedBlobs = blobs.sort((a, b) => a.identifier < b.identifier ? -1 : 1);
  const identifiers = /* @__PURE__ */ new Set();
  const index = {
    patches: []
  };
  const tags = sortedBlobs.map(
    (blob) => blob.tags && Object.keys(blob.tags).length > 0 ? QuiltPatchTags.serialize(blob.tags).toBytes() : null
  );
  for (const blob of sortedBlobs) {
    if (identifiers.has(blob.identifier)) {
      throw new Error(`Duplicate blob identifier: ${blob.identifier}`);
    }
    identifiers.add(blob.identifier);
    index.patches.push({
      startIndex: 0,
      endIndex: 0,
      identifier: blob.identifier,
      tags: blob.tags ?? {}
    });
  }
  const indexSize = QUILT_INDEX_PREFIX_SIZE + QuiltIndexV1.serialize(index).toBytes().length;
  const blobMetadata = sortedBlobs.map((blob, i) => {
    const identifierBytes = bcs.string().serialize(blob.identifier).toBytes();
    let metadataSize = QUILT_PATCH_BLOB_HEADER_SIZE + BLOB_IDENTIFIER_SIZE_BYTES_LENGTH + identifierBytes.length;
    let mask = 0;
    let offset = 0;
    if (tags[i]) {
      metadataSize += TAGS_SIZE_BYTES_LENGTH + tags[i].length;
      mask |= HAS_TAGS_FLAG << 0;
    }
    const metadata = new Uint8Array(metadataSize);
    const metadataView = new DataView(metadata.buffer);
    const header = QuiltPatchBlobHeader.serialize({
      version: 1,
      length: metadataSize - QUILT_PATCH_BLOB_HEADER_SIZE + blob.contents.length,
      mask
    }).toBytes();
    metadata.set(header, offset);
    offset += header.length;
    metadataView.setUint16(offset, identifierBytes.length, true);
    offset += BLOB_IDENTIFIER_SIZE_BYTES_LENGTH;
    metadata.set(identifierBytes, offset);
    offset += identifierBytes.length;
    if (tags[i]) {
      metadataView.setUint16(offset, tags[i].length, true);
      offset += TAGS_SIZE_BYTES_LENGTH;
      metadata.set(tags[i], offset);
      offset += tags[i].length;
    }
    return metadata;
  });
  const blobSizes = [
    indexSize,
    ...sortedBlobs.map((blob, i) => {
      if (blob.identifier.length > MAX_BLOB_IDENTIFIER_BYTES_LENGTH) {
        throw new Error(`Blob identifier too long: ${blob.identifier}`);
      }
      return blobMetadata[i].length + blob.contents.length;
    })
  ];
  const symbolSize = computeSymbolSize(
    blobSizes,
    nCols,
    nRows,
    MAX_NUM_SLIVERS_FOR_QUILT_INDEX,
    encodingType
  );
  const rowSize = symbolSize * nCols;
  const columnSize = symbolSize * nRows;
  const indexColumnsNeeded = Math.ceil(indexSize / columnSize);
  if (indexColumnsNeeded > MAX_NUM_SLIVERS_FOR_QUILT_INDEX) {
    throw new Error("Index too large");
  }
  const quilt = new Uint8Array(rowSize * nRows);
  let currentColumn = indexColumnsNeeded;
  for (let i = 0; i < sortedBlobs.length; i++) {
    const blob = sortedBlobs[i];
    index.patches[i].startIndex = currentColumn;
    currentColumn += writeBlobToQuilt(
      quilt,
      blob.contents,
      rowSize,
      columnSize,
      symbolSize,
      currentColumn,
      blobMetadata[i]
    );
    index.patches[i].endIndex = currentColumn;
  }
  const indexBytes = QuiltIndexV1.serialize(index).toBytes();
  const quiltIndex = new Uint8Array(QUILT_INDEX_PREFIX_SIZE + indexBytes.length);
  const view = new DataView(quiltIndex.buffer);
  view.setUint8(0, 1);
  view.setUint32(1, indexBytes.length, true);
  quiltIndex.set(indexBytes, QUILT_INDEX_PREFIX_SIZE);
  writeBlobToQuilt(quilt, quiltIndex, rowSize, columnSize, symbolSize, 0);
  return { quilt, index };
}
function writeBlobToQuilt(quilt, blob, rowSize, columnSize, symbolSize, startColumn, prefix) {
  const nRows = columnSize / symbolSize;
  let bytesWritten = 0;
  if (rowSize % symbolSize !== 0) {
    throw new Error("Row size must be divisible by symbol size");
  }
  if (columnSize % symbolSize !== 0) {
    throw new Error("Column size must be divisible by symbol size");
  }
  if (prefix) {
    writeBytes(prefix);
  }
  writeBytes(blob);
  return Math.ceil(bytesWritten / columnSize);
  function writeBytes(bytes) {
    const offset = bytesWritten;
    const symbolsToSkip = Math.floor(offset / symbolSize);
    let remainingOffset = offset % symbolSize;
    let currentCol = startColumn + Math.floor(symbolsToSkip / nRows);
    let currentRow = symbolsToSkip % nRows;
    let index = 0;
    while (index < bytes.length) {
      const baseIndex = currentRow * rowSize + currentCol * symbolSize;
      const startIndex = baseIndex + remainingOffset;
      const len = Math.min(symbolSize - remainingOffset, bytes.length - index);
      for (let i = 0; i < len; i++) {
        quilt[startIndex + i] = bytes[index + i];
      }
      index += len;
      remainingOffset = 0;
      currentRow = (currentRow + 1) % nRows;
      if (currentRow === 0) {
        currentCol++;
      }
    }
    bytesWritten += bytes.length;
  }
}

// node_modules/@mysten/walrus/dist/esm/files/readers/quilt-file.js
var __typeError15 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck15 = (obj, member, msg) => member.has(obj) || __typeError15("Cannot " + msg);
var __privateGet14 = (obj, member, getter) => (__accessCheck15(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd15 = (obj, member, value) => member.has(obj) ? __typeError15("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet13 = (obj, member, value, setter) => (__accessCheck15(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var _quilt;
var _sliverIndex;
var _identifier;
var _tags;
var QuiltFileReader = class {
  constructor({
    quilt,
    sliverIndex,
    identifier,
    tags
  }) {
    __privateAdd15(this, _quilt);
    __privateAdd15(this, _sliverIndex);
    __privateAdd15(this, _identifier);
    __privateAdd15(this, _tags);
    __privateSet13(this, _quilt, quilt);
    __privateSet13(this, _sliverIndex, sliverIndex);
    __privateSet13(this, _identifier, identifier ?? null);
    __privateSet13(this, _tags, tags);
  }
  async getBytes() {
    const { blobContents, identifier, tags } = await __privateGet14(this, _quilt).readBlob(__privateGet14(this, _sliverIndex));
    __privateSet13(this, _identifier, identifier);
    __privateSet13(this, _tags, tags ?? {});
    return blobContents;
  }
  async getIdentifier() {
    if (__privateGet14(this, _identifier) !== null) {
      return __privateGet14(this, _identifier);
    }
    const header = await __privateGet14(this, _quilt).getBlobHeader(__privateGet14(this, _sliverIndex));
    __privateSet13(this, _identifier, header.identifier);
    return __privateGet14(this, _identifier);
  }
  async getTags() {
    if (__privateGet14(this, _tags) !== void 0) {
      return __privateGet14(this, _tags);
    }
    const header = await __privateGet14(this, _quilt).getBlobHeader(__privateGet14(this, _sliverIndex));
    __privateSet13(this, _tags, header.tags ?? {});
    return __privateGet14(this, _tags);
  }
};
_quilt = /* @__PURE__ */ new WeakMap();
_sliverIndex = /* @__PURE__ */ new WeakMap();
_identifier = /* @__PURE__ */ new WeakMap();
_tags = /* @__PURE__ */ new WeakMap();

// node_modules/@mysten/walrus/dist/esm/files/readers/quilt.js
var __typeError16 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck16 = (obj, member, msg) => member.has(obj) || __typeError16("Cannot " + msg);
var __privateGet15 = (obj, member, getter) => (__accessCheck16(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd16 = (obj, member, value) => member.has(obj) ? __typeError16("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet14 = (obj, member, value, setter) => (__accessCheck16(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateMethod10 = (obj, member, method) => (__accessCheck16(obj, member, "access private method"), method);
var _blob;
var _cache3;
var _QuiltReader_instances;
var readBytesFromSlivers_fn;
var readBytesFromBlob_fn;
var readBytes_fn;
var QuiltReader = class {
  constructor({ blob }) {
    __privateAdd16(this, _QuiltReader_instances);
    __privateAdd16(this, _blob);
    __privateAdd16(this, _cache3, new ClientCache());
    __privateSet14(this, _blob, blob);
  }
  async getBlobHeader(sliverIndex) {
    return __privateGet15(this, _cache3).read(["getBlobHeader", sliverIndex.toString()], async () => {
      const blobHeader = QuiltPatchBlobHeader.parse(
        await __privateMethod10(this, _QuiltReader_instances, readBytes_fn).call(this, sliverIndex, QUILT_PATCH_BLOB_HEADER_SIZE)
      );
      let offset = QUILT_PATCH_BLOB_HEADER_SIZE;
      let blobSize = blobHeader.length;
      const identifierLength = new DataView(
        (await __privateMethod10(this, _QuiltReader_instances, readBytes_fn).call(this, sliverIndex, 2, offset)).buffer
      ).getUint16(0, true);
      blobSize -= 2 + identifierLength;
      offset += 2;
      const identifier = bcs.string().parse(await __privateMethod10(this, _QuiltReader_instances, readBytes_fn).call(this, sliverIndex, identifierLength, offset));
      offset += identifierLength;
      let tags = null;
      if (blobHeader.mask & HAS_TAGS_FLAG) {
        const tagsSize = new DataView(
          (await __privateMethod10(this, _QuiltReader_instances, readBytes_fn).call(this, sliverIndex, 2, offset)).buffer
        ).getUint16(0, true);
        offset += 2;
        tags = QuiltPatchTags.parse(await __privateMethod10(this, _QuiltReader_instances, readBytes_fn).call(this, sliverIndex, tagsSize, offset));
        blobSize -= tagsSize + 2;
        offset += tagsSize;
      }
      return {
        identifier,
        tags,
        blobSize,
        contentOffset: offset
      };
    });
  }
  async readBlob(sliverIndex) {
    const { identifier, tags, blobSize, contentOffset } = await this.getBlobHeader(sliverIndex);
    const blobContents = await __privateMethod10(this, _QuiltReader_instances, readBytes_fn).call(this, sliverIndex, blobSize, contentOffset);
    return {
      identifier,
      tags,
      blobContents
    };
  }
  readerForPatchId(id) {
    const { quiltId, patchId } = parseQuiltPatchId(id);
    if (quiltId !== __privateGet15(this, _blob).blobId) {
      throw new Error(
        `The requested patch ${patchId} is not part of the quilt ${__privateGet15(this, _blob).blobId}`
      );
    }
    return new QuiltFileReader({ quilt: this, sliverIndex: patchId.startIndex });
  }
  async readIndex() {
    const header = new DataView((await __privateMethod10(this, _QuiltReader_instances, readBytes_fn).call(this, 0, 5)).buffer);
    const version = header.getUint8(0);
    if (version !== 1) {
      throw new Error(`Unsupported quilt version ${version}`);
    }
    const indexSize = header.getUint32(1, true);
    const indexBytes = await __privateMethod10(this, _QuiltReader_instances, readBytes_fn).call(this, 0, indexSize, 5);
    const columnSize = await __privateGet15(this, _blob).getColumnSize();
    const indexSlivers = Math.ceil(indexSize / columnSize);
    const index = QuiltIndexV1.parse(indexBytes);
    return index.patches.map((patch, i) => {
      const startIndex = i === 0 ? indexSlivers : index.patches[i - 1].endIndex;
      const reader = new QuiltFileReader({
        quilt: this,
        sliverIndex: startIndex,
        identifier: patch.identifier,
        tags: patch.tags
      });
      return {
        identifier: patch.identifier,
        patchId: urlSafeBase64(
          QuiltPatchId.serialize({
            quiltId: __privateGet15(this, _blob).blobId,
            patchId: {
              version: 1,
              startIndex,
              endIndex: patch.endIndex
            }
          }).toBytes()
        ),
        tags: patch.tags,
        reader
      };
    });
  }
};
_blob = /* @__PURE__ */ new WeakMap();
_cache3 = /* @__PURE__ */ new WeakMap();
_QuiltReader_instances = /* @__PURE__ */ new WeakSet();
readBytesFromSlivers_fn = async function(sliver, length, offset = 0, columnSize) {
  if (!length) {
    return new Uint8Array(0);
  }
  __privateGet15(this, _blob).getSecondarySliver({ sliverIndex: sliver }).catch(() => {
  });
  columnSize = columnSize ?? await __privateGet15(this, _blob).getColumnSize();
  const columnOffset = Math.floor(offset / columnSize);
  let remainingOffset = offset % columnSize;
  const bytes = new Uint8Array(length);
  let bytesRead = 0;
  const nSlivers = Math.ceil(length / columnSize);
  const slivers = new Array(nSlivers).fill(0).map((_, i) => __privateGet15(this, _blob).getSecondarySliver({ sliverIndex: sliver + columnOffset + i }));
  slivers.forEach((p) => p.catch(() => {
  }));
  for (const sliverPromise of slivers) {
    const sliver2 = await sliverPromise;
    let chunk2 = remainingOffset > 0 ? sliver2.subarray(remainingOffset) : sliver2;
    remainingOffset -= chunk2.length;
    if (chunk2.length > length - bytesRead) {
      chunk2 = chunk2.subarray(0, length - bytesRead);
    }
    bytes.set(chunk2, bytesRead);
    bytesRead += chunk2.length;
    if (bytesRead >= length) {
      break;
    }
  }
  return bytes;
};
readBytesFromBlob_fn = async function(startColumn, length, offset = 0) {
  const result = new Uint8Array(length);
  if (!length) {
    return result;
  }
  const blob = await __privateGet15(this, _blob).getBytes();
  const [rowSize, symbolSize] = await Promise.all([
    __privateGet15(this, _blob).getRowSize(),
    __privateGet15(this, _blob).getSymbolSize()
  ]);
  const nRows = blob.length / rowSize;
  const symbolsToSkip = Math.floor(offset / symbolSize);
  let remainingOffset = offset % symbolSize;
  let currentCol = startColumn + Math.floor(symbolsToSkip / nRows);
  let currentRow = symbolsToSkip % nRows;
  let bytesRead = 0;
  while (bytesRead < length) {
    const baseIndex = currentRow * rowSize + currentCol * symbolSize;
    const startIndex = baseIndex + remainingOffset;
    const endIndex = Math.min(
      baseIndex + symbolSize,
      startIndex + length - bytesRead,
      blob.length
    );
    if (startIndex >= blob.length) {
      throw new Error("Index out of bounds");
    }
    const size = endIndex - startIndex;
    for (let i = 0; i < size; i++) {
      result[bytesRead + i] = blob[startIndex + i];
    }
    bytesRead += size;
    remainingOffset = 0;
    currentRow = (currentRow + 1) % nRows;
    if (currentRow === 0) {
      currentCol += 1;
    }
  }
  return result;
};
readBytes_fn = async function(sliver, length, offset = 0, columnSize) {
  if (__privateGet15(this, _blob).hasStartedLoadingFullBlob) {
    return __privateMethod10(this, _QuiltReader_instances, readBytesFromBlob_fn).call(this, sliver, length, offset);
  }
  try {
    const bytes = await __privateMethod10(this, _QuiltReader_instances, readBytesFromSlivers_fn).call(this, sliver, length, offset, columnSize);
    return bytes;
  } catch {
    return __privateMethod10(this, _QuiltReader_instances, readBytesFromBlob_fn).call(this, sliver, length, offset);
  }
};

// node_modules/@mysten/walrus/dist/esm/files/readers/blob.js
var __typeError17 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck17 = (obj, member, msg) => member.has(obj) || __typeError17("Cannot " + msg);
var __privateGet16 = (obj, member, getter) => (__accessCheck17(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd17 = (obj, member, value) => member.has(obj) ? __typeError17("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet15 = (obj, member, value, setter) => (__accessCheck17(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var _cache4;
var _client2;
var _secondarySlivers;
var _numShards;
var BlobReader = class {
  constructor({ client, blobId, numShards }) {
    __privateAdd17(this, _cache4, new ClientCache());
    __privateAdd17(this, _client2);
    __privateAdd17(this, _secondarySlivers, /* @__PURE__ */ new Map());
    this.hasStartedLoadingFullBlob = false;
    __privateAdd17(this, _numShards);
    __privateSet15(this, _client2, client);
    this.blobId = blobId;
    __privateSet15(this, _numShards, numShards);
  }
  async getIdentifier() {
    return null;
  }
  async getTags() {
    return {};
  }
  getQuiltReader() {
    return new QuiltReader({ blob: this });
  }
  async getBytes() {
    return __privateGet16(this, _cache4).read(["getBytes"], async () => {
      this.hasStartedLoadingFullBlob = true;
      try {
        const blob = await __privateGet16(this, _client2).readBlob({ blobId: this.blobId });
        return blob;
      } catch (error) {
        this.hasStartedLoadingFullBlob = false;
        throw error;
      }
    });
  }
  getMetadata() {
    return __privateGet16(this, _cache4).read(
      ["getMetadata"],
      () => __privateGet16(this, _client2).getBlobMetadata({ blobId: this.blobId })
    );
  }
  async getColumnSize() {
    return __privateGet16(this, _cache4).read(["getColumnSize"], async () => {
      const loadingSlivers = [...__privateGet16(this, _secondarySlivers).values()];
      if (loadingSlivers.length > 0) {
        const sliver = await Promise.any(loadingSlivers).catch(() => null);
        if (sliver) {
          return sliver.length;
        }
      }
      if (this.hasStartedLoadingFullBlob) {
        const blob = await this.getBytes();
        const { columnSize: columnSize2 } = getSizes(blob.length, __privateGet16(this, _numShards));
        return columnSize2;
      }
      const metadata = await this.getMetadata();
      const { columnSize } = getSizes(
        Number(metadata.metadata.V1.unencoded_length),
        __privateGet16(this, _numShards)
      );
      return columnSize;
    });
  }
  async getSymbolSize() {
    const columnSize = await this.getColumnSize();
    const { primarySymbols } = getSourceSymbols(__privateGet16(this, _numShards));
    if (columnSize % primarySymbols !== 0) {
      throw new Error("column size should be divisible by primary symbols");
    }
    return columnSize / primarySymbols;
  }
  async getRowSize() {
    const symbolSize = await this.getSymbolSize();
    const { secondarySymbols } = getSourceSymbols(__privateGet16(this, _numShards));
    return symbolSize * secondarySymbols;
  }
  async getSecondarySliver({ sliverIndex, signal }) {
    if (__privateGet16(this, _secondarySlivers).has(sliverIndex)) {
      return __privateGet16(this, _secondarySlivers).get(sliverIndex);
    }
    const sliverPromise = __privateGet16(this, _client2).getSecondarySliver({
      blobId: this.blobId,
      index: sliverIndex,
      signal
    }).then((sliver) => SliverData.parse(sliver).symbols.data);
    __privateGet16(this, _secondarySlivers).set(sliverIndex, sliverPromise);
    try {
      const sliver = await sliverPromise;
      __privateGet16(this, _secondarySlivers).set(sliverIndex, sliver);
      return sliver;
    } catch (error) {
      __privateGet16(this, _secondarySlivers).delete(sliverIndex);
      throw error;
    }
  }
};
_cache4 = /* @__PURE__ */ new WeakMap();
_client2 = /* @__PURE__ */ new WeakMap();
_secondarySlivers = /* @__PURE__ */ new WeakMap();
_numShards = /* @__PURE__ */ new WeakMap();

// node_modules/@mysten/walrus/dist/esm/files/readers/local.js
var __typeError18 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck18 = (obj, member, msg) => member.has(obj) || __typeError18("Cannot " + msg);
var __privateGet17 = (obj, member, getter) => (__accessCheck18(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd18 = (obj, member, value) => member.has(obj) ? __typeError18("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet16 = (obj, member, value, setter) => (__accessCheck18(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var _contents;
var _identifier2;
var _tags2;
var LocalReader = class {
  constructor({
    contents,
    identifier,
    tags
  }) {
    __privateAdd18(this, _contents);
    __privateAdd18(this, _identifier2);
    __privateAdd18(this, _tags2);
    __privateSet16(this, _contents, contents);
    __privateSet16(this, _identifier2, identifier ?? null);
    __privateSet16(this, _tags2, tags ?? {});
  }
  async getBytes() {
    if ("arrayBuffer" in __privateGet17(this, _contents)) {
      return new Uint8Array(await __privateGet17(this, _contents).arrayBuffer());
    }
    return __privateGet17(this, _contents);
  }
  async getIdentifier() {
    return __privateGet17(this, _identifier2);
  }
  async getTags() {
    return __privateGet17(this, _tags2);
  }
};
_contents = /* @__PURE__ */ new WeakMap();
_identifier2 = /* @__PURE__ */ new WeakMap();
_tags2 = /* @__PURE__ */ new WeakMap();

// node_modules/@mysten/walrus/dist/esm/files/file.js
var __typeError19 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck19 = (obj, member, msg) => member.has(obj) || __typeError19("Cannot " + msg);
var __privateGet18 = (obj, member, getter) => (__accessCheck19(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd19 = (obj, member, value) => member.has(obj) ? __typeError19("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet17 = (obj, member, value, setter) => (__accessCheck19(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var _reader;
var _WalrusFile = class _WalrusFile2 {
  constructor({ reader }) {
    __privateAdd19(this, _reader);
    __privateSet17(this, _reader, reader);
  }
  static from(options) {
    return new _WalrusFile2({
      reader: new LocalReader(options)
    });
  }
  getIdentifier() {
    return __privateGet18(this, _reader).getIdentifier();
  }
  getTags() {
    return __privateGet18(this, _reader).getTags();
  }
  bytes() {
    return __privateGet18(this, _reader).getBytes();
  }
  async text() {
    const bytes = await this.bytes();
    return new TextDecoder().decode(bytes);
  }
  async json() {
    return JSON.parse(await this.text());
  }
};
_reader = /* @__PURE__ */ new WeakMap();
var WalrusFile = _WalrusFile;

// node_modules/@mysten/walrus/dist/esm/files/blob.js
var __typeError20 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck20 = (obj, member, msg) => member.has(obj) || __typeError20("Cannot " + msg);
var __privateGet19 = (obj, member, getter) => (__accessCheck20(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd20 = (obj, member, value) => member.has(obj) ? __typeError20("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet18 = (obj, member, value, setter) => (__accessCheck20(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateMethod11 = (obj, member, method) => (__accessCheck20(obj, member, "access private method"), method);
var _reader2;
var _client3;
var _cache5;
var _WalrusBlob_instances;
var blobStatus_fn;
var WalrusBlob = class {
  constructor({ reader, client }) {
    __privateAdd20(this, _WalrusBlob_instances);
    __privateAdd20(this, _reader2);
    __privateAdd20(this, _client3);
    __privateAdd20(this, _cache5, new ClientCache());
    __privateSet18(this, _reader2, reader);
    __privateSet18(this, _client3, client);
  }
  // Get the blob as a file (i.e. do not use Quilt encoding)
  asFile() {
    return new WalrusFile({ reader: __privateGet19(this, _reader2) });
  }
  async blobId() {
    return __privateGet19(this, _reader2).blobId;
  }
  // Gets quilt-based files associated with this blob.
  async files(filters = {}) {
    const quiltReader = await __privateGet19(this, _reader2).getQuiltReader();
    const index = await quiltReader.readIndex();
    const files = [];
    for (const patch of index) {
      if (filters.ids && !filters.ids.includes(patch.patchId)) {
        continue;
      }
      if (filters.identifiers && !filters.identifiers.includes(patch.identifier)) {
        continue;
      }
      if (filters.tags && !filters.tags.some(
        (tags) => Object.entries(tags).every(([tagName, tagValue]) => patch.tags[tagName] === tagValue)
      )) {
        continue;
      }
      files.push(new WalrusFile({ reader: quiltReader.readerForPatchId(patch.patchId) }));
    }
    return files;
  }
  async exists() {
    const status = await __privateMethod11(this, _WalrusBlob_instances, blobStatus_fn).call(this);
    return status.type === "permanent" || status.type === "deletable";
  }
  async storedUntil() {
    const status = await __privateMethod11(this, _WalrusBlob_instances, blobStatus_fn).call(this);
    if (status.type === "permanent") {
      return status.endEpoch;
    }
    return null;
  }
};
_reader2 = /* @__PURE__ */ new WeakMap();
_client3 = /* @__PURE__ */ new WeakMap();
_cache5 = /* @__PURE__ */ new WeakMap();
_WalrusBlob_instances = /* @__PURE__ */ new WeakSet();
blobStatus_fn = async function() {
  return __privateGet19(this, _cache5).read(
    ["blobStatus", __privateGet19(this, _reader2).blobId],
    () => __privateGet19(this, _client3).getVerifiedBlobStatus({ blobId: __privateGet19(this, _reader2).blobId })
  );
};

// node_modules/@mysten/walrus/dist/esm/utils/retry.js
async function retry(fn, options) {
  let remaining = options.count ?? 3;
  while (remaining > 0) {
    try {
      remaining -= 1;
      return await fn();
    } catch (error) {
      if (remaining <= 0 || options.condition && !options.condition(error)) {
        throw error;
      }
      if (options.delay) {
        await new Promise(
          (resolve) => setTimeout(
            resolve,
            (options.delay ?? 1e3) + (options.jitter ? Math.random() * options.jitter : 0)
          )
        );
      }
    }
  }
  throw new Error("Retry count exceeded");
}

// node_modules/@mysten/walrus/dist/esm/client.js
var __typeError21 = (msg) => {
  throw TypeError(msg);
};
var __accessCheck21 = (obj, member, msg) => member.has(obj) || __typeError21("Cannot " + msg);
var __privateGet20 = (obj, member, getter) => (__accessCheck21(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd21 = (obj, member, value) => member.has(obj) ? __typeError21("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet19 = (obj, member, value, setter) => (__accessCheck21(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateMethod12 = (obj, member, method) => (__accessCheck21(obj, member, "access private method"), method);
var _storageNodeClient;
var _wasmUrl;
var _packageConfig;
var _suiClient;
var _objectLoader;
var _blobMetadataConcurrencyLimit;
var _readCommittee;
var _cache6;
var _uploadRelayConfig;
var _uploadRelayClient;
var _WalrusClient_instances;
var walType_fn;
var getPackageId_fn;
var getWalrusPackageId_fn;
var wasmBindings_fn;
var internalReadBlob_fn;
var getCertificationEpoch_fn;
var getReadCommittee_fn;
var forceGetReadCommittee_fn;
var storageCostFromEncodedSize_fn;
var withWal_fn;
var loadTipConfig_fn;
var getCreatedBlob_fn;
var writeBlobAttributesForRef_fn;
var executeTransaction_fn;
var getCommittee_fn;
var getActiveCommittee_fn;
var stakingPool_fn;
var getNodeByShardIndex_fn;
var retryOnPossibleEpochChange_fn;
var _WalrusClient = class _WalrusClient2 {
  constructor(config) {
    __privateAdd21(this, _WalrusClient_instances);
    __privateAdd21(this, _storageNodeClient);
    __privateAdd21(this, _wasmUrl);
    __privateAdd21(this, _packageConfig);
    __privateAdd21(this, _suiClient);
    __privateAdd21(this, _objectLoader);
    __privateAdd21(this, _blobMetadataConcurrencyLimit, 10);
    __privateAdd21(this, _readCommittee);
    __privateAdd21(this, _cache6);
    __privateAdd21(this, _uploadRelayConfig, null);
    __privateAdd21(this, _uploadRelayClient, null);
    this.readBlob = __privateMethod12(this, _WalrusClient_instances, retryOnPossibleEpochChange_fn).call(this, __privateMethod12(this, _WalrusClient_instances, internalReadBlob_fn));
    this.getSecondarySliver = __privateMethod12(this, _WalrusClient_instances, retryOnPossibleEpochChange_fn).call(this, this.internalGetSecondarySliver);
    if (config.network && !config.packageConfig) {
      const network = config.network;
      switch (network) {
        case "testnet":
          __privateSet19(this, _packageConfig, TESTNET_WALRUS_PACKAGE_CONFIG);
          break;
        case "mainnet":
          __privateSet19(this, _packageConfig, MAINNET_WALRUS_PACKAGE_CONFIG);
          break;
        default:
          throw new WalrusClientError(`Unsupported network: ${network}`);
      }
    } else {
      __privateSet19(this, _packageConfig, config.packageConfig);
    }
    __privateSet19(this, _wasmUrl, config.wasmUrl);
    __privateSet19(this, _uploadRelayConfig, config.uploadRelay ?? null);
    if (__privateGet20(this, _uploadRelayConfig)) {
      __privateSet19(this, _uploadRelayClient, new UploadRelayClient(__privateGet20(this, _uploadRelayConfig)));
    }
    __privateSet19(this, _suiClient, config.suiClient ?? new SuiJsonRpcClient({
      url: config.suiRpcUrl
    }));
    __privateSet19(this, _storageNodeClient, new StorageNodeClient(config.storageNodeClientOptions));
    __privateSet19(this, _objectLoader, new SuiObjectDataLoader(__privateGet20(this, _suiClient)));
    __privateSet19(this, _cache6, __privateGet20(this, _suiClient).cache.scope("@mysten/walrus"));
  }
  /** @deprecated use `walrus()` instead */
  static experimental_asClientExtension({
    packageConfig,
    network,
    ...options
  } = {}) {
    return {
      name: "walrus",
      register: (client) => {
        const walrusNetwork = network || client.network;
        if (walrusNetwork !== "mainnet" && walrusNetwork !== "testnet") {
          throw new WalrusClientError("Walrus client only supports mainnet and testnet");
        }
        return new _WalrusClient2(
          packageConfig ? {
            packageConfig,
            suiClient: client,
            ...options
          } : {
            network: walrusNetwork,
            suiClient: client,
            ...options
          }
        );
      }
    };
  }
  /** The Move type for a Blob object */
  getBlobType() {
    return __privateGet20(this, _cache6).read(["getBlobType"], async () => {
      return `${await __privateMethod12(this, _WalrusClient_instances, getPackageId_fn).call(this)}::blob::Blob`;
    });
  }
  /** The cached system object for the walrus package */
  systemObject() {
    return __privateGet20(this, _objectLoader).load(__privateGet20(this, _packageConfig).systemObjectId, System);
  }
  /** The cached staking pool object for the walrus package */
  stakingObject() {
    return __privateGet20(this, _objectLoader).load(__privateGet20(this, _packageConfig).stakingPoolId, Staking);
  }
  /** The system state for the current version of walrus contract  */
  async systemState() {
    const systemState = await __privateGet20(this, _objectLoader).loadFieldObject(
      __privateGet20(this, _packageConfig).systemObjectId,
      { type: "u64", value: (await this.systemObject()).version },
      SystemStateInnerV1
    );
    return systemState;
  }
  /** The staking state for the current version of walrus contract */
  async stakingState() {
    return __privateGet20(this, _objectLoader).loadFieldObject(
      __privateGet20(this, _packageConfig).stakingPoolId,
      {
        type: "u64",
        value: (await this.stakingObject()).version
      },
      StakingInnerV1
    );
  }
  async computeBlobMetadata({ bytes, numShards }) {
    let shardCount;
    if (typeof numShards === "number") {
      shardCount = numShards;
    } else {
      const systemState = await this.systemState();
      shardCount = systemState.committee.n_shards;
    }
    const bindings = await __privateMethod12(this, _WalrusClient_instances, wasmBindings_fn).call(this);
    const { blobId, rootHash, unencodedLength, encodingType } = bindings.computeMetadata(
      shardCount,
      bytes
    );
    let sha256Hash;
    const nonce = crypto.getRandomValues(new Uint8Array(32));
    return {
      rootHash,
      blobId,
      metadata: {
        encodingType,
        unencodedLength
      },
      nonce,
      blobDigest: () => {
        if (!sha256Hash) {
          sha256Hash = crypto.subtle.digest("SHA-256", bytes).then((hash) => new Uint8Array(hash));
        }
        return sha256Hash;
      }
    };
  }
  async getBlobMetadata({ blobId, signal }) {
    const committee = await __privateMethod12(this, _WalrusClient_instances, getReadCommittee_fn).call(this, { blobId, signal });
    const randomizedNodes = shuffle(committee.nodes);
    const stakingState = await this.stakingState();
    const numShards = stakingState.n_shards;
    let numNotFoundWeight = 0;
    let numBlockedWeight = 0;
    let totalErrorCount = 0;
    const controller = new AbortController();
    const metadataExecutors = randomizedNodes.map((node) => async () => {
      try {
        return await __privateGet20(this, _storageNodeClient).getBlobMetadata(
          { blobId },
          {
            nodeUrl: node.networkUrl,
            signal: signal ? AbortSignal.any([controller.signal, signal]) : controller.signal
          }
        );
      } catch (error) {
        if (error instanceof NotFoundError) {
          numNotFoundWeight += node.shardIndices.length;
        } else if (error instanceof LegallyUnavailableError) {
          numBlockedWeight += node.shardIndices.length;
        }
        totalErrorCount += 1;
        throw error;
      }
    });
    try {
      const attemptGetMetadata = metadataExecutors.shift();
      return await attemptGetMetadata();
    } catch {
      const chunkSize = Math.floor(metadataExecutors.length / __privateGet20(this, _blobMetadataConcurrencyLimit));
      const chunkedExecutors = chunk(metadataExecutors, chunkSize);
      return await new Promise((resolve, reject) => {
        chunkedExecutors.forEach(async (executors) => {
          for (const executor of executors) {
            try {
              const result = await executor();
              controller.abort("Blob metadata successfully retrieved.");
              resolve(result);
            } catch (error) {
              if (error instanceof UserAbortError) {
                reject(error);
                return;
              } else if (isQuorum(numBlockedWeight + numNotFoundWeight, numShards)) {
                const abortError = numNotFoundWeight > numBlockedWeight ? new BlobNotCertifiedError(`The specified blob ${blobId} is not certified.`) : new BlobBlockedError(`The specified blob ${blobId} is blocked.`);
                controller.abort(abortError);
                reject(abortError);
                return;
              }
              if (totalErrorCount === metadataExecutors.length) {
                reject(
                  new NoBlobMetadataReceivedError(
                    "No valid blob metadata could be retrieved from any storage node."
                  )
                );
              }
            }
          }
        });
      });
    }
  }
  async internalGetSecondarySliver({ blobId, index, signal }) {
    const committee = await __privateMethod12(this, _WalrusClient_instances, getActiveCommittee_fn).call(this);
    const stakingState = await this.stakingState();
    const numShards = stakingState.n_shards;
    const sliverPairIndex = sliverPairIndexFromSecondarySliverIndex(index, numShards);
    const shardIndex = toShardIndex(sliverPairIndex, blobId, numShards);
    const node = await __privateMethod12(this, _WalrusClient_instances, getNodeByShardIndex_fn).call(this, committee, shardIndex);
    if (!node) {
      throw new Error(`No node found for shard index ${shardIndex}`);
    }
    const sliver = await __privateGet20(this, _storageNodeClient).getSliver(
      { blobId, sliverPairIndex, sliverType: "secondary" },
      {
        nodeUrl: node.networkUrl,
        signal
      }
    );
    return sliver;
  }
  async getSlivers({ blobId, signal }) {
    const committee = await __privateMethod12(this, _WalrusClient_instances, getReadCommittee_fn).call(this, { blobId, signal });
    const randomizedNodes = weightedShuffle(
      committee.nodes.map((node) => ({
        value: node,
        weight: node.shardIndices.length
      }))
    );
    const stakingState = await this.stakingState();
    const numShards = stakingState.n_shards;
    const { primarySymbols: minSymbols } = getSourceSymbols(numShards);
    const sliverPairIndices = randomizedNodes.flatMap(
      (node) => node.shardIndices.map((shardIndex) => ({
        url: node.networkUrl,
        sliverPairIndex: toPairIndex(shardIndex, blobId, numShards)
      }))
    );
    const controller = new AbortController();
    const chunkedSliverPairIndices = chunk(sliverPairIndices, minSymbols);
    const slivers = [];
    const failedNodes = /* @__PURE__ */ new Set();
    let numNotFoundWeight = 0;
    let numBlockedWeight = 0;
    let totalErrorCount = 0;
    return new Promise((resolve, reject) => {
      chunkedSliverPairIndices[0].forEach(async (_, colIndex) => {
        for (let rowIndex = 0; rowIndex < chunkedSliverPairIndices.length; rowIndex += 1) {
          const value = chunkedSliverPairIndices.at(rowIndex)?.at(colIndex);
          if (!value) break;
          const { url, sliverPairIndex } = value;
          try {
            if (failedNodes.has(url)) {
              throw new Error(`Skipping node at ${url} due to previous failure.`);
            }
            const sliver = await __privateGet20(this, _storageNodeClient).getSliver(
              { blobId, sliverPairIndex, sliverType: "primary" },
              {
                nodeUrl: url,
                signal: signal ? AbortSignal.any([controller.signal, signal]) : controller.signal
              }
            );
            if (slivers.length === minSymbols) {
              controller.abort("Enough slivers successfully retrieved.");
              resolve(slivers);
              return;
            }
            slivers.push(sliver);
          } catch (error) {
            if (error instanceof NotFoundError) {
              numNotFoundWeight += 1;
            } else if (error instanceof LegallyUnavailableError) {
              numBlockedWeight += 1;
            } else if (error instanceof UserAbortError) {
              reject(error);
              return;
            }
            if (isQuorum(numBlockedWeight + numNotFoundWeight, numShards)) {
              const abortError = numNotFoundWeight > numBlockedWeight ? new BlobNotCertifiedError(`The specified blob ${blobId} is not certified.`) : new BlobBlockedError(`The specified blob ${blobId} is blocked.`);
              controller.abort(abortError);
              reject(abortError);
              return;
            }
            failedNodes.add(url);
            totalErrorCount += 1;
            const remainingTasks = sliverPairIndices.length - (slivers.length + totalErrorCount);
            const tooManyFailures = slivers.length + remainingTasks < minSymbols;
            if (tooManyFailures) {
              const abortError = new NotEnoughSliversReceivedError(
                `Unable to retrieve enough slivers to decode blob ${blobId}.`
              );
              controller.abort(abortError);
              reject(abortError);
            }
          }
        }
      });
    });
  }
  /**
   * Gets the blob status from multiple storage nodes and returns the latest status that can be verified.
   */
  async getVerifiedBlobStatus({ blobId, signal }) {
    const committee = await __privateMethod12(this, _WalrusClient_instances, getActiveCommittee_fn).call(this);
    const stakingState = await this.stakingState();
    const numShards = stakingState.n_shards;
    const controller = new AbortController();
    const statuses = await new Promise(
      (resolve, reject) => {
        const results = [];
        let successWeight = 0;
        let numNotFoundWeight = 0;
        let settledCount = 0;
        committee.nodes.forEach(async (node) => {
          const weight = node.shardIndices.length;
          try {
            const status = await __privateGet20(this, _storageNodeClient).getBlobStatus(
              { blobId },
              {
                nodeUrl: node.networkUrl,
                signal: signal ? AbortSignal.any([controller.signal, signal]) : controller.signal
              }
            );
            if (isQuorum(successWeight, numShards)) {
              controller.abort("Quorum of blob statuses retrieved successfully.");
              resolve(results);
            } else {
              successWeight += weight;
              results.push({ status, weight });
            }
          } catch (error) {
            if (error instanceof NotFoundError) {
              numNotFoundWeight += weight;
            } else if (error instanceof UserAbortError) {
              reject(error);
            }
            if (isQuorum(numNotFoundWeight, numShards)) {
              const abortError = new BlobNotCertifiedError("The blob does not exist.");
              controller.abort(abortError);
              reject(abortError);
            }
          } finally {
            settledCount += 1;
            if (settledCount === committee.nodes.length) {
              reject(
                new NoBlobStatusReceivedError(
                  "Not enough statuses were retrieved to achieve quorum."
                )
              );
            }
          }
        });
      }
    );
    const aggregatedStatuses = statuses.reduce((accumulator, value) => {
      const { status, weight } = value;
      const key = JSON.stringify(status);
      const existing = accumulator.get(key);
      if (existing) {
        existing.totalWeight += weight;
      } else {
        accumulator.set(key, { status, totalWeight: weight });
      }
      return accumulator;
    }, /* @__PURE__ */ new Map());
    const uniqueStatuses = [...aggregatedStatuses.values()];
    const sortedStatuses = uniqueStatuses.toSorted(
      (a, b) => statusLifecycleRank[b.status.type] - statusLifecycleRank[a.status.type]
    );
    for (const value of sortedStatuses) {
      if (isAboveValidity(value.totalWeight, numShards)) {
        return value.status;
      }
    }
    throw new NoVerifiedBlobStatusReceivedError(
      `The blob status could not be verified for blob ${blobId},`
    );
  }
  /**
   * Calculate the cost of storing a blob for a given a size and number of epochs.
   */
  async storageCost(size, epochs) {
    const systemState = await this.systemState();
    const encodedSize = encodedBlobLength(size, systemState.committee.n_shards);
    return __privateMethod12(this, _WalrusClient_instances, storageCostFromEncodedSize_fn).call(this, encodedSize, epochs);
  }
  /**
   * A utility for creating a storage object in a transaction.
   *
   * @example
   * ```ts
   * tx.transferObjects([client.createStorage({ size: 1000, epochs: 3 })], owner);
   * ```
   */
  createStorage({ size, epochs, walCoin }) {
    return async (tx) => {
      const systemObject = await this.systemObject();
      const systemState = await this.systemState();
      const encodedSize = encodedBlobLength(size, systemState.committee.n_shards);
      const [{ storageCost }, walrusPackageId] = await Promise.all([
        this.storageCost(size, epochs),
        __privateMethod12(this, _WalrusClient_instances, getWalrusPackageId_fn).call(this)
      ]);
      return tx.add(
        __privateMethod12(this, _WalrusClient_instances, withWal_fn).call(this, storageCost, walCoin ?? null, (coin, tx2) => {
          return tx2.add(
            reserveSpace({
              package: walrusPackageId,
              arguments: {
                self: systemObject.id.id,
                storageAmount: encodedSize,
                epochsAhead: epochs,
                payment: coin
              }
            })
          );
        })
      );
    };
  }
  /**
   * Create a transaction that creates a storage object
   *
   * @example
   * ```ts
   * const tx = client.createStorageTransaction({ size: 1000, epochs: 3, owner: signer.toSuiAddress() });
   * ```
   */
  createStorageTransaction({
    transaction = new Transaction(),
    size,
    epochs,
    owner
  }) {
    transaction.transferObjects([this.createStorage({ size, epochs })], owner);
    return transaction;
  }
  /**
   * Execute a transaction that creates a storage object
   *
   * @example
   * ```ts
   * const { digest, storage } = await client.executeCreateStorageTransaction({ size: 1000, epochs: 3, signer });
   * ```
   */
  async executeCreateStorageTransaction({
    signer,
    ...options
  }) {
    const transaction = this.createStorageTransaction({
      ...options,
      owner: options.transaction?.getData().sender ?? signer.toSuiAddress()
    });
    const blobType = await this.getBlobType();
    const { digest, effects } = await __privateMethod12(this, _WalrusClient_instances, executeTransaction_fn).call(this, transaction, signer, "create storage");
    const createdObjectIds = effects?.changedObjects.filter((object2) => object2.idOperation === "Created").map((object2) => object2.id);
    const createdObjects = await __privateGet20(this, _suiClient).core.getObjects({
      objectIds: createdObjectIds
    });
    const suiBlobObject = createdObjects.objects.find(
      (object2) => !(object2 instanceof Error) && object2.type === blobType
    );
    if (suiBlobObject instanceof Error || !suiBlobObject) {
      throw new WalrusClientError(
        `Storage object not found in transaction effects for transaction (${digest})`
      );
    }
    return {
      digest,
      storage: Storage.parse(await suiBlobObject.content)
    };
  }
  /**
   * Register a blob in a transaction
   *
   * @example
   * ```ts
   * tx.transferObjects([client.registerBlob({ size: 1000, epochs: 3, blobId, rootHash, deletable: true })], owner);
   * ```
   */
  registerBlob({
    size,
    epochs,
    blobId,
    rootHash,
    deletable,
    walCoin,
    attributes
  }) {
    return async (tx) => {
      const { writeCost } = await this.storageCost(size, epochs);
      const walrusPackageId = await __privateMethod12(this, _WalrusClient_instances, getWalrusPackageId_fn).call(this);
      return tx.add(
        __privateMethod12(this, _WalrusClient_instances, withWal_fn).call(this, writeCost, walCoin ?? null, async (writeCoin, tx2) => {
          const blob = tx2.add(
            registerBlob({
              package: walrusPackageId,
              arguments: {
                self: tx2.object(__privateGet20(this, _packageConfig).systemObjectId),
                storage: this.createStorage({ size, epochs, walCoin }),
                blobId: blobIdToInt(blobId),
                rootHash: BigInt(bcs.u256().parse(rootHash)),
                size,
                encodingType: 1,
                deletable,
                writePayment: writeCoin
              }
            })
          );
          if (attributes) {
            tx2.add(
              __privateMethod12(this, _WalrusClient_instances, writeBlobAttributesForRef_fn).call(this, {
                attributes,
                existingAttributes: null,
                blob
              })
            );
          }
          return blob;
        })
      );
    };
  }
  addAuthPayload({
    size,
    blobDigest,
    nonce
  }) {
    return async (transaction) => {
      const nonceDigest = await crypto.subtle.digest("SHA-256", nonce);
      const lengthBytes = bcs.u64().serialize(size).toBytes();
      const digest = typeof blobDigest === "function" ? await blobDigest() : blobDigest;
      const authPayload = new Uint8Array(
        nonceDigest.byteLength + digest.byteLength + lengthBytes.byteLength
      );
      authPayload.set(digest, 0);
      authPayload.set(new Uint8Array(nonceDigest), digest.byteLength);
      authPayload.set(lengthBytes, nonceDigest.byteLength + digest.byteLength);
      transaction.pure(authPayload);
    };
  }
  async calculateUploadRelayTip(options) {
    const systemState = await this.systemState();
    const encodedSize = encodedBlobLength(options.size, systemState.committee.n_shards);
    const tipConfig = await __privateMethod12(this, _WalrusClient_instances, loadTipConfig_fn).call(this);
    if (!tipConfig) {
      return 0n;
    }
    const { max, kind } = tipConfig;
    const amount = "const" in kind ? kind.const : BigInt(kind.linear.base) + BigInt(kind.linear.perEncodedKib) * ((BigInt(encodedSize) + 1023n) / 1024n);
    if (max != null && amount > max) {
      throw new WalrusClientError(
        `Tip amount (${amount}) exceeds the maximum allowed tip (${max})`
      );
    }
    return amount;
  }
  sendUploadRelayTip({
    size,
    blobDigest,
    nonce
  }) {
    return async (transaction) => {
      const tipConfig = await __privateMethod12(this, _WalrusClient_instances, loadTipConfig_fn).call(this);
      if (tipConfig) {
        transaction.add(this.addAuthPayload({ size, blobDigest, nonce }));
        const amount = await this.calculateUploadRelayTip({ size });
        const { address } = tipConfig;
        transaction.transferObjects(
          [
            coinWithBalance({
              balance: amount
            })
          ],
          address
        );
      }
    };
  }
  /**
   * Create a transaction that registers a blob
   *
   * @example
   * ```ts
   * const tx = client.registerBlobTransaction({ size: 1000, epochs: 3, blobId, rootHash, deletable: true });
   * ```
   */
  registerBlobTransaction({
    transaction = new Transaction(),
    ...options
  }) {
    const registration = transaction.add(this.registerBlob(options));
    transaction.transferObjects([registration], options.owner);
    return transaction;
  }
  /**
   * Execute a transaction that registers a blob
   *
   * @example
   * ```ts
   * const { digest, blob } = await client.executeRegisterBlobTransaction({ size: 1000, epochs: 3, signer });
   * ```
   */
  async executeRegisterBlobTransaction({
    signer,
    ...options
  }) {
    const transaction = this.registerBlobTransaction({
      ...options,
      owner: options.owner ?? options.transaction?.getData().sender ?? signer.toSuiAddress()
    });
    const blobType = await this.getBlobType();
    const { digest, effects } = await __privateMethod12(this, _WalrusClient_instances, executeTransaction_fn).call(this, transaction, signer, "register blob");
    const createdObjectIds = effects?.changedObjects.filter((object2) => object2.idOperation === "Created").map((object2) => object2.id);
    const createdObjects = await __privateGet20(this, _suiClient).core.getObjects({
      objectIds: createdObjectIds
    });
    const suiBlobObject = createdObjects.objects.find(
      (object2) => !(object2 instanceof Error) && object2.type === blobType
    );
    if (suiBlobObject instanceof Error || !suiBlobObject) {
      throw new WalrusClientError(
        `Blob object not found in transaction effects for transaction (${digest})`
      );
    }
    return {
      digest,
      blob: Blob2.parse(await suiBlobObject.content)
    };
  }
  async certificateFromConfirmations({
    confirmations,
    blobId,
    deletable,
    blobObjectId
  }) {
    const systemState = await this.systemState();
    const committee = await __privateMethod12(this, _WalrusClient_instances, getActiveCommittee_fn).call(this);
    if (confirmations.length !== systemState.committee.members.length) {
      throw new WalrusClientError(
        "Invalid number of confirmations. Confirmations array must contain an entry for each node"
      );
    }
    const confirmationMessage = StorageConfirmation.serialize({
      intent: IntentType.BLOB_CERT_MSG,
      epoch: systemState.committee.epoch,
      messageContents: {
        blobId,
        blobType: deletable ? {
          Deletable: {
            objectId: blobObjectId
          }
        } : {
          Permanent: null
        }
      }
    }).toBase64();
    const bindings = await __privateMethod12(this, _WalrusClient_instances, wasmBindings_fn).call(this);
    const verifySignature = bindings.getVerifySignature();
    const filteredConfirmations = confirmations.map((confirmation, index) => {
      const isValid = confirmation?.serializedMessage === confirmationMessage && verifySignature(
        confirmation,
        new Uint8Array(committee.nodes[index].info.public_key.bytes)
      );
      return isValid ? {
        index,
        ...confirmation
      } : null;
    }).filter((confirmation) => confirmation !== null);
    if (!isQuorum(filteredConfirmations.length, systemState.committee.members.length)) {
      throw new NotEnoughBlobConfirmationsError(
        `Too many invalid confirmations received for blob (${filteredConfirmations.length} of ${systemState.committee.members.length})`
      );
    }
    return bindings.combineSignatures(
      filteredConfirmations,
      filteredConfirmations.map(({ index }) => index)
    );
  }
  /**
   * Certify a blob in a transaction
   *
   * @example
   * ```ts
   * tx.add(client.certifyBlob({ blobId, blobObjectId, confirmations }));
   * ```
   */
  certifyBlob({ blobId, blobObjectId, confirmations, certificate, deletable }) {
    return async (tx) => {
      const systemState = await this.systemState();
      const combinedSignature = certificate ?? await this.certificateFromConfirmations({
        confirmations,
        blobId,
        deletable,
        blobObjectId
      });
      const walrusPackageId = await __privateMethod12(this, _WalrusClient_instances, getWalrusPackageId_fn).call(this);
      tx.add(
        certifyBlob({
          package: walrusPackageId,
          arguments: {
            self: __privateGet20(this, _packageConfig).systemObjectId,
            blob: blobObjectId,
            signature: tx.pure.vector("u8", combinedSignature.signature),
            signersBitmap: tx.pure.vector(
              "u8",
              signersToBitmap(combinedSignature.signers, systemState.committee.members.length)
            ),
            message: tx.pure.vector("u8", combinedSignature.serializedMessage)
          }
        })
      );
    };
  }
  /**
   * Create a transaction that certifies a blob
   *
   * @example
   * ```ts
   * const tx = client.certifyBlobTransaction({ blobId, blobObjectId, confirmations });
   * ```
   */
  certifyBlobTransaction({
    transaction = new Transaction(),
    ...options
  }) {
    transaction.add(this.certifyBlob(options));
    return transaction;
  }
  /**
   * Execute a transaction that certifies a blob
   *
   * @example
   * ```ts
   * const { digest } = await client.executeCertifyBlobTransaction({ blobId, blobObjectId, confirmations, signer });
   * ```
   */
  async executeCertifyBlobTransaction({
    signer,
    ...options
  }) {
    const transaction = this.certifyBlobTransaction(options);
    const { digest } = await __privateMethod12(this, _WalrusClient_instances, executeTransaction_fn).call(this, transaction, signer, "certify blob");
    return { digest };
  }
  /**
   * Delete a blob in a transaction
   *
   * @example
   * ```ts
   * const storage = await client.deleteBlob({ blobObjectId });
   * tx.transferObjects([storage], owner);
   * ```
   */
  deleteBlob({ blobObjectId }) {
    return async (tx) => {
      const walrusPackageId = await __privateMethod12(this, _WalrusClient_instances, getWalrusPackageId_fn).call(this);
      const storage = tx.add(
        deleteBlob({
          package: walrusPackageId,
          arguments: {
            self: __privateGet20(this, _packageConfig).systemObjectId,
            blob: blobObjectId
          }
        })
      );
      return storage;
    };
  }
  /**
   * Create a transaction that deletes a blob
   *
   * @example
   * ```ts
   * const tx = client.deleteBlobTransaction({ blobObjectId, owner });
   * ```
   */
  deleteBlobTransaction({
    owner,
    blobObjectId,
    transaction = new Transaction()
  }) {
    const storage = transaction.add(this.deleteBlob({ blobObjectId }));
    transaction.transferObjects([storage], owner);
    return transaction;
  }
  /**
   * Execute a transaction that deletes a blob
   *
   * @example
   * ```ts
   * const { digest } = await client.executeDeleteBlobTransaction({ blobObjectId, signer });
   * ```
   */
  async executeDeleteBlobTransaction({
    signer,
    transaction = new Transaction(),
    blobObjectId
  }) {
    const { digest } = await __privateMethod12(this, _WalrusClient_instances, executeTransaction_fn).call(this, this.deleteBlobTransaction({
      blobObjectId,
      transaction,
      owner: transaction.getData().sender ?? signer.toSuiAddress()
    }), signer, "delete blob");
    return { digest };
  }
  /**
   * Extend a blob in a transaction
   *
   * @example
   * ```ts
   * const tx = client.extendBlobTransaction({ blobObjectId, epochs });
   * ```
   */
  extendBlob({ blobObjectId, epochs, endEpoch, walCoin }) {
    return async (tx) => {
      const blob = await __privateGet20(this, _objectLoader).load(blobObjectId, Blob2);
      const numEpochs = typeof epochs === "number" ? epochs : endEpoch - blob.storage.end_epoch;
      if (numEpochs <= 0) {
        return;
      }
      const { storageCost } = await __privateMethod12(this, _WalrusClient_instances, storageCostFromEncodedSize_fn).call(this, Number(blob.storage.storage_size), numEpochs);
      const walrusPackageId = await __privateMethod12(this, _WalrusClient_instances, getWalrusPackageId_fn).call(this);
      return tx.add(
        __privateMethod12(this, _WalrusClient_instances, withWal_fn).call(this, storageCost, walCoin ?? null, async (coin, tx2) => {
          tx2.add(
            extendBlob({
              package: walrusPackageId,
              arguments: {
                self: __privateGet20(this, _packageConfig).systemObjectId,
                blob: blobObjectId,
                extendedEpochs: numEpochs,
                payment: coin
              }
            })
          );
        })
      );
    };
  }
  /**
   * Create a transaction that extends a blob
   *
   * @example
   * ```ts
   * const tx = client.extendBlobTransaction({ blobObjectId, epochs });
   * ```
   */
  async extendBlobTransaction({
    transaction = new Transaction(),
    ...options
  }) {
    transaction.add(this.extendBlob(options));
    return transaction;
  }
  /**
   * Execute a transaction that extends a blob
   *
   * @example
   * ```ts
   * const { digest } = await client.executeExtendBlobTransaction({ blobObjectId, signer });
   * ```
   */
  async executeExtendBlobTransaction({
    signer,
    ...options
  }) {
    const { digest } = await __privateMethod12(this, _WalrusClient_instances, executeTransaction_fn).call(this, await this.extendBlobTransaction(options), signer, "extend blob");
    return { digest };
  }
  async readBlobAttributes({
    blobObjectId
  }) {
    const response = await __privateGet20(this, _suiClient).core.getDynamicField({
      parentId: blobObjectId,
      name: {
        type: "vector<u8>",
        bcs: bcs.string().serialize("metadata").toBytes()
      }
    });
    const parsedMetadata = Metadata.parse(response.dynamicField.value.bcs);
    return Object.fromEntries(
      parsedMetadata.metadata.contents.map(({ key, value }) => [key, value])
    );
  }
  /**
   * Write attributes to a blob
   *
   * If attributes already exists, their previous values will be overwritten
   * If an attribute is set to `null`, it will be removed from the blob
   *
   * @example
   * ```ts
   * tx.add(client.writeBlobAttributes({ blobObjectId, attributes: { key: 'value', keyToRemove: null } }));
   * ```
   */
  writeBlobAttributes({ blobObject, blobObjectId, attributes }) {
    return async (tx) => {
      const existingAttributes = blobObjectId ? await this.readBlobAttributes({ blobObjectId }) : null;
      const blob = blobObject ?? tx.object(blobObjectId);
      tx.add(
        __privateMethod12(this, _WalrusClient_instances, writeBlobAttributesForRef_fn).call(this, {
          attributes,
          existingAttributes,
          blob
        })
      );
    };
  }
  /**
   * Create a transaction that writes attributes to a blob
   *
   * If attributes already exists, their previous values will be overwritten
   * If an attribute is set to `null`, it will be removed from the blob
   *
   * @example
   * ```ts
   * const tx = client.writeBlobAttributesTransaction({ blobObjectId, attributes: { key: 'value', keyToRemove: null } });
   * ```
   */
  async writeBlobAttributesTransaction({
    transaction = new Transaction(),
    ...options
  }) {
    transaction.add(await this.writeBlobAttributes(options));
    return transaction;
  }
  /**
   * Execute a transaction that writes attributes to a blob
   *
   * If attributes already exists, their previous values will be overwritten
   * If an attribute is set to `null`, it will be removed from the blob
   *
   * @example
   * ```ts
   * const { digest } = await client.executeWriteBlobAttributesTransaction({ blobObjectId, signer });
   * ```
   */
  async executeWriteBlobAttributesTransaction({
    signer,
    ...options
  }) {
    const { digest } = await __privateMethod12(this, _WalrusClient_instances, executeTransaction_fn).call(this, await this.writeBlobAttributesTransaction(options), signer, "write blob attributes");
    return { digest };
  }
  /**
   * Write a sliver to a storage node
   *
   * @example
   * ```ts
   * const res = await client.writeSliver({ blobId, sliverPairIndex, sliverType, sliver });
   * ```
   */
  async writeSliver({ blobId, sliverPairIndex, sliverType, sliver, signal }) {
    const systemState = await this.systemState();
    const committee = await __privateMethod12(this, _WalrusClient_instances, getActiveCommittee_fn).call(this);
    const shardIndex = toShardIndex(sliverPairIndex, blobId, systemState.committee.n_shards);
    const node = await __privateMethod12(this, _WalrusClient_instances, getNodeByShardIndex_fn).call(this, committee, shardIndex);
    return __privateGet20(this, _storageNodeClient).storeSliver(
      { blobId, sliverPairIndex, sliverType, sliver },
      { nodeUrl: node.networkUrl, signal }
    );
  }
  /**
   * Write metadata to a storage node
   *
   * @example
   * ```ts
   * const res = await client.writeMetadataToNode({ nodeIndex, blobId, metadata });
   * ```
   */
  async writeMetadataToNode({ nodeIndex, blobId, metadata: metadata2, signal }) {
    const committee = await __privateMethod12(this, _WalrusClient_instances, getActiveCommittee_fn).call(this);
    const node = committee.nodes[nodeIndex];
    return retry(
      () => __privateGet20(this, _storageNodeClient).storeBlobMetadata(
        { blobId, metadata: metadata2 },
        { nodeUrl: node.networkUrl, signal }
      ),
      {
        count: 3,
        delay: 1e3,
        condition: (error) => error instanceof BlobNotRegisteredError
      }
    );
  }
  /**
   * Get a storage confirmation from a storage node
   *
   * @example
   * ```ts
   * const confirmation = await client.getStorageConfirmationFromNode({ nodeIndex, blobId, deletable, objectId });
   * ```
   */
  async getStorageConfirmationFromNode({
    nodeIndex,
    blobId,
    deletable,
    objectId,
    signal
  }) {
    const committee = await __privateMethod12(this, _WalrusClient_instances, getActiveCommittee_fn).call(this);
    const node = committee.nodes[nodeIndex];
    const result = deletable ? await __privateGet20(this, _storageNodeClient).getDeletableBlobConfirmation(
      { blobId, objectId },
      { nodeUrl: node.networkUrl, signal }
    ) : await __privateGet20(this, _storageNodeClient).getPermanentBlobConfirmation(
      { blobId },
      { nodeUrl: node.networkUrl, signal }
    );
    return result?.success?.data?.signed ?? null;
  }
  /**
   * Encode a blob into slivers for each node
   *
   * @example
   * ```ts
   * const { blobId, metadata, sliversByNode, rootHash } = await client.encodeBlob(blob);
   * ```
   */
  async encodeBlob(blob) {
    const systemState = await this.systemState();
    const committee = await __privateMethod12(this, _WalrusClient_instances, getActiveCommittee_fn).call(this);
    const numShards = systemState.committee.n_shards;
    const bindings = await __privateMethod12(this, _WalrusClient_instances, wasmBindings_fn).call(this);
    const { blobId, metadata: metadata2, primarySlivers, secondarySlivers, rootHash } = bindings.encodeBlob(
      numShards,
      blob
    );
    const sliversByNodeMap = /* @__PURE__ */ new Map();
    for (let sliverPairIndex = 0; sliverPairIndex < primarySlivers.length; sliverPairIndex++) {
      const primarySliver = primarySlivers[sliverPairIndex];
      const secondarySliver = secondarySlivers[sliverPairIndex];
      const shardIndex = toShardIndex(sliverPairIndex, blobId, numShards);
      const node = await __privateMethod12(this, _WalrusClient_instances, getNodeByShardIndex_fn).call(this, committee, shardIndex);
      if (!sliversByNodeMap.has(node.nodeIndex)) {
        sliversByNodeMap.set(node.nodeIndex, { primary: [], secondary: [] });
      }
      sliversByNodeMap.get(node.nodeIndex).primary.push({
        sliverIndex: sliverPairIndex,
        sliverPairIndex,
        shardIndex,
        sliver: primarySliver
      });
      sliversByNodeMap.get(node.nodeIndex).secondary.push({
        sliverIndex: sliverPairIndex,
        sliverPairIndex,
        shardIndex,
        sliver: secondarySliver
      });
    }
    const sliversByNode = new Array();
    for (let i = 0; i < systemState.committee.members.length; i++) {
      sliversByNode.push(sliversByNodeMap.get(i) ?? { primary: [], secondary: [] });
    }
    return { blobId, metadata: metadata2, rootHash, sliversByNode };
  }
  /**
   * Write slivers to a storage node
   *
   * @example
   * ```ts
   * await client.writeSliversToNode({ blobId, slivers, signal });
   * ```
   */
  async writeSliversToNode({ blobId, slivers, signal }) {
    const controller = new AbortController();
    const combinedSignal = signal ? AbortSignal.any([controller.signal, signal]) : controller.signal;
    const primarySliverWrites = slivers.primary.map(({ sliverPairIndex, sliver }) => {
      return this.writeSliver({
        blobId,
        sliverPairIndex,
        sliverType: "primary",
        sliver,
        signal: combinedSignal
      });
    });
    const secondarySliverWrites = slivers.secondary.map(({ sliverPairIndex, sliver }) => {
      return this.writeSliver({
        blobId,
        sliverPairIndex,
        sliverType: "secondary",
        sliver,
        signal: combinedSignal
      });
    });
    await Promise.all([...primarySliverWrites, ...secondarySliverWrites]).catch((error) => {
      controller.abort(error);
      throw error;
    });
  }
  /**
   * Write a blob to all storage nodes
   *
   * @example
   * ```ts
   * await client.writeEncodedBlobToNodes({ blob, deletable, epochs, signer });
   * ```
   */
  async writeEncodedBlobToNodes({
    blobId,
    metadata: metadata2,
    sliversByNode,
    signal,
    ...options
  }) {
    const systemState = await this.systemState();
    const committee = await __privateMethod12(this, _WalrusClient_instances, getActiveCommittee_fn).call(this);
    const controller = new AbortController();
    let failures = 0;
    const confirmations = await Promise.all(
      sliversByNode.map((slivers, nodeIndex) => {
        return this.writeEncodedBlobToNode({
          blobId,
          nodeIndex,
          metadata: metadata2,
          slivers,
          signal: signal ? AbortSignal.any([controller.signal, signal]) : controller.signal,
          ...options
        }).catch(() => {
          failures += committee.nodes[nodeIndex].shardIndices.length;
          if (isAboveValidity(failures, systemState.committee.n_shards)) {
            const error = new NotEnoughBlobConfirmationsError(
              `Too many failures while writing blob ${blobId} to nodes`
            );
            controller.abort(error);
            throw error;
          }
          return null;
        });
      })
    );
    return confirmations;
  }
  /**
   * Writes a blob to to an upload relay
   *
   * @example
   * ```ts
   * await client.writeBlobToUploadRelay({ blob, deletable, epochs, signer });
   * ```
   */
  async writeBlobToUploadRelay(options) {
    if (!__privateGet20(this, _uploadRelayClient)) {
      throw new WalrusClientError("Upload relay not configured");
    }
    return __privateGet20(this, _uploadRelayClient).writeBlob({
      ...options,
      requiresTip: !!__privateGet20(this, _uploadRelayConfig)?.sendTip
    });
  }
  /**
   * Write encoded blob to a storage node
   *
   * @example
   * ```ts
   * const res = await client.writeEncodedBlobToNode({ nodeIndex, blobId, metadata, slivers });
   * ```
   */
  async writeEncodedBlobToNode({
    nodeIndex,
    blobId,
    metadata: metadata2,
    slivers,
    signal,
    ...options
  }) {
    await this.writeMetadataToNode({
      nodeIndex,
      blobId,
      metadata: metadata2,
      signal
    });
    await this.writeSliversToNode({ blobId, slivers, signal, nodeIndex });
    return this.getStorageConfirmationFromNode({
      nodeIndex,
      blobId,
      ...options
    });
  }
  /**
   * Write a blob to all storage nodes
   *
   * @example
   * ```ts
   * const { blobId, blobObject } = await client.writeBlob({ blob, deletable, epochs, signer });
   * ```
   */
  async writeBlob({
    blob,
    deletable,
    epochs,
    signer,
    signal,
    owner,
    attributes
  }) {
    if (!__privateGet20(this, _uploadRelayConfig)) {
      const encoded = await this.encodeBlob(blob);
      const blobId = encoded.blobId;
      const { sliversByNode, metadata: metadata2, rootHash } = encoded;
      const suiBlobObject = await this.executeRegisterBlobTransaction({
        signer,
        size: blob.length,
        epochs,
        blobId,
        rootHash,
        deletable,
        owner: owner ?? signer.toSuiAddress(),
        attributes
      });
      const blobObjectId = suiBlobObject.blob.id.id;
      const confirmations = await this.writeEncodedBlobToNodes({
        blobId,
        metadata: metadata2,
        sliversByNode,
        deletable,
        objectId: blobObjectId,
        signal
      });
      await this.executeCertifyBlobTransaction({
        signer,
        blobId,
        blobObjectId,
        confirmations,
        deletable
      });
      return {
        blobId,
        blobObject: await __privateGet20(this, _objectLoader).load(blobObjectId, Blob2)
      };
    } else {
      const metadata2 = await this.computeBlobMetadata({
        bytes: blob
      });
      const blobId = metadata2.blobId;
      const transaction = new Transaction();
      transaction.add(
        this.sendUploadRelayTip({
          size: blob.length,
          blobDigest: metadata2.blobDigest,
          nonce: metadata2.nonce
        })
      );
      const registerResult = await this.executeRegisterBlobTransaction({
        signer,
        transaction,
        size: blob.length,
        epochs,
        blobId: metadata2.blobId,
        rootHash: metadata2.rootHash,
        deletable,
        owner: owner ?? signer.toSuiAddress(),
        attributes
      });
      await __privateGet20(this, _suiClient).core.waitForTransaction({
        digest: registerResult.digest
      });
      const result = await this.writeBlobToUploadRelay({
        blobId,
        blob,
        nonce: metadata2.nonce,
        txDigest: registerResult.digest,
        signal,
        deletable,
        blobObjectId: registerResult.blob.id.id,
        encodingType: metadata2.metadata.encodingType
      });
      const certificate = result.certificate;
      const blobObjectId = registerResult.blob.id.id;
      await this.executeCertifyBlobTransaction({
        signer,
        blobId,
        blobObjectId,
        certificate,
        deletable
      });
      return {
        blobId,
        blobObject: await __privateGet20(this, _objectLoader).load(blobObjectId, Blob2)
      };
    }
  }
  async writeQuilt({ blobs, ...options }) {
    const encoded = await this.encodeQuilt({ blobs });
    const result = await this.writeBlob({
      ...options,
      blob: encoded.quilt,
      attributes: {
        _walrusBlobType: "quilt",
        ...options.attributes
      }
    });
    return {
      ...result,
      index: {
        ...encoded.index,
        patches: encoded.index.patches.map((patch) => ({
          ...patch,
          patchId: encodeQuiltPatchId({
            quiltId: result.blobId,
            patchId: {
              version: 1,
              startIndex: patch.startIndex,
              endIndex: patch.endIndex
            }
          })
        }))
      }
    };
  }
  async encodeQuilt({
    blobs
  }) {
    const systemState = await this.systemState();
    const encoded = encodeQuilt({
      blobs,
      numShards: systemState.committee.n_shards
    });
    return encoded;
  }
  /**
   * Reset cached data in the client
   *
   * @example
   * ```ts
   * client.reset();
   * ```
   */
  reset() {
    __privateGet20(this, _objectLoader).clearAll();
    __privateGet20(this, _cache6).clear();
  }
  async getBlob({ blobId }) {
    return new WalrusBlob({
      reader: new BlobReader({
        client: this,
        blobId,
        numShards: (await this.systemState()).committee.n_shards
      }),
      client: this
    });
  }
  async getFiles({ ids }) {
    const readersByBlobId = /* @__PURE__ */ new Map();
    const quiltReadersByBlobId = /* @__PURE__ */ new Map();
    const parsedIds = ids.map((id) => parseWalrusId(id));
    const numShards = (await this.systemState()).committee.n_shards;
    for (const id of parsedIds) {
      const blobId = id.kind === "blob" ? id.id : id.id.quiltId;
      if (!readersByBlobId.has(blobId)) {
        readersByBlobId.set(
          blobId,
          new BlobReader({
            client: this,
            blobId,
            numShards
          })
        );
      }
      if (id.kind === "quiltPatch") {
        if (!quiltReadersByBlobId.has(blobId)) {
          quiltReadersByBlobId.set(
            blobId,
            new QuiltReader({
              blob: readersByBlobId.get(blobId)
            })
          );
        }
      }
    }
    return parsedIds.map((id) => {
      if (id.kind === "blob") {
        return new WalrusFile({
          reader: readersByBlobId.get(id.id)
        });
      }
      return new WalrusFile({
        reader: new QuiltFileReader({
          quilt: quiltReadersByBlobId.get(id.id.quiltId),
          sliverIndex: id.id.patchId.startIndex
        })
      });
    });
  }
  async writeFiles({ files, ...options }) {
    const { blobId, index, blobObject } = await this.writeQuilt({
      ...options,
      blobs: await Promise.all(
        files.map(async (file, i) => ({
          contents: await file.bytes(),
          identifier: await file.getIdentifier() ?? `file-${i}`,
          tags: await file.getTags() ?? {}
        }))
      )
    });
    return index.patches.map((patch) => ({
      id: patch.patchId,
      blobId,
      blobObject
    }));
  }
  writeFilesFlow({ files }) {
    const encode = async () => {
      const { quilt, index } = await this.encodeQuilt({
        blobs: await Promise.all(
          files.map(async (file, i) => ({
            contents: await file.bytes(),
            identifier: await file.getIdentifier() ?? `file-${i}`,
            tags: await file.getTags() ?? {}
          }))
        )
      });
      const metadata2 = __privateGet20(this, _uploadRelayClient) ? await this.computeBlobMetadata({
        bytes: quilt
      }) : await this.encodeBlob(quilt);
      return {
        metadata: metadata2,
        size: quilt.length,
        data: __privateGet20(this, _uploadRelayClient) ? quilt : void 0,
        index
      };
    };
    const register = ({ data, metadata: metadata2, index, size }, { epochs, deletable, owner, attributes }) => {
      const transaction = new Transaction();
      transaction.setSenderIfNotSet(owner);
      if (__privateGet20(this, _uploadRelayClient)) {
        const meta = metadata2;
        transaction.add(
          this.sendUploadRelayTip({
            size,
            blobDigest: meta.blobDigest,
            nonce: meta.nonce
          })
        );
      }
      transaction.transferObjects(
        [
          this.registerBlob({
            size,
            epochs,
            blobId: metadata2.blobId,
            rootHash: metadata2.rootHash,
            deletable,
            attributes: {
              _walrusBlobType: "quilt",
              ...attributes
            }
          })
        ],
        owner
      );
      return {
        registerTransaction: transaction,
        index,
        data,
        metadata: metadata2,
        deletable
      };
    };
    const upload = async ({ index, data, metadata: metadata2, deletable }, { digest }) => {
      const blobObject = await __privateMethod12(this, _WalrusClient_instances, getCreatedBlob_fn).call(this, digest);
      if (__privateGet20(this, _uploadRelayClient)) {
        const meta2 = metadata2;
        return {
          index,
          blobObject,
          metadata: metadata2,
          deletable,
          certificate: (await this.writeBlobToUploadRelay({
            blobId: metadata2.blobId,
            blob: data,
            nonce: meta2.nonce,
            txDigest: digest,
            blobObjectId: blobObject.id.id,
            deletable,
            encodingType: meta2.metadata.encodingType
          })).certificate
        };
      }
      const meta = metadata2;
      return {
        index,
        blobObject,
        metadata: metadata2,
        deletable,
        confirmations: await this.writeEncodedBlobToNodes({
          blobId: metadata2.blobId,
          objectId: blobObject.id.id,
          metadata: meta.metadata,
          sliversByNode: meta.sliversByNode,
          deletable
        })
      };
    };
    const certify = ({
      index,
      metadata: metadata2,
      confirmations,
      certificate,
      blobObject,
      deletable
    }) => {
      return {
        index,
        blobObject,
        metadata: metadata2,
        transaction: confirmations ? this.certifyBlobTransaction({
          blobId: metadata2.blobId,
          blobObjectId: blobObject.id.id,
          confirmations,
          deletable
        }) : this.certifyBlobTransaction({
          certificate,
          blobId: metadata2.blobId,
          blobObjectId: blobObject.id.id,
          deletable
        })
      };
    };
    async function listFiles({ index, blobObject, metadata: metadata2 }) {
      return index.patches.map((patch) => ({
        id: encodeQuiltPatchId({
          quiltId: metadata2.blobId,
          patchId: {
            version: 1,
            startIndex: patch.startIndex,
            endIndex: patch.endIndex
          }
        }),
        blobId: metadata2.blobId,
        blobObject
      }));
    }
    const stepResults = {};
    function getResults(step, current) {
      if (!stepResults[step]) {
        throw new Error(`${step} must be executed before calling ${current}`);
      }
      return stepResults[step];
    }
    return {
      encode: async () => {
        if (!stepResults.encode) {
          stepResults.encode = await encode();
        }
      },
      register: (options) => {
        stepResults.register = register(getResults("encode", "register"), options);
        return stepResults.register.registerTransaction;
      },
      upload: async (options) => {
        stepResults.upload = await upload(getResults("register", "upload"), options);
      },
      certify: () => {
        stepResults.certify = certify(getResults("upload", "certify"));
        return stepResults.certify.transaction;
      },
      listFiles: async () => {
        return listFiles(getResults("certify", "listFiles"));
      }
    };
  }
  writeBlobFlow({ blob }) {
    const encode = async () => {
      const metadata2 = __privateGet20(this, _uploadRelayClient) ? await this.computeBlobMetadata({
        bytes: blob
      }) : await this.encodeBlob(blob);
      return {
        metadata: metadata2,
        size: blob.length,
        data: __privateGet20(this, _uploadRelayClient) ? blob : void 0
      };
    };
    const register = ({ data, metadata: metadata2, size }, { epochs, deletable, owner, attributes }) => {
      const transaction = new Transaction();
      transaction.setSenderIfNotSet(owner);
      if (__privateGet20(this, _uploadRelayClient)) {
        const meta = metadata2;
        transaction.add(
          this.sendUploadRelayTip({
            size,
            blobDigest: meta.blobDigest,
            nonce: meta.nonce
          })
        );
      }
      transaction.transferObjects(
        [
          this.registerBlob({
            size,
            epochs,
            blobId: metadata2.blobId,
            rootHash: metadata2.rootHash,
            deletable,
            attributes
          })
        ],
        owner
      );
      return {
        registerTransaction: transaction,
        data,
        metadata: metadata2,
        deletable
      };
    };
    const upload = async ({ data, metadata: metadata2, deletable }, { digest }) => {
      const blobObject = await __privateMethod12(this, _WalrusClient_instances, getCreatedBlob_fn).call(this, digest);
      if (__privateGet20(this, _uploadRelayClient)) {
        const meta2 = metadata2;
        return {
          blobObject,
          metadata: metadata2,
          deletable,
          certificate: (await this.writeBlobToUploadRelay({
            blobId: metadata2.blobId,
            blob: data,
            nonce: meta2.nonce,
            txDigest: digest,
            blobObjectId: blobObject.id.id,
            deletable,
            encodingType: meta2.metadata.encodingType
          })).certificate
        };
      }
      const meta = metadata2;
      return {
        blobObject,
        metadata: metadata2,
        deletable,
        confirmations: await this.writeEncodedBlobToNodes({
          blobId: metadata2.blobId,
          objectId: blobObject.id.id,
          metadata: meta.metadata,
          sliversByNode: meta.sliversByNode,
          deletable
        })
      };
    };
    const certify = ({
      metadata: metadata2,
      confirmations,
      certificate,
      blobObject,
      deletable
    }) => {
      return {
        blobObject,
        metadata: metadata2,
        transaction: confirmations ? this.certifyBlobTransaction({
          blobId: metadata2.blobId,
          blobObjectId: blobObject.id.id,
          confirmations,
          deletable
        }) : this.certifyBlobTransaction({
          certificate,
          blobId: metadata2.blobId,
          blobObjectId: blobObject.id.id,
          deletable
        })
      };
    };
    async function getBlob({ blobObject, metadata: metadata2 }) {
      return {
        blobId: metadata2.blobId,
        blobObject
      };
    }
    const stepResults = {};
    function getResults(step, current) {
      if (!stepResults[step]) {
        throw new Error(`${step} must be executed before calling ${current}`);
      }
      return stepResults[step];
    }
    return {
      encode: async () => {
        if (!stepResults.encode) {
          stepResults.encode = await encode();
        }
      },
      register: (options) => {
        stepResults.register = register(getResults("encode", "register"), options);
        return stepResults.register.registerTransaction;
      },
      upload: async (options) => {
        stepResults.upload = await upload(getResults("register", "upload"), options);
      },
      certify: () => {
        stepResults.certify = certify(getResults("upload", "certify"));
        return stepResults.certify.transaction;
      },
      getBlob: async () => {
        return getBlob(getResults("certify", "getBlob"));
      }
    };
  }
};
_storageNodeClient = /* @__PURE__ */ new WeakMap();
_wasmUrl = /* @__PURE__ */ new WeakMap();
_packageConfig = /* @__PURE__ */ new WeakMap();
_suiClient = /* @__PURE__ */ new WeakMap();
_objectLoader = /* @__PURE__ */ new WeakMap();
_blobMetadataConcurrencyLimit = /* @__PURE__ */ new WeakMap();
_readCommittee = /* @__PURE__ */ new WeakMap();
_cache6 = /* @__PURE__ */ new WeakMap();
_uploadRelayConfig = /* @__PURE__ */ new WeakMap();
_uploadRelayClient = /* @__PURE__ */ new WeakMap();
_WalrusClient_instances = /* @__PURE__ */ new WeakSet();
walType_fn = function() {
  return __privateGet20(this, _cache6).read(["walType"], async () => {
    const stakeWithPool = await __privateGet20(this, _suiClient).core.getMoveFunction({
      packageId: await __privateMethod12(this, _WalrusClient_instances, getPackageId_fn).call(this),
      moduleName: "staking",
      name: "stake_with_pool"
    });
    const toStake = stakeWithPool.function.parameters[1];
    const toStakeCoin = toStake.body.$kind === "datatype" ? toStake.body.datatype : null;
    const toStakeCoinType = toStakeCoin?.typeParameters[0]?.$kind === "datatype" ? toStakeCoin.typeParameters[0] : null;
    if (toStakeCoinType?.$kind !== "datatype") {
      throw new WalrusClientError("WAL type not found");
    }
    return normalizeStructTag(toStakeCoinType.datatype.typeName);
  });
};
getPackageId_fn = function() {
  return __privateGet20(this, _cache6).read(["getPackageId"], async () => {
    const system = await __privateGet20(this, _objectLoader).load(__privateGet20(this, _packageConfig).systemObjectId);
    return parseStructTag(system.type).address;
  });
};
getWalrusPackageId_fn = function() {
  return __privateGet20(this, _cache6).read(["getSystemPackageId"], async () => {
    const { package_id } = await this.systemObject();
    return package_id;
  });
};
wasmBindings_fn = function() {
  return __privateGet20(this, _cache6).read(["wasmBindings"], async () => {
    return getWasmBindings(__privateGet20(this, _wasmUrl));
  });
};
internalReadBlob_fn = async function({ blobId, signal }) {
  const systemState = await this.systemState();
  const numShards = systemState.committee.n_shards;
  const blobMetadata = await this.getBlobMetadata({ blobId, signal });
  const slivers = await this.getSlivers({ blobId, signal });
  const bindings = await __privateMethod12(this, _WalrusClient_instances, wasmBindings_fn).call(this);
  const blobBytes = bindings.decodePrimarySlivers(
    blobId,
    numShards,
    blobMetadata.metadata.V1.unencoded_length,
    slivers
  );
  const reconstructedBlobMetadata = bindings.computeMetadata(
    systemState.committee.n_shards,
    blobBytes
  );
  if (reconstructedBlobMetadata.blobId !== blobId) {
    throw new InconsistentBlobError("The specified blob was encoded incorrectly.");
  }
  return blobBytes;
};
getCertificationEpoch_fn = async function({ blobId, signal }) {
  const stakingState = await this.stakingState();
  const currentEpoch = stakingState.epoch;
  if (stakingState.epoch_state.$kind === "EpochChangeSync") {
    const status = await this.getVerifiedBlobStatus({ blobId, signal });
    if (status.type === "nonexistent" || status.type === "invalid") {
      throw new BlobNotCertifiedError(`The specified blob ${blobId} is ${status.type}.`);
    }
    if (typeof status.initialCertifiedEpoch !== "number") {
      throw new BlobNotCertifiedError(`The specified blob ${blobId} is not certified.`);
    }
    if (status.initialCertifiedEpoch > currentEpoch) {
      throw new BehindCurrentEpochError(
        `The client is at epoch ${currentEpoch} while the specified blob was certified at epoch ${status.initialCertifiedEpoch}.`
      );
    }
    return status.initialCertifiedEpoch;
  }
  return currentEpoch;
};
getReadCommittee_fn = async function(options) {
  if (!__privateGet20(this, _readCommittee)) {
    __privateSet19(this, _readCommittee, __privateMethod12(this, _WalrusClient_instances, forceGetReadCommittee_fn).call(this, options));
  }
  return __privateGet20(this, _readCommittee);
};
forceGetReadCommittee_fn = async function({ blobId, signal }) {
  const stakingState = await this.stakingState();
  const isTransitioning = stakingState.epoch_state.$kind === "EpochChangeSync";
  const certificationEpoch = await __privateMethod12(this, _WalrusClient_instances, getCertificationEpoch_fn).call(this, { blobId, signal });
  if (isTransitioning && certificationEpoch < stakingState.epoch) {
    return await __privateMethod12(this, _WalrusClient_instances, getCommittee_fn).call(this, stakingState.previous_committee);
  }
  return await __privateMethod12(this, _WalrusClient_instances, getActiveCommittee_fn).call(this);
};
storageCostFromEncodedSize_fn = async function(encodedSize, epochs) {
  const systemState = await this.systemState();
  const storageUnits = storageUnitsFromSize(encodedSize);
  const storageCost = BigInt(storageUnits) * BigInt(systemState.storage_price_per_unit_size) * BigInt(epochs);
  const writeCost = BigInt(storageUnits) * BigInt(systemState.write_price_per_unit_size);
  return { storageCost, writeCost, totalCost: storageCost + writeCost };
};
withWal_fn = function(amount, source, fn) {
  return async (tx) => {
    const walType = await __privateMethod12(this, _WalrusClient_instances, walType_fn).call(this);
    const coin = source ? tx.splitCoins(source, [amount])[0] : tx.add(
      coinWithBalance({
        balance: amount,
        type: walType
      })
    );
    const result = await fn(coin, tx);
    tx.moveCall({
      target: "0x2::coin::destroy_zero",
      typeArguments: [walType],
      arguments: [coin]
    });
    return result;
  };
};
loadTipConfig_fn = function() {
  return __privateGet20(this, _cache6).read(["upload-relay-tip-config"], async () => {
    if (!__privateGet20(this, _uploadRelayConfig)?.sendTip || !__privateGet20(this, _uploadRelayClient)) {
      return null;
    }
    if ("kind" in __privateGet20(this, _uploadRelayConfig).sendTip) {
      return __privateGet20(this, _uploadRelayConfig).sendTip;
    }
    const tipConfig = await __privateGet20(this, _uploadRelayClient).tipConfig();
    if (!tipConfig) {
      return null;
    }
    return {
      ...tipConfig,
      max: __privateGet20(this, _uploadRelayConfig).sendTip.max
    };
  });
};
getCreatedBlob_fn = async function(digest) {
  const blobType = await this.getBlobType();
  const {
    transaction: { effects }
  } = await __privateGet20(this, _suiClient).core.waitForTransaction({
    digest
  });
  const createdObjectIds = effects?.changedObjects.filter((object2) => object2.idOperation === "Created").map((object2) => object2.id);
  const createdObjects = await __privateGet20(this, _suiClient).core.getObjects({
    objectIds: createdObjectIds
  });
  const suiBlobObject = createdObjects.objects.find(
    (object2) => !(object2 instanceof Error) && object2.type === blobType
  );
  if (suiBlobObject instanceof Error || !suiBlobObject) {
    throw new WalrusClientError(
      `Blob object not found in transaction effects for transaction (${digest})`
    );
  }
  return Blob2.parse(await suiBlobObject.content);
};
writeBlobAttributesForRef_fn = function({
  attributes,
  existingAttributes,
  blob
}) {
  return async (tx) => {
    const walrusPackageId = await __privateMethod12(this, _WalrusClient_instances, getWalrusPackageId_fn).call(this);
    if (!existingAttributes) {
      tx.add(
        addMetadata({
          package: walrusPackageId,
          arguments: {
            self: blob,
            metadata: _new({
              package: walrusPackageId
            })
          }
        })
      );
    }
    Object.keys(attributes).forEach((key) => {
      const value = attributes[key];
      if (value === null) {
        if (existingAttributes && key in existingAttributes) {
          tx.add(
            removeMetadataPair({
              package: walrusPackageId,
              arguments: {
                self: blob,
                key
              }
            })
          );
        }
      } else {
        tx.add(
          insertOrUpdateMetadataPair({
            package: walrusPackageId,
            arguments: {
              self: blob,
              key,
              value
            }
          })
        );
      }
    });
  };
};
executeTransaction_fn = async function(transaction, signer, action) {
  transaction.setSenderIfNotSet(signer.toSuiAddress());
  const { digest, effects } = await signer.signAndExecuteTransaction({
    transaction,
    client: __privateGet20(this, _suiClient)
  });
  if (effects?.status.error) {
    throw new WalrusClientError(`Failed to ${action} (${digest}): ${effects?.status.error}`);
  }
  await __privateGet20(this, _suiClient).core.waitForTransaction({
    digest
  });
  return { digest, effects };
};
getCommittee_fn = async function(committee) {
  const stakingPool = await __privateMethod12(this, _WalrusClient_instances, stakingPool_fn).call(this, committee);
  const shardIndicesByNodeId = getShardIndicesByNodeId(committee);
  const byShardIndex = /* @__PURE__ */ new Map();
  const nodes = stakingPool.map(({ node_info }, nodeIndex) => {
    const shardIndices = shardIndicesByNodeId.get(node_info.node_id) ?? [];
    const node = {
      id: node_info.node_id,
      info: node_info,
      networkUrl: `https://${node_info.network_address}`,
      shardIndices,
      nodeIndex
    };
    for (const shardIndex of shardIndices) {
      byShardIndex.set(shardIndex, node);
    }
    return node;
  });
  return {
    byShardIndex,
    nodes
  };
};
getActiveCommittee_fn = function() {
  return __privateGet20(this, _cache6).read(["getActiveCommittee"], async () => {
    const stakingState = await this.stakingState();
    return __privateMethod12(this, _WalrusClient_instances, getCommittee_fn).call(this, stakingState.committee);
  });
};
stakingPool_fn = async function(committee) {
  const nodeIds = committee[0].contents.map((node) => node.key);
  return __privateGet20(this, _objectLoader).loadManyOrThrow(nodeIds, StakingPool);
};
getNodeByShardIndex_fn = async function(committeeInfo, index) {
  const node = committeeInfo.byShardIndex.get(index);
  if (!node) {
    throw new WalrusClientError(`Node for shard index ${index} not found`);
  }
  return node;
};
retryOnPossibleEpochChange_fn = function(fn) {
  return async (...args) => {
    try {
      return await fn.apply(this, args);
    } catch (error) {
      if (error instanceof RetryableWalrusClientError) {
        this.reset();
        return await fn.apply(this, args);
      }
      throw error;
    }
  };
};
var WalrusClient = _WalrusClient;

// clients.ts
var _WalrusStorageClients = class _WalrusStorageClients {
  constructor(config) {
    this.suiClient = null;
    this.walrusClient = null;
    this.config = config;
  }
  getSuiClient() {
    if (!this.suiClient) {
      const rpcUrl = getFullnodeUrl(this.config.suiNetwork);
      this.suiClient = new SuiJsonRpcClient({ url: rpcUrl });
    }
    return this.suiClient;
  }
  getWalrusClient() {
    if (!this.walrusClient) {
      this.walrusClient = new WalrusClient({
        network: this.config.walrusNetwork,
        suiRpcUrl: getFullnodeUrl(this.config.suiNetwork),
        wasmUrl: "https://unpkg.com/@mysten/walrus-wasm@0.2.0/web/walrus_wasm_bg.wasm"
      });
    }
    return this.walrusClient;
  }
  updateNetwork(config) {
    this.config = config;
    this.suiClient = null;
    this.walrusClient = null;
  }
  createSigner(privateKey) {
    const trimmedKey = privateKey.trim();
    const parsed = decodeSuiPrivateKey(trimmedKey);
    switch (parsed.scheme) {
      case "ED25519":
        return Ed25519Keypair.fromSecretKey(trimmedKey);
      case "Secp256k1":
        return Secp256k1Keypair.fromSecretKey(trimmedKey);
      case "Secp256r1":
        return Secp256r1Keypair.fromSecretKey(trimmedKey);
      default:
        throw new Error(`Unsupported Sui private key scheme: ${parsed.scheme}`);
    }
  }
  getAddressFromPrivateKey(privateKey) {
    return this.createSigner(privateKey).toSuiAddress();
  }
  deriveEd25519KeypairFromMnemonic(mnemonics, path) {
    const normalized = mnemonics.trim().replace(/\s+/g, " ");
    if (!normalized) {
      throw new Error("Mnemonic cannot be empty.");
    }
    return Ed25519Keypair.deriveKeypair(normalized, path?.trim() || void 0);
  }
  async uploadBlob({
    blob,
    epochs,
    signer,
    owner,
    attributes
  }) {
    const walrusClient = this.getWalrusClient();
    const traceId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    (() => {
      const u = "http://127.0.0.1:7777/event";
      const s = "upload-fails-balances";
      fetch(u, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sessionId: s,
          runId: "post-fix",
          hypothesisId: "B",
          location: "clients.ts:uploadBlob",
          traceId,
          msg: "[DEBUG] writeBlob start",
          data: {
            epochs,
            blobSize: blob.byteLength,
            owner,
            signerAddress: signer.toSuiAddress(),
            hasAttributes: Boolean(attributes),
            attributeKeys: attributes ? Object.keys(attributes) : []
          },
          ts: Date.now()
        })
      }).catch(() => {
      });
    })();
    const result = await walrusClient.writeBlob({
      blob,
      deletable: true,
      epochs,
      signer,
      owner,
      attributes
    }).catch((error) => {
      (() => {
        const u = "http://127.0.0.1:7777/event";
        const s = "upload-fails-balances";
        fetch(u, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            sessionId: s,
            runId: "post-fix",
            hypothesisId: "C",
            location: "clients.ts:uploadBlob:writeBlob",
            traceId,
            msg: "[DEBUG] writeBlob error",
            data: {
              error: error instanceof Error ? {
                name: error.name,
                message: error.message,
                stack: error.stack
              } : { value: String(error) }
            },
            ts: Date.now()
          })
        }).catch(() => {
        });
      })();
      throw error;
    });
    (() => {
      const u = "http://127.0.0.1:7777/event";
      const s = "upload-fails-balances";
      fetch(u, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sessionId: s,
          runId: "post-fix",
          hypothesisId: "B",
          location: "clients.ts:uploadBlob",
          traceId,
          msg: "[DEBUG] writeBlob success",
          data: {
            blobId: result.blobId,
            blobObjectId: result.blobObject.id.id,
            size: result.blobObject.size
          },
          ts: Date.now()
        })
      }).catch(() => {
      });
    })();
    return {
      blobId: result.blobId,
      blobObjectId: result.blobObject.id.id,
      size: Number(result.blobObject.size)
    };
  }
  async downloadBlob(blobId) {
    const walrusClient = this.getWalrusClient();
    const traceId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    (() => {
      fetch("http://127.0.0.1:7778/event", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sessionId: "download-blob-metadata",
          runId: "pre-fix",
          hypothesisId: "D",
          location: "clients.ts:downloadBlob:start",
          traceId,
          msg: "[DEBUG] downloadBlob start",
          data: { blobId },
          ts: Date.now()
        })
      }).catch(() => {
      });
    })();
    try {
      const metadata = await walrusClient.getBlobMetadata({ blobId });
      (() => {
        fetch("http://127.0.0.1:7778/event", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            sessionId: "download-blob-metadata",
            runId: "pre-fix",
            hypothesisId: "D",
            location: "clients.ts:downloadBlob:metadata",
            traceId,
            msg: "[DEBUG] getBlobMetadata success",
            data: {
              blobId,
              metadataKind: metadata.metadata.$kind,
              metadataBlobId: metadata.blobId
            },
            ts: Date.now()
          })
        }).catch(() => {
        });
      })();
    } catch (error) {
      (() => {
        fetch("http://127.0.0.1:7778/event", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            sessionId: "download-blob-metadata",
            runId: "pre-fix",
            hypothesisId: "D",
            location: "clients.ts:downloadBlob:metadata",
            traceId,
            msg: "[DEBUG] getBlobMetadata error",
            data: {
              blobId,
              error: error instanceof Error ? { name: error.name, message: error.message, stack: error.stack } : { value: String(error) }
            },
            ts: Date.now()
          })
        }).catch(() => {
        });
      })();
    }
    return walrusClient.readBlob({ blobId }).catch((error) => {
      (() => {
        fetch("http://127.0.0.1:7778/event", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            sessionId: "download-blob-metadata",
            runId: "pre-fix",
            hypothesisId: "D",
            location: "clients.ts:downloadBlob:readBlob",
            traceId,
            msg: "[DEBUG] readBlob error",
            data: {
              blobId,
              error: error instanceof Error ? { name: error.name, message: error.message, stack: error.stack } : { value: String(error) }
            },
            ts: Date.now()
          })
        }).catch(() => {
        });
      })();
      throw error;
    });
  }
  async downloadBlobByObjectId(blobObjectId) {
    return this.downloadFromAggregators(
      `/v1/blobs/by-object-id/${encodeURIComponent(blobObjectId)}`,
      "Object ID download failed"
    );
  }
  async getAddressBalances(owner) {
    const suiClient = this.getSuiClient();
    const walCoinType = this.config.walrusNetwork === "testnet" ? _WalrusStorageClients.TESTNET_WAL_COIN_TYPE : _WalrusStorageClients.MAINNET_WAL_COIN_TYPE;
    const [suiBalance, walBalance, suiMetadata, walMetadata] = await Promise.all([
      suiClient.getBalance({ owner, coinType: "0x2::sui::SUI" }),
      suiClient.getBalance({ owner, coinType: walCoinType }),
      suiClient.getCoinMetadata({ coinType: "0x2::sui::SUI" }),
      suiClient.getCoinMetadata({ coinType: walCoinType })
    ]);
    return {
      sui: this.formatBalanceSummary(
        "0x2::sui::SUI",
        suiBalance.totalBalance,
        suiMetadata?.decimals ?? 9,
        suiMetadata?.symbol ?? "SUI"
      ),
      wal: this.formatBalanceSummary(
        walCoinType,
        walBalance.totalBalance,
        walMetadata?.decimals ?? 9,
        walMetadata?.symbol ?? "WAL"
      )
    };
  }
  async listOwnedBlobs(owner) {
    const suiClient = this.getSuiClient();
    const walrusClient = this.getWalrusClient();
    const blobType = await walrusClient.getBlobType();
    const response = await suiClient.getOwnedObjects({
      owner,
      filter: { StructType: blobType },
      options: {
        showType: true,
        showContent: true
      }
    });
    return response.data.map((item) => {
      const content = item.data?.content;
      const fields = content && content.dataType === "moveObject" ? content.fields : null;
      const rawBlobId = fields?.blob_id;
      const rawSize = fields?.size;
      const normalizedBlobId = this.normalizeBlobId(rawBlobId, item.data?.objectId ?? "");
      (() => {
        fetch("http://127.0.0.1:7778/event", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            sessionId: "download-blob-metadata",
            runId: "pre-fix",
            hypothesisId: "A",
            location: "clients.ts:listOwnedBlobs",
            msg: "[DEBUG] owned blob mapped",
            data: {
              objectId: item.data?.objectId ?? "",
              rawBlobId,
              rawBlobIdType: Array.isArray(rawBlobId) ? "array" : typeof rawBlobId,
              normalizedBlobId,
              type: item.data?.type ?? null
            },
            ts: Date.now()
          })
        }).catch(() => {
        });
      })();
      return {
        blobId: normalizedBlobId,
        blobObjectId: item.data?.objectId ?? "",
        size: typeof rawSize === "string" ? Number(rawSize) : typeof rawSize === "number" ? rawSize : null,
        type: item.data?.type ?? null
      };
    });
  }
  async testConnection() {
    const results = { sui: false, walrus: false };
    try {
      const suiClient = this.getSuiClient();
      await suiClient.getLatestCheckpointSequenceNumber();
      results.sui = true;
    } catch (error) {
      console.error("Sui connection test failed:", error);
    }
    try {
      const walrusClient = this.getWalrusClient();
      await walrusClient.systemState();
      results.walrus = true;
    } catch (error) {
      console.error("Walrus connection test failed:", error);
    }
    return results;
  }
  normalizeBlobId(rawBlobId, fallback) {
    try {
      const bcsUtils = require("@mysten/walrus/dist/cjs/utils/bcs.js");
      if (typeof rawBlobId === "string") {
        return /^\d+$/.test(rawBlobId) ? bcsUtils.blobIdFromInt(rawBlobId) : rawBlobId;
      }
      if (typeof rawBlobId === "number" || typeof rawBlobId === "bigint") {
        return bcsUtils.blobIdFromInt(String(rawBlobId));
      }
      if (rawBlobId instanceof Uint8Array) {
        return bcsUtils.blobIdFromBytes(rawBlobId);
      }
      if (Array.isArray(rawBlobId) && rawBlobId.every((value) => typeof value === "number")) {
        return bcsUtils.blobIdFromBytes(new Uint8Array(rawBlobId));
      }
      if (rawBlobId && typeof rawBlobId === "object" && "bytes" in rawBlobId && Array.isArray(rawBlobId.bytes)) {
        return bcsUtils.blobIdFromBytes(
          new Uint8Array(rawBlobId.bytes)
        );
      }
    } catch (error) {
      (() => {
        fetch("http://127.0.0.1:7778/event", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            sessionId: "download-blob-metadata",
            runId: "pre-fix",
            hypothesisId: "B",
            location: "clients.ts:normalizeBlobId",
            msg: "[DEBUG] normalizeBlobId error",
            data: {
              rawBlobId,
              fallback,
              error: error instanceof Error ? { name: error.name, message: error.message, stack: error.stack } : { value: String(error) }
            },
            ts: Date.now()
          })
        }).catch(() => {
        });
      })();
      console.error("Failed to normalize blob ID:", rawBlobId, error);
    }
    return fallback;
  }
  async downloadFromAggregators(pathname, errorPrefix) {
    const errors = [];
    for (const baseUrl of this.getAggregatorUrls()) {
      try {
        const response = await fetch(`${baseUrl}${pathname}`);
        if (!response.ok) {
          errors.push(`${baseUrl} -> ${response.status}`);
          continue;
        }
        return new Uint8Array(await response.arrayBuffer());
      } catch (error) {
        errors.push(
          `${baseUrl} -> ${error instanceof Error ? error.message : String(error)}`
        );
      }
    }
    throw new Error(`${errorPrefix}. Aggregators tried: ${errors.join("; ")}`);
  }
  getAggregatorUrls() {
    return this.config.walrusNetwork === "mainnet" ? _WalrusStorageClients.MAINNET_AGGREGATORS : _WalrusStorageClients.TESTNET_AGGREGATORS;
  }
  formatBalanceSummary(coinType, totalBalance, decimals, symbol) {
    return {
      coinType,
      symbol,
      totalBalance,
      decimals,
      formatted: this.formatAmount(totalBalance, decimals)
    };
  }
  formatAmount(rawAmount, decimals) {
    const normalized = rawAmount.replace(/^0+(\d)/, "$1");
    const digits = normalized === "" ? "0" : normalized;
    if (decimals <= 0) {
      return digits;
    }
    if (digits.length <= decimals) {
      const paddedFraction = digits.padStart(decimals, "0").replace(/0+$/, "");
      return paddedFraction ? `0.${paddedFraction}` : "0";
    }
    const integerPart = digits.slice(0, digits.length - decimals);
    const fractionPart = digits.slice(digits.length - decimals).replace(/0+$/, "");
    return fractionPart ? `${integerPart}.${fractionPart}` : integerPart;
  }
};
_WalrusStorageClients.TESTNET_WAL_COIN_TYPE = "0x8270feb7375eee355e64fdb69c50abb6b5f9393a722883c1cf45f8e26048810a::wal::WAL";
_WalrusStorageClients.MAINNET_WAL_COIN_TYPE = "0x356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59::wal::WAL";
_WalrusStorageClients.TESTNET_AGGREGATORS = [
  "https://aggregator.walrus-testnet.walrus.space",
  "https://aggregator.walrus-testnet.h2o-nodes.com",
  "https://aggregator.testnet.walrus.mirai.cloud",
  "https://testnet-aggregator.walrus.graphyte.dev",
  "https://walrus-testnet-aggregator.chainflow.io"
];
_WalrusStorageClients.MAINNET_AGGREGATORS = [
  "https://aggregator.walrus-mainnet.walrus.space",
  "https://aggregator.walrus-mainnet.h2o-nodes.com",
  "https://aggregator.mainnet.walrus.mirai.cloud",
  "https://mainnet-aggregator.walrus.graphyte.dev",
  "https://walmain.agg.chainflow.io"
];
var WalrusStorageClients = _WalrusStorageClients;

// main.ts
var DEFAULT_SETTINGS = {
  suiNetwork: "testnet",
  walrusNetwork: "testnet",
  suiAddress: "",
  walrusAddress: "",
  suiPrivateKey: "",
  walrusPrivateKey: "",
  storageEpochs: 5,
  downloadFolder: "walrus-downloads",
  uploads: []
};
var WALRUS_STORAGE_VIEW_TYPE = "walrus-storage-right-view";
var WalrusStoragePlugin = class extends import_obsidian.Plugin {
  async onload() {
    await this.loadSettings();
    this.initializeClients();
    this.settingTab = new WalrusStorageSettingTab(this.app, this);
    this.addSettingTab(this.settingTab);
    this.registerView(
      WALRUS_STORAGE_VIEW_TYPE,
      (leaf) => new WalrusStorageRightView(leaf, this)
    );
    this.addRibbonIcon("database", "Open Walrus Storage", () => {
      void this.openPluginHome();
    });
    this.addCommand({
      id: "show-current-config",
      name: "Show network config",
      callback: () => {
        new import_obsidian.Notice(
          `Sui: ${this.settings.suiNetwork} | Walrus: ${this.settings.walrusNetwork}`
        );
      }
    });
    this.addCommand({
      id: "derive-addresses-from-private-key",
      name: "Derive Sui/Walrus addresses from private key",
      callback: () => {
        void this.deriveAddressesFromPrivateKeys();
      }
    });
    this.addCommand({
      id: "prepare-note-for-upload",
      name: "Preview current note (before upload)",
      checkCallback: (checking) => {
        const file = this.app.workspace.getActiveFile();
        if (!(file instanceof import_obsidian.TFile) || file.extension !== "md") {
          return false;
        }
        if (!checking) {
          void this.previewActiveNote(file);
        }
        return true;
      }
    });
    this.addCommand({
      id: "upload-active-note-to-walrus",
      name: "Upload current note to Walrus",
      checkCallback: (checking) => {
        const file = this.app.workspace.getActiveFile();
        if (!(file instanceof import_obsidian.TFile) || file.extension !== "md") {
          return false;
        }
        if (!checking) {
          void this.uploadActiveNote();
        }
        return true;
      }
    });
    this.addCommand({
      id: "show-walrus-uploads",
      name: "Open Walrus panel",
      callback: () => {
        void this.openPluginHome();
      }
    });
    this.addCommand({
      id: "download-latest-upload-for-active-note",
      name: "Download latest uploaded version for current note",
      checkCallback: (checking) => {
        const file = this.app.workspace.getActiveFile();
        if (!(file instanceof import_obsidian.TFile) || file.extension !== "md") {
          return false;
        }
        if (!checking) {
          void this.downloadLatestForActiveNote(file);
        }
        return true;
      }
    });
    this.addCommand({
      id: "test-sdk-connections",
      name: "Test Sui/Walrus connection",
      callback: () => {
        void this.testSdkConnections();
      }
    });
  }
  onunload() {
    this.app.workspace.detachLeavesOfType(WALRUS_STORAGE_VIEW_TYPE);
  }
  async loadSettings() {
    this.settings = Object.assign({}, DEFAULT_SETTINGS, await this.loadData());
  }
  async saveSettings() {
    await this.saveData(this.settings);
  }
  async openPluginHome() {
    this.app.workspace.detachLeavesOfType(WALRUS_STORAGE_VIEW_TYPE);
    const leaf = this.app.workspace.getRightLeaf(false);
    if (!leaf) {
      new import_obsidian.Notice("Unable to open the right panel.");
      return;
    }
    await leaf.setViewState({
      type: WALRUS_STORAGE_VIEW_TYPE,
      active: true
    });
    await this.app.workspace.revealLeaf(leaf);
    this.refreshRightView();
  }
  openPluginSettings() {
    const setting = this.app.setting;
    if (!setting) {
      new import_obsidian.Notice("Unable to open Obsidian settings. Please open settings manually.");
      return;
    }
    if (typeof setting.open === "function") {
      setting.open();
    }
    if (typeof setting.openTabById === "function") {
      setting.openTabById(this.manifest.id);
      return;
    }
    new import_obsidian.Notice("Unable to locate the plugin settings tab. Please select Walrus Storage in settings.");
  }
  refreshSettingTab() {
    if (this.settingTab) {
      this.settingTab.display();
    }
  }
  refreshRightView() {
    const leaves = this.app.workspace.getLeavesOfType(WALRUS_STORAGE_VIEW_TYPE);
    for (const leaf of leaves) {
      const view = leaf.view;
      if (view instanceof WalrusStorageRightView) {
        void view.render();
      }
    }
  }
  getDownloadFolderPath() {
    const fallback = DEFAULT_SETTINGS.downloadFolder;
    const raw = (this.settings.downloadFolder || fallback).trim();
    const cleaned = raw.replace(/^\/+/, "").replace(/\/+$/, "");
    return (0, import_obsidian.normalizePath)(cleaned || fallback);
  }
  async openDownloadFolder() {
    const folder = this.getDownloadFolderPath();
    await this.ensureFolderExists(folder);
    try {
      const adapter = this.app.vault.adapter;
      const fullPath = typeof adapter?.getFullPath === "function" ? adapter.getFullPath(folder) : "";
      if (fullPath) {
        const electron = require("electron");
        if (electron?.shell?.openPath) {
          await electron.shell.openPath(fullPath);
          return;
        }
      }
    } catch {
    }
    const abstract = this.app.vault.getAbstractFileByPath(folder);
    const leaf = this.app.workspace.getLeavesOfType("file-explorer")[0];
    const view = leaf?.view;
    if (abstract && typeof view?.revealInFolder === "function") {
      view.revealInFolder(abstract);
      return;
    }
    new import_obsidian.Notice("Unable to open download folder.");
  }
  initializeClients() {
    this.clients = new WalrusStorageClients({
      suiNetwork: this.settings.suiNetwork,
      walrusNetwork: this.settings.walrusNetwork
    });
  }
  async refreshClients() {
    this.clients.updateNetwork({
      suiNetwork: this.settings.suiNetwork,
      walrusNetwork: this.settings.walrusNetwork
    });
  }
  getWalrusPrivateKey() {
    return this.settings.walrusPrivateKey.trim() || this.settings.suiPrivateKey.trim();
  }
  getWalrusSigner() {
    const privateKey = this.getWalrusPrivateKey();
    if (!privateKey) {
      throw new Error("Please set a Sui private key or a Walrus private key in settings first.");
    }
    return this.clients.createSigner(privateKey);
  }
  async deriveAddressesFromPrivateKeys() {
    try {
      if (!this.settings.suiPrivateKey.trim() && !this.getWalrusPrivateKey()) {
        throw new Error("Please provide at least one private key first.");
      }
      if (this.settings.suiPrivateKey.trim()) {
        this.settings.suiAddress = this.clients.getAddressFromPrivateKey(
          this.settings.suiPrivateKey
        );
      }
      const walrusPrivateKey = this.getWalrusPrivateKey();
      if (walrusPrivateKey) {
        this.settings.walrusAddress = this.clients.getAddressFromPrivateKey(walrusPrivateKey);
      }
      await this.saveSettings();
      this.refreshSettingTab();
      this.refreshRightView();
      new import_obsidian.Notice("Addresses derived from private key.");
    } catch (error) {
      new import_obsidian.Notice(this.formatError(error, "Failed to derive addresses"));
    }
  }
  async initializeSuiFromMnemonic(mnemonics, path) {
    try {
      const keypair = this.clients.deriveEd25519KeypairFromMnemonic(
        mnemonics,
        path
      );
      this.settings.suiPrivateKey = keypair.getSecretKey();
      await this.deriveAddressesFromPrivateKeys();
      new import_obsidian.Notice("Sui address initialized from mnemonic.");
    } catch (error) {
      new import_obsidian.Notice(this.formatError(error, "Failed to initialize from mnemonic"));
    }
  }
  async testSdkConnections() {
    new import_obsidian.Notice("Testing Sui/Walrus connection...");
    const results = await this.clients.testConnection();
    new import_obsidian.Notice(
      `Sui: ${results.sui ? "OK" : "FAILED"} | Walrus: ${results.walrus ? "OK" : "FAILED"}`
    );
  }
  async uploadActiveNote() {
    try {
      const traceId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      const file = this.app.workspace.getActiveFile();
      if (!(file instanceof import_obsidian.TFile) || file.extension !== "md") {
        throw new Error("Please open a Markdown note first.");
      }
      const content = await this.app.vault.read(file);
      if (!content.trim()) {
        throw new Error("Current note is empty.");
      }
      const signer = this.getWalrusSigner();
      (() => {
        const u = "http://127.0.0.1:7777/event";
        const s = "upload-fails-balances";
        fetch(u, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            sessionId: s,
            runId: "post-fix",
            hypothesisId: "D",
            location: "main.ts:uploadActiveNote",
            traceId,
            msg: "[DEBUG] upload start",
            data: {
              filePath: file.path,
              fileName: file.name,
              contentLength: content.length,
              suiNetwork: this.settings.suiNetwork,
              walrusNetwork: this.settings.walrusNetwork,
              epochs: this.settings.storageEpochs,
              signerAddress: signer.toSuiAddress(),
              hasSuiAddress: Boolean(this.settings.suiAddress),
              hasWalrusAddress: Boolean(this.settings.walrusAddress)
            },
            ts: Date.now()
          })
        }).catch(() => {
        });
      })();
      if (!this.settings.suiAddress || !this.settings.walrusAddress) {
        await this.deriveAddressesFromPrivateKeys();
      }
      (() => {
        const u = "http://127.0.0.1:7777/event";
        const s = "upload-fails-balances";
        this.clients.getSuiClient().getAllBalances({ owner: signer.toSuiAddress() }).then((balances) => {
          fetch(u, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              sessionId: s,
              runId: "post-fix",
              hypothesisId: "A",
              location: "main.ts:balances",
              traceId,
              msg: "[DEBUG] balances snapshot",
              data: { balances },
              ts: Date.now()
            })
          }).catch(() => {
          });
        }).catch((err) => {
          fetch(u, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              sessionId: s,
              runId: "post-fix",
              hypothesisId: "A",
              location: "main.ts:balances",
              traceId,
              msg: "[DEBUG] balances snapshot failed",
              data: {
                error: err instanceof Error ? { name: err.name, message: err.message, stack: err.stack } : { value: String(err) }
              },
              ts: Date.now()
            })
          }).catch(() => {
          });
        });
      })();
      new import_obsidian.Notice("Uploading current note to Walrus...");
      const encoder = new TextEncoder();
      const result = await this.clients.uploadBlob({
        blob: encoder.encode(content),
        epochs: Math.max(1, this.settings.storageEpochs),
        signer,
        owner: signer.toSuiAddress(),
        attributes: {
          fileName: file.name,
          sourcePath: file.path,
          uploadedAt: (/* @__PURE__ */ new Date()).toISOString()
        }
      });
      this.upsertUploadRecord({
        blobId: result.blobId,
        blobObjectId: result.blobObjectId,
        sourcePath: file.path,
        sourceName: file.name,
        size: result.size,
        uploadedAt: (/* @__PURE__ */ new Date()).toISOString(),
        suiAddress: signer.toSuiAddress(),
        suiNetwork: this.settings.suiNetwork,
        walrusNetwork: this.settings.walrusNetwork
      });
      await this.saveSettings();
      this.refreshSettingTab();
      this.refreshRightView();
      new import_obsidian.Notice(`Upload succeeded. Blob ID: ${result.blobId}`);
    } catch (error) {
      (() => {
        const u = "http://127.0.0.1:7777/event";
        const s = "upload-fails-balances";
        fetch(u, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            sessionId: s,
            runId: "post-fix",
            hypothesisId: "E",
            location: "main.ts:uploadActiveNote:catch",
            msg: "[DEBUG] upload failed",
            data: {
              error: error instanceof Error ? { name: error.name, message: error.message, stack: error.stack } : { value: String(error) }
            },
            ts: Date.now()
          })
        }).catch(() => {
        });
      })();
      new import_obsidian.Notice(this.formatError(error, "Upload failed"));
    }
  }
  async downloadLatestForActiveNote(file) {
    const matchedRecords = this.settings.uploads.filter((record2) => record2.sourcePath === file.path).sort((left, right) => right.uploadedAt.localeCompare(left.uploadedAt));
    if (matchedRecords.length === 0) {
      new import_obsidian.Notice("No uploaded version found for the current note.");
      return;
    }
    await this.downloadRecord(matchedRecords[0]);
  }
  async downloadRecord(record2) {
    new import_obsidian.Notice(`Downloading ${record2.sourceName}...`);
    try {
      let bytes;
      try {
        bytes = await this.clients.downloadBlob(record2.blobId);
      } catch (primaryError) {
        bytes = await this.clients.downloadBlobByObjectId(record2.blobObjectId).catch(
          (fallbackError) => {
            const primaryMessage = this.formatError(
              primaryError,
              "Blob ID download failed"
            );
            const fallbackMessage = this.formatError(
              fallbackError,
              "Object ID fallback download failed"
            );
            throw new Error(`${primaryMessage}; ${fallbackMessage}`);
          }
        );
      }
      const content = new TextDecoder().decode(bytes);
      const targetPath = this.buildDownloadPath(record2);
      await this.ensureFolderExists(this.getDownloadFolderPath());
      const existingFile = this.app.vault.getAbstractFileByPath(targetPath);
      if (existingFile instanceof import_obsidian.TFile) {
        await this.app.vault.modify(existingFile, content);
      } else {
        await this.app.vault.create(targetPath, content);
      }
      this.refreshRightView();
      new import_obsidian.Notice(`Downloaded to ${targetPath}`);
      this.openDownloadModal({ success: true, vaultPath: targetPath });
      return targetPath;
    } catch (error) {
      const message = this.formatError(error, "Download failed");
      new import_obsidian.Notice(message);
      this.openDownloadModal({ success: false, vaultPath: record2.sourceName, error: message });
      throw error;
    }
  }
  async downloadBlobToVault(blobId, blobObjectId) {
    new import_obsidian.Notice(`Downloading blob ${blobId}...`);
    try {
      let bytes;
      try {
        bytes = await this.clients.downloadBlob(blobId);
      } catch (primaryError) {
        if (!blobObjectId) {
          throw primaryError;
        }
        bytes = await this.clients.downloadBlobByObjectId(blobObjectId).catch(
          (fallbackError) => {
            const primaryMessage = this.formatError(
              primaryError,
              "Blob ID download failed"
            );
            const fallbackMessage = this.formatError(
              fallbackError,
              "Object ID fallback download failed"
            );
            throw new Error(`${primaryMessage}; ${fallbackMessage}`);
          }
        );
      }
      const suffix = blobId.slice(0, 8);
      const folder = this.getDownloadFolderPath();
      const targetPath = (0, import_obsidian.normalizePath)(
        `${folder}/blob-${suffix}.bin`
      );
      const buffer = bytes.slice().buffer;
      await this.ensureFolderExists(folder);
      const existingFile = this.app.vault.getAbstractFileByPath(targetPath);
      if (existingFile instanceof import_obsidian.TFile) {
        await this.app.vault.modifyBinary(existingFile, buffer);
      } else {
        await this.app.vault.createBinary(targetPath, buffer);
      }
      this.refreshRightView();
      new import_obsidian.Notice(`Downloaded to ${targetPath}`);
      this.openDownloadModal({ success: true, vaultPath: targetPath });
      return targetPath;
    } catch (error) {
      const message = this.formatError(error, "Download failed");
      new import_obsidian.Notice(message);
      this.openDownloadModal({ success: false, vaultPath: blobId, error: message });
      throw error;
    }
  }
  openDownloadModal(payload) {
    const fullPath = payload.success ? this.getFullPath(payload.vaultPath) : "";
    try {
      new DownloadResultModal(this.app, {
        success: payload.success,
        vaultPath: payload.vaultPath,
        fullPath,
        error: payload.error || ""
      }).open();
    } catch {
    }
  }
  getFullPath(vaultPath) {
    try {
      const adapter = this.app.vault.adapter;
      if (typeof adapter?.getFullPath === "function") {
        return String(adapter.getFullPath(vaultPath) ?? "");
      }
    } catch {
    }
    return "";
  }
  async getOwnedBlobs() {
    const address = await this.ensureSuiAddress();
    return this.clients.listOwnedBlobs(address);
  }
  async getCurrentBalances() {
    const address = await this.ensureSuiAddress();
    return this.clients.getAddressBalances(address);
  }
  async ensureSuiAddress() {
    if (!this.settings.suiAddress.trim()) {
      await this.deriveAddressesFromPrivateKeys();
    }
    if (!this.settings.suiAddress.trim()) {
      throw new Error("Missing Sui address. Please set a private key and derive addresses first.");
    }
    return this.settings.suiAddress.trim();
  }
  buildDownloadPath(record2) {
    const suffix = record2.blobId.slice(0, 8);
    const baseName = this.getBaseName(record2.sourceName);
    const folder = this.getDownloadFolderPath();
    return (0, import_obsidian.normalizePath)(
      `${folder}/${baseName}-${suffix}.md`
    );
  }
  getBaseName(fileName) {
    const lastDotIndex = fileName.lastIndexOf(".");
    return lastDotIndex > 0 ? fileName.slice(0, lastDotIndex) : fileName;
  }
  async ensureFolderExists(folderPath) {
    const normalizedFolder = (0, import_obsidian.normalizePath)(folderPath.trim());
    if (!normalizedFolder || normalizedFolder === ".") {
      return;
    }
    const segments = normalizedFolder.split("/").filter(Boolean);
    let currentPath = "";
    for (const segment of segments) {
      currentPath = currentPath ? `${currentPath}/${segment}` : segment;
      if (!this.app.vault.getAbstractFileByPath(currentPath)) {
        await this.app.vault.createFolder(currentPath);
      }
    }
  }
  upsertUploadRecord(record2) {
    const uploads = this.settings.uploads.filter(
      (item) => item.blobId !== record2.blobId
    );
    uploads.unshift(record2);
    this.settings.uploads = uploads;
  }
  async previewActiveNote(file) {
    const content = await this.app.vault.read(file);
    const preview = content.trim().length > 120 ? `${content.trim().slice(0, 120)}...` : content.trim();
    if (!preview) {
      new import_obsidian.Notice("Current note is empty.");
      return;
    }
    new import_obsidian.Notice(`Preview: ${preview}`);
  }
  formatError(error, fallback) {
    if (error instanceof Error && error.message) {
      return `${fallback}: ${error.message}`;
    }
    return fallback;
  }
};
var WalrusStorageSettingTab = class extends import_obsidian.PluginSettingTab {
  constructor(app, plugin) {
    super(app, plugin);
    this.mnemonicDraft = "";
    this.mnemonicPathDraft = "";
    this.plugin = plugin;
  }
  display() {
    const { containerEl } = this;
    containerEl.empty();
    containerEl.createEl("h2", { text: "Walrus Storage Settings" });
    containerEl.createEl("p", {
      text: "This version supports address derivation, uploading the current note, viewing upload records, and downloading."
    });
    new import_obsidian.Setting(containerEl).setName("Sui network").setDesc("Select the Sui network.").addDropdown((dropdown) => {
      dropdown.addOption("testnet", "Testnet").addOption("mainnet", "Mainnet").setValue(this.plugin.settings.suiNetwork).onChange(async (value) => {
        this.plugin.settings.suiNetwork = value === "mainnet" ? "mainnet" : "testnet";
        await this.plugin.saveSettings();
        await this.plugin.refreshClients();
      });
    });
    new import_obsidian.Setting(containerEl).setName("Walrus network").setDesc("Select the Walrus network.").addDropdown((dropdown) => {
      dropdown.addOption("testnet", "Testnet").addOption("mainnet", "Mainnet").setValue(this.plugin.settings.walrusNetwork).onChange(async (value) => {
        this.plugin.settings.walrusNetwork = value === "mainnet" ? "mainnet" : "testnet";
        await this.plugin.saveSettings();
        await this.plugin.refreshClients();
      });
    });
    new import_obsidian.Setting(containerEl).setName("Storage epochs").setDesc("Epochs to use when uploading to Walrus.").addText((text) => {
      text.setPlaceholder("5").setValue(String(this.plugin.settings.storageEpochs)).onChange(async (value) => {
        const parsedValue = Number.parseInt(value || "0", 10);
        this.plugin.settings.storageEpochs = Number.isFinite(parsedValue) && parsedValue > 0 ? parsedValue : 1;
        await this.plugin.saveSettings();
      });
    });
    new import_obsidian.Setting(containerEl).setName("Download folder").setDesc("Downloaded files will be written to this folder.").addText((text) => {
      text.setPlaceholder("walrus-downloads").setValue(this.plugin.settings.downloadFolder).onChange(async (value) => {
        const sanitized = value.trim().replace(/^\/+/, "").replace(/\/+$/, "");
        this.plugin.settings.downloadFolder = (0, import_obsidian.normalizePath)(sanitized || "walrus-downloads");
        await this.plugin.saveSettings();
      });
    });
    new import_obsidian.Setting(containerEl).setName("Test SDK connection").setDesc("Initialize clients with current network config and run a connectivity test.").addButton((button) => {
      button.setButtonText("Test").onClick(() => {
        void this.plugin.testSdkConnections();
      });
    });
    new import_obsidian.Setting(containerEl).setName("Derive addresses").setDesc("Derive Sui/Walrus addresses from private key.").addButton((button) => {
      button.setButtonText("Derive").onClick(() => {
        void this.plugin.deriveAddressesFromPrivateKeys();
      });
    });
    new import_obsidian.Setting(containerEl).setName("Initialize from mnemonic (Sui)").setDesc("Derive suiprivkey and addresses from mnemonic. The mnemonic is not stored; only the derived suiprivkey is saved.").addTextArea((textArea) => {
      textArea.setPlaceholder("word1 word2 ...").setValue(this.mnemonicDraft).onChange((value) => {
        this.mnemonicDraft = value;
      });
      textArea.inputEl.rows = 3;
    }).addText((text) => {
      text.setPlaceholder("Derivation path (optional)").setValue(this.mnemonicPathDraft).onChange((value) => {
        this.mnemonicPathDraft = value;
      });
    }).addButton((button) => {
      button.setButtonText("Initialize").onClick(() => {
        const mnemonics = this.mnemonicDraft;
        const path = this.mnemonicPathDraft;
        this.mnemonicDraft = "";
        this.mnemonicPathDraft = "";
        void this.plugin.initializeSuiFromMnemonic(mnemonics, path);
        this.display();
      });
    });
    new import_obsidian.Setting(containerEl).setName("Sui address").setDesc("Current Sui address.").addText((text) => {
      text.setPlaceholder("0x...").setValue(this.plugin.settings.suiAddress).onChange(async (value) => {
        this.plugin.settings.suiAddress = value.trim();
        await this.plugin.saveSettings();
      });
    });
    new import_obsidian.Setting(containerEl).setName("Walrus address").setDesc("Address used for Walrus writes. Defaults to reusing the Sui private key.").addText((text) => {
      text.setPlaceholder("0x...").setValue(this.plugin.settings.walrusAddress).onChange(async (value) => {
        this.plugin.settings.walrusAddress = value.trim();
        await this.plugin.saveSettings();
      });
    });
    new import_obsidian.Setting(containerEl).setName("Sui private key").setDesc("Used to derive Sui address and sign transactions.").addText((text) => {
      text.setPlaceholder("suiprivkey...").setValue(this.plugin.settings.suiPrivateKey).onChange(async (value) => {
        this.plugin.settings.suiPrivateKey = value.trim();
        await this.plugin.saveSettings();
      });
      text.inputEl.type = "password";
    });
    new import_obsidian.Setting(containerEl).setName("Walrus private key").setDesc("Optional. If empty, the Sui private key will be used.").addText((text) => {
      text.setPlaceholder("suiprivkey...").setValue(this.plugin.settings.walrusPrivateKey).onChange(async (value) => {
        this.plugin.settings.walrusPrivateKey = value.trim();
        await this.plugin.saveSettings();
      });
      text.inputEl.type = "password";
    });
    new import_obsidian.Setting(containerEl).setName("Upload").setDesc("Upload the currently opened Markdown note to Walrus.").addButton((button) => {
      button.setButtonText("Upload current note").onClick(() => {
        void this.plugin.uploadActiveNote();
      });
    });
    new import_obsidian.Setting(containerEl).setName("Open panel").setDesc(`Local upload records: ${this.plugin.settings.uploads.length}`).addButton((button) => {
      button.setButtonText("Open").onClick(() => {
        void this.plugin.openPluginHome();
      });
    });
  }
};
var WalrusStorageRightView = class extends import_obsidian.ItemView {
  constructor(leaf, plugin) {
    super(leaf);
    this.actionsAdded = false;
    this.localUploadsCollapsed = false;
    this.onChainBlobsCollapsed = false;
    this.plugin = plugin;
  }
  getViewType() {
    return WALRUS_STORAGE_VIEW_TYPE;
  }
  getDisplayText() {
    return "Walrus Storage";
  }
  getIcon() {
    return "database";
  }
  async onOpen() {
    if (!this.actionsAdded) {
      this.addAction("gear", "Open settings", () => {
        this.plugin.openPluginSettings();
      });
      this.addAction("refresh-cw", "Refresh panel", () => {
        void this.render();
      });
      this.actionsAdded = true;
    }
    await this.render();
  }
  async render() {
    this.contentEl.empty();
    await this.renderBalanceSummary();
    this.renderQuickActions();
    this.renderLocalUploads();
    await this.renderOwnedBlobs();
  }
  async onClose() {
    this.contentEl.empty();
  }
  renderQuickActions() {
    this.contentEl.createEl("h3", { text: "Quick Actions" });
    const actions = this.contentEl.createDiv();
    const createIconButton = (icon, label, onClick) => {
      const button = actions.createEl("button");
      button.setAttribute("aria-label", label);
      button.setAttribute("title", label);
      (0, import_obsidian.setIcon)(button, icon);
      button.onclick = onClick;
    };
    createIconButton("gear", "Open settings", () => {
      this.plugin.openPluginSettings();
    });
    createIconButton("key", "Derive addresses", () => {
      void this.plugin.deriveAddressesFromPrivateKeys();
    });
    createIconButton("link", "Test connection", () => {
      void this.plugin.testSdkConnections();
    });
    createIconButton("upload", "Upload current note", () => {
      void this.plugin.uploadActiveNote();
    });
    createIconButton("folder-open", "Open download folder", () => {
      void this.plugin.openDownloadFolder();
    });
  }
  async renderBalanceSummary() {
    const header = this.contentEl.createDiv();
    header.createEl("h3", { text: "Balances" });
    const refreshButton = header.createEl("button");
    refreshButton.setAttribute("aria-label", "Refresh balances");
    refreshButton.setAttribute("title", "Refresh balances");
    (0, import_obsidian.setIcon)(refreshButton, "refresh-cw");
    refreshButton.onclick = () => {
      void this.render();
    };
    const addressText = this.plugin.settings.suiAddress.trim();
    if (!addressText) {
      this.contentEl.createEl("p", {
        text: "No address yet. Please derive addresses in settings first."
      });
      return;
    }
    this.contentEl.createEl("p", {
      text: `Address: ${addressText}`
    });
    try {
      const balances = await this.plugin.getCurrentBalances();
      this.renderBalanceItem("SUI", balances.sui.formatted, balances.sui.totalBalance);
      this.renderBalanceItem("WAL", balances.wal.formatted, balances.wal.totalBalance);
    } catch (error) {
      this.contentEl.createEl("p", {
        text: this.plugin.formatError(error, "Failed to fetch balances")
      });
    }
  }
  renderBalanceItem(symbol, amount, rawAmount) {
    const row = this.contentEl.createDiv();
    row.createEl("strong", { text: `${symbol}: ` });
    row.createSpan({ text: amount });
    row.createEl("small", { text: ` (${rawAmount})` });
  }
  setCollapseToggleIcon(button, collapsed) {
    (0, import_obsidian.setIcon)(button, collapsed ? "chevron-right" : "chevron-down");
  }
  renderLocalUploads() {
    const header = this.contentEl.createDiv();
    const title = header.createEl("h3", { text: "Local Uploads" });
    const toggleButton = header.createEl("button");
    toggleButton.setAttribute(
      "aria-label",
      this.localUploadsCollapsed ? "Expand local uploads" : "Collapse local uploads"
    );
    toggleButton.setAttribute(
      "title",
      this.localUploadsCollapsed ? "Expand local uploads" : "Collapse local uploads"
    );
    this.setCollapseToggleIcon(toggleButton, this.localUploadsCollapsed);
    const body = this.contentEl.createDiv();
    body.style.display = this.localUploadsCollapsed ? "none" : "";
    toggleButton.onclick = () => {
      this.localUploadsCollapsed = !this.localUploadsCollapsed;
      body.style.display = this.localUploadsCollapsed ? "none" : "";
      toggleButton.setAttribute(
        "aria-label",
        this.localUploadsCollapsed ? "Expand local uploads" : "Collapse local uploads"
      );
      toggleButton.setAttribute(
        "title",
        this.localUploadsCollapsed ? "Expand local uploads" : "Collapse local uploads"
      );
      this.setCollapseToggleIcon(toggleButton, this.localUploadsCollapsed);
    };
    const uploadCount = this.plugin.settings.uploads.length;
    title.setText(`Local Uploads (${uploadCount})`);
    if (this.plugin.settings.uploads.length === 0) {
      body.createEl("p", { text: "No local upload records yet." });
      return;
    }
    for (const record2 of this.plugin.settings.uploads) {
      const row = body.createDiv();
      row.createEl("strong", { text: record2.sourceName });
      row.createEl("div", {
        text: `Blob ID: ${record2.blobId}`
      });
      row.createEl("div", {
        text: `Uploaded at: ${record2.uploadedAt}`
      });
      row.createEl("div", {
        text: `Network: Sui ${record2.suiNetwork} / Walrus ${record2.walrusNetwork}`
      });
      const downloadButton = row.createEl("button", {
        text: "Download"
      });
      downloadButton.onclick = () => {
        void this.plugin.downloadRecord(record2);
      };
    }
  }
  async renderOwnedBlobs() {
    const header = this.contentEl.createDiv();
    const title = header.createEl("h3", { text: "On-chain Blobs" });
    const toggleButton = header.createEl("button");
    toggleButton.setAttribute(
      "aria-label",
      this.onChainBlobsCollapsed ? "Expand on-chain blobs" : "Collapse on-chain blobs"
    );
    toggleButton.setAttribute(
      "title",
      this.onChainBlobsCollapsed ? "Expand on-chain blobs" : "Collapse on-chain blobs"
    );
    this.setCollapseToggleIcon(toggleButton, this.onChainBlobsCollapsed);
    const body = this.contentEl.createDiv();
    body.style.display = this.onChainBlobsCollapsed ? "none" : "";
    toggleButton.onclick = () => {
      this.onChainBlobsCollapsed = !this.onChainBlobsCollapsed;
      body.style.display = this.onChainBlobsCollapsed ? "none" : "";
      toggleButton.setAttribute(
        "aria-label",
        this.onChainBlobsCollapsed ? "Expand on-chain blobs" : "Collapse on-chain blobs"
      );
      toggleButton.setAttribute(
        "title",
        this.onChainBlobsCollapsed ? "Expand on-chain blobs" : "Collapse on-chain blobs"
      );
      this.setCollapseToggleIcon(toggleButton, this.onChainBlobsCollapsed);
    };
    try {
      const blobs = await this.plugin.getOwnedBlobs();
      title.setText(`On-chain Blobs (${blobs.length})`);
      if (blobs.length === 0) {
        body.createEl("p", {
          text: "No on-chain blob objects found for this address."
        });
        return;
      }
      for (const blob of blobs) {
        const row = body.createDiv();
        row.createEl("div", { text: `Blob ID: ${blob.blobId}` });
        row.createEl("div", { text: `Object ID: ${blob.blobObjectId}` });
        row.createEl("div", {
          text: `Size: ${blob.size ?? "Unknown"} bytes`
        });
        const downloadButton = row.createEl("button");
        downloadButton.setAttribute("aria-label", "Download blob");
        downloadButton.setAttribute("title", "Download");
        (0, import_obsidian.setIcon)(downloadButton, "download");
        downloadButton.onclick = () => {
          void this.plugin.downloadBlobToVault(blob.blobId, blob.blobObjectId);
        };
      }
    } catch (error) {
      body.createEl("p", {
        text: this.plugin.formatError(error, "Failed to fetch on-chain blobs")
      });
    }
  }
};
var DownloadResultModal = class extends import_obsidian.Modal {
  constructor(app, result) {
    super(app);
    this.result = result;
  }
  onOpen() {
    const { contentEl } = this;
    contentEl.empty();
    contentEl.createEl("h3", {
      text: this.result.success ? "Download complete" : "Download failed"
    });
    contentEl.createEl("p", { text: `Vault path: ${this.result.vaultPath}` });
    if (this.result.fullPath) {
      contentEl.createEl("p", { text: `Full path: ${this.result.fullPath}` });
    }
    if (!this.result.success && this.result.error) {
      contentEl.createEl("p", { text: this.result.error });
    }
    const actions = contentEl.createDiv();
    if (this.result.fullPath) {
      const showButton = actions.createEl("button", { text: "Show in folder" });
      showButton.onclick = () => {
        try {
          const electron = require("electron");
          if (electron?.shell?.showItemInFolder && this.result.fullPath) {
            electron.shell.showItemInFolder(this.result.fullPath);
          } else if (electron?.shell?.openPath && this.result.fullPath) {
            void electron.shell.openPath(this.result.fullPath);
          }
        } catch {
        }
        this.close();
      };
    }
    const closeButton = actions.createEl("button", { text: "Close" });
    closeButton.onclick = () => {
      this.close();
    };
  }
};
/*! Bundled license information:

@scure/base/lib/esm/index.js:
  (*! scure-base - MIT License (c) 2022 Paul Miller (paulmillr.com) *)

@noble/hashes/esm/utils.js:
  (*! noble-hashes - MIT License (c) 2022 Paul Miller (paulmillr.com) *)

@noble/curves/esm/utils.js:
@noble/curves/esm/abstract/modular.js:
@noble/curves/esm/abstract/curve.js:
@noble/curves/esm/abstract/weierstrass.js:
@noble/curves/esm/_shortw_utils.js:
@noble/curves/esm/nist.js:
@noble/curves/esm/p256.js:
@noble/curves/esm/abstract/edwards.js:
@noble/curves/esm/ed25519.js:
@noble/curves/esm/secp256k1.js:
  (*! noble-curves - MIT License (c) 2022 Paul Miller (paulmillr.com) *)

@scure/bip39/esm/index.js:
  (*! scure-bip39 - MIT License (c) 2022 Patricio Palladino, Paul Miller (paulmillr.com) *)

@scure/bip32/lib/esm/index.js:
  (*! scure-bip32 - MIT License (c) 2022 Patricio Palladino, Paul Miller (paulmillr.com) *)
*/
