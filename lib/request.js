'use strict';
const Long = require('bson').Long;
const snappy = require('snappy');
const zlib = require('zlib');
const opcodes = require('./utils').opcodes;
const compressorIDs = require('./utils').compressorIDs;

/*
 * Request class
 */
const Request = function(server, connection, response) {
  this.server = server;
  this.connection = connection;
  this.response = response;
  this.bson = server.bson;
};

Request.prototype.receive = function() {
  return Promise.resolve();
};

Request.prototype.reply = function(documents, options) {
  options = options || {};
  documents = Array.isArray(documents) ? documents : [documents];

  // Unpack any variables we need
  let cursorId = options.cursorId || Long.ZERO;
  let responseFlags = typeof options.responseFlags === 'number' ? options.responseFlags : 0;
  let startingFrom = typeof options.startingFrom === 'number' ? options.startingFrom : 0;
  let numberReturned = documents.length;

  // Additional response Options
  let killConnectionAfterNBytes =
    typeof options.killConnectionAfterNBytes === 'number'
      ? options.killConnectionAfterNBytes
      : null;

  // Create the Response document
  const R = this.response.opCode === opcodes.OP_MSG ? MsgResponse : Response;
  let response = new R(this.bson, documents, {
    // Header field
    responseTo: this.response.requestId,
    requestId: this.response.requestId + 1,

    // The OP_REPLY message field
    cursorId: cursorId,
    responseFlags: responseFlags,
    startingFrom: startingFrom,
    numberReturned: numberReturned
  });

  if (options.compression) {
    response = new CompressedResponse(this.bson, response, {
      // Header field
      responseTo: this.response.requestId,
      requestId: this.response.requestId + 1,
      originalOpCode: options.originalOpCode,
      compressorID: compressorIDs[options.compression.compressor] || 0
    });
  }

  // Get the buffers
  let buffer = response.toBin();

  // Do we kill connection after n bytes
  if (killConnectionAfterNBytes == null) {
    this.connection.write(buffer);
  } else {
    // Fail to send whole reply
    if (killConnectionAfterNBytes <= buffer.length) {
      this.connection.write(buffer.slice(0, killConnectionAfterNBytes));
      this.connection.destroy();
    }
  }
};

Object.defineProperty(Request.prototype, 'type', {
  get: function() {
    return this.response.type;
  }
});

Object.defineProperty(Request.prototype, 'document', {
  get: function() {
    return this.response.documents[0];
  }
});

const Response = function(bson, documents, options) {
  this.bson = bson;
  // Header
  this.requestId = options.requestId;
  this.responseTo = options.responseTo;
  this.opCode = 1;

  // Message fields
  (this.cursorId = options.cursorId),
    (this.responseFlags = options.responseFlags),
    (this.startingFrom = options.startingFrom),
    (this.numberReturned = options.numberReturned);

  // Store documents
  this.documents = documents;
};

const MsgResponse = function(bson, documents, options) {
  this.bson = bson;
  // Header
  this.requestId = options.requestId;
  this.responseTo = options.responseTo;
  this.opCode = 2013;

  this.responseFlags = 0;

  // Store documents
  this.documents = documents;
};

MsgResponse.prototype.toBin = function() {
  // Serialize all the docs
  const docs = this.documents.map(x => this.bson.serialize(x)).reduce((arr, doc) => {
    const segmentType = new Buffer(1);
    segmentType[0] = 0;
    arr.push(segmentType);
    arr.push(doc);
    return arr;
  }, []);

  const docSize = docs.reduce((total, item) => total + item.length, 0);
  const headerSize = 20;
  const totalSize = headerSize + docSize;

  const header = new Buffer(headerSize);

  // Write total size
  writeInt32(header, 0, totalSize);
  // Write requestId
  writeInt32(header, 4, this.requestId);
  // Write responseId
  writeInt32(header, 8, this.responseTo);
  // Write opcode
  writeInt32(header, 12, this.opCode);
  // Write responseflags
  writeInt32(header, 16, this.responseFlags);

  const buffers = [header].concat(docs);

  return Buffer.concat(buffers);
};

/**
 * @ignore
 * Preparing a compressed response of the OP_COMPRESSED type
 */
const CompressedResponse = function(bson, uncompressedResponse, options) {
  this.bson = bson;

  // Header
  this.requestId = options.requestId;
  this.responseTo = options.responseTo;
  this.opCode = opcodes.OP_COMPRESSED;

  // OP_COMPRESSED fields
  this.originalOpCode = opcodes.OP_REPLY;
  this.compressorID = options.compressorID;

  this.uncompressedResponse = uncompressedResponse;
};

Response.prototype.toBin = function() {
  let self = this;
  let buffers = [];

  // Serialize all the docs
  let docs = this.documents.map(function(x) {
    return self.bson.serialize(x);
  });

  // Document total size
  let docsSize = 0;
  docs.forEach(function(x) {
    docsSize = docsSize + x.length;
  });

  // Calculate total size
  let totalSize =
    4 +
    4 +
    4 +
    4 + // Header size
    4 +
    8 +
    4 +
    4 + // OP_REPLY Header size
    docsSize; // OP_REPLY Documents

  // Header and op_reply fields
  let header = new Buffer(4 + 4 + 4 + 4 + 4 + 8 + 4 + 4);

  // Write total size
  writeInt32(header, 0, totalSize);
  // Write requestId
  writeInt32(header, 4, this.requestId);
  // Write responseId
  writeInt32(header, 8, this.responseTo);
  // Write opcode
  writeInt32(header, 12, this.opCode);
  // Write responseflags
  writeInt32(header, 16, this.responseFlags);
  // Write cursorId
  writeInt64(header, 20, this.cursorId);
  // Write startingFrom
  writeInt32(header, 28, this.startingFrom);
  // Write startingFrom
  writeInt32(header, 32, this.numberReturned);

  // Add header to the list of buffers
  buffers.push(header);
  // Add docs to list of buffers
  buffers = buffers.concat(docs);
  // Return all the buffers
  return Buffer.concat(buffers);
};

CompressedResponse.prototype.toBin = function() {
  let buffers = [];

  const dataToBeCompressed = this.uncompressedResponse.toBin().slice(16);
  const uncompressedSize = dataToBeCompressed.length;

  // Compress the data
  let compressedData;
  switch (this.compressorID) {
    case compressorIDs.snappy:
      compressedData = snappy.compressSync(dataToBeCompressed);
      break;
    case compressorIDs.zlib:
      compressedData = zlib.deflateSync(dataToBeCompressed);
      break;
    default:
      compressedData = dataToBeCompressed;
  }

  // Calculate total size
  let totalSize =
    4 +
    4 +
    4 +
    4 + // Header size
    4 +
    4 +
    1 + // OP_COMPRESSED fields
    compressedData.length; // OP_REPLY fields

  // Header and op_reply fields
  let header = new Buffer(totalSize - compressedData.length);

  // Write total size
  writeInt32(header, 0, totalSize);
  // Write requestId
  writeInt32(header, 4, this.requestId);
  // Write responseId
  writeInt32(header, 8, this.responseTo);
  // Write opcode
  writeInt32(header, 12, this.opCode);
  // Write original opcode`
  writeInt32(header, 16, this.originalOpCode);
  // Write uncompressed message size
  writeInt64(header, 20, Long.fromNumber(uncompressedSize));
  // Write compressorID
  header[24] = this.compressorID & 0xff;

  // Add header to the list of buffers
  buffers.push(header);
  // Add docs to list of buffers
  buffers = buffers.concat(compressedData);

  return Buffer.concat(buffers);
};

const writeInt32 = function(buffer, index, value) {
  buffer[index] = value & 0xff;
  buffer[index + 1] = (value >> 8) & 0xff;
  buffer[index + 2] = (value >> 16) & 0xff;
  buffer[index + 3] = (value >> 24) & 0xff;
  return;
};

const writeInt64 = function(buffer, index, value) {
  let lowBits = value.getLowBits();
  let highBits = value.getHighBits();
  // Encode low bits
  buffer[index] = lowBits & 0xff;
  buffer[index + 1] = (lowBits >> 8) & 0xff;
  buffer[index + 2] = (lowBits >> 16) & 0xff;
  buffer[index + 3] = (lowBits >> 24) & 0xff;
  // Encode high bits
  buffer[index + 4] = highBits & 0xff;
  buffer[index + 5] = (highBits >> 8) & 0xff;
  buffer[index + 6] = (highBits >> 16) & 0xff;
  buffer[index + 7] = (highBits >> 24) & 0xff;
  return;
};

module.exports = Request;
