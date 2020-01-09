'use strict';

// Reads in a C style string
const readCStyleStringSpecial = function(buffer, index) {
  // Get the start search index
  let i = index;
  // Locate the end of the c string
  while (buffer[i] !== 0x00 && i < buffer.length) {
    i++;
  }
  // If are at the end of the buffer there is a problem with the document
  if (i >= buffer.length) throw new Error('Bad BSON Document: illegal CString');
  // Grab utf8 encoded string
  const string = buffer.toString('utf8', index, i);
  // Update index position
  index = i + 1;
  // Return string
  return { s: string, i: index };
};

const Query = function(bson, data) {
  // The type of message
  this.type = 'op_query';
  // The number of documents
  this.documents = [];
  // Unpack the message
  let index = 0;
  // Message size
  this.size =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);
  index = index + 4;
  // requestId
  this.requestId =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);
  index = index + 4;
  // responseTo
  this.responseTo =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);
  index = index + 4;
  // opCode
  this.opCode =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);
  index = index + 4;

  // flags
  this.flags =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);
  index = index + 4;

  // Read the full collection name
  const result = readCStyleStringSpecial(data, index);
  this.ns = result.s;
  index = result.i;

  // numberToSkip
  this.numberToSkip =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);
  index = index + 4;

  // numberToReturn
  this.numberToReturn =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);
  index = index + 4;

  // Read the document size
  let docSize =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);

  // Deserialize the document
  this.documents.push(bson.deserialize(data.slice(index, index + docSize)));
  index = index + docSize;

  // No field selection
  if (index === data.length) {
    return;
  }

  // Read the projection document size
  docSize =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);
  this.projection = bson.deserialize(data.slice(index, index + docSize));
};

const GetMore = function() {};
const KillCursor = function() {};
const Insert = function() {};
const Update = function() {};
const Delete = function() {};

const Msg = function(bson, data) {
  // The type of message
  this.type = 'op_query';
  // The number of documents
  this.documents = [];
  // Unpack the message
  let index = 0;
  // Message size
  this.size =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);
  index = index + 4;
  // requestId
  this.requestId =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);
  index = index + 4;
  // responseTo
  this.responseTo =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);
  index = index + 4;
  // opCode
  this.opCode =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);
  index = index + 4;
  // flags
  this.flags =
    data[index] | (data[index + 1] << 8) | (data[index + 2] << 16) | (data[index + 3] << 24);
  index = index + 4;

  this.hasChecksum = !!(this.flags & 0x1);
  this.moreToCome = !!(this.flags & 0x2);

  const endOfSegments = this.size - (this.hasChecksum ? 4 : 0);

  while (index < endOfSegments) {
    const segmentType = data[index++];

    if (segmentType === 1) {
      const segmentInfo = readSeriesAt(bson, data, index);
      const l = this.documents.length;
      if (l <= 0) {
        throw new Error('Bad BSON Document: illegal type1 segment');
      }
      const field = this.documents[l - 1][segmentInfo.label];
      this.documents[l - 1][segmentInfo.label] = Array.isArray(field)
        ? field.concat(segmentInfo.doc)
        : segmentInfo.docs;
    } else {
      const segmentInfo = readDocumentAt(bson, data, index);
      this.documents.push(segmentInfo.doc);
      index += segmentInfo.size;
    }
  }

  if (this.hasChecksum) {
    this.checksum = data.readInt32LE(index);
  }
};

function readSeriesAt(bson, data, index) {
  const size = data.readInt32LE(index);
  const end = size + index;
  const docs = [];
  const labelAndIndex = readCStyleStringSpecial(data, index + 4);
  const label = labelAndIndex.s;
  index = label.i;

  while (index < end) {
    const docAndSize = readDocumentAt(bson, data, index);
    docs.push(docAndSize.doc);
    index += docAndSize.size;
  }

  return { docs, size, label };
}

function readDocumentAt(bson, data, index) {
  const size = data.readInt32LE(index);
  const doc = bson.deserialize(data.slice(index, index + size));
  return { doc, size };
}

module.exports = {
  Msg: Msg,
  Query: Query,
  GetMore: GetMore,
  KillCursor: KillCursor,
  Insert: Insert,
  Update: Update,
  Delete: Delete
};
