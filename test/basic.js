'use strict';
const mock = require('..');

describe('MockServer', function() {
  afterEach(() => mock.cleanup());

  it('should work', function() {
    mock.createServer().then(server => {
      server.setMessageHandler(req => req.reply({ ok: 1 }));
    });
  });
});
