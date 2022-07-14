import {StreamDemux} from 'stream-demux';
import {AsyncStreamEmitter} from 'async-stream-emitter';
import {AGChannel} from 'ag-channel';
import {AuthEngine} from './auth';
import {formatter} from 'sc-formatter';
import {AGTransport} from './transport';
import {LinkedList} from 'linked-list';
import {cloneDeep} from 'clone-deep';
import {Buffer} from 'buffer';
import {wait} from './wait';
import * as scErrors from 'sc-errors';

const InvalidArgumentsError = scErrors.InvalidArgumentsError;
const InvalidMessageError = scErrors.InvalidMessageError;
const InvalidActionError = scErrors.InvalidActionError;
const SocketProtocolError = scErrors.SocketProtocolError;
const TimeoutError = scErrors.TimeoutError;
const BadConnectionError = scErrors.BadConnectionError;

const isBrowser = typeof window !== 'undefined';

export class AGClientSocket extends AsyncStreamEmitter {

  static CONNECTING = AGClientSocket.CONNECTING = AGTransport.CONNECTING;
  static OPEN = AGClientSocket.OPEN = AGTransport.OPEN;
  static CLOSED = AGClientSocket.CLOSED = AGTransport.CLOSED;
  
  static AUTHENTICATED = AGClientSocket.AUTHENTICATED = 'authenticated';
  static UNAUTHENTICATED = AGClientSocket.UNAUTHENTICATED = 'unauthenticated';
  
  static SUBSCRIBED = AGClientSocket.SUBSCRIBED = AGChannel.SUBSCRIBED;
  static PENDING = AGClientSocket.PENDING = AGChannel.PENDING;
  static UNSUBSCRIBED = AGClientSocket.UNSUBSCRIBED = AGChannel.UNSUBSCRIBED;
  
  static ignoreStatuses = scErrors.socketProtocolIgnoreStatuses;
  static errorStatuses = scErrors.socketProtocolErrorStatuses;

  constructor(socketOptions) {
    AsyncStreamEmitter.call(this);
  
    let defaultOptions = {
      path: '/socketcluster/',
      secure: false,
      protocolScheme: null,
      socketPath: null,
      autoConnect: true,
      autoReconnect: true,
      autoSubscribeOnConnect: true,
      connectTimeout: 20000,
      ackTimeout: 10000,
      timestampRequests: false,
      timestampParam: 't',
      authTokenName: 'socketcluster.authToken',
      binaryType: 'arraybuffer',
      batchOnHandshake: false,
      batchOnHandshakeDuration: 100,
      batchInterval: 50,
      protocolVersion: 2,
      wsOptions: {},
      cloneData: false
    };
    let opts = Object.assign(defaultOptions, socketOptions);
  
    this.id = null;
    this.version = opts.version || null;
    this.protocolVersion = opts.protocolVersion;
    this.state = this.CLOSED;
    this.authState = this.UNAUTHENTICATED;
    this.signedAuthToken = null;
    this.authToken = null;
    this.pendingReconnect = false;
    this.pendingReconnectTimeout = null;
    this.preparingPendingSubscriptions = false;
    this.clientId = opts.clientId;
    this.wsOptions = opts.wsOptions;
  
    this.connectTimeout = opts.connectTimeout;
    this.ackTimeout = opts.ackTimeout;
    this.channelPrefix = opts.channelPrefix || null;
    this.disconnectOnUnload = opts.disconnectOnUnload == null ? true : opts.disconnectOnUnload;
    this.authTokenName = opts.authTokenName;
  
    // pingTimeout will be connectTimeout at the start, but it will
    // be updated with values provided by the 'connect' event
    opts.pingTimeout = opts.connectTimeout;
    this.pingTimeout = opts.pingTimeout;
    this.pingTimeoutDisabled = !!opts.pingTimeoutDisabled;
  
    let maxTimeout = Math.pow(2, 31) - 1;
  
    let verifyDuration = (propertyName) => {
      if (this[propertyName] > maxTimeout) {
        throw new InvalidArgumentsError(
          `The ${propertyName} value provided exceeded the maximum amount allowed`
        );
      }
    };
  
    verifyDuration('connectTimeout');
    verifyDuration('ackTimeout');
    verifyDuration('pingTimeout');
  
    this.connectAttempts = 0;
  
    this.isBatching = false;
    this.batchOnHandshake = opts.batchOnHandshake;
    this.batchOnHandshakeDuration = opts.batchOnHandshakeDuration;
  
    this._batchingIntervalId = null;
    this._outboundBuffer = new LinkedList();
    this._channelMap = {};
  
    this._channelEventDemux = new StreamDemux();
    this._channelDataDemux = new StreamDemux();
  
    this._receiverDemux = new StreamDemux();
    this._procedureDemux = new StreamDemux();
  
    this.options = opts;
  
    this._cid = 1;
  
    this.options.callIdGenerator = () => {
      return this._cid++;
    };
  
    if (this.options.autoReconnect) {
      if (this.options.autoReconnectOptions == null) {
        this.options.autoReconnectOptions = {};
      }
  
      // Add properties to the this.options.autoReconnectOptions object.
      // We assign the reference to a reconnectOptions variable to avoid repetition.
      let reconnectOptions = this.options.autoReconnectOptions;
      if (reconnectOptions.initialDelay == null) {
        reconnectOptions.initialDelay = 10000;
      }
      if (reconnectOptions.randomness == null) {
        reconnectOptions.randomness = 10000;
      }
      if (reconnectOptions.multiplier == null) {
        reconnectOptions.multiplier = 1.5;
      }
      if (reconnectOptions.maxDelay == null) {
        reconnectOptions.maxDelay = 60000;
      }
    }
  
    if (this.options.subscriptionRetryOptions == null) {
      this.options.subscriptionRetryOptions = {};
    }
  
    if (this.options.authEngine) {
      this.auth = this.options.authEngine;
    } else {
      this.auth = new AuthEngine();
    }
  
    if (this.options.codecEngine) {
      this.codec = this.options.codecEngine;
    } else {
      // Default codec engine
      this.codec = formatter;
    }
  
    if (this.options.protocol) {
      let protocolOptionError = new InvalidArgumentsError(
        'The "protocol" option does not affect socketcluster-client - ' +
        'If you want to utilize SSL/TLS, use "secure" option instead'
      );
      this._onError(protocolOptionError);
    }
  
    this.options.query = opts.query || {};
    if (typeof this.options.query === 'string') {
      let searchParams = new URLSearchParams(this.options.query);
      let queryObject = {};
      for (let [key, value] of searchParams.entries()) {
        let currentValue = queryObject[key];
        if (currentValue == null) {
          queryObject[key] = value;
        } else {
          if (!Array.isArray(currentValue)) {
            queryObject[key] = [currentValue];
          }
          queryObject[key].push(value);
        }
      }
      this.options.query = queryObject;
    }
  
    if (isBrowser && this.disconnectOnUnload && global.addEventListener && global.removeEventListener) {
      this._handleBrowserUnload();
    }
  
    if (this.options.autoConnect) {
      this.connect();
    }
  }

  get isBufferingBatch() {
    return this.transport.isBufferingBatch;
  }

  getBackpressure() {
    return Math.max(
      this.getAllListenersBackpressure(),
      this.getAllReceiversBackpressure(),
      this.getAllProceduresBackpressure(),
      this.getAllChannelsBackpressure()
    );
  };

  _handleBrowserUnload() {
    let unloadHandler = () => {
      this.disconnect();
    };
    let isUnloadHandlerAttached = false;
  
    let attachUnloadHandler = () => {
      if (!isUnloadHandlerAttached) {
        isUnloadHandlerAttached = true;
        global.addEventListener('beforeunload', unloadHandler, false);
      }
    };
  
    let detachUnloadHandler = () => {
      if (isUnloadHandlerAttached) {
        isUnloadHandlerAttached = false;
        global.removeEventListener('beforeunload', unloadHandler, false);
      }
    };
  
    (async () => {
      let consumer = this.listener('connecting').createConsumer();
      while (true) {
        let packet = await consumer.next();
        if (packet.done) break;
        attachUnloadHandler();
      }
    })();
  
    (async () => {
      let consumer = this.listener('close').createConsumer();
      while (true) {
        let packet = await consumer.next();
        if (packet.done) break;
        detachUnloadHandler();
      }
    })();
  };

  _setAuthToken(data) {
    this._changeToAuthenticatedState(data.token);
  
    (async () => {
      try {
        await this.auth.saveToken(this.authTokenName, data.token, {});
      } catch (err) {
        this._onError(err);
      }
    })();
  };

  _removeAuthToken(data) {
    (async () => {
      let oldAuthToken;
      try {
        oldAuthToken = await this.auth.removeToken(this.authTokenName);
      } catch (err) {
        // Non-fatal error - Do not close the connection
        this._onError(err);
        return;
      }
      this.emit('removeAuthToken', {oldAuthToken});
    })();
  
    this._changeToUnauthenticatedStateAndClearTokens();
  };

  _privateDataHandlerMap = {
    '#publish': function (data) {
      let undecoratedChannelName = this._undecorateChannelName(data.channel);
      let isSubscribed = this.isSubscribed(undecoratedChannelName, true);
  
      if (isSubscribed) {
        this._channelDataDemux.write(undecoratedChannelName, data.data);
      }
    },
    '#kickOut': function (data) {
      let undecoratedChannelName = this._undecorateChannelName(data.channel);
      let channel = this._channelMap[undecoratedChannelName];
      if (channel) {
        this.emit('kickOut', {
          channel: undecoratedChannelName,
          message: data.message
        });
        this._channelEventDemux.write(`${undecoratedChannelName}/kickOut`, {message: data.message});
        this._triggerChannelUnsubscribe(channel);
      }
    },
    '#setAuthToken': function (data) {
      if (data) {
        this._setAuthToken(data);
      }
    },
    '#removeAuthToken': function (data) {
      this._removeAuthToken(data);
    }
  };

  _privateRPCHandlerMap = {
    '#setAuthToken': function (data, request) {
      if (data) {
        this._setAuthToken(data);
  
        request.end();
      } else {
        request.error(new InvalidMessageError('No token data provided by #setAuthToken event'));
      }
    },
    '#removeAuthToken': function (data, request) {
      this._removeAuthToken(data);
      request.end();
    }
  };

  getState() {
    return this.state;
  };

  getBytesReceived() {
    return this.transport.getBytesReceived();
  };

  deauthenticate() {
    (async () => {
      let oldAuthToken;
      try {
        oldAuthToken = await this.auth.removeToken(this.authTokenName);
      } catch (err) {
        this._onError(err);
        return;
      }
      this.emit('removeAuthToken', {oldAuthToken});
    })();
  
    if (this.state !== this.CLOSED) {
      this.transmit('#removeAuthToken');
    }
    this._changeToUnauthenticatedStateAndClearTokens();
    await wait(0);
  };

  connect() {
    if (this.state === this.CLOSED) {
      this.pendingReconnect = false;
      this.pendingReconnectTimeout = null;
      clearTimeout(this._reconnectTimeoutRef);
  
      this.state = this.CONNECTING;
      this.emit('connecting', {});
  
      if (this.transport) {
        this.transport.clearAllListeners();
      }
  
      let transportHandlers = {
        onOpen: (value) => {
          this.state = this.OPEN;
          this._onOpen(value);
        },
        onOpenAbort: (value) => {
          if (this.state !== this.CLOSED) {
            this.state = this.CLOSED;
            this._destroy(value.code, value.reason, true);
          }
        },
        onClose: (value) => {
          if (this.state !== this.CLOSED) {
            this.state = this.CLOSED;
            this._destroy(value.code, value.reason);
          }
        },
        onEvent: (value) => {
          this.emit(value.event, value.data);
        },
        onError: (value) => {
          this._onError(value.error);
        },
        onInboundInvoke: (value) => {
          this._onInboundInvoke(value);
        },
        onInboundTransmit: (value) => {
          this._onInboundTransmit(value.event, value.data);
        }
      };
  
      this.transport = new AGTransport(this.auth, this.codec, this.options, this.wsOptions, transportHandlers);
    }
  };
  
  reconnect(code, reason) {
    this.disconnect(code, reason);
    this.connect();
  };

  disconnect(code, reason) {
    code = code || 1000;
  
    if (typeof code !== 'number') {
      throw new InvalidArgumentsError('If specified, the code argument must be a number');
    }
  
    let isConnecting = this.state === this.CONNECTING;
    if (isConnecting || this.state === this.OPEN) {
      this.state = this.CLOSED;
      this._destroy(code, reason, isConnecting);
      this.transport.close(code, reason);
    } else {
      this.pendingReconnect = false;
      this.pendingReconnectTimeout = null;
      clearTimeout(this._reconnectTimeoutRef);
    }
  };

  _changeToUnauthenticatedStateAndClearTokens() {
    if (this.authState !== this.UNAUTHENTICATED) {
      let oldAuthState = this.authState;
      let oldAuthToken = this.authToken;
      let oldSignedAuthToken = this.signedAuthToken;
      this.authState = this.UNAUTHENTICATED;
      this.signedAuthToken = null;
      this.authToken = null;
  
      let stateChangeData = {
        oldAuthState,
        newAuthState: this.authState
      };
      this.emit('authStateChange', stateChangeData);
      this.emit('deauthenticate', {oldSignedAuthToken, oldAuthToken});
    }
  };

  _changeToAuthenticatedState(signedAuthToken) {
    this.signedAuthToken = signedAuthToken;
    this.authToken = this._extractAuthTokenData(signedAuthToken);
  
    if (this.authState !== this.AUTHENTICATED) {
      let oldAuthState = this.authState;
      this.authState = this.AUTHENTICATED;
      let stateChangeData = {
        oldAuthState,
        newAuthState: this.authState,
        signedAuthToken: signedAuthToken,
        authToken: this.authToken
      };
      if (!this.preparingPendingSubscriptions) {
        this.processPendingSubscriptions();
      }
  
      this.emit('authStateChange', stateChangeData);
    }
    this.emit('authenticate', {signedAuthToken, authToken: this.authToken});
  };


  decodeBase64(encodedString) {
    return Buffer.from(encodedString, 'base64').toString('utf8');
  };

  encodeBase64(decodedString) {
    return Buffer.from(decodedString, 'utf8').toString('base64');
  };

  _extractAuthTokenData(signedAuthToken) {
    let tokenParts = (signedAuthToken || '').split('.');
    let encodedTokenData = tokenParts[1];
    if (encodedTokenData != null) {
      let tokenData = encodedTokenData;
      try {
        tokenData = this.decodeBase64(tokenData);
        return JSON.parse(tokenData);
      } catch (e) {
        return tokenData;
      }
    }
    return null;
  };

  getAuthToken() {
    return this.authToken;
  };

  getSignedAuthToken() {
    return this.signedAuthToken;
  };

  authenticate = async function (signedAuthToken) {
    let authStatus;
  
    try {
      authStatus = await this.invoke('#authenticate', signedAuthToken);
    } catch (err) {
      if (err.name !== 'BadConnectionError' && err.name !== 'TimeoutError') {
        // In case of a bad/closed connection or a timeout, we maintain the last
        // known auth state since those errors don't mean that the token is invalid.
        this._changeToUnauthenticatedStateAndClearTokens();
      }
      await wait(0);
      throw err;
    }
  
    if (authStatus && authStatus.isAuthenticated != null) {
      // If authStatus is correctly formatted (has an isAuthenticated property),
      // then we will rehydrate the authError.
      if (authStatus.authError) {
        authStatus.authError = scErrors.hydrateError(authStatus.authError);
      }
    } else {
      // Some errors like BadConnectionError and TimeoutError will not pass a valid
      // authStatus object to the current function, so we need to create it ourselves.
      authStatus = {
        isAuthenticated: this.authState,
        authError: null
      };
    }
  
    if (authStatus.isAuthenticated) {
      this._changeToAuthenticatedState(signedAuthToken);
    } else {
      this._changeToUnauthenticatedStateAndClearTokens();
    }
  
    (async () => {
      try {
        await this.auth.saveToken(this.authTokenName, signedAuthToken, {});
      } catch (err) {
        this._onError(err);
      }
    })();
  
    await wait(0);
    return authStatus;
  };



  _tryReconnect(initialDelay) {
    let exponent = this.connectAttempts++;
    let reconnectOptions = this.options.autoReconnectOptions;
    let timeout;
  
    if (initialDelay == null || exponent > 0) {
      let initialTimeout = Math.round(reconnectOptions.initialDelay + (reconnectOptions.randomness || 0) * Math.random());
  
      timeout = Math.round(initialTimeout * Math.pow(reconnectOptions.multiplier, exponent));
    } else {
      timeout = initialDelay;
    }
  
    if (timeout > reconnectOptions.maxDelay) {
      timeout = reconnectOptions.maxDelay;
    }
  
    clearTimeout(this._reconnectTimeoutRef);
  
    this.pendingReconnect = true;
    this.pendingReconnectTimeout = timeout;
    this._reconnectTimeoutRef = setTimeout(() => {
      this.connect();
    }, timeout);
  };
  
  _onOpen(status) {
    if (this.isBatching) {
      this._startBatching();
    } else if (this.batchOnHandshake) {
      this._startBatching();
      setTimeout(() => {
        if (!this.isBatching) {
          this._stopBatching();
        }
      }, this.batchOnHandshakeDuration);
    }
    this.preparingPendingSubscriptions = true;
  
    if (status) {
      this.id = status.id;
      this.pingTimeout = status.pingTimeout;
      if (status.isAuthenticated) {
        this._changeToAuthenticatedState(status.authToken);
      } else {
        this._changeToUnauthenticatedStateAndClearTokens();
      }
    } else {
      // This can happen if auth.loadToken (in transport.js) fails with
      // an error - This means that the signedAuthToken cannot be loaded by
      // the auth engine and therefore, we need to unauthenticate the client.
      this._changeToUnauthenticatedStateAndClearTokens();
    }
  
    this.connectAttempts = 0;
  
    if (this.options.autoSubscribeOnConnect) {
      this.processPendingSubscriptions();
    }
  
    // If the user invokes the callback while in autoSubscribeOnConnect mode, it
    // won't break anything.
    this.emit('connect', {
      ...status,
      processPendingSubscriptions: () => {
        this.processPendingSubscriptions();
      }
    });
  
    if (this.state === this.OPEN) {
      this._flushOutboundBuffer();
    }
  };
  
  _onError(error) {
    this.emit('error', {error});
  };
  
  _suspendSubscriptions() {
    Object.keys(this._channelMap).forEach((channelName) => {
      let channel = this._channelMap[channelName];
      this._triggerChannelUnsubscribe(channel, true);
    });
  };
  
  _abortAllPendingEventsDueToBadConnection(failureType) {
    let currentNode = this._outboundBuffer.head;
    let nextNode;
  
    while (currentNode) {
      nextNode = currentNode.next;
      let eventObject = currentNode.data;
      clearTimeout(eventObject.timeout);
      delete eventObject.timeout;
      currentNode.detach();
      currentNode = nextNode;
  
      let callback = eventObject.callback;
  
      if (callback) {
        delete eventObject.callback;
        let errorMessage = `Event "${eventObject.event}" was aborted due to a bad connection`;
        let error = new BadConnectionError(errorMessage, failureType);
  
        callback.call(eventObject, error, eventObject);
      }
      // Cleanup any pending response callback in the transport layer too.
      if (eventObject.cid) {
        this.transport.cancelPendingResponse(eventObject.cid);
      }
    }
  };
  
  _destroy(code, reason, openAbort) {
    this.id = null;
    this._cancelBatching();
  
    if (this.transport) {
      this.transport.clearAllListeners();
    }
  
    this.pendingReconnect = false;
    this.pendingReconnectTimeout = null;
    clearTimeout(this._reconnectTimeoutRef);
  
    this._suspendSubscriptions();
  
    if (openAbort) {
      this.emit('connectAbort', {code, reason});
    } else {
      this.emit('disconnect', {code, reason});
    }
    this.emit('close', {code, reason});
  
    if (!AGClientSocket.ignoreStatuses[code]) {
      let closeMessage;
      if (reason) {
        closeMessage = 'Socket connection closed with status code ' + code + ' and reason: ' + reason;
      } else {
        closeMessage = 'Socket connection closed with status code ' + code;
      }
      let err = new SocketProtocolError(AGClientSocket.errorStatuses[code] || closeMessage, code);
      this._onError(err);
    }
  
    this._abortAllPendingEventsDueToBadConnection(openAbort ? 'connectAbort' : 'disconnect');
  
    // Try to reconnect
    // on server ping timeout (4000)
    // or on client pong timeout (4001)
    // or on close without status (1005)
    // or on handshake failure (4003)
    // or on handshake rejection (4008)
    // or on socket hung up (1006)
    if (this.options.autoReconnect) {
      if (code === 4000 || code === 4001 || code === 1005) {
        // If there is a ping or pong timeout or socket closes without
        // status, don't wait before trying to reconnect - These could happen
        // if the client wakes up after a period of inactivity and in this case we
        // want to re-establish the connection as soon as possible.
        this._tryReconnect(0);
  
        // Codes 4500 and above will be treated as permanent disconnects.
        // Socket will not try to auto-reconnect.
      } else if (code !== 1000 && code < 4500) {
        this._tryReconnect();
      }
    }
  };
  
  _onInboundTransmit(event, data) {
    let handler = this._privateDataHandlerMap[event];
    if (handler) {
      handler.call(this, data);
    } else {
      this._receiverDemux.write(event, data);
    }
  };
  
  _onInboundInvoke(request) {
    let {procedure, data} = request;
    let handler = this._privateRPCHandlerMap[procedure];
    if (handler) {
      handler.call(this, data, request);
    } else {
      this._procedureDemux.write(procedure, request);
    }
  };
  
  decode(message) {
    return this.transport.decode(message);
  };
  
  encode(object) {
    return this.transport.encode(object);
  };
  
  _flushOutboundBuffer() {
    let currentNode = this._outboundBuffer.head;
    let nextNode;
  
    while (currentNode) {
      nextNode = currentNode.next;
      let eventObject = currentNode.data;
      currentNode.detach();
      this.transport.transmitObject(eventObject);
      currentNode = nextNode;
    }
  };
  
  _handleEventAckTimeout(eventObject, eventNode) {
    if (eventNode) {
      eventNode.detach();
    }
    delete eventObject.timeout;
  
    let callback = eventObject.callback;
    if (callback) {
      delete eventObject.callback;
      let error = new TimeoutError(`Event response for "${eventObject.event}" timed out`);
      callback.call(eventObject, error, eventObject);
    }
    // Cleanup any pending response callback in the transport layer too.
    if (eventObject.cid) {
      this.transport.cancelPendingResponse(eventObject.cid);
    }
  };
  
  _processOutboundEvent(event, data, options, expectResponse) {
    options = options || {};
  
    if (this.state === this.CLOSED) {
      this.connect();
    }
    let eventObject = {
      event
    };
  
    let promise;
  
    if (expectResponse) {
      promise = new Promise((resolve, reject) => {
        eventObject.callback = (err, data) => {
          if (err) {
            reject(err);
            return;
          }
          resolve(data);
        };
      });
    } else {
      promise = Promise.resolve();
    }
  
    let eventNode = (new LinkedList).Item();
  
    if (this.options.cloneData) {
      eventObject.data = cloneDeep(data);
    } else {
      eventObject.data = data;
    }
    eventNode.data = eventObject;
  
    let ackTimeout = options.ackTimeout == null ? this.ackTimeout : options.ackTimeout;
  
    eventObject.timeout = setTimeout(() => {
      this._handleEventAckTimeout(eventObject, eventNode);
    }, ackTimeout);
  
    this._outboundBuffer.append(eventNode);
    if (this.state === this.OPEN) {
      this._flushOutboundBuffer();
    }
    return promise;
  };
  
  send(data) {
    this.transport.send(data);
  };
  
  transmit(event, data, options) {
    return this._processOutboundEvent(event, data, options);
  };
  
  invoke(event, data, options) {
    return this._processOutboundEvent(event, data, options, true);
  };
  
  transmitPublish(channelName, data) {
    let pubData = {
      channel: this._decorateChannelName(channelName),
      data
    };
    return this.transmit('#publish', pubData);
  };
  
  invokePublish(channelName, data) {
    let pubData = {
      channel: this._decorateChannelName(channelName),
      data
    };
    return this.invoke('#publish', pubData);
  };
  
  _triggerChannelSubscribe(channel, subscriptionOptions) {
    let channelName = channel.name;
  
    if (channel.state !== AGChannel.SUBSCRIBED) {
      let oldChannelState = channel.state;
      channel.state = AGChannel.SUBSCRIBED;
  
      let stateChangeData = {
        oldChannelState,
        newChannelState: channel.state,
        subscriptionOptions
      };
      this._channelEventDemux.write(`${channelName}/subscribeStateChange`, stateChangeData);
      this._channelEventDemux.write(`${channelName}/subscribe`, {
        subscriptionOptions
      });
      this.emit('subscribeStateChange', {
        channel: channelName,
        ...stateChangeData
      });
      this.emit('subscribe', {
        channel: channelName,
        subscriptionOptions
      });
    }
  };
  
  _triggerChannelSubscribeFail(err, channel, subscriptionOptions) {
    let channelName = channel.name;
    let meetsAuthRequirements = !channel.options.waitForAuth || this.authState === this.AUTHENTICATED;
    let hasChannel = !!this._channelMap[channelName];
  
    if (hasChannel && meetsAuthRequirements) {
      delete this._channelMap[channelName];
  
      this._channelEventDemux.write(`${channelName}/subscribeFail`, {
        error: err,
        subscriptionOptions
      });
      this.emit('subscribeFail', {
        error: err,
        channel: channelName,
        subscriptionOptions: subscriptionOptions
      });
    }
  };
  
  // Cancel any pending subscribe callback
  _cancelPendingSubscribeCallback(channel) {
    if (channel._pendingSubscriptionCid != null) {
      this.transport.cancelPendingResponse(channel._pendingSubscriptionCid);
      delete channel._pendingSubscriptionCid;
    }
  };
  
  _decorateChannelName(channelName) {
    if (this.channelPrefix) {
      channelName = this.channelPrefix + channelName;
    }
    return channelName;
  };
  
  _undecorateChannelName(decoratedChannelName) {
    if (this.channelPrefix && decoratedChannelName.indexOf(this.channelPrefix) === 0) {
      return decoratedChannelName.replace(this.channelPrefix, '');
    }
    return decoratedChannelName;
  };
  
  startBatch() {
    this.transport.startBatch();
  };
  
  flushBatch() {
    this.transport.flushBatch();
  };
  
  cancelBatch() {
    this.transport.cancelBatch();
  };
  
  _startBatching() {
    if (this._batchingIntervalId != null) {
      return;
    }
    this.startBatch();
    this._batchingIntervalId = setInterval(() => {
      this.flushBatch();
      this.startBatch();
    }, this.options.batchInterval);
  };
  
  startBatching() {
    this.isBatching = true;
    this._startBatching();
  };
  
  _stopBatching() {
    if (this._batchingIntervalId != null) {
      clearInterval(this._batchingIntervalId);
    }
    this._batchingIntervalId = null;
    this.flushBatch();
  };
  
  stopBatching() {
    this.isBatching = false;
    this._stopBatching();
  };
  
  _cancelBatching() {
    if (this._batchingIntervalId != null) {
      clearInterval(this._batchingIntervalId);
    }
    this._batchingIntervalId = null;
    this.cancelBatch();
  };
  
  cancelBatching() {
    this.isBatching = false;
    this._cancelBatching();
  };
  
  _trySubscribe(channel) {
    let meetsAuthRequirements = !channel.options.waitForAuth || this.authState === this.AUTHENTICATED;
  
    // We can only ever have one pending subscribe action at any given time on a channel
    if (
      this.state === this.OPEN &&
      !this.preparingPendingSubscriptions &&
      channel._pendingSubscriptionCid == null &&
      meetsAuthRequirements
    ) {
  
      let options = {
        noTimeout: true
      };
  
      let subscriptionOptions = {};
      if (channel.options.waitForAuth) {
        options.waitForAuth = true;
        subscriptionOptions.waitForAuth = options.waitForAuth;
      }
      if (channel.options.data) {
        subscriptionOptions.data = channel.options.data;
      }
  
      channel._pendingSubscriptionCid = this.transport.invokeRaw(
        '#subscribe',
        {
          channel: this._decorateChannelName(channel.name),
          ...subscriptionOptions
        },
        options,
        (err) => {
          if (err) {
            if (err.name === 'BadConnectionError') {
              // In case of a failed connection, keep the subscription
              // as pending; it will try again on reconnect.
              return;
            }
            delete channel._pendingSubscriptionCid;
            this._triggerChannelSubscribeFail(err, channel, subscriptionOptions);
          } else {
            delete channel._pendingSubscriptionCid;
            this._triggerChannelSubscribe(channel, subscriptionOptions);
          }
        }
      );
      this.emit('subscribeRequest', {
        channel: channel.name,
        subscriptionOptions
      });
    }
  };
  
  subscribe(channelName, options) {
    options = options || {};
    let channel = this._channelMap[channelName];
  
    let sanitizedOptions = {
      waitForAuth: !!options.waitForAuth
    };
  
    if (options.priority != null) {
      sanitizedOptions.priority = options.priority;
    }
    if (options.data !== undefined) {
      sanitizedOptions.data = options.data;
    }
  
    if (!channel) {
      channel = {
        name: channelName,
        state: AGChannel.PENDING,
        options: sanitizedOptions
      };
      this._channelMap[channelName] = channel;
      this._trySubscribe(channel);
    } else if (options) {
      channel.options = sanitizedOptions;
    }
  
    let channelIterable = new AGChannel(
      channelName,
      this,
      this._channelEventDemux,
      this._channelDataDemux
    );
  
    return channelIterable;
  };
  
  _triggerChannelUnsubscribe(channel, setAsPending) {
    let channelName = channel.name;
  
    this._cancelPendingSubscribeCallback(channel);
  
    if (channel.state === AGChannel.SUBSCRIBED) {
      let stateChangeData = {
        oldChannelState: channel.state,
        newChannelState: setAsPending ? AGChannel.PENDING : AGChannel.UNSUBSCRIBED
      };
      this._channelEventDemux.write(`${channelName}/subscribeStateChange`, stateChangeData);
      this._channelEventDemux.write(`${channelName}/unsubscribe`, {});
      this.emit('subscribeStateChange', {
        channel: channelName,
        ...stateChangeData
      });
      this.emit('unsubscribe', {channel: channelName});
    }
  
    if (setAsPending) {
      channel.state = AGChannel.PENDING;
    } else {
      delete this._channelMap[channelName];
    }
  };
  
  _tryUnsubscribe(channel) {
    if (this.state === this.OPEN) {
      let options = {
        noTimeout: true
      };
      // If there is a pending subscribe action, cancel the callback
      this._cancelPendingSubscribeCallback(channel);
  
      // This operation cannot fail because the TCP protocol guarantees delivery
      // so long as the connection remains open. If the connection closes,
      // the server will automatically unsubscribe the client and thus complete
      // the operation on the server side.
      let decoratedChannelName = this._decorateChannelName(channel.name);
      this.transport.transmit('#unsubscribe', decoratedChannelName, options);
    }
  };
  
  unsubscribe(channelName) {
    let channel = this._channelMap[channelName];
  
    if (channel) {
      this._triggerChannelUnsubscribe(channel);
      this._tryUnsubscribe(channel);
    }
  };
  
  // ---- Receiver logic ----
  
  receiver(receiverName) {
    return this._receiverDemux.stream(receiverName);
  };
  
  closeReceiver(receiverName) {
    this._receiverDemux.close(receiverName);
  };
  
  closeAllReceivers() {
    this._receiverDemux.closeAll();
  };
  
  killReceiver(receiverName) {
    this._receiverDemux.kill(receiverName);
  };
  
  killAllReceivers() {
    this._receiverDemux.killAll();
  };
  
  killReceiverConsumer(consumerId) {
    this._receiverDemux.killConsumer(consumerId);
  };
  
  getReceiverConsumerStats(consumerId) {
    return this._receiverDemux.getConsumerStats(consumerId);
  };
  
  getReceiverConsumerStatsList(receiverName) {
    return this._receiverDemux.getConsumerStatsList(receiverName);
  };
  
  getAllReceiversConsumerStatsList() {
    return this._receiverDemux.getConsumerStatsListAll();
  };
  
  getReceiverBackpressure(receiverName) {
    return this._receiverDemux.getBackpressure(receiverName);
  };
  
  getAllReceiversBackpressure() {
    return this._receiverDemux.getBackpressureAll();
  };
  
  getReceiverConsumerBackpressure(consumerId) {
    return this._receiverDemux.getConsumerBackpressure(consumerId);
  };
  
  hasReceiverConsumer(receiverName, consumerId) {
    return this._receiverDemux.hasConsumer(receiverName, consumerId);
  };
  
  hasAnyReceiverConsumer(consumerId) {
    return this._receiverDemux.hasConsumerAll(consumerId);
  };
  
  // ---- Procedure logic ----
  
  procedure(procedureName) {
    return this._procedureDemux.stream(procedureName);
  };
  
  closeProcedure(procedureName) {
    this._procedureDemux.close(procedureName);
  };
  
  closeAllProcedures() {
    this._procedureDemux.closeAll();
  };
  
  killProcedure(procedureName) {
    this._procedureDemux.kill(procedureName);
  };
  
  killAllProcedures() {
    this._procedureDemux.killAll();
  };
  
  killProcedureConsumer(consumerId) {
    this._procedureDemux.killConsumer(consumerId);
  };
  
  getProcedureConsumerStats(consumerId) {
    return this._procedureDemux.getConsumerStats(consumerId);
  };
  
  getProcedureConsumerStatsList(procedureName) {
    return this._procedureDemux.getConsumerStatsList(procedureName);
  };
  
  getAllProceduresConsumerStatsList() {
    return this._procedureDemux.getConsumerStatsListAll();
  };
  
  getProcedureBackpressure(procedureName) {
    return this._procedureDemux.getBackpressure(procedureName);
  };
  
  getAllProceduresBackpressure() {
    return this._procedureDemux.getBackpressureAll();
  };
  
  getProcedureConsumerBackpressure(consumerId) {
    return this._procedureDemux.getConsumerBackpressure(consumerId);
  };
  
  hasProcedureConsumer(procedureName, consumerId) {
    return this._procedureDemux.hasConsumer(procedureName, consumerId);
  };
  
  hasAnyProcedureConsumer(consumerId) {
    return this._procedureDemux.hasConsumerAll(consumerId);
  };
  
  // ---- Channel logic ----
  
  channel(channelName) {
    let currentChannel = this._channelMap[channelName];
  
    let channelIterable = new AGChannel(
      channelName,
      this,
      this._channelEventDemux,
      this._channelDataDemux
    );
  
    return channelIterable;
  };
  
  closeChannel(channelName) {
    this.channelCloseOutput(channelName);
    this.channelCloseAllListeners(channelName);
  };
  
  closeAllChannelOutputs() {
    this._channelDataDemux.closeAll();
  };
  
  closeAllChannelListeners() {
    this._channelEventDemux.closeAll();
  };
  
  closeAllChannels() {
    this.closeAllChannelOutputs();
    this.closeAllChannelListeners();
  };
  
  killChannel(channelName) {
    this.channelKillOutput(channelName);
    this.channelKillAllListeners(channelName);
  };
  
  killAllChannelOutputs() {
    this._channelDataDemux.killAll();
  };
  
  killAllChannelListeners() {
    this._channelEventDemux.killAll();
  };
  
  killAllChannels() {
    this.killAllChannelOutputs();
    this.killAllChannelListeners();
  };
  
  killChannelOutputConsumer(consumerId) {
    this._channelDataDemux.killConsumer(consumerId);
  };
  
  killChannelListenerConsumer(consumerId) {
    this._channelEventDemux.killConsumer(consumerId);
  };
  
  getChannelOutputConsumerStats(consumerId) {
    return this._channelDataDemux.getConsumerStats(consumerId);
  };
  
  getChannelListenerConsumerStats(consumerId) {
    return this._channelEventDemux.getConsumerStats(consumerId);
  };
  
  getAllChannelOutputsConsumerStatsList() {
    return this._channelDataDemux.getConsumerStatsListAll();
  };
  
  getAllChannelListenersConsumerStatsList() {
    return this._channelEventDemux.getConsumerStatsListAll();
  };
  
  getChannelBackpressure(channelName) {
    return Math.max(
      this.channelGetOutputBackpressure(channelName),
      this.channelGetAllListenersBackpressure(channelName)
    );
  };
  
  getAllChannelOutputsBackpressure() {
    return this._channelDataDemux.getBackpressureAll();
  };
  
  getAllChannelListenersBackpressure() {
    return this._channelEventDemux.getBackpressureAll();
  };
  
  getAllChannelsBackpressure() {
    return Math.max(
      this.getAllChannelOutputsBackpressure(),
      this.getAllChannelListenersBackpressure()
    );
  };
  
  getChannelListenerConsumerBackpressure(consumerId) {
    return this._channelEventDemux.getConsumerBackpressure(consumerId);
  };
  
  getChannelOutputConsumerBackpressure(consumerId) {
    return this._channelDataDemux.getConsumerBackpressure(consumerId);
  };
  
  hasAnyChannelOutputConsumer(consumerId) {
    return this._channelDataDemux.hasConsumerAll(consumerId);
  };
  
  hasAnyChannelListenerConsumer(consumerId) {
    return this._channelEventDemux.hasConsumerAll(consumerId);
  };
  
  getChannelState(channelName) {
    let channel = this._channelMap[channelName];
    if (channel) {
      return channel.state;
    }
    return AGChannel.UNSUBSCRIBED;
  };
  
  getChannelOptions(channelName) {
    let channel = this._channelMap[channelName];
    if (channel) {
      return {...channel.options};
    }
    return {};
  };
  
  _getAllChannelStreamNames(channelName) {
    let streamNamesLookup = this._channelEventDemux.getConsumerStatsListAll()
    .filter((stats) => {
      return stats.stream.indexOf(`${channelName}/`) === 0;
    })
    .reduce((accumulator, stats) => {
      accumulator[stats.stream] = true;
      return accumulator;
    }, {});
    return Object.keys(streamNamesLookup);
  };
  
  channelCloseOutput(channelName) {
    this._channelDataDemux.close(channelName);
  };
  
  channelCloseListener(channelName, eventName) {
    this._channelEventDemux.close(`${channelName}/${eventName}`);
  };
  
  channelCloseAllListeners(channelName) {
    let listenerStreams = this._getAllChannelStreamNames(channelName)
    .forEach((streamName) => {
      this._channelEventDemux.close(streamName);
    });
  };
  
  channelKillOutput(channelName) {
    this._channelDataDemux.kill(channelName);
  };
  
  channelKillListener(channelName, eventName) {
    this._channelEventDemux.kill(`${channelName}/${eventName}`);
  };
  
  channelKillAllListeners(channelName) {
    let listenerStreams = this._getAllChannelStreamNames(channelName)
    .forEach((streamName) => {
      this._channelEventDemux.kill(streamName);
    });
  };
  
  channelGetOutputConsumerStatsList(channelName) {
    return this._channelDataDemux.getConsumerStatsList(channelName);
  };
  
  channelGetListenerConsumerStatsList(channelName, eventName) {
    return this._channelEventDemux.getConsumerStatsList(`${channelName}/${eventName}`);
  };
  
  channelGetAllListenersConsumerStatsList(channelName) {
    return this._getAllChannelStreamNames(channelName)
    .map((streamName) => {
      return this._channelEventDemux.getConsumerStatsList(streamName);
    })
    .reduce((accumulator, statsList) => {
      statsList.forEach((stats) => {
        accumulator.push(stats);
      });
      return accumulator;
    }, []);
  };
  
  channelGetOutputBackpressure(channelName) {
    return this._channelDataDemux.getBackpressure(channelName);
  };
  
  channelGetListenerBackpressure(channelName, eventName) {
    return this._channelEventDemux.getBackpressure(`${channelName}/${eventName}`);
  };
  
  channelGetAllListenersBackpressure(channelName) {
    let listenerStreamBackpressures = this._getAllChannelStreamNames(channelName)
    .map((streamName) => {
      return this._channelEventDemux.getBackpressure(streamName);
    });
    return Math.max(...listenerStreamBackpressures.concat(0));
  };
  
  channelHasOutputConsumer(channelName, consumerId) {
    return this._channelDataDemux.hasConsumer(channelName, consumerId);
  };
  
  channelHasListenerConsumer(channelName, eventName, consumerId) {
    return this._channelEventDemux.hasConsumer(`${channelName}/${eventName}`, consumerId);
  };
  
  channelHasAnyListenerConsumer(channelName, consumerId) {
    return this._getAllChannelStreamNames(channelName)
    .some((streamName) => {
      return this._channelEventDemux.hasConsumer(streamName, consumerId);
    });
  };
  
  subscriptions(includePending) {
    let subs = [];
    Object.keys(this._channelMap).forEach((channelName) => {
      if (includePending || this._channelMap[channelName].state === AGChannel.SUBSCRIBED) {
        subs.push(channelName);
      }
    });
    return subs;
  };
  
  isSubscribed(channelName, includePending) {
    let channel = this._channelMap[channelName];
    if (includePending) {
      return !!channel;
    }
    return !!channel && channel.state === AGChannel.SUBSCRIBED;
  };
  
  processPendingSubscriptions() {
    this.preparingPendingSubscriptions = false;
    let pendingChannels = [];
  
    Object.keys(this._channelMap).forEach((channelName) => {
      let channel = this._channelMap[channelName];
      if (channel.state === AGChannel.PENDING) {
        pendingChannels.push(channel);
      }
    });
  
    pendingChannels.sort((a, b) => {
      let ap = a.options.priority || 0;
      let bp = b.options.priority || 0;
      if (ap > bp) {
        return -1;
      }
      if (ap < bp) {
        return 1;
      }
      return 0;
    });
  
    pendingChannels.forEach((channel) => {
      this._trySubscribe(channel);
    });
  };

}

