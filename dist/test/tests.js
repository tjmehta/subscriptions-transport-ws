"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
require("mocha");
var chai_1 = require("chai");
var sinon = require("sinon");
var WebSocket = require("ws");
var graphql_1 = require("graphql");
Object.assign(global, {
    WebSocket: WebSocket,
});
var wait = function (ms) { return new Promise(function (resolve) { return setTimeout(resolve, ms); }); };
var graphql_2 = require("graphql");
var graphql_subscriptions_1 = require("graphql-subscriptions");
var message_types_1 = require("../message-types");
var protocol_1 = require("../protocol");
var http_1 = require("http");
var server_1 = require("../server");
var client_1 = require("../client");
var iterall_1 = require("iterall");
var TEST_PORT = 4953;
var KEEP_ALIVE_TEST_PORT = TEST_PORT + 1;
var DELAYED_TEST_PORT = TEST_PORT + 2;
var RAW_TEST_PORT = TEST_PORT + 4;
var EVENTS_TEST_PORT = TEST_PORT + 5;
var ONCONNECT_ERROR_TEST_PORT = TEST_PORT + 6;
var ERROR_TEST_PORT = TEST_PORT + 7;
var SERVER_EXECUTOR_TESTS_PORT = ERROR_TEST_PORT + 8;
var ACK_ONCONNECTED_TEST_PORT = TEST_PORT + 8;
var ACK_ONRECONNECTED_TEST_PORT = TEST_PORT + 9;
var data = {
    '1': {
        'id': '1',
        'name': 'Dan',
    },
    '2': {
        'id': '2',
        'name': 'Marie',
    },
    '3': {
        'id': '3',
        'name': 'Jessie',
    },
};
var userType = new graphql_2.GraphQLObjectType({
    name: 'User',
    fields: {
        id: { type: graphql_2.GraphQLString },
        name: { type: graphql_2.GraphQLString },
    },
});
var testPubsub = new graphql_subscriptions_1.PubSub();
var schema = new graphql_2.GraphQLSchema({
    query: new graphql_2.GraphQLObjectType({
        name: 'Query',
        fields: {
            testString: { type: graphql_2.GraphQLString, resolve: function () { return 'value'; } },
        },
    }),
    subscription: new graphql_2.GraphQLObjectType({
        name: 'Subscription',
        fields: {
            user: {
                type: userType,
                args: {
                    id: { type: graphql_2.GraphQLString },
                },
                resolve: function (_, _a) {
                    var id = _a.id;
                    return data[id];
                },
                subscribe: function () {
                    return testPubsub.asyncIterator('user');
                },
            },
            userFiltered: {
                type: userType,
                args: {
                    id: { type: graphql_2.GraphQLString },
                },
                resolve: function (_, _a) {
                    var id = _a.id;
                    return data[id];
                },
                subscribe: graphql_subscriptions_1.withFilter(function () { return testPubsub.asyncIterator('userFiltered'); }, function (user, args) {
                    return !args['id'] || user.id === parseInt(args['id'], 10);
                }),
            },
            context: {
                type: graphql_2.GraphQLString,
                resolve: function (root, args, ctx) {
                    return ctx;
                },
                subscribe: function () {
                    return testPubsub.asyncIterator('context');
                },
            },
            error: {
                type: graphql_2.GraphQLString,
                resolve: function () {
                    throw new Error('E1');
                },
                subscribe: function () {
                    return testPubsub.asyncIterator('error');
                },
            },
        },
    }),
});
var subscriptionsPubSub = new graphql_subscriptions_1.PubSub();
var TEST_PUBLICATION = 'test_publication';
var subscriptionAsyncIteratorSpy = sinon.spy();
var resolveAsyncIteratorSpy = sinon.spy();
var subscriptionsSchema = new graphql_2.GraphQLSchema({
    query: new graphql_2.GraphQLObjectType({
        name: 'Query',
        fields: {
            testString: { type: graphql_2.GraphQLString, resolve: function () { return 'value'; } },
        },
    }),
    subscription: new graphql_2.GraphQLObjectType({
        name: 'Subscription',
        fields: {
            somethingChanged: {
                type: graphql_2.GraphQLString,
                resolve: function (payload) {
                    resolveAsyncIteratorSpy();
                    return payload;
                },
                subscribe: function () {
                    subscriptionAsyncIteratorSpy();
                    return subscriptionsPubSub.asyncIterator(TEST_PUBLICATION);
                },
            },
        },
    }),
});
var handlers = {
    onOperation: function (msg, params, webSocketRequest) {
        return Promise.resolve(Object.assign({}, params, { context: msg.payload.context }));
    },
};
var options = {
    schema: schema,
    subscribe: graphql_1.subscribe,
    execute: graphql_1.execute,
    onOperation: function (msg, params, webSocketRequest) {
        return handlers.onOperation(msg, params, webSocketRequest);
    },
};
var eventsOptions = {
    schema: schema,
    subscribe: graphql_1.subscribe,
    execute: graphql_1.execute,
    onOperation: sinon.spy(function (msg, params, webSocketRequest) {
        return Promise.resolve(Object.assign({}, params, { context: msg.payload.context }));
    }),
    onOperationComplete: sinon.spy(),
    onConnect: sinon.spy(function () {
        return { test: 'test context' };
    }),
    onDisconnect: sinon.spy(),
};
var onConnectErrorOptions = {
    schema: schema,
    subscribe: graphql_1.subscribe,
    execute: graphql_1.execute,
    isLegacy: true,
    onConnect: function (msg, connection, connectionContext) {
        connectionContext.isLegacy = onConnectErrorOptions.isLegacy;
        throw new Error('Error');
    },
};
function notFoundRequestListener(request, response) {
    response.writeHead(404);
    response.end();
}
var httpServer = http_1.createServer(notFoundRequestListener);
httpServer.listen(TEST_PORT);
new server_1.SubscriptionServer(options, { server: httpServer });
var httpServerWithKA = http_1.createServer(notFoundRequestListener);
httpServerWithKA.listen(KEEP_ALIVE_TEST_PORT);
new server_1.SubscriptionServer(Object.assign({}, options, { keepAlive: 500 }), { server: httpServerWithKA });
var httpServerWithEvents = http_1.createServer(notFoundRequestListener);
httpServerWithEvents.listen(EVENTS_TEST_PORT);
var eventsServer = new server_1.SubscriptionServer(eventsOptions, { server: httpServerWithEvents });
var httpServerWithOnConnectError = http_1.createServer(notFoundRequestListener);
httpServerWithOnConnectError.listen(ONCONNECT_ERROR_TEST_PORT);
new server_1.SubscriptionServer(onConnectErrorOptions, { server: httpServerWithOnConnectError });
var httpServerWithDelay = http_1.createServer(notFoundRequestListener);
httpServerWithDelay.listen(DELAYED_TEST_PORT);
new server_1.SubscriptionServer(Object.assign({}, options, {
    onOperation: function (msg, params) {
        return new Promise(function (resolve, reject) {
            setTimeout(function () {
                resolve(Object.assign({}, params, { context: msg.payload.context }));
            }, 100);
        });
    },
}), { server: httpServerWithDelay });
var httpServerRaw = http_1.createServer(notFoundRequestListener);
httpServerRaw.listen(RAW_TEST_PORT);
describe('Client', function () {
    var wsServer;
    beforeEach(function () {
        wsServer = new WebSocket.Server({
            server: httpServerRaw,
        });
    });
    afterEach(function () {
        if (wsServer) {
            wsServer.close();
        }
    });
    it('should send GQL_CONNECTION_INIT message when creating the connection', function (done) {
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                chai_1.expect(parsedMessage.type).to.equals(message_types_1.default.GQL_CONNECTION_INIT);
                done();
            });
        });
        new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
    });
    it('should subscribe once after reconnect', function (done) {
        var isClientReconnected = false;
        var subscriptionsCount = 0;
        wsServer.on('headers', function () {
            if (!isClientReconnected) {
                isClientReconnected = true;
                var stop = Date.now() + 1100;
                while (Date.now() < stop) {
                }
            }
        });
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                if (parsedMessage.type === message_types_1.default.GQL_START) {
                    subscriptionsCount++;
                }
            });
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            reconnect: true,
            reconnectionAttempts: 1,
        });
        client.request({
            query: "subscription useInfo {\n        user(id: 3) {\n          id\n          name\n        }\n      }",
        }).subscribe({});
        setTimeout(function () {
            chai_1.expect(subscriptionsCount).to.be.equal(1);
            done();
        }, 1500);
    });
    it('should send GQL_CONNECTION_INIT message first, then the GQL_START message', function (done) {
        var initReceived = false;
        var sub;
        var client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                if (parsedMessage.type === message_types_1.default.GQL_CONNECTION_INIT) {
                    connection.send(JSON.stringify({ type: message_types_1.default.GQL_CONNECTION_ACK, payload: {} }));
                    initReceived = true;
                }
                if (parsedMessage.type === message_types_1.default.GQL_START) {
                    chai_1.expect(initReceived).to.be.true;
                    if (sub) {
                        sub.unsubscribe();
                        done();
                    }
                    else {
                        done(new Error('did not get subscription'));
                    }
                }
            });
        });
        sub = client.request({
            query: "subscription useInfo {\n        user(id: 3) {\n          id\n          name\n        }\n      }",
        }).subscribe({});
    });
    it('should emit connect event for client side when socket is open', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var unregister = client.onConnected(function () {
            unregister();
            done();
        });
    });
    it('should emit disconnect event for client side when socket closed', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/", {
            connectionCallback: function () {
                client.client.close();
            },
        });
        var unregister = client.onDisconnected(function () {
            unregister();
            done();
        });
    });
    it('should emit reconnect event for client side when socket closed', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/", {
            reconnect: true,
            reconnectionAttempts: 1,
            connectionCallback: function () {
                client.client.close();
            },
        });
        var unregister = client.onReconnected(function () {
            unregister();
            done();
        });
    });
    it('should emit connected event for client side when socket closed', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var onConnectingSpy = sinon.spy();
        var unregisterOnConnecting = client.onConnecting(onConnectingSpy);
        var unregister = client.onConnected(function () {
            unregisterOnConnecting();
            unregister();
            chai_1.expect(onConnectingSpy.called).to.equal(true);
            done();
        });
    });
    it('should emit connecting event for client side when socket closed', function (done) {
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var onConnectedSpy = sinon.spy();
        var unregisterOnConnected = subscriptionsClient.onConnected(onConnectedSpy);
        var unregisterOnConnecting = subscriptionsClient.onConnecting(function () {
            unregisterOnConnecting();
            unregisterOnConnected();
            chai_1.expect(onConnectedSpy.called).to.equal(false);
            done();
        });
    });
    it('should emit disconnected event for client side when socket closed', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/", {
            connectionCallback: function () {
                client.client.close();
            },
        });
        var unregister = client.onDisconnected(function () {
            unregister();
            done();
        });
    });
    it('should emit reconnected event for client side when socket closed', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/", {
            reconnect: true,
            reconnectionAttempts: 1,
            connectionCallback: function () {
                client.client.close();
            },
        });
        var onReconnectingSpy = sinon.spy();
        var unregisterOnReconnecting = client.onReconnecting(onReconnectingSpy);
        var unregister = client.onReconnected(function () {
            unregisterOnReconnecting();
            unregister();
            chai_1.expect(onReconnectingSpy.called).to.equal(true);
            done();
        });
    });
    it('should emit reconnecting event for client side when socket closed', function (done) {
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/", {
            reconnect: true,
            reconnectionAttempts: 1,
            connectionCallback: function () {
                subscriptionsClient.client.close();
            },
        });
        var onReconnectedSpy = sinon.spy();
        var unregisterOnReconnected = subscriptionsClient.onReconnected(onReconnectedSpy);
        var unregisterOnReconnecting = subscriptionsClient.onReconnecting(function () {
            unregisterOnReconnecting();
            unregisterOnReconnected();
            chai_1.expect(onReconnectedSpy.called).to.equal(false);
            done();
        });
    });
    it('should throw an exception when query is not provided', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        client.request({
            query: undefined,
            operationName: 'useInfo',
            variables: {
                id: 3,
            },
        }).subscribe({
            next: function () { return chai_1.assert(false); },
            error: function (error) {
                client.close();
                chai_1.expect(error.message).to.be.equal('Must provide a query.');
                done();
            },
        });
    });
    it('should throw an exception when query is not valid', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        client.request({
            query: {},
            operationName: 'useInfo',
            variables: {
                id: 3,
            },
        }).subscribe({
            next: function () { return chai_1.assert(false); },
            error: function () {
                client.close();
                done();
            },
        });
    });
    it('should allow both data and errors on GQL_DATA', function (done) {
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                if (parsedMessage.type === message_types_1.default.GQL_CONNECTION_INIT) {
                    connection.send(JSON.stringify({ type: message_types_1.default.GQL_CONNECTION_ACK, payload: {} }));
                }
                if (parsedMessage.type === message_types_1.default.GQL_START) {
                    var dataMessage = {
                        type: message_types_1.default.GQL_DATA,
                        id: parsedMessage.id,
                        payload: {
                            data: {
                                some: 'data',
                            },
                            errors: [{
                                    message: 'Test Error',
                                }],
                        },
                    };
                    connection.send(JSON.stringify(dataMessage));
                }
            });
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
        client.request({
            query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            name\n          }\n        }",
            operationName: 'useInfo',
            variables: {
                id: 3,
            },
        }).subscribe({
            next: function (result) {
                chai_1.expect(result.data).to.have.property('some');
                chai_1.expect(result.errors).to.be.lengthOf(1);
                done();
            },
        });
    });
    it('should send connectionParams along with init message', function (done) {
        var connectionParams = {
            test: true,
        };
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                chai_1.expect(JSON.stringify(parsedMessage.payload)).to.equal(JSON.stringify(connectionParams));
                done();
            });
        });
        new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            connectionParams: connectionParams,
        });
    });
    it('should send connectionParams which resolves from a promise along with init message', function (done) {
        var connectionParams = {
            test: true,
        };
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                chai_1.expect(JSON.stringify(parsedMessage.payload)).to.equal(JSON.stringify(connectionParams));
                done();
            });
        });
        new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            connectionParams: new Promise(function (resolve) {
                setTimeout(function () {
                    resolve(connectionParams);
                }, 100);
            }),
        });
    });
    it('should send connectionParams as a function which returns a promise along with init message', function (done) {
        var connectionParams = {
            test: true,
        };
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                chai_1.expect(JSON.stringify(parsedMessage.payload)).to.equal(JSON.stringify(connectionParams));
                done();
            });
        });
        new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            connectionParams: new Promise(function (resolve) {
                setTimeout(function () {
                    resolve(connectionParams);
                }, 100);
            }),
        });
    });
    it('should catch errors in connectionParams which came from a promise', function (done) {
        var error = 'foo';
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                chai_1.expect(parsedMessage.payload).to.equal(error);
                done();
            });
        });
        new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            connectionParams: new Promise(function (_, reject) {
                setTimeout(function () {
                    reject(error);
                }, 100);
            }),
        });
    });
    it('should override OperationOptions with middleware', function (done) {
        var client3 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var asyncFunc = function (next) {
            setTimeout(function () {
                next();
            }, 100);
        };
        var spyApplyMiddlewareAsyncContents = sinon.spy(asyncFunc);
        var middleware = {
            applyMiddleware: function (opts, next) {
                spyApplyMiddlewareAsyncContents(next);
            },
        };
        var spyApplyMiddlewareFunction = sinon.spy(middleware, 'applyMiddleware');
        client3.use([middleware]);
        client3.request({
            query: "subscription useInfo($id: String) {\n            user(id: $id) {\n              id\n              name\n            }\n          }",
            operationName: 'useInfo',
            variables: {
                id: '3',
            },
        }).subscribe({
            next: function (result) {
                try {
                    client3.unsubscribeAll();
                    if (result.errors) {
                        chai_1.assert(false, 'got error during subscription creation');
                    }
                    if (result.data) {
                        chai_1.assert.equal(spyApplyMiddlewareFunction.called, true);
                        chai_1.assert.equal(spyApplyMiddlewareAsyncContents.called, true);
                    }
                    done();
                }
                catch (e) {
                    done(e);
                }
            },
            error: function (e) { return done(e); },
        });
        setTimeout(function () {
            testPubsub.publish('user', {});
        }, 200);
    });
    it('should handle correctly GQL_CONNECTION_ERROR message', function (done) {
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                connection.send(JSON.stringify({
                    type: message_types_1.default.GQL_CONNECTION_ERROR,
                    payload: { message: 'test error' },
                }));
            });
        });
        new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            connectionCallback: function (error) {
                chai_1.expect(error.message).to.equals('test error');
                done();
            },
        });
    });
    it('should handle connection_error message and handle server that closes connection', function (done) {
        var client = null;
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                connection.send(JSON.stringify({
                    type: message_types_1.default.GQL_CONNECTION_ERROR,
                    payload: { message: 'test error' },
                }), function () {
                    connection.close();
                    setTimeout(function () {
                        chai_1.expect(client.status).to.equals(WebSocket.CLOSED);
                        done();
                    }, 500);
                });
            });
        });
        client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
    });
    it('should handle correctly GQL_CONNECTION_ACK message', function (done) {
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                connection.send(JSON.stringify({ type: message_types_1.default.GQL_CONNECTION_ACK }));
            });
        });
        new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            connectionCallback: function (error) {
                chai_1.expect(error).to.equals(undefined);
                done();
            },
        });
    });
    it('should handle correctly GQL_CONNECTION_ACK payload for onConnected', function (done) {
        var server = http_1.createServer(notFoundRequestListener);
        server.listen(ACK_ONCONNECTED_TEST_PORT);
        var wss = new WebSocket.Server({ server: server });
        wss.on('connection', function (connection) {
            connection.on('message', function (message) {
                connection.send(JSON.stringify({ type: message_types_1.default.GQL_CONNECTION_ACK, payload: { appVersion: '1.0.0' } }));
                wss.close();
            });
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + ACK_ONCONNECTED_TEST_PORT + "/");
        client.onConnected(function (payload) {
            chai_1.expect(payload.appVersion).to.equal('1.0.0');
            done();
        });
    });
    it('should handle correctly GQL_CONNECTION_ACK payload for onReconnected', function (done) {
        var server = http_1.createServer(notFoundRequestListener);
        server.listen(ACK_ONRECONNECTED_TEST_PORT);
        var wss = new WebSocket.Server({ server: server });
        wss.on('connection', function (connection) {
            connection.on('message', function (message) {
                connection.send(JSON.stringify({ type: message_types_1.default.GQL_CONNECTION_ACK, payload: { appVersion: '1.0.0' } }));
                wss.close();
            });
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + ACK_ONRECONNECTED_TEST_PORT + "/", { reconnect: true });
        client.close(false, false);
        client.onReconnected(function (payload) {
            chai_1.expect(payload.appVersion).to.equal('1.0.0');
            done();
        });
    });
    it('removes subscription when it unsubscribes from it', function () {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        return new Promise(function (resolve, reject) {
            var sub = client.request({
                query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            name\n          }\n        }",
                operationName: 'useInfo',
                variables: {
                    id: 3,
                },
            }).subscribe({
                next: function (result) {
                    try {
                        sub.unsubscribe();
                        chai_1.expect(Object.keys(client.operations).length).to.equals(0);
                        resolve();
                    }
                    catch (e) {
                        reject(e);
                    }
                },
                error: function (e) { return reject(e); },
            });
            setTimeout(function () {
                testPubsub.publish('user', {});
            }, 100);
        });
    });
    it('queues messages while websocket is still connecting', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var sub = client.request({
            query: "subscription useInfo($id: String) {\n        user(id: $id) {\n          id\n          name\n        }\n      }",
            operationName: 'useInfo',
            variables: {
                id: 3,
            },
        }).subscribe({});
        client.onConnecting(function () {
            chai_1.expect(client.unsentMessagesQueue.length).to.equals(1);
            sub.unsubscribe();
            setTimeout(function () {
                chai_1.expect(client.unsentMessagesQueue.length).to.equals(0);
                done();
            }, 100);
        });
    });
    it('should call error handler when graphql result has errors', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        setTimeout(function () {
            client.request({
                query: "subscription useInfo{\n          error\n        }",
                variables: {},
            }).subscribe({
                next: function (result) {
                    if (result.errors.length) {
                        client.unsubscribeAll();
                        done();
                        return;
                    }
                    if (result) {
                        client.unsubscribeAll();
                        chai_1.assert(false);
                    }
                },
            });
        }, 100);
        setTimeout(function () {
            testPubsub.publish('error', {});
        }, 200);
    });
    it('should call error handler when graphql query is not valid', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        setTimeout(function () {
            client.request({
                query: "subscription useInfo{\n          invalid\n        }",
                variables: {},
            }).subscribe({
                next: function (result) {
                    if (result.errors.length) {
                        chai_1.expect(result.errors[0].message).to.equals('Cannot query field "invalid" on type "Subscription".');
                        done();
                    }
                    else {
                        chai_1.assert(false);
                    }
                },
            });
        }, 100);
    });
    function testBadServer(payload, errorMessage, done) {
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                if (parsedMessage.type === message_types_1.default.GQL_START) {
                    connection.send(JSON.stringify({
                        type: message_types_1.default.GQL_ERROR,
                        id: parsedMessage.id,
                        payload: payload,
                    }));
                }
            });
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
        client.request({
            query: "\n        subscription useInfo{\n          invalid\n        }\n      ",
            variables: {},
        }).subscribe({
            next: function () { return chai_1.assert(false); },
            error: function (error) {
                chai_1.expect(error.message).to.equals(errorMessage);
                done();
            },
        });
    }
    it('should not connect until subscribe is called if lazy mode', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            lazy: true,
        });
        chai_1.expect(client.client).to.be.null;
        var sub = client.request({
            query: "subscription useInfo($id: String) {\n        user(id: $id) {\n          id\n          name\n        }\n      }",
            operationName: 'useInfo',
            variables: {
                id: 3,
            },
        }).subscribe({
            error: function (e) { return done(e); },
        });
        var isDone = false;
        wsServer.on('connection', function (connection) {
            connection.on('message', function () {
                if (!isDone) {
                    isDone = true;
                    try {
                        chai_1.expect(client.client).to.not.be.null;
                        sub.unsubscribe();
                        done();
                    }
                    catch (e) {
                        done(e);
                    }
                }
            });
        });
    });
    it('should call the connectionParams function upon connection to get connectionParams if connectionParams is a function', function (done) {
        var connectionParams = sinon.spy(function () { return ({
            foo: 'bar',
        }); });
        var client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            lazy: true,
            connectionParams: connectionParams,
        });
        var isDone = false, sub = null;
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                if (!isDone) {
                    isDone = true;
                    try {
                        var parsedMessage = JSON.parse(message);
                        if (sub) {
                            sub.unsubscribe();
                        }
                        chai_1.expect(parsedMessage.payload).to.eql({
                            foo: 'bar',
                        });
                        done();
                    }
                    catch (e) {
                        done(e);
                    }
                }
            });
        });
        sub = client.request({
            query: "subscription useInfo($id: String) {\n        user(id: $id) {\n          id\n          name\n        }\n      }",
            operationName: 'useInfo',
            variables: {
                id: 3,
            },
        }).subscribe({});
    });
    it('should handle missing errors', function (done) {
        var errorMessage = 'Unknown error';
        var payload = {};
        testBadServer(payload, errorMessage, done);
    });
    it('should handle errors that are not an array', function (done) {
        var errorMessage = 'Just an error';
        var payload = {
            message: errorMessage,
        };
        testBadServer(payload, errorMessage, done);
    });
    it('should reconnect to the server', function (done) {
        var connections = 0;
        var client;
        var originalClient;
        wsServer.on('connection', function (connection) {
            connection.on('error', function (error) {
            });
            connections += 1;
            if (connections === 1) {
                originalClient.close();
            }
            else {
                chai_1.expect(client.client).to.not.be.equal(originalClient);
                done();
            }
        });
        client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", { reconnect: true });
        originalClient = client.client;
    });
    it('should resubscribe after reconnect', function (done) {
        var connections = 0;
        var sub;
        var client = null;
        wsServer.on('connection', function (connection) {
            connections += 1;
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                if (parsedMessage.type === message_types_1.default.GQL_START) {
                    if (connections === 1) {
                        client.client.close();
                    }
                    else {
                        sub.unsubscribe();
                        done();
                    }
                }
            });
        });
        client = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", { reconnect: true });
        sub = client.request({
            query: "\n        subscription useInfo{\n          invalid\n        }\n      ",
            variables: {},
        }).subscribe({
            next: function () {
                chai_1.assert(false);
            },
        });
    });
    it('should emit event when an websocket error occurs', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + ERROR_TEST_PORT + "/");
        client.request({
            query: "subscription useInfo{\n        invalid\n      }",
            variables: {},
        }).subscribe({
            next: function () {
                chai_1.assert(false);
            },
        });
        client.onError(function (err) {
            chai_1.expect(err.message).to.be.equal("connect ECONNREFUSED 127.0.0.1:" + ERROR_TEST_PORT);
            done();
        });
    });
    it('should stop trying to reconnect to the server', function (done) {
        wsServer.on('connection', function (connection) {
            connection.close();
        });
        var errorCount = 0;
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            timeout: 500,
            reconnect: true,
            reconnectionAttempts: 2,
        });
        subscriptionsClient.onError(function (error) {
            chai_1.expect(error.message).to.contain('A message was not sent');
            errorCount += 1;
        });
        var connectSpy = sinon.spy(subscriptionsClient, 'connect');
        setTimeout(function () {
            chai_1.expect(connectSpy.callCount).to.be.equal(2);
            chai_1.expect(errorCount).to.be.equal(1);
            done();
        }, 1500);
    });
    it('should stop trying to reconnect to the server if it does not receives the ack', function (done) {
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            timeout: 500,
            reconnect: true,
            reconnectionAttempts: 2,
        });
        var connectSpy = sinon.spy(subscriptionsClient, 'connect');
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                if (parsedMessage.type === message_types_1.default.GQL_CONNECTION_INIT) {
                    connection.close();
                }
            });
        });
        setTimeout(function () {
            chai_1.expect(connectSpy.callCount).to.be.equal(2);
            done();
        }, 1500);
    });
    it('should keep trying to reconnect if receives the ack from the server', function (done) {
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            timeout: 500,
            reconnect: true,
            reconnectionAttempts: 2,
        });
        var connectSpy = sinon.spy(subscriptionsClient, 'connect');
        var connections = 0;
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                if (parsedMessage.type === message_types_1.default.GQL_CONNECTION_INIT) {
                    ++connections;
                    connection.send(JSON.stringify({ type: message_types_1.default.GQL_CONNECTION_ACK, payload: {} }));
                    connection.close();
                }
            });
        });
        setTimeout(function () {
            chai_1.expect(connections).to.be.greaterThan(3);
            chai_1.expect(connectSpy.callCount).to.be.greaterThan(2);
            wsServer.close();
            done();
        }, 1900);
    });
    it('should take care of received keep alive', function (done) {
        var wasKAReceived = false;
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + KEEP_ALIVE_TEST_PORT + "/", { timeout: 600 });
        var originalOnMessage = subscriptionsClient.client.onmessage;
        subscriptionsClient.client.onmessage = function (dataReceived) {
            var receivedDataParsed = JSON.parse(dataReceived.data);
            if (receivedDataParsed.type === message_types_1.default.GQL_CONNECTION_KEEP_ALIVE) {
                if (!wasKAReceived) {
                    wasKAReceived = true;
                    originalOnMessage(dataReceived);
                }
            }
        };
        setTimeout(function () {
            chai_1.expect(wasKAReceived).to.equal(true);
            chai_1.expect(subscriptionsClient.status).to.equal(WebSocket.CLOSED);
            done();
        }, 1200);
    });
    it('should correctly clear timeout if receives ka too early', function (done) {
        var receivedKeepAlive = 0;
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + KEEP_ALIVE_TEST_PORT + "/", { timeout: 600 });
        var checkConnectionSpy = sinon.spy(subscriptionsClient, 'checkConnection');
        var originalOnMessage = subscriptionsClient.client.onmessage;
        subscriptionsClient.client.onmessage = function (dataReceived) {
            var receivedDataParsed = JSON.parse(dataReceived.data);
            if (receivedDataParsed.type === message_types_1.default.GQL_CONNECTION_KEEP_ALIVE) {
                ++receivedKeepAlive;
                originalOnMessage(dataReceived);
            }
        };
        setTimeout(function () {
            chai_1.expect(checkConnectionSpy.callCount).to.be.equal(receivedKeepAlive);
            chai_1.expect(subscriptionsClient.status).to.be.equal(subscriptionsClient.client.OPEN);
            done();
        }, 1300);
    });
    it('should take care of invalid message received', function (done) {
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
        var originalOnMessage = subscriptionsClient.client.onmessage;
        var dataToSend = {
            data: JSON.stringify({ type: 'invalid' }),
        };
        chai_1.expect(function () {
            originalOnMessage.call(subscriptionsClient, dataToSend)();
        }).to.throw('Invalid message type!');
        done();
    });
    it('should throw if received data is not JSON-parseable', function (done) {
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
        var originalOnMessage = subscriptionsClient.client.onmessage;
        var dataToSend = {
            data: 'invalid',
        };
        chai_1.expect(function () {
            originalOnMessage.call(subscriptionsClient, dataToSend)();
        }).to.throw('Message must be JSON-parseable. Got: invalid');
        done();
    });
    it('should not send GQL_STOP on unsubscribe after operation received a GQL_COMPLETE', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var sendMessageSpy = sinon.spy(client, 'sendMessage');
        client.onConnected(function () {
            var subscription = client.request({
                query: "subscription { somethingChanged }",
                variables: {},
            }).subscribe({
                next: function () {
                    client.client.onmessage({
                        data: JSON.stringify({ id: 1, type: message_types_1.default.GQL_COMPLETE }),
                    });
                },
                complete: function () {
                    subscription.unsubscribe();
                },
            });
            setTimeout(function () {
                chai_1.expect(sendMessageSpy.calledWith(undefined, 'connection_init', sinon.match.any)).to.be.true;
                chai_1.expect(sendMessageSpy.calledWith('1', 'start', sinon.match.any)).to.be.true;
                chai_1.expect(sendMessageSpy.calledWith('1', 'stop', sinon.match.any)).to.be.false;
                done();
            }, 1000);
        });
    });
    it('should delete operation when receive a GQL_COMPLETE', function (done) {
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/");
        subscriptionsClient.operations['1'] = {
            options: {
                query: 'invalid',
            },
            handler: function () {
            },
        };
        var originalOnMessage = subscriptionsClient.client.onmessage;
        var dataToSend = {
            data: JSON.stringify({ id: 1, type: message_types_1.default.GQL_COMPLETE }),
        };
        chai_1.expect(subscriptionsClient.operations).to.have.property('1');
        originalOnMessage(dataToSend);
        chai_1.expect(subscriptionsClient.operations).to.not.have.property('1');
        done();
    });
    it('should force close the connection without tryReconnect', function (done) {
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            reconnect: true,
            reconnectionAttempts: 1,
        });
        var tryReconnectSpy = sinon.spy(subscriptionsClient, 'tryReconnect');
        var receivedConnecitonTerminate = false;
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                if (parsedMessage.type === message_types_1.default.GQL_CONNECTION_INIT) {
                    connection.send(JSON.stringify({ type: message_types_1.default.GQL_CONNECTION_ACK, payload: {} }));
                }
                if (parsedMessage.type === message_types_1.default.GQL_CONNECTION_TERMINATE) {
                    receivedConnecitonTerminate = true;
                }
            });
        });
        var originalOnMessage = subscriptionsClient.client.onmessage;
        subscriptionsClient.client.onmessage = function (dataReceived) {
            var receivedDataParsed = JSON.parse(dataReceived.data);
            if (receivedDataParsed.type === message_types_1.default.GQL_CONNECTION_ACK) {
                originalOnMessage(dataReceived);
                subscriptionsClient.close();
            }
        };
        setTimeout(function () {
            chai_1.expect(receivedConnecitonTerminate).to.be.equal(true);
            chai_1.expect(tryReconnectSpy.callCount).to.be.equal(0);
            chai_1.expect(subscriptionsClient.status).to.be.equal(WebSocket.CLOSED);
            done();
        }, 500);
    });
    it('should close the connection without sent connection terminate and reconnect', function (done) {
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            reconnect: true,
            reconnectionAttempts: 1,
        });
        var tryReconnectSpy = sinon.spy(subscriptionsClient, 'tryReconnect');
        var receivedConnecitonTerminate = false;
        wsServer.on('connection', function (connection) {
            connection.on('message', function (message) {
                var parsedMessage = JSON.parse(message);
                if (parsedMessage.type === message_types_1.default.GQL_CONNECTION_INIT) {
                    connection.send(JSON.stringify({ type: message_types_1.default.GQL_CONNECTION_ACK, payload: {} }));
                }
                if (parsedMessage.type === message_types_1.default.GQL_CONNECTION_TERMINATE) {
                    receivedConnecitonTerminate = true;
                }
            });
        });
        var originalOnMessage = subscriptionsClient.client.onmessage;
        subscriptionsClient.client.onmessage = function (dataReceived) {
            var receivedDataParsed = JSON.parse(dataReceived.data);
            if (receivedDataParsed.type === message_types_1.default.GQL_CONNECTION_ACK) {
                originalOnMessage(dataReceived);
                subscriptionsClient.close(false);
            }
        };
        setTimeout(function () {
            chai_1.expect(tryReconnectSpy.callCount).to.be.equal(1);
            chai_1.expect(subscriptionsClient.status).to.be.equal(WebSocket.OPEN);
            chai_1.expect(receivedConnecitonTerminate).to.be.equal(false);
            done();
        }, 500);
    });
    it('should close the connection after inactivityTimeout and zero active subscriptions', function (done) {
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + RAW_TEST_PORT + "/", {
            inactivityTimeout: 100,
        });
        var sub = subscriptionsClient.request({
            query: "subscription useInfo($id: String) {\n        user(id: $id) {\n          id\n          name\n        }\n      }",
            operationName: 'useInfo',
            variables: {
                id: 3,
            },
        }).subscribe({});
        setTimeout(function () {
            chai_1.expect(Object.keys(subscriptionsClient.operations).length).to.be.equal(1);
            sub.unsubscribe();
            setTimeout(function () {
                chai_1.expect(Object.keys(subscriptionsClient.operations).length).to.be.equal(0);
                setTimeout(function () {
                    chai_1.expect(subscriptionsClient.status).to.be.equal(WebSocket.CLOSED);
                    done();
                }, 101);
            }, 50);
        }, 50);
    });
    it('should allow passing custom WebSocket protocols', function () {
        var testCases = ['custom-protocol', ['custom', 'protocols']];
        for (var _i = 0, testCases_1 = testCases; _i < testCases_1.length; _i++) {
            var testCase = testCases_1[_i];
            var mockWebSocket = sinon.spy();
            new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT, {}, mockWebSocket, testCase);
            chai_1.expect(mockWebSocket.calledOnce).to.be.true;
            chai_1.expect(mockWebSocket.firstCall.args[1]).to.equal(testCase);
        }
    });
});
describe('Server', function () {
    var onOperationSpy;
    var server;
    beforeEach(function () {
        onOperationSpy = sinon.spy(handlers, 'onOperation');
    });
    afterEach(function () {
        if (server) {
            server.close();
        }
        if (onOperationSpy) {
            onOperationSpy.restore();
        }
        if (eventsOptions) {
            eventsOptions.onConnect.resetHistory();
            eventsOptions.onDisconnect.resetHistory();
            eventsOptions.onOperation.resetHistory();
            eventsOptions.onOperationComplete.resetHistory();
        }
    });
    it('should throw an exception when creating a server without execute', function () {
        chai_1.expect(function () {
            new server_1.SubscriptionServer({ execute: undefined }, { server: httpServer });
        }).to.throw();
    });
    it('should throw an exception when creating a server with subscribe only', function () {
        chai_1.expect(function () {
            new server_1.SubscriptionServer({ subscribe: {} }, { server: httpServer });
        }).to.throw();
    });
    it('should throw an exception when execute is missing', function () {
        chai_1.expect(function () {
            new server_1.SubscriptionServer({}, { server: httpServer });
        }).to.throw();
    });
    it('should throw an exception when schema is not provided', function (done) {
        server = http_1.createServer(notFoundRequestListener);
        server.listen(SERVER_EXECUTOR_TESTS_PORT);
        server_1.SubscriptionServer.create({
            execute: graphql_1.execute,
        }, {
            server: server,
            path: '/',
        });
        var errorMessage;
        var client = new client_1.SubscriptionClient("ws://localhost:" + SERVER_EXECUTOR_TESTS_PORT + "/");
        client.onConnected(function () {
            client.request({
                query: "query { testString }",
                variables: {},
            }).subscribe({
                next: function (res) {
                    chai_1.assert(false, 'expected error to be thrown');
                },
                error: function (err) {
                    errorMessage = err.message;
                    chai_1.expect(errorMessage).to.contain('Missing schema information');
                    done();
                },
                complete: function () {
                    chai_1.assert(false, 'expected error to be thrown');
                },
            });
        });
    });
    it('should use schema provided in onOperation', function (done) {
        server = http_1.createServer(notFoundRequestListener);
        server.listen(SERVER_EXECUTOR_TESTS_PORT);
        server_1.SubscriptionServer.create({
            execute: graphql_1.execute,
            onOperation: function () {
                return {
                    schema: schema,
                };
            },
        }, {
            server: server,
            path: '/',
        });
        var msgCnt = 0;
        var client = new client_1.SubscriptionClient("ws://localhost:" + SERVER_EXECUTOR_TESTS_PORT + "/");
        client.onConnected(function () {
            client.request({
                query: "query { testString }",
                variables: {},
            }).subscribe({
                next: function (res) {
                    if (res.errors) {
                        chai_1.assert(false, 'unexpected error from request');
                    }
                    chai_1.expect(res.data).to.deep.equal({ testString: 'value' });
                    msgCnt++;
                },
                error: function (err) {
                    chai_1.assert(false, 'unexpected error from request');
                },
                complete: function () {
                    chai_1.expect(msgCnt).to.equals(1);
                    done();
                },
            });
        });
    });
    it('should accept execute method than returns a Promise (original execute)', function (done) {
        server = http_1.createServer(notFoundRequestListener);
        server.listen(SERVER_EXECUTOR_TESTS_PORT);
        var msgCnt = 0;
        server_1.SubscriptionServer.create({
            schema: schema,
            execute: graphql_1.execute,
        }, {
            server: server,
            path: '/',
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + SERVER_EXECUTOR_TESTS_PORT + "/");
        client.onConnected(function () {
            client.request({
                query: "query { testString }",
                variables: {},
            }).subscribe({
                next: function (res) {
                    if (res.errors) {
                        chai_1.assert(false, 'unexpected error from request');
                    }
                    chai_1.expect(res.data).to.deep.equal({ testString: 'value' });
                    msgCnt++;
                },
                error: function (err) {
                    chai_1.assert(false, 'unexpected error from request');
                },
                complete: function () {
                    chai_1.expect(msgCnt).to.equals(1);
                    done();
                },
            });
        });
    });
    it('server close should work', function (done) {
        server = http_1.createServer(notFoundRequestListener);
        server.listen(SERVER_EXECUTOR_TESTS_PORT);
        var subServer = server_1.SubscriptionServer.create({
            schema: schema,
            execute: graphql_1.execute,
        }, {
            server: server,
            path: '/',
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + SERVER_EXECUTOR_TESTS_PORT + "/");
        client.onDisconnected(function () {
            done();
        });
        client.onConnected(function () {
            client.request({
                query: "query { testString }",
                variables: {},
            }).subscribe({
                next: function (res) {
                    if (res.errors) {
                        chai_1.assert(false, 'unexpected error from request');
                    }
                    else {
                        chai_1.expect(res.data).to.deep.equal({ testString: 'value' });
                    }
                },
                complete: function () { return subServer.close(); },
            });
        });
    });
    it('should have request interface (apollo client 2.0)', function (done) {
        server = http_1.createServer(notFoundRequestListener);
        server.listen(SERVER_EXECUTOR_TESTS_PORT);
        server_1.SubscriptionServer.create({
            schema: schema,
            execute: graphql_1.execute,
        }, {
            server: server,
            path: '/',
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + SERVER_EXECUTOR_TESTS_PORT + "/");
        client.onConnected(function () {
            var hasValue = false;
            client.request({
                query: "query { testString }",
                variables: {},
            }).subscribe({
                next: function (res) {
                    chai_1.expect(hasValue).to.equal(false);
                    chai_1.expect(res).to.deep.equal({ data: { testString: 'value' } });
                    hasValue = true;
                },
                error: function (err) {
                    done(new Error('unexpected error from subscribe'));
                },
                complete: function () {
                    if (false === hasValue) {
                        return done(new Error('No value recived from observable'));
                    }
                    done();
                },
            });
        });
    });
    it('should accept execute method than returns an AsyncIterator', function (done) {
        server = http_1.createServer(notFoundRequestListener);
        server.listen(SERVER_EXECUTOR_TESTS_PORT);
        var executeWithAsyncIterable = function () {
            var _a;
            var called = false;
            return _a = {
                    next: function () {
                        if (called === true) {
                            return this.return();
                        }
                        called = true;
                        return Promise.resolve({ value: { data: { testString: 'value' } }, done: false });
                    },
                    return: function () {
                        return Promise.resolve({ value: undefined, done: true });
                    },
                    throw: function (e) {
                        return Promise.reject(e);
                    }
                },
                _a[iterall_1.$$asyncIterator] = function () {
                    return this;
                },
                _a;
        };
        server_1.SubscriptionServer.create({
            schema: schema,
            execute: executeWithAsyncIterable,
        }, {
            server: server,
            path: '/',
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + SERVER_EXECUTOR_TESTS_PORT + "/");
        client.onConnected(function () {
            client.request({
                query: "query { testString }",
                variables: {},
            }).subscribe({
                next: function (res) {
                    if (res.errors) {
                        chai_1.assert(false, 'unexpected error from request');
                    }
                    else {
                        chai_1.expect(res.data).to.deep.equal({ testString: 'value' });
                    }
                    done();
                },
            });
        });
    });
    it('should handle socket error and close the connection on error', function (done) {
        var spy = sinon.spy();
        var httpServerForError = http_1.createServer(notFoundRequestListener);
        httpServerForError.listen(ERROR_TEST_PORT);
        new server_1.SubscriptionServer({
            schema: schema,
            execute: graphql_1.execute,
            onConnect: function (payload, socket) {
                setTimeout(function () {
                    socket.emit('error', new Error('test'));
                    setTimeout(function () {
                        chai_1.assert(spy.calledOnce);
                        httpServerForError.close();
                        done();
                    }, 500);
                }, 100);
            },
        }, { server: httpServerForError });
        var client = new client_1.SubscriptionClient("ws://localhost:" + ERROR_TEST_PORT + "/");
        client.onDisconnected(spy);
    });
    it('should trigger onConnect when client connects and validated', function (done) {
        new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/");
        setTimeout(function () {
            chai_1.assert(eventsOptions.onConnect.calledOnce);
            done();
        }, 200);
    });
    it('should trigger onConnect with the correct connectionParams', function (done) {
        var connectionParams = {
            test: true,
        };
        new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/", {
            connectionParams: connectionParams,
        });
        setTimeout(function () {
            chai_1.assert(eventsOptions.onConnect.calledOnce);
            chai_1.expect(JSON.stringify(eventsOptions.onConnect.getCall(0).args[0])).to.equal(JSON.stringify(connectionParams));
            done();
        }, 200);
    });
    it('should trigger onConnect with the request available in ConnectionContext', function (done) {
        new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/");
        setTimeout(function () {
            chai_1.assert(eventsOptions.onConnect.calledOnce);
            chai_1.expect(eventsOptions.onConnect.getCall(0).args[2].request).to.be.an.instanceof(http_1.IncomingMessage);
            done();
        }, 200);
    });
    it('should trigger onConnect and return GQL_CONNECTION_ERROR with error', function (done) {
        var connectionCallbackSpy = sinon.spy();
        onConnectErrorOptions.isLegacy = false;
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + ONCONNECT_ERROR_TEST_PORT + "/", {
            connectionCallback: connectionCallbackSpy,
        });
        setTimeout(function () {
            chai_1.expect(connectionCallbackSpy.calledOnce).to.be.true;
            chai_1.expect(connectionCallbackSpy.getCall(0).args[0]).to.eql({ message: 'Error' });
            subscriptionsClient.close();
            done();
        }, 200);
    });
    it('should trigger onConnect and return INIT_FAIL with error', function (done) {
        var connectionCallbackSpy = sinon.spy();
        onConnectErrorOptions.isLegacy = true;
        var subscriptionsClient = new client_1.SubscriptionClient("ws://localhost:" + ONCONNECT_ERROR_TEST_PORT + "/", {
            connectionCallback: connectionCallbackSpy,
        });
        var originalOnMessage = subscriptionsClient.client.onmessage;
        subscriptionsClient.client.onmessage = function (dataReceived) {
            var messageData = JSON.parse(dataReceived.data);
            if (messageData.type === message_types_1.default.INIT_FAIL) {
                messageData.type = message_types_1.default.GQL_CONNECTION_ERROR;
            }
            dataReceived.data = JSON.stringify(messageData);
            originalOnMessage(dataReceived);
        };
        setTimeout(function () {
            chai_1.expect(connectionCallbackSpy.calledOnce).to.be.true;
            chai_1.expect(connectionCallbackSpy.getCall(0).args[0]).to.eql({ error: 'Error' });
            subscriptionsClient.close();
            done();
        }, 200);
    });
    it('should trigger onDisconnect when client disconnects', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/");
        setTimeout(function () {
            client.client.close();
        }, 100);
        setTimeout(function () {
            chai_1.assert(eventsOptions.onDisconnect.calledOnce);
            done();
        }, 200);
    });
    it('should trigger onDisconnect with ConnectionContext as second argument', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/");
        setTimeout(function () {
            client.client.close();
        }, 100);
        setTimeout(function () {
            chai_1.assert(eventsOptions.onDisconnect.calledOnce);
            chai_1.expect(eventsOptions.onConnect.getCall(0).args[1]).to.not.be.undefined;
            done();
        }, 200);
    });
    it('should call unsubscribe when client closes the connection', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/");
        var spy = sinon.spy(eventsServer, 'unsubscribe');
        client.request({
            query: "subscription useInfo($id: String) {\n        user(id: $id) {\n          id\n          name\n        }\n      }",
            operationName: 'useInfo',
            variables: {
                id: '3',
            },
        }).subscribe({});
        setTimeout(function () {
            client.client.close();
        }, 500);
        setTimeout(function () {
            chai_1.assert(spy.calledOnce);
            done();
        }, 1000);
    });
    it('should trigger onOperation when client subscribes', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/");
        client.request({
            query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            name\n          }\n        }",
            operationName: 'useInfo',
            variables: {
                id: '3',
            },
        }).subscribe({
            next: function (result) {
                if (result.errors) {
                    chai_1.assert(false);
                }
            },
        });
        setTimeout(function () {
            chai_1.assert(eventsOptions.onOperation.calledOnce);
            done();
        }, 200);
    });
    it('should trigger onOperationComplete when client unsubscribes', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + EVENTS_TEST_PORT + "/");
        var sub = client.request({
            query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            name\n          }\n        }",
            operationName: 'useInfo',
            variables: {
                id: '3',
            },
        }).subscribe({
            next: function (result) {
                if (result.errors) {
                    sub.unsubscribe();
                    chai_1.assert(false);
                    done();
                }
                if (result.data) {
                    sub.unsubscribe();
                    setTimeout(function () {
                        chai_1.assert(eventsOptions.onOperationComplete.calledOnce);
                        done();
                    }, 200);
                }
            },
        });
        setTimeout(function () {
            testPubsub.publish('user', {});
        }, 100);
    });
    it('should send correct results to multiple clients with subscriptions', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var client1 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var numResults = 0;
        setTimeout(function () {
            client.request({
                query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            name\n          }\n        }",
                operationName: 'useInfo',
                variables: {
                    id: '3',
                },
            }).subscribe({
                next: function (result) {
                    if (result.errors) {
                        chai_1.assert(false);
                    }
                    if (result.data) {
                        chai_1.assert.property(result.data, 'user');
                        chai_1.assert.equal(result.data.user.id, '3');
                        chai_1.assert.equal(result.data.user.name, 'Jessie');
                        numResults++;
                    }
                },
            });
        }, 100);
        var client11 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var numResults1 = 0;
        setTimeout(function () {
            client11.request({
                query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            name\n          }\n        }",
                operationName: 'useInfo',
                variables: {
                    id: '2',
                },
            }).subscribe({
                next: function (result) {
                    if (result.errors) {
                        chai_1.assert(false);
                    }
                    if (result.data) {
                        chai_1.assert.property(result.data, 'user');
                        chai_1.assert.equal(result.data.user.id, '2');
                        chai_1.assert.equal(result.data.user.name, 'Marie');
                        numResults1++;
                    }
                },
            });
        }, 100);
        setTimeout(function () {
            testPubsub.publish('user', {});
        }, 200);
        setTimeout(function () {
            client.unsubscribeAll();
            chai_1.expect(numResults).to.equals(1);
            client1.unsubscribeAll();
            chai_1.expect(numResults1).to.equals(1);
            done();
        }, 400);
    });
    it('should send a gql_data with errors message to client with invalid query', function (done) {
        var client1 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        setTimeout(function () {
            client1.client.onmessage = function (message) {
                var messageData = JSON.parse(message.data);
                chai_1.assert.isTrue(messageData.type === message_types_1.default.GQL_DATA
                    || messageData.type === message_types_1.default.GQL_COMPLETE);
                if (messageData.type === message_types_1.default.GQL_COMPLETE) {
                    done();
                    return;
                }
                var result = messageData.payload;
                chai_1.assert.isAbove(result.errors.length, 0, 'Query should\'ve failed');
            };
            client1.request({
                query: "subscription useInfo($id: String) {\n          user(id: $id) {\n            id\n            birthday\n          }\n        }",
                operationName: 'useInfo',
                variables: {
                    id: '3',
                },
            }).subscribe({});
        }, 100);
    });
    it('should set up the proper filters when subscribing', function (done) {
        var numTriggers = 0;
        var client3 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var client4 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        setTimeout(function () {
            client3.request({
                query: "subscription userInfoFilter1($id: String) {\n            userFiltered(id: $id) {\n              id\n              name\n            }\n          }",
                operationName: 'userInfoFilter1',
                variables: {
                    id: '3',
                },
            }).subscribe({
                next: function (result) {
                    if (result.errors) {
                        chai_1.assert(false);
                    }
                    if (result.data) {
                        numTriggers += 1;
                        chai_1.assert.property(result.data, 'userFiltered');
                        chai_1.assert.equal(result.data.userFiltered.id, '3');
                        chai_1.assert.equal(result.data.userFiltered.name, 'Jessie');
                    }
                },
            });
            client4.request({
                query: "subscription userInfoFilter1($id: String) {\n            userFiltered(id: $id) {\n              id\n              name\n            }\n          }",
                operationName: 'userInfoFilter1',
                variables: {
                    id: '1',
                },
            }).subscribe({
                next: function (result) {
                    if (result.errors) {
                        chai_1.assert(false);
                    }
                    if (result.data) {
                        numTriggers += 1;
                        chai_1.assert.property(result.data, 'userFiltered');
                        chai_1.assert.equal(result.data.userFiltered.id, '1');
                        chai_1.assert.equal(result.data.userFiltered.name, 'Dan');
                    }
                },
            });
        }, 100);
        setTimeout(function () {
            testPubsub.publish('userFiltered', { id: 1 });
            testPubsub.publish('userFiltered', { id: 2 });
            testPubsub.publish('userFiltered', { id: 3 });
        }, 200);
        setTimeout(function () {
            chai_1.assert.equal(numTriggers, 2);
            done();
        }, 300);
    });
    it('correctly sets the context in onOperation', function (done) {
        var CTX = 'testContext';
        var client3 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        client3.request({
            query: "subscription context {\n          context\n        }",
            variables: {},
            context: CTX,
        }).subscribe({
            next: function (result) {
                client3.unsubscribeAll();
                if (result.errors) {
                    chai_1.assert(false);
                }
                if (result.data) {
                    chai_1.assert.property(result.data, 'context');
                    chai_1.assert.equal(result.data.context, CTX);
                }
                done();
            },
        });
        setTimeout(function () {
            testPubsub.publish('context', {});
        }, 100);
    });
    it('passes through webSocketRequest to onOperation', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        client.request({
            query: "\n        subscription context {\n          context\n        }\n      ",
            variables: {},
        }).subscribe({});
        setTimeout(function () {
            client.close();
            chai_1.assert(onOperationSpy.calledOnce);
            chai_1.expect(onOperationSpy.getCall(0).args[2]).to.not.be.undefined;
            done();
        }, 100);
    });
    it('does not send more subscription data after client unsubscribes', function (done) {
        var client4 = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        var sub;
        setTimeout(function () {
            sub.unsubscribe();
        }, 50);
        setTimeout(function () {
            testPubsub.publish('user', {});
        }, 100);
        setTimeout(function () {
            client4.close();
            done();
        }, 150);
        client4.client.onmessage = function (message) {
            if (JSON.parse(message.data).type === message_types_1.default.GQL_DATA) {
                chai_1.assert(false);
            }
        };
        sub = client4.request({
            query: "subscription useInfo($id: String) {\n      user(id: $id) {\n        id\n        name\n      }\n    }",
            operationName: 'useInfo',
            variables: {
                id: '3',
            },
        }).subscribe({});
    });
    it('rejects a client that does not specify a supported protocol', function (done) {
        var client = new WebSocket("ws://localhost:" + TEST_PORT + "/");
        client.on('close', function (code) {
            chai_1.expect(code).to.be.eq(1002);
            done();
        });
    });
    it('rejects unparsable message', function (done) {
        var client = new WebSocket("ws://localhost:" + TEST_PORT + "/", protocol_1.GRAPHQL_SUBSCRIPTIONS);
        client.onmessage = function (message) {
            var messageData = JSON.parse(message.data);
            chai_1.assert.equal(messageData.type, message_types_1.default.GQL_CONNECTION_ERROR);
            chai_1.assert.isDefined(messageData.payload, 'Number of errors is greater than 0.');
            client.close();
            done();
        };
        client.onopen = function () {
            client.send('HI');
        };
    });
    it('rejects nonsense message', function (done) {
        var client = new WebSocket("ws://localhost:" + TEST_PORT + "/", protocol_1.GRAPHQL_SUBSCRIPTIONS);
        client.onmessage = function (message) {
            var messageData = JSON.parse(message.data);
            chai_1.assert.equal(messageData.type, message_types_1.default.GQL_ERROR);
            chai_1.assert.isDefined(messageData.payload, 'Number of errors is greater than 0.');
            client.close();
            done();
        };
        client.onopen = function () {
            client.send(JSON.stringify({}));
        };
    });
    it('does not crash on unsub for Object.prototype member', function (done) {
        var client = new WebSocket("ws://localhost:" + TEST_PORT + "/", protocol_1.GRAPHQL_SUBSCRIPTIONS);
        client.onopen = function () {
            client.send(JSON.stringify({ type: message_types_1.default.GQL_STOP, id: 'toString' }));
            setTimeout(done, 10);
        };
    });
    it('sends back any type of error', function (done) {
        var client = new client_1.SubscriptionClient("ws://localhost:" + TEST_PORT + "/");
        client.request({
            query: "invalid useInfo{\n          error\n        }",
            variables: {},
        }).subscribe({
            next: function () { return chai_1.assert(false); },
            error: function () {
                client.unsubscribeAll();
                done();
            },
        });
    });
    it('sends a keep alive signal in the socket', function (done) {
        var client = new WebSocket("ws://localhost:" + KEEP_ALIVE_TEST_PORT + "/", protocol_1.GRAPHQL_SUBSCRIPTIONS);
        var yieldCount = 0;
        client.onmessage = function (message) {
            var parsedMessage = JSON.parse(message.data);
            if (parsedMessage.type === message_types_1.default.GQL_CONNECTION_KEEP_ALIVE) {
                yieldCount += 1;
                if (yieldCount > 2) {
                    client.close();
                    done();
                }
            }
        };
        client.onopen = function () {
            client.send(JSON.stringify({
                id: 1,
                type: message_types_1.default.GQL_CONNECTION_INIT,
            }));
        };
    });
    it('sends legacy keep alive signal in the socket', function (done) {
        var client = new WebSocket("ws://localhost:" + KEEP_ALIVE_TEST_PORT + "/", protocol_1.GRAPHQL_SUBSCRIPTIONS);
        var yieldCount = 0;
        client.onmessage = function (message) {
            var parsedMessage = JSON.parse(message.data);
            if (parsedMessage.type === message_types_1.default.KEEP_ALIVE) {
                yieldCount += 1;
                if (yieldCount > 2) {
                    client.close();
                    done();
                }
            }
        };
        client.onopen = function () {
            client.send(JSON.stringify({
                id: 1,
                type: message_types_1.default.INIT,
            }));
        };
    });
});
describe('Message Types', function () {
    it('should throw an error if static class is instantiated', function (done) {
        chai_1.expect(function () {
            new message_types_1.default();
        }).to.throw('Static Class');
        done();
    });
});
describe('Client<->Server Flow', function () {
    var server;
    afterEach(function () {
        if (server) {
            server.close();
        }
    });
    it('should reconnect after inactivityTimeout closing the connection and then resubscribing', function (done) {
        server = http_1.createServer(notFoundRequestListener);
        server.listen(SERVER_EXECUTOR_TESTS_PORT);
        server_1.SubscriptionServer.create({
            schema: subscriptionsSchema,
            execute: graphql_1.execute,
            subscribe: graphql_1.subscribe,
        }, {
            server: server,
            path: '/',
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + SERVER_EXECUTOR_TESTS_PORT + "/", {
            inactivityTimeout: 100,
        });
        var isFirstTime = true;
        client.onConnected(function () { return __awaiter(void 0, void 0, void 0, function () {
            return __generator(this, function (_a) {
                if (isFirstTime) {
                    isFirstTime = false;
                    setTimeout(function () {
                        var sub1 = client.request({
                            query: "query { testString }",
                            variables: {},
                        }).subscribe({});
                        setTimeout(function () {
                            sub1.unsubscribe();
                            setTimeout(function () {
                                var sub2 = client.request({
                                    query: "query { testString }",
                                    variables: {},
                                }).subscribe({
                                    next: function (res) {
                                        chai_1.expect(sub2).not.to.eq(null);
                                        chai_1.expect(res.errors).to.equals(undefined);
                                        chai_1.expect(res.data.testString).to.eq('value');
                                        sub2.unsubscribe();
                                        done();
                                    },
                                });
                            }, 200);
                        }, 50);
                    }, 50);
                }
                return [2];
            });
        }); });
    });
    it('should reconnect after manually closing the connection and then resubscribing', function (done) {
        server = http_1.createServer(notFoundRequestListener);
        server.listen(SERVER_EXECUTOR_TESTS_PORT);
        server_1.SubscriptionServer.create({
            schema: subscriptionsSchema,
            execute: graphql_1.execute,
            subscribe: graphql_1.subscribe,
        }, {
            server: server,
            path: '/',
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + SERVER_EXECUTOR_TESTS_PORT + "/");
        var isFirstTime = true;
        client.onConnected(function () { return __awaiter(void 0, void 0, void 0, function () {
            return __generator(this, function (_a) {
                if (isFirstTime) {
                    isFirstTime = false;
                    setTimeout(function () {
                        client.close(false);
                        var sub = client.request({
                            query: "query { testString }",
                            variables: {},
                        }).subscribe({
                            next: function (res) {
                                chai_1.expect(sub).not.to.eq(null);
                                chai_1.expect(res.errors).to.equals(undefined);
                                chai_1.expect(res.data.testString).to.eq('value');
                                sub.unsubscribe();
                                done();
                            },
                        });
                    }, 300);
                }
                return [2];
            });
        }); });
    });
    it('validate requests against schema', function (done) {
        server = http_1.createServer(notFoundRequestListener);
        server.listen(SERVER_EXECUTOR_TESTS_PORT);
        server_1.SubscriptionServer.create({
            schema: subscriptionsSchema,
            execute: graphql_1.execute,
            subscribe: graphql_1.subscribe,
            validationRules: graphql_1.specifiedRules,
        }, {
            server: server,
            path: '/',
        });
        var client = new client_1.SubscriptionClient("ws://localhost:" + SERVER_EXECUTOR_TESTS_PORT + "/");
        var isFirstTime = true;
        client.onConnected(function () { return __awaiter(void 0, void 0, void 0, function () {
            return __generator(this, function (_a) {
                if (isFirstTime) {
                    isFirstTime = false;
                    setTimeout(function () {
                        client.close(false);
                        var sub = client.request({
                            query: "query { invalid }",
                            variables: {},
                        }).subscribe({
                            next: function (res) {
                                chai_1.expect(sub).not.to.eq(null);
                                chai_1.expect(res.data).to.eq(undefined);
                                chai_1.expect(res.errors[0].message).to.eq('Cannot query field "invalid" on type "Query".');
                                sub.unsubscribe();
                                done();
                            },
                        });
                    }, 300);
                }
                return [2];
            });
        }); });
    });
    it('should close iteration over AsyncIterator when client unsubscribes', function () { return __awaiter(void 0, void 0, void 0, function () {
        var createClientAndSubscribe, client1, client2;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    subscriptionAsyncIteratorSpy.resetHistory();
                    resolveAsyncIteratorSpy.resetHistory();
                    server = http_1.createServer(notFoundRequestListener);
                    server.listen(SERVER_EXECUTOR_TESTS_PORT);
                    server_1.SubscriptionServer.create({
                        schema: subscriptionsSchema,
                        execute: graphql_1.execute,
                        subscribe: graphql_1.subscribe,
                    }, {
                        server: server,
                        path: '/',
                    });
                    createClientAndSubscribe = function () {
                        var client = new client_1.SubscriptionClient("ws://localhost:" + SERVER_EXECUTOR_TESTS_PORT + "/");
                        var sub = null;
                        var cbSpy = sinon.spy();
                        client.onConnected(function () {
                            sub = client.request({
                                query: "subscription { somethingChanged }",
                                variables: {},
                            }).subscribe({
                                next: function (res) {
                                    cbSpy(null, res);
                                },
                                error: function (err) {
                                    cbSpy(err, null);
                                },
                                complete: function () {
                                    cbSpy(null, null);
                                },
                            });
                        });
                        return new Promise(function (resolve) {
                            setTimeout(function () {
                                resolve({
                                    unsubscribe: function () { return sub && sub.unsubscribe(); },
                                    spy: cbSpy,
                                });
                            }, 300);
                        });
                    };
                    return [4, createClientAndSubscribe()];
                case 1:
                    client1 = _a.sent();
                    return [4, createClientAndSubscribe()];
                case 2:
                    client2 = _a.sent();
                    subscriptionsPubSub.publish(TEST_PUBLICATION, { somethingChanged: 'test-payload' });
                    return [4, wait(400)];
                case 3:
                    _a.sent();
                    chai_1.expect(client1.spy.callCount).to.eq(1);
                    chai_1.expect(client2.spy.callCount).to.eq(1);
                    chai_1.expect(subscriptionAsyncIteratorSpy.callCount).to.eq(2);
                    chai_1.expect(resolveAsyncIteratorSpy.callCount).to.eq(2);
                    subscriptionAsyncIteratorSpy.resetHistory();
                    resolveAsyncIteratorSpy.resetHistory();
                    client1.spy.resetHistory();
                    client2.spy.resetHistory();
                    client1.unsubscribe();
                    return [4, wait(300)];
                case 4:
                    _a.sent();
                    subscriptionsPubSub.publish(TEST_PUBLICATION, { somethingChanged: 'test-payload-2' });
                    return [4, wait(400)];
                case 5:
                    _a.sent();
                    chai_1.expect(client1.spy.callCount).to.eq(0);
                    chai_1.expect(client2.spy.callCount).to.eq(1);
                    chai_1.expect(resolveAsyncIteratorSpy.callCount).to.eq(1);
                    chai_1.expect(subscriptionAsyncIteratorSpy.callCount).to.eq(0);
                    client2.unsubscribe();
                    return [2];
            }
        });
    }); });
    it('should close iteration over AsyncIterator when client disconnects', function () { return __awaiter(void 0, void 0, void 0, function () {
        var createClientAndSubscribe, client1, client2;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    resolveAsyncIteratorSpy.resetHistory();
                    server = http_1.createServer(notFoundRequestListener);
                    server.listen(SERVER_EXECUTOR_TESTS_PORT);
                    server_1.SubscriptionServer.create({
                        schema: subscriptionsSchema,
                        execute: graphql_1.execute,
                        subscribe: graphql_1.subscribe,
                    }, {
                        server: server,
                        path: '/',
                    });
                    createClientAndSubscribe = function () {
                        var client = new client_1.SubscriptionClient("ws://localhost:" + SERVER_EXECUTOR_TESTS_PORT + "/");
                        var cbSpy = sinon.spy();
                        client.onConnected(function () {
                            client.request({
                                query: "subscription { somethingChanged }",
                                variables: {},
                            }).subscribe({
                                next: function (res) {
                                    cbSpy(null, res);
                                },
                                error: function (err) {
                                    cbSpy(err, null);
                                },
                                complete: function () {
                                    cbSpy(null, null);
                                },
                            });
                        });
                        return new Promise(function (resolve) {
                            setTimeout(function () {
                                resolve({
                                    close: function () { return client.close(); },
                                    spy: cbSpy,
                                });
                            }, 300);
                        });
                    };
                    return [4, createClientAndSubscribe()];
                case 1:
                    client1 = _a.sent();
                    return [4, createClientAndSubscribe()];
                case 2:
                    client2 = _a.sent();
                    subscriptionsPubSub.publish(TEST_PUBLICATION, { somethingChanged: 'test-payload' });
                    return [4, wait(400)];
                case 3:
                    _a.sent();
                    chai_1.expect(client1.spy.callCount).to.eq(1);
                    chai_1.expect(client2.spy.callCount).to.eq(1);
                    chai_1.expect(resolveAsyncIteratorSpy.callCount).to.eq(2);
                    resolveAsyncIteratorSpy.resetHistory();
                    client1.spy.resetHistory();
                    client2.spy.resetHistory();
                    client1.close();
                    return [4, wait(300)];
                case 4:
                    _a.sent();
                    subscriptionsPubSub.publish(TEST_PUBLICATION, { somethingChanged: 'test-payload-2' });
                    return [4, wait(400)];
                case 5:
                    _a.sent();
                    chai_1.expect(client1.spy.callCount).to.eq(0);
                    chai_1.expect(client2.spy.callCount).to.eq(1);
                    chai_1.expect(resolveAsyncIteratorSpy.callCount).to.eq(1);
                    return [2];
            }
        });
    }); });
    it('should handle correctly multiple subscriptions one after each other', function (done) {
        server = http_1.createServer(notFoundRequestListener);
        server.listen(SERVER_EXECUTOR_TESTS_PORT);
        server_1.SubscriptionServer.create({
            schema: schema,
            execute: graphql_1.execute,
        }, {
            server: server,
            path: '/',
        });
        var firstSubscriptionSpy = sinon.spy();
        var client = new client_1.SubscriptionClient("ws://localhost:" + SERVER_EXECUTOR_TESTS_PORT + "/");
        client.onConnected(function () {
            client.request({
                query: "query { testString }",
                variables: {},
            }).subscribe({
                next: function (res) {
                    chai_1.assert(res.errors === undefined, 'unexpected error from query');
                    chai_1.expect(res.data).to.deep.equal({ testString: 'value' });
                    var firstSub = client.request({
                        query: "subscription {\n              user(id: \"3\") {\n                id\n                name\n              }\n            }",
                    }).subscribe({
                        next: function (sRes) {
                            chai_1.assert(sRes.errors === undefined, 'unexpected error from 1st subscription');
                            chai_1.assert(sRes.data, 'unexpected null from 1st subscription result');
                            chai_1.expect(Object.keys(client['operations']).length).to.eq(1);
                            chai_1.expect(sRes.data.user.id).to.eq('3');
                            firstSubscriptionSpy();
                            firstSub.unsubscribe();
                            setTimeout(function () {
                                client.request({
                                    query: "subscription {\n                    user(id: \"1\") {\n                      id\n                      name\n                    }\n                  }",
                                }).subscribe({
                                    next: function (s2Res) {
                                        chai_1.assert(s2Res.errors === undefined, 'unexpected error from 2nd subscription');
                                        chai_1.assert(s2Res.data !== null, 'unexpected null from 2nd subscription result');
                                        chai_1.expect(s2Res.data.user.id).to.eq('1');
                                        chai_1.expect(Object.keys(client['operations']).length).to.eq(1);
                                        chai_1.expect(firstSubscriptionSpy.callCount).to.eq(1);
                                        done();
                                    },
                                });
                            }, 10);
                        },
                    });
                },
            });
        });
    });
    it('works with custom WebSocket implementation', function (done) {
        var MockServer = require('mock-socket-with-protocol').Server;
        var MockWebSocket = require('mock-socket-with-protocol').WebSocket;
        var CUSTOM_PORT = 234235;
        var customServer = new MockServer("ws://localhost:" + CUSTOM_PORT);
        server_1.SubscriptionServer.create({
            schema: schema,
            execute: graphql_1.execute,
            subscribe: graphql_1.subscribe,
        }, customServer);
        var client = new client_1.SubscriptionClient("ws://localhost:" + CUSTOM_PORT, {}, MockWebSocket);
        var numTriggers = 0;
        client.request({
            query: "\n            subscription userInfoFilter1($id: String) {\n              userFiltered(id: $id) {\n                id\n                name\n              }\n            }",
            operationName: 'userInfoFilter1',
            variables: {
                id: '3',
            },
        }).subscribe({
            next: function (result) {
                if (result.errors) {
                    chai_1.assert(false);
                }
                if (result.data) {
                    numTriggers += 1;
                    chai_1.assert.property(result.data, 'userFiltered');
                    chai_1.assert.equal(result.data.userFiltered.id, '3');
                    chai_1.assert.equal(result.data.userFiltered.name, 'Jessie');
                }
            },
        });
        setTimeout(function () {
            testPubsub.publish('userFiltered', { id: 1 });
            testPubsub.publish('userFiltered', { id: 2 });
            testPubsub.publish('userFiltered', { id: 3 });
        }, 50);
        setTimeout(function () {
            chai_1.expect(numTriggers).equal(1);
            done();
        }, 200);
    });
});
//# sourceMappingURL=tests.js.map