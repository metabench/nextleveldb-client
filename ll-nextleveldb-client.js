/**
 * Created by James on 15/10/2016.
 */

var http = require('http');
var url = require('url');
var WebSocketClient = require('websocket').client;
var jsgui = require('lang-mini');
var Evented_Class = jsgui.Evented_Class;
var Fns = jsgui.Fns;
var is_array = jsgui.is_array;
var each = jsgui.each;
var xas2;
//var encodings = require('../nextleveldb-server/encodings/encodings');
var x = xas2 = require('xas2');

var Binary_Encoding = require('binary-encoding');
var Binary_Encoding_Record = Binary_Encoding.Record;

//console.log('encodings.poloniex.market', encodings.poloniex.market);

var Model = require('nextleveldb-model');
var Paging = Model.Paging;
var fs = require('fs');
var request = require('request');
var protocol = 'http://';

const LL_COUNT_RECORDS = 0;
const LL_PUT_RECORDS = 1;

// USING PAGING OPTION
const LL_GET_ALL_KEYS = 2;
const LL_GET_KEYS_IN_RANGE = 3;

const LL_GET_RECORDS_IN_RANGE = 4;
const LL_COUNT_KEYS_IN_RANGE = 5;
const LL_GET_FIRST_LAST_KEYS_IN_RANGE = 6;
const LL_GET_RECORD = 7;
//const LL_COUNT_GET_FIRST_LAST_KEYS_IN_RANGE = 7;
const LL_COUNT_KEYS_IN_RANGE_UP_TO = 8;
const LL_GET_RECORDS_IN_RANGE_UP_TO = 9;
const INSERT_TABLE_RECORD = 12;
const INSERT_RECORDS = 13;
const LL_WIPE = 20;
const LL_WIPE_REPLACE = 21;
const LL_SUBSCRIBE_ALL = 60;
const LL_SUBSCRIBE_KEY_PREFIX_PUTS = 61;
const LL_UNSUBSCRIBE_SUBSCRIPTION = 62;

// LL_SUBSCRIBE_ALL will get callback with encoded data.
//  Non-LL versions would decode that data.
//  Then, alongside data loading, we should be able to populate indexed time value data structres.
//   Then should be able to do pertinent calculations quickly.

// Time offset indexes would help with this, making the data more compact.
//  Or each record is the change from the previous one, then gets decoded.

// Will have more data types with specific compressions.


// -~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~- \\

const XAS2 = 0;
const DOUBLEBE = 1;
const DATE = 2;

const STRING = 4;
const BOOL_FALSE = 6;
const BOOL_TRUE = 7;

const NULL = 8;
const BUFFER = 9;

// Specifically want to encode array as well.

const ARRAY = 10;

// -~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~- \\

/**
 * 
 * 
 * @class LL_NextLevelDB_Client
 * @extends {Evented_Class}
 */
class LL_NextLevelDB_Client extends Evented_Class {
    //'fields': [
    //	['url', String],
    //	['port', Number]
    //],

    /**
     * 
     * 
     * @param {object} spec 
     * @memberof LL_NextLevelDB_Client
     */
    'constructor' (spec) {
        //console.log('LL_NextLevelDB_Client spec', spec);

        super();

        this.server_url = spec.server_url;
        this.server_address = spec.server_address;
        this.server_port = spec.server_port;

    }

    // Want a way to stop / disconnect the client.
    //  Would be useful to help the program end / release resources.

    /**
     * 
     * 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    'stop' (callback) {
        this.auto_reconnect = false;
        this.websocket_connection.close();
        if (callback) callback(null, true);
    }

    /**
     * 
     * 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    'start' (callback) {
        // Maybe connect here?
        // Better to connect to the socket server

        console.log('NextLevelDB_Client start');
        var that = this;

        this.id_ws_req = 0;
        var ws_response_handlers = this.ws_response_handlers = {};
        var client = this.websocket_client = new WebSocketClient({
            maxReceivedFrameSize: 512000000,
            maxReceivedMessageSize: 512000000,
            fragmentOutgoingMessages: false,
            //assembleFragments: false,
            closeTimeout: 10000
        });

        client.on('connectFailed', function (error) {
            // socket could be closed.
            if (error) {
                console.log('connectFailed error', error);
                //console.log('Object.keys(error)', Object.keys(error)); // [ 'code', 'errno', 'syscall', 'address', 'port' ]
                callback(error);
            }
        });

        that.connected = false;
        var attempting_reconnection = false;

        var reconnection_attempts = function () {
            first_connect = false;
            if (that.connected === false && attempting_reconnection === false) {
                attempting_reconnection = true;
                client.connect(ws_address, 'echo-protocol');
                setTimeout(function () {
                    attempting_reconnection = false;

                    reconnection_attempts();

                }, 1000);
            }
        };

        var first_connect = true;

        //console.log('pre connect');
        client.on('connect', function (connection) {
            that.websocket_connection = connection;
            that.auto_reconnect = true;
            that.connected = true;

            if (!first_connect) {
                //console.log('pre fns all go');
                that.fns_on_reconnect.go((err, res_all) => {
                    // Raising the final callback too many times? Currently being called after each operation.
                    //console.log('res_all', res_all);
                });
            }
            attempting_reconnection = false;
            console.log('WebSocket Client Connected');
            // if its a reconnection, don't need to assign these things.

            var assign_connection_events = function () {
                connection.on('error', function (error) {
                    console.log("Connection Error: " + error.toString());
                    console.log('error', error);
                    //console.log('typeof error', typeof error);
                    console.trace();
                    var str_err = error.toString();

                    if (str_err === 'Error: This socket is closed') {
                        // disconnect event.
                        that.connected = false;
                        attempting_reconnection = false;
                    }
                });
                connection.on('connectFailed', function (error) {
                    console.log('connection failed, err', err);

                    // Probably in response to attempting to write to a closed stream?

                    //console.log('\nerror', error);
                    //console.log('Object.keys(error)', Object.keys(error));
                    //console.log("Connection Error: " + error.toString());
                    //console.log('typeof error', typeof error);

                    //attempting_reconnection = false;
                    //reconnection_attempts();
                });
                connection.on('close', function () {
                    console.log('echo-protocol Connection Closed');
                    // At this point its worth noticing the connection has been closed.
                    // When the connection is closed, don't try to send.
                    that.connected = false;
                    attempting_reconnection = false;
                    first_connect = false;
                    // attempt reconnections...
                    //fns_on_reconnect = Fns();
                    that.fns_on_reconnect = Fns();
                    if (that.auto_reconnect) {
                        reconnection_attempts();
                    }


                });
                connection.on('message', function (message) {
                    //console.log('message', message);
                    if (message.type === 'utf8') {
                        var obj_message = JSON.parse(message.utf8Data);

                        if (is_array(obj_message)) {
                            var request_key = obj_message[0];
                            var res;

                            if (obj_message.length === 2) {
                                res = obj_message[1];
                            } else {
                                res = obj_message.slice(1);
                            }
                            if (ws_response_handlers[request_key]) {
                                ws_response_handlers[request_key](res);
                            }
                        }

                        if (obj_message.type === 'response') {
                            var request_key = obj_message.request_key;
                            if (ws_response_handlers[request_key]) {
                                ws_response_handlers[request_key](obj_message);
                            }
                        }
                    }
                    // binary messages.

                    if (message.type === 'binary') {
                        that.receive_binary_message(message.binaryData);
                    }
                });
            };

            assign_connection_events();

            if (first_connect) {
                callback(null, true);
            };
        });
        // need the url without the protocol.

        var ws_address = this.server_url || 'ws://' + this.server_address + ':' + this.server_port + '/';
        client.connect(ws_address, 'echo-protocol');
    }

    /**
     * 
     * 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    wipe(callback) {
        var buf_command = xas2(LL_WIPE).buffer;
        this.send_binary_message(buf_command, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                callback(null, true);

            }
        });
    }

    /**
     * 
     * 
     * @param {buffer} buf_replacement 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    wipe_replace(buf_replacement, callback) {
        //var buf_command = xas2(LL_WIPE_REPLACE).buffer;
        var buf_command = Buffer.concat([xas2(LL_WIPE_REPLACE).buffer, buf_replacement]);
        this.send_binary_message(buf_command, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                callback(null, true);
            }
        });
    }

    /**
     * 
     * 
     * @param {buffer} buf_message 
     * @memberof LL_NextLevelDB_Client
     */
    receive_binary_message(buf_message) {
        // decode the first byte...
        var message_id, pos = 0;
        //console.log('buf_message', buf_message);
        [message_id, pos] = xas2.read(buf_message, pos);
        //console.log('message_id', message_id);

        // then the rest of the message
        var buf_the_rest = Buffer.alloc(buf_message.length - pos);
        buf_message.copy(buf_the_rest, 0, pos);
        this.ws_response_handlers[message_id](buf_the_rest);
    }
    // with callback!

    /**
     * 
     * 
     * @param {buffer} message 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    send_binary_message(message, callback) {
        // no callback on this
        // Better to stream this message to the server.
        //  Probably best to always use the streaming connection.
        var idx = this.id_ws_req++,
            ws_response_handlers = this.ws_response_handlers;
        var buf_2 = Buffer.concat([xas2(idx).buffer, message]);

        ws_response_handlers[idx] = function (obj_message) {
            callback(null, obj_message);
            ws_response_handlers[idx] = null;
        };
        this.websocket_connection.sendBytes(buf_2);
    }

    // Multi callback messages / subscriptions
    //  Keeps the response handler until its closed / unsubscribed.

    send_binary_subscription_message(message, subscription_event_callback) {
        var idx = this.id_ws_req++,
            ws_response_handlers = this.ws_response_handlers;

        var buf_2 = Buffer.concat([xas2(idx).buffer, message]);
        var that = this;

        ws_response_handlers[idx] = function (obj_message) {
            subscription_event_callback(obj_message);
        };

        // Should be able to unsubscribe

        var unsubscribe = () => {
            // Send unsubscribe with the idx to the server.
            // LL_UNSUBSCRIBE_SUBSCRIPTION
            var buf_query = Buffer.concat([xas2(LL_UNSUBSCRIBE_SUBSCRIPTION).buffer]);
            // Only needs to give the subscription index. That is unique per client.
            var buf_2 = Buffer.concat([xas2(idx).buffer, buf_query]);
            that.websocket_connection.sendBytes(buf_2);

        }
        //console.log('pre send', buf_2);
        this.websocket_connection.sendBytes(buf_2);

        return unsubscribe;

    }

    //ll_get_table_records_by_kp(table_key_prefix, callback) {

    //}

    /**
     * 
     * 
     * @param {int >= 0} key_prefix 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    ll_get_records_by_key_prefix(key_prefix, callback) {
        var buf_kp = xas2(key_prefix).buffer;
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_kp, buf_0]);
        var buf_u = Buffer.concat([buf_kp, buf_1]);

        this.ll_get_records_in_range(buf_l, buf_u, callback);
    }

    ll_get_records_by_key_prefix_up_to(key_prefix, limit, callback) {
        var buf_kp = xas2(key_prefix).buffer;
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_kp, buf_0]);
        var buf_u = Buffer.concat([buf_kp, buf_1]);

        this.ll_get_records_in_range_up_to(buf_l, buf_u, limit, callback);
    }

    /**
     * 
     * 
     * @param {buffer} buf_beginning 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    ll_get_keys_beginning(buf_beginning, callback) {
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_beginning, buf_0]);
        var buf_u = Buffer.concat([buf_beginning, buf_1]);

        this.ll_get_keys_in_range(buf_l, buf_u, callback);
    }

    /**
     * 
     * 
     * @param {int >= 0} key_prefix 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    ll_get_keys_by_key_prefix(key_prefix, callback) {
        var buf_kp = xas2(key_prefix).buffer;
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_kp, buf_0]);
        var buf_u = Buffer.concat([buf_kp, buf_1]);

        this.ll_get_keys_in_range(buf_l, buf_u, callback);
    }

    // get keys in range
    //  higher level version would decode the results using Model_DB.deocde_keys
    //  higher level version could encode the key buffers itself, as well as take key prefix?

    /**
     * 
     * 
     * @param {buffer} buf_l 
     * @param {buffer} buf_u 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    ll_get_keys_in_range(buf_l, buf_u, callback) {
        var paging = new Paging.None();
        var buf_command = xas2(LL_GET_KEYS_IN_RANGE).buffer;
        var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);
        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                var arr_key_buffers = Binary_Encoding.split_length_item_encoded_buffer(res_binary_message);
                callback(null, arr_key_buffers);
            }
        });
    }


    ll_get_buf_records_in_range(buf_l, buf_u, callback) {
        var paging = new Paging.None();
        var buf_command = xas2(LL_GET_RECORDS_IN_RANGE).buffer;
        // the lengths of the buffers too...
        var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);
        //var buf_l = 
        // Include a paging buffer too...?
        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                callback(null, res_binary_message);
            }
        });
    }

    // Think we will need to get the paging right in order to retrieve the large numbers of trades.
    //  Incremental loading would be better to see in a browser too.

    /**
     * 
     * 
     * @param {buffer} buf_l 
     * @param {buffer} buf_u 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */

    ll_get_records_keys_beginning(buf_keys_beginning, callback) {
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);

        var buf_l = Buffer.concat([buf_keys_beginning, buf_0]);
        var buf_u = Buffer.concat([buf_keys_beginning, buf_1]);
        this.ll_get_records_in_range(buf_l, buf_u, callback);
    }


    ll_get_records_in_range(buf_l, buf_u, callback) {
        var paging = new Paging.None();
        var buf_command = xas2(LL_GET_RECORDS_IN_RANGE).buffer;
        // the lengths of the buffers too...
        var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);
        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                var arr_kv_buffers = Binary_Encoding.split_length_item_encoded_buffer_to_kv(res_binary_message);
                callback(null, arr_kv_buffers);
            }
        });
    }


    ll_get_records_in_range_up_to(buf_l, buf_u, limit, callback) {
        // table prefix number, then the rest of the pk
        // need to know the table key prefixes.
        // LL_GET_RECORDS_IN_RANGE

        // no paging right now.

        var paging = new Paging.None();
        var buf_command = xas2(LL_GET_RECORDS_IN_RANGE_UP_TO).buffer;
        // the lengths of the buffers too...
        var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(limit).buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);
        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                var arr_kv_buffers = Binary_Encoding.split_length_item_encoded_buffer_to_kv(res_binary_message);
                callback(null, arr_kv_buffers);
            }
        });

    }

    // ll_query = send_binary_message?

    /**
     * 
     * 
     * @param {buffer} buf_l 
     * @param {buffer} buf_u 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    ll_get_first_last_keys_in_range(buf_l, buf_u, callback) {
        var paging = new Paging.None();
        var buf_command = xas2(LL_GET_FIRST_LAST_KEYS_IN_RANGE).buffer;
        // the lengths of the buffers too...
        var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);
        //var buf_l = 
        // Include a paging buffer too...?
        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                var arr_buffers = Binary_Encoding.split_length_item_encoded_buffer(res_binary_message);
                callback(null, arr_buffers);
            }
        });
    }

    /**
     * 
     * 
     * @param {buffer} buf_beginning 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    ll_get_first_last_keys_beginning(buf_beginning, callback) {
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_beginning, buf_0]);
        var buf_u = Buffer.concat([buf_beginning, buf_1]);
        this.ll_get_first_last_keys_in_range(buf_l, buf_u, callback);
    }

    // // ll_get_first_last_keys_beginning
    //  ll_get_first_last_keys_in_range

    // LL_GET_FIRST_LAST_KEYS_IN_RANGE

    /**
     * 
     * 
     * @param {buffer} buf_beginning 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    ll_count_keys_beginning(buf_beginning, callback) {
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);

        var buf_l = Buffer.concat([buf_beginning, buf_0]);
        var buf_u = Buffer.concat([buf_beginning, buf_1]);

        this.ll_count_keys_in_range(buf_l, buf_u, callback);
    }

    ll_count_keys_beginning_up_to(buf_beginning, limit, callback) {
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_beginning, buf_0]);
        var buf_u = Buffer.concat([buf_beginning, buf_1]);

        this.ll_count_keys_in_range_up_to(buf_l, buf_u, limit, callback);
    }

    // keys beginning up to

    // Need to count the keys for a range of table keys
    //  Need to adapt this to deal with table prefixes.

    /**
     * 
     * 
     * @param {buffer} buf_l 
     * @param {buffer} buf_u 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    ll_count_keys_in_range(buf_l, buf_u, callback) {
        var paging = new Paging.None();
        var buf_command = xas2(LL_COUNT_KEYS_IN_RANGE).buffer;
        // the lengths of the buffers too...
        var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);
        //var buf_l = 
        // Include a paging buffer too...?

        //console.log('buf_query', buf_query);
        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                var count, pos;
                [count, pos] = xas2.read(res_binary_message, 0);
                callback(null, count);
            }
        });
    }

    // LL_COUNT_KEYS_IN_RANGE_UP_TO
    ll_count_keys_in_range_up_to(buf_l, buf_u, limit, callback) {
        var paging = new Paging.None();
        var buf_command = xas2(LL_COUNT_KEYS_IN_RANGE_UP_TO).buffer;
        // the lengths of the buffers too...
        var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(limit).buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);
        //var buf_l = 
        // Include a paging buffer too...?

        //console.log('buf_query', buf_query);
        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                var count, pos;
                [count, pos] = xas2.read(res_binary_message, 0);
                callback(null, count);
            }
        });
    }

    ll_get_record(buf_key, callback) {
        //console.log('ll_get_record');
        let buf_query = Buffer.concat([xas2(LL_GET_RECORD).buffer, buf_key]);
        //console.log('buf_query', buf_query);
        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {

                callback(null, res_binary_message);
            }
        });
    }

    /**
     * 
     * 
     * @param {any} paging 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    'll_get_all_keys' (paging, callback) {
        var buf_query, pos = 0;
        if (!callback) {
            callback = arguments[0];
            paging = new Paging.None();
        }
        if (!paging instanceof Paging) paging = new Paging.Record_Paging(paging);
        buf_query = Buffer.concat([xas2(LL_GET_ALL_KEYS).buffer, paging.buffer]);

        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                callback(null, res_binary_message);
            }
        });
        //this._json_get_request('query/all_keys', callback);
    }

    /**
     * 
     * 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    'll_count_records' (callback) {
        var buf_query = Buffer.concat([xas2(LL_COUNT_RECORDS).buffer]);
        // set up the response handler...
        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                var count, pos = 0;
                [count, pos] = x.read(res_binary_message, pos);
                callback(null, count);
            }
        });
    }

    /**
     * 
     * 
     * @param {buffer} buf_records 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    'll_put_records_buffer' (buf_records, callback) {
        // PUT_RECORDS
        var buf_query = Buffer.concat([xas2(LL_PUT_RECORDS).buffer, buf_records]);
        //console.log('buf_query', buf_query);

        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                callback(null, true);
                // Don't know if it would be useful to get back the ids.
            }
        });
    }

    // put record
    //  would need to encode the record.

    'll_subscribe_all' (subscription_event_callback) {
        var buf_query = Buffer.concat([xas2(LL_SUBSCRIBE_ALL).buffer]);
        var unsubscribe = this.send_binary_subscription_message(buf_query, (sub_event) => {
            console.log('sub_event', sub_event);
            subscription_event_callback(sub_event);
        });
        return unsubscribe;
    }

    'll_subscribe_key_prefix_puts' (buf_kp, subscription_event_callback) {
        console.log('buf_kp', buf_kp);
        console.log('1) buf_kp hex', buf_kp.toString('hex'));
        var buf_query = Buffer.concat([xas2(LL_SUBSCRIBE_KEY_PREFIX_PUTS).buffer, buf_kp]);


        var unsubscribe = this.send_binary_subscription_message(buf_query, (sub_event) => {
            console.log('sub_event', sub_event);

            // still a low level function.
            //  just deals with the binary data.

            // And a higher level wrapper would process / decode these subscription events.
            subscription_event_callback(sub_event);

        });
        return unsubscribe;

    }

    // A paging definition object may help.
    //  Could help on the server side, reading the paging definition from the buffer
}


// Will get rid of these streaming followthrough functions.
//  It's standard for some things, and there will be a Query object with paging options too.

// need different create_ws_followthrough functions for each prototype.

var local_info = {
    'server_address': 'localhost',
    //'server_address': 'localhost',
    //'db_path': 'localhost',
    'server_port': 420
}


if (require.main === module) {
    var lc = new NextLevelDB_Client(local_info);

    // Looks like the level client keeps itself open.
    //  console.log('pre start');

    lc.start((err, res_start) => {
        if (err) {
            throw err;
        } else {
            console.log('res_start', res_start);
            var test_get_all_records = function () {
                // And different tests for using paging.

            };

            var test_get_all_records_100_per_page = function () {
                // And different tests for using paging.

            };

            var test_get_all_keys = function () {
                // And different tests for using paging.
                lc.ll_get_all_keys((err, buf_all_keys) => {
                    // Should get them as an array.


                    console.log('buf_all_keys', buf_all_keys);
                });

            };
            //test_get_all_keys();

            var test_save_all_records = function () {
                fs.open('dbsave.nl', 'w', (err, write_file_descriptor) => {
                    if (err) {
                        throw err;
                    } else {
                        lc.ws_streaming_get_all_records((err, res_get_all_records) => {
                            if (err) {
                                throw err;
                            } else {

                                var page_num = res_get_all_records[0];
                                var page_arr_records = res_get_all_records[1];

                                console.log('page_num', page_num);
                                console.log('page_arr_records.length', page_arr_records.length);

                                // try writing it as JSON

                                each(page_arr_records, (record) => {
                                    fs.write(write_file_descriptor, JSON.stringify(record));
                                });;

                            }
                        });
                    }
                });
            };

            var test_subscribe_put = () => {
                // Subscriptions don't get error events back?
                //  Is that the difference?
                //  Maybe don't do it that way for the moment.

                lc.ws_subscribe('put', (pointless_error, e_put) => {

                    console.log('e_put', e_put);
                });
            };
            //test_subscribe_put();
        }
    });
    //console.log('pre get all');

    var all_data = [];

} else {
    //console.log('required as a module');
}


module.exports = LL_NextLevelDB_Client;