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

const Model = require("nextleveldb-model");
const Model_Database = Model.Database;

//console.log('encodings.poloniex.market', encodings.poloniex.market);
var Paging = Model.Paging;
var fs = require('fs');
var request = require('request');
var protocol = 'http://';

const LL_COUNT_RECORDS = 0;
const LL_PUT_RECORDS = 1;
const LL_GET_ALL_KEYS = 2;
const LL_GET_ALL_RECORDS = 3;
const LL_GET_KEYS_IN_RANGE = 4;
const LL_GET_RECORDS_IN_RANGE = 5;
const LL_COUNT_KEYS_IN_RANGE = 6;
const LL_GET_FIRST_LAST_KEYS_IN_RANGE = 7;
const LL_GET_RECORD = 8;
const LL_COUNT_KEYS_IN_RANGE_UP_TO = 9;
const LL_GET_RECORDS_IN_RANGE_UP_TO = 10;
const LL_FIND_COUNT_TABLE_RECORDS_INDEX_MATCH = 11;
const INSERT_TABLE_RECORD = 12;
const INSERT_RECORDS = 13;
const ENSURE_TABLE = 20;
const LL_SUBSCRIBE_ALL = 60;
const LL_SUBSCRIBE_KEY_PREFIX_PUTS = 61;
const LL_UNSUBSCRIBE_SUBSCRIPTION = 62;
const LL_WIPE = 100;
const LL_WIPE_REPLACE = 101;

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

const return_message_type = true;

const BINARY_PAGING_NONE = 0;
const BINARY_PAGING_FLOW = 1;
const BINARY_PAGING_LAST = 2;

const RECORD_PAGING_NONE = 3;
const RECORD_PAGING_FLOW = 4;
const RECORD_PAGING_LAST = 5;


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

    // Could have a version that gets more data out of the response.
    //  Or will need to make this aware of paging.
    //  Changing all of the server functions to say that they are not paged would make sense.
    //  With it noting that it's a paged response, more can be automatically handled here.
    //   However, other ll functions could handle paging fine and present an observable API.

    // Including another response_type flag into the response would help.

    // 0 - NO_PAGING
    // 1 - PAGING_FLOW
    // 2 - PAGING_LAST
    // 3 - BLOCKCHAIN_PAGING_FLOW ?? 3 - BLOCKCHAIN_PAGING
    // 4 - BLOCKCHAIN_PAGING_LAST ??
    //  With BLOCKCHAIN_PAGING packets able to say they are the last in the chain.



    receive_binary_message(buf_message) {
        var message_id, pos = 0,
            message_type;
        [message_id, pos] = xas2.read(buf_message, pos);

        //console.log('message_id', message_id);

        var buf_the_rest = Buffer.alloc(buf_message.length - pos);
        buf_message.copy(buf_the_rest, 0, pos);

        if (return_message_type) {
            [message_type, pos] = xas2.read(buf_message, pos);


        }
        //console.log('message_type', message_type);


        if (return_message_type) {
            //console.log('buf_the_rest', buf_the_rest);
            if (message_type === BINARY_PAGING_NONE) {

                // Could even strip the paging / structure flag here.

                this.ws_response_handlers[message_id](buf_the_rest);
                // could remove the response handler here
                this.ws_response_handlers[message_id] = null;
            }
            if (message_type === BINARY_PAGING_FLOW) {
                this.ws_response_handlers[message_id](buf_the_rest);
                // could remove the response handler here
            }
            if (message_type === BINARY_PAGING_LAST) {
                this.ws_response_handlers[message_id](buf_the_rest);
                // could remove the response handler here
                this.ws_response_handlers[message_id] = null;
            }

            if (message_type === RECORD_PAGING_NONE) {

                // Could even strip the paging / structure flag here.

                this.ws_response_handlers[message_id](buf_the_rest);
                // could remove the response handler here
                this.ws_response_handlers[message_id] = null;
            }
            if (message_type === RECORD_PAGING_FLOW) {
                this.ws_response_handlers[message_id](buf_the_rest);
                // could remove the response handler here
            }
            if (message_type === RECORD_PAGING_LAST) {
                this.ws_response_handlers[message_id](buf_the_rest);
                // could remove the response handler here
                this.ws_response_handlers[message_id] = null;
            }
        } else {
            this.ws_response_handlers[message_id](buf_the_rest);
            this.ws_response_handlers[message_id] = null;
        }

    }

    /**
     * 
     * 
     * @param {buffer} message 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */

    // a decode option parameter would be quite useful.
    //  That may mean we no longer need the 'll' versions of functions, but can have whether or not to decode as an option.
    //  Could reduce the codebase size that way by getting rid of the structure of a normal function that calls a ll function and decodes the results.



    send_binary_message(message, message_type = BINARY_PAGING_NONE, decode = false, callback) {

        let a = arguments;

        console.log('a.length', a.length);

        if (a.length === 2) {
            callback = a[1];
            message_type = BINARY_PAGING_NONE;
            decode = false;
        }

        // no callback on this
        // Better to stream this message to the server.
        //  Probably best to always use the streaming connection.
        let pos = 0,
            buf_the_rest, response_type_code;


        // Can choose the message type here.
        //  Try using some kind of es6 optional params.




        // Have a way of choosing the message / paging type here?

        var idx = this.id_ws_req++,
            ws_response_handlers = this.ws_response_handlers;

        var buf_2 = Buffer.concat([xas2(idx).buffer, message]);

        // Could extract a paging info and message id value from the obj_message

        // Would be helpful in many cases to have a paging info byte in the response.

        console.log('idx', idx);
        //  Inefficient in some ways when we know it's not needed.
        //  Many functions can handle paging though, and I think it's quite a priority in terms of replication and having the dbs able to talk to each other.

        if (return_message_type) {
            // may be possible for this to return an observable.
            //  Sometimes it will be called with a callback, but not always.

            // need to read the message type.
            //  don't think we know it at this stage.
            //  Need to set up the return handlers so that it can 

            // But we don't know the message type exactly.
            //  It's already been encoded into the message.
            //  At least the paging option has.

            //[message_type, pos] = xas2.read(buf_message, pos);

            // The expected response type is given when the message_type is chosen upon calling the function.
            //  Still, the encoding type is given in the response.

            // Could have further code within Model that is an OO message request and response encoder and decoder.
            //  There will be a number of different options for encoding and decoding messages, and it gets a little longwinded all in liner if statement code.



            // The extra complexity here will mean that 'll' functions will be able to act as normal functions, so won't need to be called 'll', and the normal functions that 
            if (message_type === BINARY_PAGING_NONE) {

                if (decode) {
                    ws_response_handlers[idx] = function (obj_message) {
                        [response_type_code, pos] = xas2.read(obj_message, pos);
                        var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                        obj_message.copy(buf_the_rest, 0, pos);

                        // decode this buffer, it's binary encoding.

                        let decoded = Binary_Encoding.decode(buf_the_rest);
                        console.log('decoded', decoded);

                        callback(null, decoded);
                        ws_response_handlers[idx] = null;
                    };
                } else {
                    ws_response_handlers[idx] = function (obj_message) {
                        [response_type_code, pos] = xas2.read(obj_message, pos);
                        //console.log('PAGING_NONE obj_message', obj_message);

                        let buf_the_rest = Buffer.alloc(obj_message.length - pos);
                        obj_message.copy(buf_the_rest, 0, pos);

                        // Remove the paging info from it.

                        // Could look in the response to see what message we have, if it indicates paging.
                        //  However, it's a binary message

                        callback(null, buf_the_rest);
                        ws_response_handlers[idx] = null;
                    };
                }
                // could remove the response handler here
            }
            // The response handler itself could be an observable object.


            if (message_type === BINARY_PAGING_FLOW) {
                ws_response_handlers[idx] = function (obj_message, page_number) {

                    console.log('PAGING_FLOW obj_message', obj_message);

                    //let [response_type_code, pos] = xas2.read(obj_message, 0);
                    //console.log('response_type_code', response_type_code);



                    //let buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    //obj_message.copy(buf_the_rest, 0, pos);


                    // xas2 read and copy?
                    //  where we don't need the position, but just need the rest of the buffer.


                    // Could look in the response to see what message we have, if it indicates paging.
                    //  However, it's a binary message

                    callback(null, obj_message, page_number);
                    //ws_response_handlers[idx] = null;
                };
                // could remove the response handler here
            }
            if (message_type === BINARY_PAGING_LAST) {
                ws_response_handlers[idx] = function (obj_message, page_number) {

                    // Could look in the response to see what message we have, if it indicates paging.
                    //  However, it's a binary message

                    callback(null, obj_message, page_number, true);
                    ws_response_handlers[idx] = null;
                };
                // could remove the response handler here
            }

        } else {
            ws_response_handlers[idx] = function (obj_message) {

                // Could look in the response to see what message we have, if it indicates paging.
                //  However, it's a binary message




                callback(null, obj_message);
                ws_response_handlers[idx] = null;
            };
        }



        this.websocket_connection.sendBytes(buf_2);
    }

    // and have a decode option

    observe_send_binary_message(message, decode = false) {
        // Could possibly encode the message if it's not in a buffer already.

        // Could have Message objects in the model.
        //  BinaryMessage

        // Still use the WS response handler.
        //  Assume it's a paging style response.
        //  Worth having that byte in the message encoding so that we can tell if it's a paged response or not, even if assuming not.
        //   That will be the default mode.

        // Maybe this will be the ll version that does not decode the values.

        var idx = this.id_ws_req++,
            ws_response_handlers = this.ws_response_handlers,
            pos = 0,
            message_type, response_type_code, page_number;


        var buf_2 = Buffer.concat([xas2(idx).buffer, message]);

        let res = new Evented_Class();


        // [message_type, pos] = xas2.read(buf_message, pos);
        ws_response_handlers[idx] = (obj_message) => {
            pos = 0;
            // read the paging / message type option out of the message.


            //console.log('PAGING_NONE obj_message', obj_message);

            [message_type, pos] = xas2.read(obj_message, pos);

            //console.log('message_type', message_type);
            //console.trace();
            //throw 'stop';

            //console.log('pos', pos);
            var buf_the_rest = Buffer.alloc(obj_message.length - pos);
            obj_message.copy(buf_the_rest, 0, pos);

            if (decode) {

                if (message_type === BINARY_PAGING_NONE) {
                    let decoded = Binary_Encoding.decode(buf_the_rest);
                    res.raise('onNext', decoded);
                    res.raise('onComplete');
                }
                if (message_type === BINARY_PAGING_FLOW) {
                    let decoded = Binary_Encoding.decode(buf_the_rest);
                    res.raise('onNext', decoded);
                }
                if (message_type === BINARY_PAGING_LAST) {
                    let decoded = Binary_Encoding.decode(buf_the_rest);
                    res.raise('onNext', decoded);
                    res.raise('onComplete');
                }

                if (message_type === RECORD_PAGING_NONE) {
                    let arr_bufs_kv = Binary_Encoding.split_length_item_encoded_buffer_to_kv(buf_the_rest);
                    let remove_kp = true;
                    let arr_decoded = Model_Database.decode_model_row(arr_bufs_kv[0], remove_kp);

                    res.raise('onNext', arr_decoded);
                    res.raise('onComplete');
                }
                if (message_type === RECORD_PAGING_FLOW) {
                    //console.log('buf_the_rest', buf_the_rest);

                    // Need to get some specific values for the flow decoding.
                    //  

                    //[response_type_code, pos] = xas2.read(obj_message, pos);
                    //console.log('response_type_code', response_type_code);
                    //console.log('pos', pos);

                    [page_number, pos] = xas2.read(buf_the_rest, 0);
                    //console.log('page_number', page_number);

                    let buf2 = Buffer.alloc(buf_the_rest.length - pos);
                    buf_the_rest.copy(buf2, 0, pos);

                    //console.log('buf2', buf2);




                    // read and copy buffer.



                    let arr_bufs_kv = Binary_Encoding.split_length_item_encoded_buffer_to_kv(buf2);


                    let remove_kp = true;
                    //console.log('arr_bufs_kv[0]', arr_bufs_kv[0]);
                    //console.log('arr_bufs_kv', arr_bufs_kv);
                    //throw 'stop';
                    let arr_decoded = Model_Database.decode_model_rows(arr_bufs_kv, remove_kp);

                    res.raise('onNext', arr_decoded);
                }
                if (message_type === RECORD_PAGING_LAST) {
                    [page_number, pos] = xas2.read(buf_the_rest, 0);
                    console.log('page_number', page_number);
                    let buf2 = Buffer.alloc(buf_the_rest.length - pos);
                    buf_the_rest.copy(buf2, 0, pos);
                    let arr_bufs_kv = Binary_Encoding.split_length_item_encoded_buffer_to_kv(buf2);
                    let remove_kp = true;
                    console.log('arr_bufs_kv', arr_bufs_kv);
                    let arr_decoded = Model_Database.decode_model_rows(arr_bufs_kv, remove_kp);

                    res.raise('onNext', arr_decoded);
                    res.raise('onComplete');
                }
            } else {
                if (message_type === BINARY_PAGING_NONE) {
                    res.raise('onNext', buf_the_rest);
                    res.raise('onComplete');
                }
                if (message_type === BINARY_PAGING_FLOW) {
                    res.raise('onNext', buf_the_rest);
                }
                if (message_type === BINARY_PAGING_LAST) {
                    res.raise('onNext', buf_the_rest);
                    res.raise('onComplete');
                }

                if (message_type === RECORD_PAGING_NONE) {
                    res.raise('onNext', buf_the_rest);
                    res.raise('onComplete');
                }
                if (message_type === RECORD_PAGING_FLOW) {
                    res.raise('onNext', buf_the_rest);
                }
                if (message_type === RECORD_PAGING_LAST) {
                    res.raise('onNext', buf_the_rest);
                    res.raise('onComplete');
                }
            }



            // Could look in the response to see what message we have, if it indicates paging.
            //  However, it's a binary message

            //callback(null, obj_message);
            //ws_response_handlers[idx] = null;
        };

        this.websocket_connection.sendBytes(buf_2);
        return res;

    }


    // decode records?
    //  

    // There are different types of decoding at the moment.
    //  Records & index records have a more concide encoding system, without some data types such as xas2 being specified where it's known that's what they are.

    //  Could put in different decoding type options
    //   Different response types.
    //    Encoded records
    //    Binary_Encoded data

    // Whether there is paging or not, where in the paging



    observe_send_binary_message_decode(message) {
        var idx = this.id_ws_req++,
            ws_response_handlers = this.ws_response_handlers,
            pos = 0,
            message_type;
        var buf_2 = Buffer.concat([xas2(idx).buffer, message]);
        let res = new Evented_Class();
        ws_response_handlers[idx] = (obj_message) => {
            pos = 0;
            [message_type, pos] = xas2.read(obj_message, pos);
            console.log('observe_send_binary_message_decode message_type', message_type);

            var buf_the_rest = Buffer.alloc(obj_message.length - pos);
            obj_message.copy(buf_the_rest, 0, pos);

            // Then decode these buf the rest records.

            // message with paging and record encoding
            //  other encoding is 'binary'





            if (message_type === PAGING_NONE) {
                res.raise('onNext', buf_the_rest);
                res.raise('onComplete');
            }
            if (message_type === PAGING_FLOW) {
                res.raise('onNext', buf_the_rest);
            }
            if (message_type === PAGING_LAST) {
                res.raise('onNext', buf_the_rest);
                res.raise('onComplete');
            }
        };
        this.websocket_connection.sendBytes(buf_2);
        return res;
    }

    // send_binary_message_paging
    //  would know to read paging data from the message reply, to see if it's the last.

    // reading the page_number and whether it is the last item or not.
    //  A more flexible system for getting the page data?
    //   Could use different flags to indicate the page type. 0 - normal, 1 - normal last, 2 blockchain, 3 blockchain last
    //    etc. So 1 byte would allow 256 possible paging values, and we look into the response to see the paging type

    // Could put paging info into every response, with the unpaged ones always being 0


    //  0 - not paged, 1 - normal, 2 - normal last, 3 blockchain, 4 blockchain last


    // used in cases of paging.
    //  with every message response, checks to see if it's the last.


    // This will return an observable.

    // Paging will be deeper integrated into the client and server.
    //  Many messages will have paging as an option.

    // Could have a response handler that has an observable handler.

    /*
    send_binary_paged_message(message) {
        // callback for when the message has done one page, callback for the whole thing being complete.
        //  or put it into an object that just goes to the callback.

        // Returning a promise or promise chain, or multi-callback promise equivalent would be nicest.




        var idx = this.id_ws_req++,
            ws_response_handlers = this.ws_response_handlers;
        var buf_2 = Buffer.concat([xas2(idx).buffer, message]);

        // Could extract a paging info and message id value from the obj_message

        // Would be helpful in many cases to have a paging info byte in the response.
        //  Inefficient in some ways when we know it's not needed.
        //  Many functions can handle paging though, and I think it's quite a priority in terms of replication and having the dbs able to talk to each other.

        // 

        var res = new Evented_Class();

        ws_response_handlers[idx] = function (obj_message) {

            // Could look in the response to see what message we have, if it indicates paging.
            //  However, it's a binary message

            // Need to look into the obj_message.
            //  will have the page number as well as the paging type.
            //  Though could maybe 

            console.log('obj_message', obj_message);

            throw 'stop';


            //callback(null, obj_message);

            ws_response_handlers[idx] = null;
        };
        this.websocket_connection.sendBytes(buf_2);

        return res;
    }

    */


    // Or 



    // Paging could use a subscription.
    //  The subscription would be ended after the server finishes.


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

    // Could raise events on the very paging object passed in.
    //  Could return a promise chain too.

    // Generators could be useful for allowing the stream to be stopped or paused.
    //  Interrupting a streaming message will be one feature that could be done client-side through another 

    // Make this one return an observable for the moment.

    ll_get_all_records(paging, decode = false) {
        // LL_GET_ALL_RECORDS

        // can we do this through a single callback?
        //  There could be an evented object that gets returned.
        //  Or use tha paging object to raise page events.

        // Should return an observable if there is no callback.
        //  Generally using paging for this method makes the most sense. Would transmit much faster than without.
        var buf_query, pos = 0;
        /*
        if (!callback) {
            callback = arguments[0];
            paging = new Paging.None();
        }
        */
        if (!paging instanceof Paging) paging = new Paging.Record_Paging(paging);
        buf_query = Buffer.concat([xas2(LL_GET_ALL_RECORDS).buffer, paging.buffer]);
        // Then this send function could have multiple callbacks?
        //let res = new Evented_Class();
        // 
        // send_binary_paged_message could return an observable.
        // send binary message but with an observable as the return value.
        //  observables and similar will be very useful as an API. Could have some syntax that makes them quicker to write too.
        // observe_send 
        // observe seems like the right pattern for receiving responses.
        // this.observe_send_binary_message(buf_query)
        // the observable message system will be better than the one needing callbacks.
        // Returns an object like a promise, but it raises an event for each page.
        //  Works better with a subscription model.
        // The DB subscription system will work in a similar way, but many subscriptions are not for a specific query, but observing changes that happen to any table.

        // observe send binary message, decode



        let obs_msg = this.observe_send_binary_message(buf_query, decode);

        // We could return that observable, or a different observable if there is some processing to do.
        /*
        obs_msg.subscribe('onNext', page => {
            //console.log('page', page);
        })
        obs_msg.subscribe('onError', page => {
            //console.log('page', page);
        })
        obs_msg.subscribe('onCompleted', () => {
            //console.log('onCompleted');
        });
        */
        return obs_msg;
        /*

        
        onNext — Called each time the observable emits a value.
        onError — Called when the observable encounters an error or fails to generate the data to emit. After an error, no further values will be emitted, and `onCompleted` will not be called.
        onCompleted — Called after it has called `onNext` for the final time, but only if no errors were encountered.

        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                callback(null, res_binary_message);
            }
        });
        */


    }

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

        // 

        this.observe_send_binary_message(buf_query, (err, res_binary_message) => {
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
        // 

        var paging = new Paging.None();
        var buf_command = xas2(LL_GET_RECORDS_IN_RANGE).buffer;
        // the lengths of the buffers too...
        var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);

        // What type of paging?
        //  For the moment, no paging.

        this.send_binary_message(buf_query, RECORD_PAGING_NONE, (err, res_binary_message) => {
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
        this.send_binary_message(buf_query, RECORD_PAGING_NONE, (err, res_binary_message) => {
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

        // Easier now to integrate decoding.
        //  So these maybe won't be the low level functions.

        // Could have a 'decode' parameter in send_binary_message




        this.send_binary_message(buf_query, RECORD_PAGING_NONE, (err, res_binary_message) => {
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


    // May need a paged version of this soon.



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


    // get all records, but with paging

    // will need to handle multiple callbacks.
    //  still do send_binary_message, but expect multiple callbacks?



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
                // Read that it's got no paging too

                // Could strip that page variable previously.

                if (return_message_type) {

                }

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
            //console.log('sub_event', sub_event);
            subscription_event_callback(sub_event);
        });
        return unsubscribe;
    }

    'll_subscribe_key_prefix_puts' (buf_kp, subscription_event_callback) {
        //console.log('buf_kp', buf_kp);
        //console.log('1) buf_kp hex', buf_kp.toString('hex'));
        var buf_query = Buffer.concat([xas2(LL_SUBSCRIBE_KEY_PREFIX_PUTS).buffer, buf_kp]);


        var unsubscribe = this.send_binary_subscription_message(buf_query, (sub_event) => {
            //console.log('sub_event', sub_event);

            // still a low level function.
            //  just deals with the binary data.

            // And a higher level wrapper would process / decode these subscription events.
            subscription_event_callback(sub_event);

        });
        return unsubscribe;

    }

    'ensure_table' (arr_table, callback) {
        // arr_table could be multiple tables.

        let buf_encoded_table = Binary_Encoding.flexi_encode_item(arr_table);
        var buf_query = Buffer.concat([xas2(ENSURE_TABLE).buffer, buf_encoded_table]);

        //console.log('buf_query', buf_query);
        //console.log('buf_query.length', buf_query.length);

        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                let decoded = Binary_Encoding.decode(res_binary_message);
                //console.log('decoded', decoded);

                callback(null, decoded);

                //callback(null, true);
                // Don't know if it would be useful to get back the ids.
            }
        });

    }

    // A paging definition object may help.
    //  Could help on the server side, reading the paging definition from the buffer
}


// Will get rid of these streaming followthrough functions.
//  It's standard for some things, and there will be a Query object with paging options too.

// need different create_ws_followthrough functions for each prototype.

var local_info = {
    //'server_address': 'localhost',
    'server_address': '192.168.1.159',
    //'server_address': 'localhost',
    //'db_path': 'localhost',
    'server_port': 420
}

// Think this requires the new version of the server code.


if (require.main === module) {
    var lc = new LL_NextLevelDB_Client(local_info);

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

            var test_paged_get_all_records = () => {
                // Subscriptions don't get error events back?
                //  Is that the difference?
                //  Maybe don't do it that way for the moment.


                lc.ll_count_records((err, count) => {
                    if (err) {
                        throw err;
                    } else {
                        console.log('count', count);


                        // want a higher level get all records too.

                        //  maybe not really worth having the ll version?

                        //  may look into observable transformers.

                        let decode = true;

                        let obs = lc.ll_get_all_records(new Paging.Record_Paging(16), decode);

                        // Called without a callback, with paging option, so it returns an observable which gives the results.

                        // Want a less low level version of it that decodes the records.

                        // Could use an observable transformer.



                        console.log('obs', obs);
                        obs.subscribe('onNext', (res) => {
                            console.log('obs next', res);
                        });
                        obs.subscribe('onError', (err) => {
                            console.log('obs err', err);
                            console.trace();
                        });
                        obs.subscribe('onComplete', () => {
                            console.log('obs complete');

                        });
                    }
                })
            };
            test_paged_get_all_records();

        }
    });
    //console.log('pre get all');

    var all_data = [];

} else {
    //console.log('required as a module');
}


module.exports = LL_NextLevelDB_Client;