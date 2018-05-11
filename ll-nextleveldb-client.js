/**
 * Created by James on 15/10/2016.
 */


// Could do with more client-side functionality to select data from a table
//  Select data from a key range (could be within a table)
// Server-side select_from table is working nicely, want it on the client too.


// 16/04/2018
//  Much of the code here has to do with encodeing and decoding messages.
//  Putting Message into Model would help encode and decode all messages.
//   Could even encode / decode messages into English or SQL.

// Messages would themselves be commands and options.

// Message = id + Command (including params) + Communication Options




const http = require('http');
const url = require('url');

const WebSocket = require('ws');

const lang = require('lang-mini');
const get_item_sig = lang.get_item_sig;
const get_arr_sig = lang.get_arr_sig;

const Evented_Class = lang.Evented_Class;
const Fns = lang.Fns;
const is_array = lang.is_array;
const each = lang.each;
const get_a_sig = lang.get_a_sig;
const tof = lang.tof;
//const xas2;
//var encodings = require('../nextleveldb-server/encodings/encodings');
const x = xas2 = require('xas2');

const Binary_Encoding = require('binary-encoding');
const Binary_Encoding_Record = Binary_Encoding.Record;

const Model = require("nextleveldb-model");
const Model_Database = Model.Database;
const Command_Response_Message = Model.Command_Response_Message;

//console.log('encodings.poloniex.market', encodings.poloniex.market);
const Paging = Model.Paging;
const fs = require('fs');
const request = require('request');
const protocol = 'http://';

const path = require('path');
const Key_List = Model.Key_List;



// Will renumber these at some point.

const LL_COUNT_RECORDS = 0;
const LL_PUT_RECORDS = 1;
const LL_GET_ALL_KEYS = 2;
const LL_GET_ALL_RECORDS = 3;
const LL_GET_KEYS_IN_RANGE = 4;
const LL_GET_RECORDS_IN_RANGE = 5;
const LL_GET_RECORDS_IN_RANGES = 50;
const LL_COUNT_KEYS_IN_RANGE = 6;
const LL_GET_FIRST_LAST_KEYS_IN_RANGE = 7;
// And not just in range, first and last keys beginning with something would be useful

const LL_GET_FIRST_LAST_KEYS_BEGINNING = 36; // Maybe don't implement yet.
const LL_GET_FIRST_KEY_BEGINNING = 37;
const LL_GET_LAST_KEY_BEGINNING = 38;



const LL_GET_RECORD = 8;

// This one may be made obselete when 'limit' is made as a communication option.
const LL_COUNT_KEYS_IN_RANGE_UP_TO = 9;
const LL_GET_RECORDS_IN_RANGE_UP_TO = 10;
const LL_FIND_COUNT_TABLE_RECORDS_INDEX_MATCH = 11;
const INSERT_TABLE_RECORD = 12;
const INSERT_RECORDS = 13;
const DELETE_RECORDS_BY_KEYS = 18;
const ENSURE_TABLE = 20;
const ENSURE_TABLES = 21;
const TABLE_EXISTS = 22;
const TABLE_ID_BY_NAME = 23;

const SELECT_FROM_TABLE = 41;

// RENAME_TABLE
const GET_TABLE_FIELDS_INFO = 24;
const GET_TABLE_KEY_SUBDIVISIONS = 25;

const LL_SUBSCRIBE_ALL = 60;
const LL_SUBSCRIBE_KEY_PREFIX_PUTS = 61;
const LL_UNSUBSCRIBE_SUBSCRIPTION = 62;

const LL_WIPE = 100;
const LL_WIPE_REPLACE = 101;

const LL_SEND_MESSAGE_RECEIPT = 120;

const LL_MESSAGE_STOP = 121;
const LL_MESSAGE_PAUSE = 122;
const LL_MESSAGE_RESUME = 123;




//const LL_PAUSE_MESSAGE_RESPONSES = 120;
//const LL_RESUME_MESSAGE_RESPONSES = 120;

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





// Separate out the type of paging from the type of whatever is encoded.
//  Returning function most likely knows what kind of data to expect, or can be made to be that way.
//   Don't need to tell the server to return encoded records.
//   Don't need to tell the client if results are encoded as records or binary as it should know what to expect.
//    It could know by checking against the command_id

// 



const NO_PAGING = 0;
const PAGING_RECORD_COUNT = 1;
const PAGING_KEY_COUNT = 2;
// Followed by p number
const PAGING_BYTE_COUNT = 3;
const PAGING_TIMED = 4;
const PAGING_AND_EXTENDED_OPTIONS = 5;


const BINARY_PAGING_NONE = 0;
const BINARY_PAGING_FLOW = 1;
const BINARY_PAGING_LAST = 2;

const RECORD_PAGING_NONE = 3;
const RECORD_PAGING_FLOW = 4;
const RECORD_PAGING_LAST = 5;
const RECORD_UNDEFINED = 6;

// A whole message type for undefined record?

const KEY_PAGING_NONE = 7;
const KEY_PAGING_FLOW = 8;
const KEY_PAGING_LAST = 9;

// Simplest error message.
//  Could have a number, then could have encoded text.
//  
const ERROR_MESSAGE = 10;







// Is going to have much more advanced client<>server functionality, with much more advanced server-side functionality too.
//  Will be able to upload a binary encoded array of table definitions for it to ensure, and it will do that.
//   It is going to use a Model on the server side to translate these definitions within the commands for the appropriate rows / row updates in the database.
//    Should be able to use a model diff to see which rows have changed, and then put them into the database.
//     Would need to be careful about updating indexes too.
//      That could be done with a full model of the core.
//    Some lower level diff processing in the db, or batching put and delete changes together into one operation.

// The size of the protocol is changing, meaning more complex objects can be got from the DB, but the underlying record structure is staying the same.
//  The protocol will handle different paging systems, enabling convenient use of the Observable pattern in JavaScript.#


// Need some more server-side functionality to do with adding a binary encoded table struture (list) to the database.
//  This could use Binary Paging as an option. Not dealing with records in particular? Or if it's Record Paging, could return the records that are added / changed / deleted.
//   More flexibility in the binary paging systems to add encoding for deleted records.



// 26/03/2018
//  Backpressure seems like it could be an issue. The client's receive buffer gets full quicker than it raises the message events.

// Want a way to moderate this....
//  Could have a way to moderate the backpressure.
//  Could try reading it slower from the server.

// LL_SEND_MESSAGE_RECEIPT - Whenever we receive a message on the client, we send a receipt for it over to the server.
//  That way, there would be a way for the server to tell when it's got behind with any client's messages.
//  When we receive paged data, then using LL_SEND_MESSAGE_RECEIPT would be useful to help the server judge if it needs to pause reading the db while send/receive catches up.
//   The server can send much quicker than the client receives in the case I am working on.

// Backpressure has been solved

// 27/03/2018
//  Data being sent is quite big, so usage of compression would make sense.
//   Currently it can be sent at a decent rate at least, and WebSocket buffer problem has been solved, but not implemented everywhere.

//  Table and data syncing looks like a pressing thing to solve. Protocol looks OK for the moment, but could benefit from compression (LZ4) in various places, or other compression options.
//   Limit option could also go in the protocol, so require fewer ll functions, and make for better DRY code.

// Maybe this would take a few hours to sync a few weeks' of data.
//  Getting every thousand or million sampled keys from a range would help.
//   Or according to the page sizes. Percentage complete statistic of a download would greatly help, with 1 decimal place. So out of 1000.

// Just a count in range would be useful to start with.
// count_table_records sometimes would do.






// Have it elsewhere too.
//  Seems more like it's part of the Model.
//   Something which is for modelling the DB and it data flow whether or not it's actually connected / live.
//   The Model is isomorphic JS code.
//   Then the connected parts make use of it.





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
        this.access_token = spec.access_token;

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

        let access_token = this.access_token;

        console.log('NextLevelDB_Client start');
        var that = this;



        this.id_ws_req = 0;
        var ws_response_handlers = this.ws_response_handlers = {};
        var ws_address = this.server_url || 'ws://' + this.server_address + ':' + this.server_port + '/';

        var client = this.websocket_client = new WebSocket(ws_address, 'echo-protocol', {
            'headers': {
                'cookie': 'access_token=' + encodeURIComponent(access_token)
            }
        });


        /*

        var client = this.websocket_client = new WebSocketClient({
            maxReceivedFrameSize: 512000000,
            maxReceivedMessageSize: 512000000,
            fragmentOutgoingMessages: false,
            //assembleFragments: false,
            closeTimeout: 1000000
        });
        */

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

                client.connect(ws_address, 'echo-protocol', null, {
                    'cookie': 'access_token=' + encodeURIComponent(access_token)
                });

                setTimeout(function () {
                    attempting_reconnection = false;
                    reconnection_attempts();
                }, 1000);
            }
        };

        var first_connect = true;

        let on_open = function (connection) {
            console.log('connected');
            //console.log('connection', connection);
            //console.log('connection', Object.keys(connection.target));
            //throw 'stop';

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
                client.addEventListener('error', function (error) {
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
                client.addEventListener('connectFailed', function (error) {
                    console.log('connection failed, err', err);

                    // Probably in response to attempting to write to a closed stream?

                    //console.log('\nerror', error);
                    //console.log('Object.keys(error)', Object.keys(error));
                    //console.log("Connection Error: " + error.toString());
                    //console.log('typeof error', typeof error);

                    //attempting_reconnection = false;
                    //reconnection_attempts();
                });
                client.addEventListener('close', function () {


                    console.log('echo-protocol Connection Closed');
                    //  Nice if the long response got cancelled on the server side.

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
                client.addEventListener('message', function (message) {



                    //console.log('message', message);
                    //throw 'stop';

                    /*
                    if (message.type === 'utf8' || message.type === 'message') {
                        //var obj_message = JSON.parse(message.utf8Data || message.data);
                        var obj_message = JSON.parse(message.data);

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
                    */

                    //console.log('client.bufferedAmount', client.bufferedAmount);
                    if (message.type === 'binary' || message.type === 'message') {
                        that.receive_binary_message(message.data || message.binaryData);
                    }
                });
            };

            assign_connection_events();

            if (first_connect) {
                callback(null, true);
            };
        }

        //console.log('pre connect');
        //client.on('connect', on_open);
        //client.on('open', on_open);
        //console.log('client.onopen', client.onopen);
        client.addEventListener('open', on_open);

        // need the url without the protocol.




        //client.connect(ws_address, 'echo-protocol');
        /*
        client.connect(ws_address, 'echo-protocol', null, {
            'headers': {
                'cookie': 'access_token=' + access_token
            }

        });
        */

        /*

        client.connect(ws_address, 'echo-protocol', null, {
            'cookie': 'access_token=' + encodeURIComponent(access_token)

        });

        */

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


        // This strips out the message id.
        //  That seems a bit inefficient, would be worth changing it in the future.
        //   Could consolidate code paths to help with that.
        //   There will be code to make it easier for the handlers, so less decoding / processing needed here.



        //console.log('receive_binary_message buf_message', buf_message);
        var message_id, pos = 0,
            message_type;
        [message_id, pos] = xas2.read(buf_message, pos);

        //console.log('1) message_id', message_id);

        var buf_the_rest = Buffer.alloc(buf_message.length - pos);
        buf_message.copy(buf_the_rest, 0, pos);

        if (return_message_type) {
            [message_type, pos] = xas2.read(buf_message, pos);
        }

        // Message type is being encoded on the server.
        //  May be best to separate out paging option from data type.
        //  Seems most logical.
        //  

        //console.log('message_type', message_type);
        //console.log('return_message_type', return_message_type);

        if (return_message_type) {
            //console.log('buf_the_rest', buf_the_rest);
            if (message_type === BINARY_PAGING_NONE) {

                // Could even strip the paging / structure flag here.

                this.ws_response_handlers[message_id](buf_the_rest, message_id);
                // could remove the response handler here
                this.ws_response_handlers[message_id] = null;
            }
            if (message_type === BINARY_PAGING_FLOW) {
                this.ws_response_handlers[message_id](buf_the_rest, message_id);
                // could remove the response handler here
            }
            if (message_type === BINARY_PAGING_LAST) {
                this.ws_response_handlers[message_id](buf_the_rest, message_id);
                // could remove the response handler here
                this.ws_response_handlers[message_id] = null;
            }

            if (message_type === RECORD_PAGING_NONE) {
                // Could even strip the paging / structure flag here.
                //console.log('2) message_id', message_id);
                this.ws_response_handlers[message_id](buf_the_rest, message_id);
                // could remove the response handler here
                this.ws_response_handlers[message_id] = null;
            }
            if (message_type === RECORD_UNDEFINED) {
                //let buf_null = Binary_Encoding.flexi_encode_item(undefined);

                //console.log('buf_the_rest', buf_the_rest);
                this.ws_response_handlers[message_id](buf_the_rest, message_id);
                // could remove the response handler here
                this.ws_response_handlers[message_id] = null;
            }

            if (message_type === RECORD_PAGING_FLOW) {
                //console.log('RECORD_PAGING_FLOW');
                this.ws_response_handlers[message_id](buf_the_rest, message_id);
                // could remove the response handler here
            }
            if (message_type === RECORD_PAGING_LAST) {
                this.ws_response_handlers[message_id](buf_the_rest, message_id);
                // could remove the response handler here
                this.ws_response_handlers[message_id] = null;
            }

            if (message_type === KEY_PAGING_NONE) {
                // Could even strip the paging / structure flag here.
                //console.log('2) message_id', message_id);
                this.ws_response_handlers[message_id](buf_the_rest, message_id);
                // could remove the response handler here
                this.ws_response_handlers[message_id] = null;
            }
            if (message_type === KEY_PAGING_FLOW) {
                this.ws_response_handlers[message_id](buf_the_rest, message_id);
                // could remove the response handler here
            }
            if (message_type === KEY_PAGING_LAST) {
                this.ws_response_handlers[message_id](buf_the_rest, message_id);
                // could remove the response handler here
                this.ws_response_handlers[message_id] = null;
            }

            if (message_type === ERROR_MESSAGE) {
                //console.log('client has received an error from the server');
                this.ws_response_handlers[message_id](buf_the_rest, message_id);
                this.ws_response_handlers[message_id] = null;
            }

        } else {
            this.ws_response_handlers[message_id](buf_the_rest, message_id);
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

    // Not so sure about enhancing this code further, as it's already fairly large and complex.
    //  It's quite flexible.
    // 


    // send_binary_no_paging_message
    //  That could be easier to use. Maybe it could even call send_binary_message, though that would be less optimal.

    // Auto send message receipt would be useful.
    //  Maybe don't need it on all of them, such as non-paged messages.
    send_message_receipt(message_id, page_number) {
        //console.log('send_message_receipt', message_id, page_number);
        //console.trace();
        var buf = Buffer.concat([xas2(this.id_ws_req++).buffer, xas2(LL_SEND_MESSAGE_RECEIPT).buffer, xas2(message_id).buffer, xas2(page_number).buffer]);
        this.websocket_client.send(buf);
    }




    // send_paged_command is simpler because it uses classes to handle message encoding / decoding / paging.
    //  will use this for ll_get_records_in_ranges, which will enable (much) faster syncing.

    // May want to get the whole pages back.
    //  could have options to make parsing easier.
    //  May want to get the full page data buffers back.
    //   Easier to split them up later, or at least more efficient if we are to put them in the DB soon.
    //   Could make a batch_put that handles already split records.




    send_paged_command(command_id, params) {
        // This should be the simplest API yet.
        //  Not sure we even should have to send paging details in terms of what type of paging, as it would not vary by command.

        // Binary paging, Record Paging, Timed paging?
        //  Probably eplace binary and record paging with just 'count' paging.
        //  See if the server can interpret one of them as the other.
        //   Change it so we only use 1 of them, then get rid of the other.


        let message_id = this.id_ws_req++;

        console.log('send_paged_command command_id', command_id);
        // encode the message with the params.

        let return_options = new Paging.Count_Paging(1024);
        let buf_msg = Buffer.concat([xas2(message_id).buffer, xas2(command_id).buffer, return_options.buffer, Binary_Encoding.encode_to_buffer(params)]);

        console.log('buf_msg', buf_msg);
        this.websocket_client.send(buf_msg);
        //return res;

        // And want a simple return message processor.
        //  The code paths in various places have become too long & boilerplatery. Need to make it call explicitly named, clear functions to get it to do what is required.

        let res = new Evented_Class();

        // And probably worth returning an observable where all messages go.
        //  Returning a parsed message makes sense.

        // also want to use Command_Response_Message to build the messages on the server-side.
        //  When both client and server side messages are using the same code, it will then be possible / much easier to change the messaging protocol.


        // The message id has been removed.
        //  Not sure that's best.


        this.ws_response_handlers[message_id] = (obj_message, message_id) => {
            let response_message;
            if (typeof message_id === 'undefined') {
                // We must / should have been given the full binary message
                response_message = new Command_Response_Message(obj_message);
            } else {
                // Some earlier 'clever' programming strips out the message id from the beginning of the buffer.
                // Now more use of Binary_Encoding with its greater functionality.

                response_message = new Command_Response_Message(Buffer.concat([xas2(message_id).buffer, obj_message]));
            }
            console.log('response_message', response_message);
            // response_message.items
            //  response_message.value
            console.log('response_message.value.length', response_message.value.length);
            console.log('response_message.kv_buffers', response_message.kvp_buffers);

            res.raise('next', response_message.value_buffer);


            //each(response_message.kvp_buffers, kvbp => res.raise('next', kvbp));

            //console.log('response_message.items.length', response_message.items.length);
            // 



            // Not quite. It's worth unpaging it all.
            //  Or maybe having un unpage part that wraps send_paged_command
            //  Need to have decoding of the command upon reception on the client.
            //  Anyway, use Command_Response_Message to deconstruct the message.
            //   Could then raise the output event for each of the deconstructed / unpaged item.


            // Unpage the responses if necessary and then return them
            //  This also seems like a place for a decoding option.
            //  And KP removal (though that has a benefit when done server-side)


            //res.raise('next', response_message);


            // The response message class will deconstruct the pages.
            //  Would be possible to use an unpage observer; We want to keep extra params to do with paging out of here for the moment. 

            // and if it's the last message, get rid of the response handler
            console.log('response_message.is_last', response_message.is_last);

            if (response_message.is_last) {
                res.raise('complete');
            }
        }

        return res;

    }


    setup_binary_no_paging_no_decode_handler(idx, callback) {

        // The message has had its idx removed already. Not sure that's best.

        let pos = 0,
            response_type_code;

        this.ws_response_handlers[idx] = (obj_message, idx) => {
            //console.log('obj_message', obj_message);
            //console.log('idx', idx);

            [response_type_code, pos] = xas2.read(obj_message, pos);
            //console.log('PAGING_NONE obj_message', obj_message);
            //console.log('response_type_code', response_type_code);

            // check to see if we get an error response.
            let buf_the_rest = Buffer.alloc(obj_message.length - pos);
            obj_message.copy(buf_the_rest, 0, pos);
            //console.log('buf_the_rest', buf_the_rest);

            if (response_type_code === ERROR_MESSAGE) {
                callback(buf_the_rest);

            } else if (response_type_code === BINARY_PAGING_NONE) {
                // Still don't want decoding. Just it's signified that there is no paging.
                //  Function call could have its own custom encoding but moving away from that into the future.

                // Specifically this is the no decode handler, so no decoding here!

                //console.log('buf_the_rest', buf_the_rest);
                //let decoded = Binary_Encoding.decode_buffer(buf_the_rest)[0];

                //console.log('decoded', decoded);
                callback(null, buf_the_rest);


                //callback(buf_the_rest);
            } else {

                // 

                callback(null, buf_the_rest);
            }


            // Remove the paging info from it.

            // Could look in the response to see what message we have, if it indicates paging.
            //  However, it's a binary message

            this.ws_response_handlers[idx] = null;
        };
    }

    /*
                    
    */


    // Option of removing the kp from the results.
    //  another option alongside decode?

    send_binary_message(message, message_type = BINARY_PAGING_NONE, decode = false, remove_kp = false, callback) {

        console.log('send_binary_message');


        //const remove_kp = false;


        // Encoding the message into a buffer would be very useful.
        //  


        let a = arguments;


        // Should not need to supply the message type - could read it.


        // Moving decoding out of the handler would help.
        //  Want to make a streamlined version of this with no decoding here.
        //  Decoding results won't be too hard, also can use functions and wrap functions to return decoded data.
        //   Also, some functions will return decoded values, rather than keys or records which can be encoded and still useful.









        // Need to read what type of message it is when the message gets returned....

        //console.log('a.length', a.length);

        if (a.length === 2) {
            callback = a[1];
            message_type = BINARY_PAGING_NONE;
            decode = false;
        }
        if (a.length === 3) {
            callback = a[2];
            decode = false;
        }
        if (a.length === 4) {
            callback = a[3];
            remove_kp = false;
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

        //console.log('idx', idx);

        //  Inefficient in some ways when we know it's not needed.
        //  Many functions can handle paging though, and I think it's quite a priority in terms of replication and having the dbs able to talk to each other.

        //console.log('return_message_type', return_message_type);
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

            //console.log('send_binary_message message_type', message_type);

            // The extra complexity here will mean that 'll' functions will be able to act as normal functions, so won't need to be called 'll', and the normal functions that 
            if (message_type === BINARY_PAGING_NONE) {
                //console.log('decode', decode);
                if (decode) {
                    ws_response_handlers[idx] = function (obj_message) {
                        //console.log('obj_message', obj_message);
                        [response_type_code, pos] = xas2.read(obj_message, pos);


                        var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                        obj_message.copy(buf_the_rest, 0, pos);

                        // decode this buffer, it's binary encoding.
                        //console.log('response_type_code', response_type_code);

                        if (response_type_code === ERROR_MESSAGE) {
                            //callback(buf_the_rest);
                            console.trace();
                            throw 'NYI';
                        } else {

                            // Change the encoding on the server, to say that any result is encoded as an xas2, and indicated that it's coded as xas2.
                            //  That's because we decode the message of BINARY type, which here means we use Binary_Encoding




                            //console.log('buf_the_rest', buf_the_rest);
                            // 

                            // Should prob get object 0
                            //  And check elsewhere to make sure this works.

                            let d1 = Binary_Encoding.decode_buffer(buf_the_rest);
                            //console.log('d1', d1);

                            let decoded = d1[0];
                            //console.log('decoded', decoded);

                            callback(null, decoded);
                        }
                        ws_response_handlers[idx] = null;
                    };
                } else {

                    // setup_binary_no_paging_no_decode_handler
                    //  Just getting back a key in its own way seems strange. Could spare a byte (I suppose) to say it's a buffer.
                    //  Key paging seems useful - 


                    this.setup_binary_no_paging_no_decode_handler(idx, callback);



                    /*
                    ws_response_handlers[idx] = function (obj_message) {
                        [response_type_code, pos] = xas2.read(obj_message, pos);
                        //console.log('PAGING_NONE obj_message', obj_message);
                        //console.log('response_type_code', response_type_code);


                        // check to see if we get an error response.
                        let buf_the_rest = Buffer.alloc(obj_message.length - pos);
                        obj_message.copy(buf_the_rest, 0, pos);

                        if (response_type_code === ERROR_MESSAGE) {
                            callback(buf_the_rest);
                        } else {
                            callback(null, buf_the_rest);
                        }


                        // Remove the paging info from it.

                        // Could look in the response to see what message we have, if it indicates paging.
                        //  However, it's a binary message

                        ws_response_handlers[idx] = null;
                    };
                    */
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

            /*
            if (message_type === ERROR_MESSAGE) {
                console.log('we have an error');

                callback(new Error('NextLevelDB Server Error'));
            }
            */

            // Want to keep the DB going totally stable.




            if (message_type === RECORD_PAGING_NONE) {

                // Server not returning records right?

                if (decode) {
                    ws_response_handlers[idx] = function (obj_message) {
                        [response_type_code, pos] = xas2.read(obj_message, pos);
                        var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                        obj_message.copy(buf_the_rest, 0, pos);

                        // decode this buffer, it's binary encoding.

                        //console.log('buf_the_rest', buf_the_rest);
                        //console.log('buf_the_rest.length', buf_the_rest.length);

                        //let decoded = Binary_Encoding.decode(buf_the_rest);



                        var row_buffers = Binary_Encoding.get_row_buffers(buf_the_rest);
                        //console.log('row_buffers', row_buffers);
                        //console.log('row_buffers', row_buffers.length);
                        //throw 'stop';

                        // Keep or remove KP?
                        //let decoded = Model_Database.decode_model_rows(row_buffers, 1);

                        let decoded;
                        if (remove_kp) {
                            decoded = Model_Database.decode_model_rows(row_buffers, 1);
                        } else {

                            // Local problem...?

                            decoded = Model_Database.decode_model_rows(row_buffers);
                        }

                        // Because we may need to keep the kps to put them into the db properly.
                        //  a remove_kp option would help.
                        //  with some function calls it would be obvious.


                        // To remove a single key prefix here.
                        //console.log('decoded', decoded);
                        //throw 'stop';

                        callback(null, decoded);
                        ws_response_handlers[idx] = null;
                    };
                } else {
                    ws_response_handlers[idx] = function (obj_message) {
                        [response_type_code, pos] = xas2.read(obj_message, pos);

                        // would still have the message id?


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


            // Need to be able to handle this without decoding

            if (message_type === RECORD_PAGING_FLOW) {

                // Has filtered out the page number already?

                if (decode) {
                    console.trace();
                    throw 'NYI';
                } else {

                    ws_response_handlers[idx] = function (obj_message, page_number) {

                        //console.log('PAGING_FLOW obj_message', obj_message);

                        //console.log('page_number', page_number);

                        // Decode it?



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

                }

                // Version of this without decoding...


                // could remove the response handler here
            }
            if (message_type === RECORD_PAGING_LAST) {
                ws_response_handlers[idx] = function (obj_message, page_number) {

                    // Could look in the response to see what message we have, if it indicates paging.
                    //  However, it's a binary message

                    callback(null, obj_message, page_number, true);
                    ws_response_handlers[idx] = null;
                };
                // could remove the response handler here
            }


            // KEY PAGING

            if (message_type === KEY_PAGING_NONE) {

                if (decode) {
                    ws_response_handlers[idx] = function (obj_message) {
                        [response_type_code, pos] = xas2.read(obj_message, pos);
                        var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                        obj_message.copy(buf_the_rest, 0, pos);

                        // decode this buffer, it's binary encoding.

                        //console.log('buf_the_rest', buf_the_rest);
                        //console.log('buf_the_rest.length', buf_the_rest.length);

                        //let decoded = Binary_Encoding.decode(buf_the_rest);

                        var row_buffers = Binary_Encoding.get_row_buffers(buf_the_rest);

                        let decoded = Model_Database.decode_model_rows(row_buffers);
                        //console.log('decoded', decoded);

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


            if (message_type === KEY_PAGING_FLOW) {
                ws_response_handlers[idx] = function (obj_message, page_number) {
                    console.log('PAGING_FLOW obj_message', obj_message);
                    // Decode it?

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
            if (message_type === KEY_PAGING_LAST) {
                ws_response_handlers[idx] = function (obj_message, page_number) {
                    // Could look in the response to see what message we have, if it indicates paging.
                    //  However, it's a binary message
                    callback(null, obj_message, page_number, true);
                    ws_response_handlers[idx] = null;
                };
                // could remove the response handler here
            }

        } else {
            //console.log('*** idx', idx);
            ws_response_handlers[idx] = function (obj_message) {
                // Could look in the response to see what message we have, if it indicates paging.
                //  However, it's a binary message
                callback(null, obj_message);
                ws_response_handlers[idx] = null;
            };
        }
        //this.websocket_connection.sendBytes(buf_2);

        this.websocket_client.send(buf_2);
    }

    // and have a decode option


    // Want different ways of calling it so that the pages get broken up
    //  


    // send(message_type_id, paging, args, decode, remove_kp, callback)
    //  Would either send an observable or callback type message
    //  Would handle some details of message encoding.
    //   Arguments after the paging object would be deconstructed.
    //   Nice to send remove kp option within the paging options.
    //    Decode option could be sent there too.
    //    Remove kp and decode are standard, and the server could process remove kp as the last step.
    //     Inner functions would not need to handle this, but many of them do.
    //  Making more advanced / API2 paging options would be nice.
    //   Incorporating kp removal into the paging options would be very cool.
    //    Would not necessarily just work with records. Try making it work with keys too.
    //  Want a standard way to send the remove_kp param.
    //   Would be cool to do it in the paging object as standard.










    // A param to remove the result from the array?
    //  We always decode an array from the server but it could be just one message.
    //   For binary, could incorporate single_item message status / code
    //    


    // This may be left out / deprecated.
    //  There will be a Command_Helper (currently Paging) that will do more to construct the message.

    // Possibly there should be OO messages within Model.
    //  Would help the messages be encoded and decoded with those params?
    //   Or just make it very easy to use an array and consider that the way to always encode params.
    //   Communication Params, Command Params.
    //    .comm

    // Info on the limit can be encoded into the message.



    observe_send_binary_message(message, decode = false, remove_kp = false, str_result_grouping = '') {

        // Right now, this returns pages of data.
        //  In many cases, we want the unpaged data returned.



        // In other cases, will be able to do decode(send)...
        //  and use a decoding observable wrapper/


        // Maybe this could also separate out individual paging results from the pages sent from the server.
        //  Paging is most important in transmission. Probably won't be passing the pages around that much as complete pages.
        //let str_result_grouping = '';



        // Reads the response message type.
        //  Will be worthwhile to make the send message type optional?
        //   Not for the 

        // Could possibly encode the message if it's not in a buffer already.

        // Could have Message objects in the model.
        //  BinaryMessage

        // Still use the WS response handler.
        //  Assume it's a paging style response.
        //  Worth having that byte in the message encoding so that we can tell if it's a paged response or not, even if assuming not.
        //   That will be the default mode.

        // Maybe this will be the ll version that does not decode the values.


        let a = arguments,
            sig = get_a_sig(arguments);
        //console.log('observe_send_binary_message sig', sig);


        if (sig === '[n,a,o]') {
            // will need to compose the message into a buffer.

            let [i_command_type, arr_args, paging] = a;
            //console.log('[i_command_type, paging, arr_args]', [i_command_type, paging, arr_args]);

            // Create the binary message, with the paging, then the params.

            let arr_bufs_msg = [xas2(i_command_type).buffer, paging.buffer, Binary_Encoding.encode_to_buffer(arr_args)];
            //console.log('arr_bufs_msg', arr_bufs_msg);
            message = Buffer.concat(arr_bufs_msg);
            decode = true;
            remove_kp = true;

            //throw 'stop';

        } else if (sig === '[n,a,o,s]') {
            let i_command_type, arr_args, paging;
            [i_command_type, arr_args, paging, str_result_grouping] = a;
            //console.log('[i_command_type, arr_args, paging, str_result_grouping]', [i_command_type, arr_args, paging, str_result_grouping]);
            let arr_bufs_msg = [xas2(i_command_type).buffer, paging.buffer, Binary_Encoding.encode_to_buffer(arr_args)];
            //console.log('arr_bufs_msg', arr_bufs_msg);
            message = Buffer.concat(arr_bufs_msg);
            decode = true;
            remove_kp = true;
        } else if (sig === '[B]') {

        } else if (sig === '[B,b]') {

        } else if (sig === '[B,b,b]') {

        } else if (sig === '[B,b,b,s]') {

        } else {
            throw 'observe_send_binary_message: Unexpected signature ' + sig
        }

        let idx = this.id_ws_req++,
            ws_response_handlers = this.ws_response_handlers,
            pos = 0,
            message_type, response_type_code, page_number = 0;

        var buf_2 = Buffer.concat([xas2(idx).buffer, message]);
        let res = new Evented_Class();

        res.unpaged = false;

        // be able to stop the observable.
        //  means telling it to stop on the server too.
        //  will send a stop command command.







        //let send_message_receipt = this.send_message_receipt;
        // [message_type, pos] = xas2.read(buf_message, pos);
        //let page_number = 0;
        // the message will contain the message idx, the message type, and then maybe further options.
        //

        ws_response_handlers[idx] = (obj_message) => {
            pos = 0;
            // read the paging / message type option out of the message.


            //console.log('obj_message', obj_message);

            [message_type, pos] = xas2.read(obj_message, pos);

            //console.log('* message_type', message_type);
            //console.trace();
            //throw 'stop';

            // console.log('pos', pos);

            // Code has become more complicated now that decoding is being built into the low level client.
            //  Not sure that's the best design decision.
            //  However, the decoding type has now been put into the protocol, within the messages, so it's on a lower level.
            //   The low level API has become more complex.

            //console.log('*** decode', decode);

            // Message decoding levels.
            //  Sometimes messages / pages are used to transport encoded data.
            //  In this case, we may need to decode the message / page / data structures to get access to the data.
            //  It appears as though there will be 2 decoding levels (at least). Decode the message / page itself so that the inner data is available
            //   Decode the inner data
            //    Previously decoding had assumed we want both.
            // Decoding has also removed data such as message id and page number.
            //  decode_message
            //  decode_content

            // In some cases need to be clear that we are sending encoded items back to the client.
            //  They would say they are a buffer, such as for part of a key.
            //  I think it may well be worth making OO message classes that handle the specifics of different message types, or the varieties of them.

            // Having the same decoder used on the server-side, wrapping an observable, would be of use.
            //  Could create more message / response types that have awareness of the decoding levels.

            // Also, switching off decoding option here will help, it can be a changable property of the result object.



            if (decode) {
                //console.log('str_result_grouping', str_result_grouping);

                //console.log('res.unpaged', res.unpaged);
                //throw 'stop';

                if (message_type === BINARY_PAGING_NONE) {

                    console.log('BINARY_PAGING_NONE', BINARY_PAGING_NONE);

                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);
                    //console.log('buf_the_rest', buf_the_rest);

                    // Maybe do full decode buffer, and enclose the results as an array in all cases.
                    //  Could change the server-side code to use the paging helper that would always do this.

                    let decoded = Binary_Encoding.decode_buffer(buf_the_rest)[0];
                    //console.log('** decoded', decoded);

                    // Events just handling true and false....
                    res.raise('next', decoded);
                    res.raise('complete', decoded);
                }
                if (message_type === BINARY_PAGING_FLOW) {
                    console.log('BINARY_PAGING_FLOW');

                    [page_number, pos] = xas2.read(obj_message, pos);


                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);

                    let decoded = Binary_Encoding.decode_buffer(buf_the_rest)[0];

                    if (res.unpaged || str_result_grouping === 'single') {
                        each(decoded, item => res.raise('next', item));
                    } else {
                        res.raise('next', decoded);
                    }

                    //console.log('decoded', decoded);


                    this.send_message_receipt(idx, page_number);
                }
                if (message_type === BINARY_PAGING_LAST) {
                    console.log('BINARY_PAGING_LAST');


                    [page_number, pos] = xas2.read(obj_message, pos);
                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);

                    //console.log('page_number', page_number);

                    // Not so sure we want the 0th item?
                    //  Or handle a page differently?

                    //console.log('buf_the_rest', buf_the_rest);


                    // Server-side the results need to be put into separate arrays.

                    // Within the page, we could get a large amount of data.
                    //  For the moment, could make the new server side send function wrap the result in an array of length 1

                    //console.log('buf_the_rest', buf_the_rest);
                    let decoded_buffer = Binary_Encoding.decode_buffer(buf_the_rest)[0];
                    //console.log('decoded_buffer', decoded_buffer);
                    //console.log('str_result_grouping', str_result_grouping);
                    if (res.unpaged || str_result_grouping === 'single') {
                        each(decoded_buffer, item => res.raise('next', item));
                    } else {
                        res.raise('next', decoded_buffer);
                    }
                    //console.log('decoded_buffer', decoded_buffer);

                    //let decoded = Binary_Encoding.decode_buffer(buf_the_rest)[0];
                    //console.log('decoded', decoded);

                    //throw 'stop';

                    //res.raise('next', decoded_buffer);
                    //res.raise('complete', decoded_buffer);
                    //res.raise('complete', decoded_buffer);
                    //console.log('pre res raise complete');
                    // Doubt passing it through will be that inefficient, but it's useful in some cases.

                    res.raise('complete', decoded_buffer);
                    this.send_message_receipt(idx, page_number);
                }

                // KEY_PAGING_NONE
                //  Returning pages or buffers of keys could be a useful base level feature.

                if (message_type === RECORD_PAGING_NONE) {
                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);
                    //console.log('buf_the_rest', buf_the_rest);


                    // Seems like another binary format is necessary, and this could make it somewhat difficult to keep it continuing with the current server.
                    //  Could use pm2 to restart it with new code.

                    // Specifying that they are records seems useful.

                    // Would be good to deploy it onto a 2nd server, keep the data coming in, and then to get records from accross servers.
                    //  Will focus on streaming data, and having servers copy that streaming data from each other.

                    // Think it will be necessary to change both the client and server handling of data.

                    // Or to get the client to work, even with the current bugs.
                    // However, the server, including the currently deployed one that's been going for days, has got incorrect / inconsistent data sending.

                    // Seems worthwhile to carefully make a new client/server version, get it running elsewhere, test it, and then deploy that onto the running DB.
                    //  Could improve the client so that the collector will buffer unsent messages, and then send them when the server becomes available again.

                    // Keeping the current process running, while making substantial fixes / improvements to the client and server makes the most sense.
                    //  Could be worth running and testing on the xeon system for the moment.

                    // Deploy upgraded / fixed client and server software to that xeon, then stream data from it.
                    //  Key streams seem like they will be a useful data transmission system.
                    //   These would be useful for checking if records exist, but blockchain verification will be better / more full featured in the long run.

                    // kv for records, but just split_length_item_encoded_buffer for keys.
                    let arr_bufs_kv = Binary_Encoding.split_length_item_encoded_buffer_to_kv(buf_the_rest);
                    //console.log('arr_bufs_kv', arr_bufs_kv);

                    //let remove_kp = true;
                    let arr_decoded = Model_Database.decode_model_rows(arr_bufs_kv[0], remove_kp);
                    res.raise('next', arr_decoded);
                    res.raise('complete');
                }
                if (message_type === RECORD_PAGING_FLOW) {
                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);
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
                    //let remove_kp = true;
                    //console.log('arr_bufs_kv[0]', arr_bufs_kv[0]);
                    //console.log('arr_bufs_kv', arr_bufs_kv);
                    //throw 'stop';
                    let arr_decoded = Model_Database.decode_model_rows(arr_bufs_kv, remove_kp);

                    if (res.unpaged || str_result_grouping === 'single') {
                        each(arr_decoded, item => res.raise('next', item));
                    } else {
                        res.raise('next', arr_decoded);
                    }

                    this.send_message_receipt(idx, page_number);
                }
                if (message_type === RECORD_PAGING_LAST) {
                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);
                    [page_number, pos] = xas2.read(buf_the_rest, 0);
                    //console.log('page_number', page_number);
                    let buf2 = Buffer.alloc(buf_the_rest.length - pos);
                    buf_the_rest.copy(buf2, 0, pos);
                    let arr_bufs_kv = Binary_Encoding.split_length_item_encoded_buffer_to_kv(buf2);
                    //let remove_kp = true;
                    //console.log('arr_bufs_kv', arr_bufs_kv);
                    let arr_decoded = Model_Database.decode_model_rows(arr_bufs_kv, remove_kp);
                    //console.log('arr_decoded', arr_decoded);

                    // Say what page it is?

                    if (res.unpaged || str_result_grouping === 'single') {
                        each(arr_decoded, item => res.raise('next', item));
                    } else {
                        res.raise('next', arr_decoded);
                    }

                    //res.raise('next', arr_decoded);
                    res.raise('complete', arr_decoded);
                    this.send_message_receipt(idx, page_number);
                }

                // KEY PAGING observe_send_binary_message

                if (message_type === KEY_PAGING_NONE) {
                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);
                    //console.log('buf_the_rest', buf_the_rest);
                    let arr_bufs_k = Binary_Encoding.split_length_item_encoded_buffer(buf_the_rest);
                    //console.log('arr_bufs_k', arr_bufs_k);

                    //let remove_kp = true;

                    //throw 'stop';
                    // Decode them as keys.
                    // decode_keys


                    //let arr_decoded = Model_Database.decode_model_rows(arr_bufs_k, remove_kp);
                    let arr_decoded = Model_Database.decode_keys(arr_bufs_k, remove_kp);

                    //console.log('arr_decoded', arr_decoded);

                    //console.trace();
                    //throw 'stop';

                    res.raise('next', arr_decoded);
                    res.raise('complete', arr_decoded);
                }
                if (message_type === KEY_PAGING_FLOW) {
                    console.log('KEY_PAGING_FLOW');

                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    //console.log('buf_the_rest', buf_the_rest);
                    obj_message.copy(buf_the_rest, 0, pos);
                    pos = 0;
                    //console.log('buf_the_rest', buf_the_rest);

                    // Need to get some specific values for the flow decoding.
                    //  

                    //[response_type_code, pos] = xas2.read(obj_message, pos);
                    //console.log('response_type_code', response_type_code);
                    //console.log('pos', pos);

                    [page_number, pos] = xas2.read(buf_the_rest, pos);
                    //console.log('page_number', page_number);

                    let buf2 = Buffer.alloc(buf_the_rest.length - pos);
                    buf_the_rest.copy(buf2, 0, pos);
                    let arr_bufs_keys = Binary_Encoding.split_length_item_encoded_buffer(buf2);


                    //let remove_kp = false;
                    //console.log('arr_bufs_kv[0]', arr_bufs_kv[0]);
                    //console.log('arr_bufs_kv', arr_bufs_kv);
                    //throw 'stop';
                    //console.log('2) * remove_kp', remove_kp);
                    let arr_decoded = Model_Database.decode_keys(arr_bufs_keys, remove_kp);

                    res.raise('next', arr_decoded);
                    //console.log('idx, page_number', idx, page_number);
                    this.send_message_receipt(idx, page_number);
                }
                if (message_type === KEY_PAGING_LAST) {

                    // 


                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);
                    [page_number, pos] = xas2.read(buf_the_rest, 0);
                    //console.log('page_number', page_number);
                    let buf2 = Buffer.alloc(buf_the_rest.length - pos);
                    buf_the_rest.copy(buf2, 0, pos);
                    let arr_bufs_k = Binary_Encoding.split_length_item_encoded_buffer(buf2);
                    //let remove_kp = false;
                    //console.log('arr_bufs_kv', arr_bufs_kv);
                    //console.log('3) * remove_kp', remove_kp);
                    let arr_decoded = Model_Database.decode_keys(arr_bufs_k, remove_kp);

                    res.raise('next', arr_decoded);
                    res.raise('complete', arr_decoded);
                    this.send_message_receipt(idx, page_number);
                }

            } else {
                if (message_type === BINARY_PAGING_NONE) {
                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);
                    res.raise('next', buf_the_rest);
                    res.raise('complete', buf_the_rest);
                }
                if (message_type === BINARY_PAGING_FLOW) {
                    [page_number, pos] = xas2.read(obj_message, pos);
                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);
                    res.raise('next', buf_the_rest);
                    this.send_message_receipt(idx, page_number++);
                }
                if (message_type === BINARY_PAGING_LAST) {
                    [page_number, pos] = xas2.read(obj_message, pos);

                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);

                    //console.log('buf_the_rest', buf_the_rest);

                    res.raise('next', buf_the_rest);
                    res.raise('complete');
                    this.send_message_receipt(idx, page_number++);
                }

                if (message_type === RECORD_PAGING_NONE) {
                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);
                    res.raise('next', buf_the_rest);
                    res.raise('complete');
                }
                if (message_type === RECORD_PAGING_FLOW) {
                    [page_number, pos] = xas2.read(obj_message, pos);
                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);


                    if (res.unpaged || str_result_grouping === 'single') {

                        // Split.

                        let arr_bufs_kv = Binary_Encoding.split_length_item_encoded_buffer_to_kv(buf_the_rest);
                        each(arr_bufs_kv, item => res.raise('next', item));
                        //let s = Binary_Encoding.spli

                        //each(arr_decoded, item => res.raise('next', item));
                    } else {
                        res.raise('next', buf_the_rest);
                    }



                    this.send_message_receipt(idx, page_number++);
                }
                if (message_type === RECORD_PAGING_LAST) {
                    [page_number, pos] = xas2.read(obj_message, pos);
                    //console.log('page_number', page_number);
                    var buf_the_rest = Buffer.alloc(obj_message.length - pos);
                    obj_message.copy(buf_the_rest, 0, pos);
                    // Series of buffer pairs.
                    //res.raise('next', buf_the_rest);

                    if (res.unpaged || str_result_grouping === 'single') {

                        // Split.

                        let arr_bufs_kv = Binary_Encoding.split_length_item_encoded_buffer_to_kv(buf_the_rest);
                        each(arr_bufs_kv, item => res.raise('next', item));
                        //let s = Binary_Encoding.spli

                        //each(arr_decoded, item => res.raise('next', item));
                    } else {
                        res.raise('next', buf_the_rest);
                    }

                    res.raise('complete', buf_the_rest);
                    this.send_message_receipt(idx, page_number++);
                }
            }
            // Could look in the response to see what message we have, if it indicates paging.
            //  However, it's a binary message

            //callback(null, obj_message);
            //ws_response_handlers[idx] = null;
        };

        //this.websocket_connection.sendBytes(buf_2);
        this.websocket_client.send(buf_2);

        //console.log('1) res.unpaged', res.unpaged);

        // Incorporating a stop function.
        //  Want it to get a response to the stop function.

        res.stop = () => {


            this.send_stop_command(idx);
        }


        return res;

    }


    // And return options could have options that guide client-side processing, such as whether or not to decode.


    //  return options could ask for a promise rather than observable.
    // send(message_type_id, message_args, return_options, [callback])


    // Send does not decode.


    // Have a function above which is simpler still.
    //  Observable only, no callback.

    cb_send_command(message_type_id, message_args, callback) {
        // No paging

        // The message args could be or contain a key set.
        //  The message args being a key set requiring a specific type of encoding?

        let message_id = this.id_ws_req++;
        // Better to use a Command_Message
        //let cm = new Command_Message();

        // The message args could be something that renders directly.
        //  Also message encoding type / communications options.

        // Some kind of buffer type int will help.
        //  Make it so it's very clear what kind of data a message holds.

        // Could use the JS Set object... exept they are all keys.

        // check if the message args are convertable to a buffer.
        //  A set of keys that is easy to put together would be great.
        //  Should become standard in some ways, and the Key_Set object would make the code more readable.

        // Would help to power ops like deleting a bunch of keys, checking which keys are present.


        // More advanced typing of the message args.
        //  They may be a list of keys.
        //  Not sure we want more complexity here though.

        // Getting params to the server so the server knows they are a collection of keys will be very helpful.

        // Try being able to encode and decode keys easily.
        //  Likely that keys would be held in encoded form.
        //   Would have a few int variables that show what part of the key is where
        //    Where each field is within the key.
        //     Flattened?
        //   There can be compound fields within keys.




        // if the message args are all buffers...

        // Just given one buffer...

        // A version to send this as a Key_List?

        let buf_msg_args;

        console.log('message_args', message_args);


        if (!Array.isArray(message_args)) {


            // could it be a buffer

            //if ()

            //console.log('message_args', message_args);
            //console.log('message_args' + message_args);

            // the message args could be a buffer backed key list.

            // Trouble is, Buffer has .buffer

            console.log('!!message_args.buffer', !!message_args.buffer);





            if (message_args.buffer) {


                if (message_args instanceof Buffer) {
                    console.log('it is a buffer');
                    buf_msg_args = Binary_Encoding.encode_to_buffer([message_args]);
                } else {
                    buf_msg_args = message_args.buffer;
                }

                // A way of declaring that a bunch of encoded items together are a key list
                //  That way they can be decoded as a key list.
                //   They would be in the messages encoded as keys, want an easy to use an unified way for sending these keys.

                // Then they will be decoded as Key_List on the server. Would then be able to check the type of the decoded data.
                //  That way the server handler functions could each use the same method to decode data.
                //   It would know what type of data to expect in various situations, and possibly do some instanceof checking to see what data types it's been given,
                //    maybe doing some conversions on the server.

                // Key_List, buffer backed Key and Value will be closer to the way it is within the messages and the DB.
                //  Will require less encoding and decoding.






            } else {
                buf_msg_args = Binary_Encoding.encode_to_buffer(message_args);
            }

            //throw 'stop';


            //message_args = [message_args];

        } else {
            buf_msg_args = Binary_Encoding.encode_to_buffer([message_args]);
        }

        //console.log('message_args', message_args);
        //console.trace();
        //throw 'stop';

        console.log('buf_msg_args', buf_msg_args);

        let buf = Buffer.concat([xas2(message_id).buffer, xas2(message_type_id).buffer, xas2(NO_PAGING).buffer, buf_msg_args]);

        //console.log('* buf', buf);

        this.setup_binary_no_paging_no_decode_handler(message_id, callback);

        //console.log('* pre send buf', buf);
        this.websocket_client.send(buf);



        //this.send_binary_message(buf, );

    }

    send(message_type_id, message_args, return_options, callback) {
        // Should probably use inner observable send?

        // default return options too

        return_options = return_options || new Paging.Record_Paging(1024);


        // Don't want to set decoding to true.
        //return_options.decode = true;

        return_options.remove_kps = true;

        // Except different functions will either be called with an observable or with a callback, could try to use observable_send_binary_message for all calls, see how that works, and then possibly change interfaces / APIs
        //  to accommodate that.

        // return_options is more like a Message_Options_Helper.
        //  or communication / command options
        let arr_bufs = [xas2(message_type_id).buffer, return_options.buffer, Binary_Encoding.encode_to_buffer(message_args)];
        let buf_msg = Buffer.concat(arr_bufs);

        // 



        // May have own send binary message code here.
        //  We have already set the 'decode' value.
        //  Remove_kp will already be encoded in paging / options?

        //console.log('return_options.decode', return_options.decode);

        //throw 'stop';

        let obs_res = this.observe_send_binary_message(buf_msg, return_options.decode || false, false, 'single'); // Don't want the kp to be removed client-side.

        // and we get back pages here.
        //  assume the client wants individual records.

        // observable wrapper

        // then if we want the results decoded, we can put it through an observable_decode_processor

        if (callback) {
            throw 'NYI 1';
        } else {

            // 

            return obs_res;
        }

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


    /*

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
                res.raise('next', buf_the_rest);
                res.raise('complete');
            }
            if (message_type === PAGING_FLOW) {
                res.raise('next', buf_the_rest);
            }
            if (message_type === PAGING_LAST) {
                res.raise('next', buf_the_rest);
                res.raise('complete');
            }
        };
        this.websocket_connection.sendBytes(buf_2);
        return res;
    }
    */

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


        // This could have an optional callback.


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

        // Paging object could be included in the observe_send_binary_message
        //  Or use observe_send_binary_paging_message
        //   observe_send_binary_nopaging_message could be for internal use.

        let obs_msg = this.observe_send_binary_message(buf_query, decode);

        // We could return that observable, or a different observable if there is some processing to do.
        /*
        obs_msg.subscribe('next', page => {
            //console.log('page', page);
        })
        obs_msg.subscribe('error', page => {
            //console.log('page', page);
        })
        obs_msg.subscribe('completed', () => {
            //console.log('completed');
        });
        */
        return obs_msg;
        /*

        
            next — Called each time the observable emits a value.
            error — Called when the observable encounters an error or fails to generate the data to emit. After an error, no further values will be emitted, and `completed` will not be called.
            completed — Called after it has called `next` for the final time, but only if no errors were encountered.

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


    // Check to see if the key prefix is given as a buffer.
    //  Looks like we are changing this so that it's not its own ll version.
    //  Paging and decoding is specified on the protocol level, and is within the handlers now.


    // Observer will be very useful for getting a load of records back over a bit of time.
    //  Will need to test it working on some larger datasets, such as a server which has been running for days.
    //  Then it will be useful for full / partial syncing.

    // Could get intermediate keys with a special type of (paging) call.
    //  For 1 million records, could get back every 1000th key, then do range download.
    //  Or for 1b records, could get every millionth key, or every thousanth, and still have more reasonable amounts to download (as pages anyway).
    //   Would be better for getting progress to divide it up into 1000 tasks.
    //    Could have checksums used too for verification.

    // Need to create and test the paging and observable usages of these functions.




    // Paging, decoding option, callback or observable.
    // A limit option in the request would be useful.
    //  



    // Also, option to remove the kp from the result.
    //  useful when getting records we know are within one table.


    // A lower level version that automatically uses paging would help.
    //  Does not automatically decode, but a decoding wrapper function should be easy enough.

    ll_get_records_by_key_prefix(key_prefix, paging, decode = false, remove_kps = false, callback) {

        // Should probably use a sig test in the client.
        //  Maybe will want decoding in the client too.
        //console.log('ll_get_records_by_key_prefix');


        // Expand this, and ll_get_records_in_range, so that they return an observable if no callback function is provided.
        //  Would have a default paging option (or use the server default).

        // Should call ll_get_records_in_range using an observable.

        // An inner observable would be a reasonable style here.

        let a = arguments,
            sig = get_a_sig(a);
        let buf_key_prefix;


        // With observables, default paging

        //const obs_default_paging = new Paging.Count(1024);

        //console.log('ll_get_records_by_key_prefix sig', sig);

        if (sig === '[n,o]') {
            buf_key_prefix = xas2(key_prefix).buffer;

        } else if (sig === '[n,f]') {
            buf_key_prefix = xas2(key_prefix).buffer;
            paging = new Paging.None();
            callback = a[1];
        } else if (sig === '[B,o]') {
            buf_key_prefix = key_prefix;
        } else if (sig === '[B]') {
            buf_key_prefix = key_prefix;
            paging = new Paging.Count(1024);
        } else if (sig === '[B,f]') {
            buf_key_prefix = key_prefix;
            callback = a[1];
            paging = new Paging.None();

            // [n,b,f]
        } else if (sig === '[B,b,f]') {
            buf_key_prefix = key_prefix;
            decode = a[1];
            callback = a[2];
            paging = new Paging.None();

            // [n,b,f]
        } else if (sig === '[n,b,f]') {
            buf_key_prefix = xas2(key_prefix).buffer;
            decode = a[1];
            paging = new Paging.None();
            callback = a[2];

        } else if (sig === '[n,b,b,f]') {
            buf_key_prefix = xas2(key_prefix).buffer;
            decode = a[1];
            remove_kps = a[2];


            paging = new Paging.None();
            callback = a[3];

        } else if (sig === '[n,b,o]') {
            //buf_key_prefix = xas2(key_prefix).buffer;
            //decode = a[1];
            //paging = new Paging.None();
            //callback = a[2];
            throw 'stop';
        } else if (sig === '[n,o,b]') {
            buf_key_prefix = xas2(key_prefix).buffer;
            //decode = a[1];
            //paging = new Paging.None();
            //callback = a[2];
            //throw 'stop';
        } else if (sig === '[n,o,b,b]') {
            buf_key_prefix = xas2(key_prefix).buffer;
            //decode = a[1];
            //paging = new Paging.None();
            //callback = a[2];
            //throw 'stop';
        } else {




            console.trace();
            throw 'Unexpected sig to ll_get_records_by_key_prefix, sig: ' + sig;
        }

        //throw 'stop';

        //var buf_kp = xas2(key_prefix).buffer;
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_key_prefix, buf_0]);
        var buf_u = Buffer.concat([buf_key_prefix, buf_1]);

        //console.log('buf_l', buf_l);
        //console.log('buf_u', buf_u);

        // This looks like it will need to handle index records too.

        if (callback) {
            this.ll_get_records_in_range(buf_l, buf_u, paging, decode, remove_kps, callback);
        } else {
            return this.ll_get_records_in_range(buf_l, buf_u, paging, decode);
        }
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

    // This will be expanded, like get_records_by_key_prefix

    /*
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
    */

    // Paging options, decoding option
    //  Expanding paging seems sensible, allow it to express a limit, and reverse.

    // Seems like repeating paging options but with more complex options to parse looks like the right way.
    //  Want to have some kind of flexible QueryResultsOptions, where we can give it a limit and tell it to go in reverse.
    //  Getting the very last record in the range would help it to check the incrementors match what they should.
    //   Want to get the last record in an efficient way, general functionality to do with limits and reverse option would help.


    // option to remove the key prefix from the results would help.
    //  when getting data for just one table, it would be nice to discard this.

    // Could also be nice to make it an option on the server that can be handled automatically by an output layer.

    // By default it's sensible to discard the key prefix.
    //  Worth having it as an option. Will make default = false for the moment.


    ll_get_keys_by_key_prefix(key_prefix, paging, decode = false, remove_kp = false, callback) {

        // Should probably use a sig test in the client.
        //  Maybe will want decoding in the client too.
        //console.log('ll_get_records_by_key_prefix');


        // Expand this, and ll_get_records_in_range, so that they return an observable if no callback function is provided.
        //  Would have a default paging option (or use the server default).

        // Should call ll_get_records_in_range using an observable.

        // An inner observable would be a reasonable style here.

        let a = arguments,
            sig = get_a_sig(a);
        let buf_key_prefix;

        //console.log('ll_get_records_by_key_prefix sig', sig);

        if (sig === '[n,o]') {
            buf_key_prefix = xas2(key_prefix).buffer;

        } else if (sig === '[n,f]') {
            buf_key_prefix = xas2(key_prefix).buffer;
            paging = new Paging.None();
            callback = a[1];
        } else if (sig === '[B,o]') {
            buf_key_prefix = key_prefix;
        } else if (sig === '[B,f]') {
            buf_key_prefix = key_prefix;
            callback = a[1];
            paging = new Paging.None();

            // [n,b,f]
        } else if (sig === '[n,b,f]') {
            buf_key_prefix = xas2(key_prefix).buffer;
            decode = a[1];
            paging = new Paging.None();
            callback = a[2];

        } else if (sig === '[n,b,o]') {
            //buf_key_prefix = xas2(key_prefix).buffer;
            //decode = a[1];
            //paging = new Paging.None();
            //callback = a[2];
            throw 'stop';
        } else if (sig === '[n,o,b]') {
            buf_key_prefix = xas2(key_prefix).buffer;
            //decode = a[1];
            //paging = new Paging.None();
            //callback = a[2];
            //throw 'stop';
        } else if (sig === '[n,o,b,b]') {
            buf_key_prefix = xas2(key_prefix).buffer;
            //decode = a[1];
            //paging = new Paging.None();
            //callback = a[2];
            //throw 'stop';
        } else {




            console.trace();
            throw 'Unexpected sig to ll_get_keys_by_key_prefix, sig: ' + sig;
        }

        //throw 'stop';

        //var buf_kp = xas2(key_prefix).buffer;
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_key_prefix, buf_0]);
        var buf_u = Buffer.concat([buf_key_prefix, buf_1]);

        if (callback) {
            this.ll_get_keys_in_range(buf_l, buf_u, paging, decode, remove_kp, callback);
        } else {
            return this.ll_get_keys_in_range(buf_l, buf_u, paging, remove_kp, decode);
        }
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
    ll_get_keys_in_range(buf_l, buf_u, paging, decode = false, remove_kp = false, callback) {

        let a = arguments;
        let sig = get_a_sig(a);

        //console.log('ll_get_keys_in_range sig', sig);

        //throw 'stop';

        // When used with a callback, this should not return an observer.

        if (sig === '[B,B]') {
            // Paging object not given
            var paging = new Paging.None();
            var buf_command = xas2(LL_GET_KEYS_IN_RANGE).buffer;
            var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);

            // 
            throw 'stop';
            let obs = this.observe_send_binary_message(buf_query, (err, res_binary_message) => {
                if (err) {
                    callback(err);
                } else {
                    var arr_key_buffers = Binary_Encoding.split_length_item_encoded_buffer(res_binary_message);
                    callback(null, arr_key_buffers);
                }
            });
        } else if (sig === '[B,B,f]') {
            // last param is a callback

            var paging = new Paging.None();
            var buf_command = xas2(LL_GET_KEYS_IN_RANGE).buffer;
            var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);
            callback = a[2];


            this.send_binary_message(buf_query, (err, res_binary_message) => {
                if (err) {
                    callback(err);
                } else {
                    var arr_key_buffers = Binary_Encoding.split_length_item_encoded_buffer(res_binary_message);
                    callback(null, arr_key_buffers);
                }
            });



        } else if (sig === '[B,B,o,b]') {

            // Could even have a server-side option to remove the KPs. This would save data in transit.


            var buf_command = xas2(LL_GET_KEYS_IN_RANGE).buffer;
            var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);

            let obs = this.observe_send_binary_message(buf_query, decode);
            return obs;



        } else if (sig === '[B,B,o,b,b]') {

            // Could even have a server-side option to remove the KPs. This would save data in transit.


            var buf_command = xas2(LL_GET_KEYS_IN_RANGE).buffer;
            var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);
            console.log('remove_kp', remove_kp);
            let obs = this.observe_send_binary_message(buf_query, decode, remove_kp);
            return obs;



        } else {
            console.log('sig', sig);

            console.log('a', a);
            throw 'NYI';
        }



    }


    /*

    ll_get_buf_records_in_range(buf_l, buf_u, paging, callback) {

        let args = this.arguments;
        let l = args.length;


        if (l === 2) {
            // Just the two buffers

            // Default paging. Try 1024 / default_page_size records.
            //paging = new Paging


        }

        if (l === 3) {
            //let sig = get_item_sig(args);
            if (a[2] instanceof Paging) {

            }
            if (typeof a[2] === 'function') {
                paging = null;
                callback = a[2];
            }
        }



        // If it's done with a callback, don't use the observable

        // Would be good to have polymorphism / optional paging.

        // Be able to return an observable.
        //  Will send the observable or standard messages.

        // Will have a get_records_in_range function here, as it will use the automatic decoding capabilities too.
        //  It's only rarely that we don't want decoding.


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

    */

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


    // optional decoding parameter here too

    // Having this function more advanced will be great for syncing the DB / copying tables.
    //  Getting key samplings would be useful to start with, to get queries that would return close to a known amount.

    // Maybe this could even be expanded to have a 'limit' option, so no need for the 'up to' version?

    // Could have paging option.
    //  Maybe we want silent paging in the background though?
    //   That will maybe happen when using a callback with no paging option specified.

    // Maybe move to main non ll part because it uses the core model, which is not available here.




    // decode option too...

    // decoding option of removing table key prefixes.
    //  



    // Definitely worth making this function simpler again.
    //  Want an optional decoding layer after this function, could maybe do get_records_in_range = hl(ll_get_records_in_range)
    //   An hl function would be useful for dealing with a variety of server-side options.
    //    Focus won't be on encoding data for transmission, but on decoding it for presentation of higher level answers.

    // Paging will be set by default. May be able to specify page size in calling it, but that's less important.
    //  ll functions in the future will always return observables. Wrapper functions could callbackify results.

    ll_get_records_in_ranges(arr_ranges) {

        // ll_send_paged_command?
        //  as it will return full pages.
        //  Don't want to have to deal with options, can decode pages later.

        return this.send_paged_command(LL_GET_RECORDS_IN_RANGES, arr_ranges);
    }



    // Not so sure that this unpages the records.
    //  In general, we always want records to be unpaged when they arrive back, but need to have it as an optional part of the process.
    //   A property on the observable would help with this, as it's not a param that really is best passed into the front of the function call.
    //   Having unpaging on by default will help.

    // observe_send_binary_message could have something to break up the pages.
    //  The new command-response-message has got functionality to help with this.
    //   Worth implementing it in observe_send_binary_message



    ll_get_records_in_range(buf_l, buf_u, paging, decode = false, remove_kps = false, callback) {
        // 
        // Could have paging and observable options here in this function.
        //  Basically need to implement them all over the place in a flexible way.

        // Could be called with a paging option

        // Will be used by the observable get_table_records.

        // The paging info could be changed to Requested_Response_Format
        //  Could also say a limit to how many records to retrieve.


        //let decode = true;


        let a = arguments,
            sig = get_a_sig(a);

        //console.log('ll_get_records_in_range sig', sig);

        // Possibly there will be a decode option too.


        if (sig === '[B,B,f]') {
            paging = new Paging.None();
            callback = a[2];
        } else if (sig === '[B,B]') {
            //paging = new Paging.None();
            //callback = a[2];
        } else if (sig === '[B,B,o]') {
            //paging = new Paging.None();
        } else if (sig === '[B,B,o,f]') {
            //paging = new Paging.None();
            callback = a[3];
        } else if (sig === '[B,B,o,b,f]') {
            callback = a[4];
            remove_kps = false
            //paging = new Paging.None();
            //callback = a[3];
        } else if (sig === '[B,B,o,b]') {
            //paging = new Paging.None();
            //callback = a[3];
        } else if (sig === '[B,B,o,b,b,f]') {
            //paging = new Paging.None();
            //callback = a[3];
        } else {
            console.log('sig', sig);
            console.trace();
            throw 'stop';
        }

        const default_record_page_size = 8192;

        paging = paging || new Paging.Record(default_record_page_size);

        var buf_command = xas2(LL_GET_RECORDS_IN_RANGE).buffer;
        //console.log('buf_command', buf_command);
        //console.log('paging.buffer', paging.buffer);

        // the lengths of the buffers too...
        var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);

        //console.log('!!callback', !!callback);

        if (callback) {
            // Do this using the callback style call.
            // buffer, buffer, obj, fn
            // buffer, buffer, callback - no paging, decode the incoming data, return it with callback=

            // console.log('sig', sig);

            // What type of paging?
            //  For the moment, no paging.

            // Do it this way if we are not using an observable

            //console.log('pre this.send_binary_message, with cb', buf_query);


            // and another option for send_binary_message

            this.send_binary_message(buf_query, RECORD_PAGING_NONE, decode, remove_kps, (err, res_binary_message) => {
                if (err) {
                    callback(err);
                } else {
                    // But if it's an error message, should call the error callback.
                    //  Likely at an earlier stage.

                    //console.log('res_binary_message', res_binary_message);

                    //var arr_kv_buffers = Binary_Encoding.split_length_item_encoded_buffer_to_kv(res_binary_message);
                    //console.log('arr_kv_buffers', arr_kv_buffers);

                    // Get used to decoding / splitting this buffer at a later stage?

                    //console.log('decode', decode);

                    //console.log('res_binary_message', res_binary_message);
                    //throw 'stop';

                    callback(null, res_binary_message);
                }
            });

        } else {
            // Observer call


            // Use the observe_send_binary_message
            //console.log('buf_query', buf_query);

            //let obs = this.observe_send_binary_message(buf_query, paging.buffer, decode);

            // Don't set the paging here, as it's automatic, behind the scenes using observe_send_binary_message

            let obs = this.observe_send_binary_message(buf_query, decode, remove_kps);

            // Want this to be stoppable.

            //console.log('obs.unpaged', obs.unpaged);
            return obs;
            //throw 'NYI';



        }



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

    ll_delete_records_by_keys(arr_keys, callback) {
        // DELETE_RECORDS_BY_KEYS

        // Would send the keys encoded as an array.
        //  Using a Key_Set, maybe Key class would help them to be encoded easily, and provide more readable code because we know that it's a key set.

        // new Key_Set(arr_keys)
        //  Could give a Key_Set its own constant to identify it.
        //  

        console.log('DELETE_RECORDS_BY_KEYS arr_keys', arr_keys);

        let kl_keys = new Key_List(arr_keys);
        console.log('kl_keys', kl_keys);
        //throw 'stop';

        // Be able to get they keys as decoded arrays.

        //lk_keys.decoded


        // then use send command with the key list.

        this.cb_send_command(DELETE_RECORDS_BY_KEYS, kl_keys, callback);
        //this.cb_send_command(DELETE_RECORDS_BY_KEYS, arr_keys, callback);
    }

    ll_get_first_key_beginning(buf_beginning, callback) {
        // want to send command, getting a callback

        this.cb_send_command(LL_GET_FIRST_KEY_BEGINNING, buf_beginning, callback);


    }
    ll_get_last_key_beginning(buf_beginning, callback) {



        this.cb_send_command(LL_GET_LAST_KEY_BEGINNING, buf_beginning, callback);
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
    ll_count_keys_in_range(buf_l, buf_u, limit = -1, callback) {
        let a = arguments,
            l = a.length;
        if (l === 3) {
            callback = a[2];
            limit = -1;
        }

        var paging;

        //console.log('client ll_count_keys_in_range');

        // Don't include the limit option if it's -1?
        //  or 0.
        //  Think it could be the the 3rd param when in the more advanced paging and options mode.


        if (callback) {
            paging = new Paging.None();
        } else {
            paging = new Paging.Timed(1000);
        }

        // Then this paging object can specify the limit.

        if (limit > 0) {
            paging.limit = limit;
        }

        var buf_command = xas2(LL_COUNT_KEYS_IN_RANGE).buffer;
        // the lengths of the buffers too...
        var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);
        //var buf_l = 
        // Include a paging buffer too...?

        //console.log('buf_query', buf_query);


        if (callback) {
            this.send_binary_message(buf_query, (err, res_binary_message) => {
                if (err) {
                    callback(err);
                } else {
                    var count, pos;
                    [count, pos] = xas2.read(res_binary_message, 0);
                    callback(null, count);
                }
            });
        } else {
            // Observe send binart message could have nicer message encoding.
            //  could use .send
            //console.log('pre obs send');
            return this.observe_send_binary_message(buf_query, true);
        }
    }


    // Having a 'limit' option within Paging / Options makes a lot of sense.
    //  Should be able to put together options objects, and have them encoded and decoded.
    //  Paging / Options and Binary_Encoding will be used to provide more speed and flexibility, while having DB core functions do the core things.

    // May move away from decoding and removing KP in the core db functions.
    //  ll versions again, and with good reasons.
    //  Don't call ll, just have in ll_nextleveldb_server.
    //     nextleveldb_coreio_server
    //  // nextleveldb_ll_server
    //     nextleveldb_server
    //     nextleveldb_safer_server
    //     (nextleveldb_localmulti_server)    - would have other local instances of NextLevelDB_Server. These would be used by other layers
    //      could have records of whenever any key was added to the db
    //      could have repeatable / undoable storage of the various operations that take place.
    //       Could be useful in terms of getting back to a previous state while also using 
    //     nextleveldb_p2p_server





    // LL_COUNT_KEYS_IN_RANGE_UP_TO
    ll_count_keys_in_range_up_to(buf_l, buf_u, limit, callback) {
        var paging = new Paging.None();
        var buf_command = xas2(LL_COUNT_KEYS_IN_RANGE_UP_TO).buffer;
        // the lengths of the buffers too...


        // send_key_range_message
        //  Could include the limit in the paging buffer?

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

                // empty record...

                if (res_binary_message.length === 0) {
                    //console.log('ll_get_record res_binary_message', res_binary_message);
                    callback(null, undefined);
                } else {
                    //console.log('ll_get_record res_binary_message', res_binary_message);
                    callback(null, res_binary_message);
                }



            }
        });
    }

    // Getting table fields data to the client, 
    get_table_field_info(table, callback) {
        // Want this to be more polymorphic.

        //  May need to ensure the model is OK to start with.
        // Probably worth consulting the model.

        // Table could either be an int or string.

        let table_name, table_id;
        let t_table = tof(table);
        if (t_table === 'string') {
            table_name = table;
        }
        if (t_table === 'number') {
            table_id = table;
        }

        // should call it using the table id, as it makes for a smaller message.

        if (table_id === undefined) {
            this.get_table_id_by_name(table_name, (err, id) => {
                if (err) {
                    callback(err);
                } else {
                    table_id = id;
                    proceed();
                }
            })
        }

        let proceed = () => {
            console.log('table_id', table_id);

            // Send the message

            // Other, easier to use message sending functions would be useful here.
            //  builds up the binary buffer itself. Always set to decode on receipt.

            // could upgrade send_binary_message, so if it does not have buffers to start with, it encodes the data into buffers.


            /*
            let model_table = this.model.map_tables_by_id(table_id);

            let model_fields = model_table.fields;
            console.log('model_fields', model_fields);

            each(model_fields, model_field => {
                console.log('model_field', model_field);
            })

            */

            //var buf_command = xas2(GET_TABLE_FIELDS_INFO).buffer;

            // Decoding wanted.

            // 
            var buf_query = Buffer.concat([xas2(GET_TABLE_FIELDS_INFO).buffer, xas2(table_id).buffer]);
            //var buf_l = 
            // Include a paging buffer too...?

            console.log('get_table_field_info buf_query', buf_query);

            this.send_binary_message(buf_query, BINARY_PAGING_NONE, true, (err, table_fields_info) => {
                if (err) {
                    callback(err);
                } else {
                    //var count, pos;
                    //[count, pos] = xas2.read(res_binary_message, 0);
                    //console.log('table_fields_info', table_fields_info);
                    callback(null, table_fields_info);
                }
            });





        }



    }


    // May need a paged version of this soon.



    /**
     * 
     * 
     * @param {any} paging 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */

    // Will change to an observable interface.
    //  However, could have an opt_cb function to allow the function to execute with a callback, possibly building up the data using an observable on the client.
    //  Generally want to encourage use of paging while communicating, even if the whole data structure is requested.
    //  


    // For the moment, should change quite a few functions to an Observer API.
    //  


    'll_get_all_keys' (paging, decode = false) {

        // Could use fn sigs, maybe will call this with a callback function.




        let a = arguments;
        a.l = a.length;

        let sig = get_item_sig(a);

        console.log('sig', sig);
        console.trace();
        throw 'stop';



        // Polymorphism here?
        //  Allow it to use a callback rather than observable?


        /*
        var buf_query, pos = 0;
        if (!callback) {
            callback = arguments[0];
            paging = new Paging.None();
        }

        */
        //if (!paging instanceof Paging) paging = new Paging.Record_Paging(paging);
        //buf_query = Buffer.concat([xas2(LL_GET_ALL_KEYS).buffer, paging.buffer]);


        // use an observer instead.
        //  Maybe just in the case of paging.



        var buf_query, pos = 0;
        if (!paging instanceof Paging) paging = new Paging.Record_Paging(paging);
        buf_query = Buffer.concat([xas2(LL_GET_ALL_KEYS).buffer, paging.buffer]);
        let obs_msg = this.observe_send_binary_message(buf_query, decode);
        return obs_msg;





        /*
        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                callback(null, res_binary_message);
            }
        });
        */
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

        let a = arguments,
            delay, paging, decode = true;
        a.l = a.length;
        let sig = get_a_sig(a);
        //console.log('sig', sig);
        //console.log('a.l', a.l);

        if (sig === '[]') {
            delay = 1000;
            paging = new Paging.Timed(delay);
        }
        if (sig === '[f]') {

        }
        if (sig === '[n]') {
            delay = a[0];
            paging = new Paging.Timed(delay);
            callback = null;
        }

        //throw 'stop';

        // Can be given a paging param rather than 


        // Could be called without a callback, meaning we get an observer back.

        if (callback) {
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
        } else {
            console.log('ll_count_records with observer');

            // would 

            if (!paging instanceof Paging) paging = new Paging.Timed(paging);
            console.log('paging', paging);
            buf_query = Buffer.concat([xas2(LL_COUNT_RECORDS).buffer, paging.buffer]);


            let obs_msg = this.observe_send_binary_message(buf_query, decode);

            // Will be getting periodic counts back from the server.
            //  They will be binary records.
            //  The server sill see that timed paging is being requested.

            //throw 'NYI';

            // Will use the observer message system.
            return obs_msg;

        }


    }


    // another put function. would load the data into the OO class thing.





    // May use Record_List in the future
    /**
     * 
     * 
     * @param {buffer} buf_records 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    'll_put_records_buffer' (buf_records, callback) {
        // PUT_RECORDS

        // Output decoding looks like it would be useful here.

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

        let a = arguments,
            sig = get_a_sig(a);

        // Without a callback, will use an observable / promise
        //  or Resolvable.

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

    'ensure_tables' (arr_tables, decode = true, callback) {
        let a = arguments,
            sig = get_a_sig(a);

        if (sig === '[a,f]') {
            decode = true;
            callback = a[1];
        }
        if (sig === '[a,b]') {
            //decode = true;
        }
        if (sig === '[a,b,f]') {

        }

        //console.log('client arr_tables', arr_tables);

        let buf_encoded_tables = Binary_Encoding.flexi_encode_item(arr_tables);
        var buf_query = Buffer.concat([xas2(ENSURE_TABLES).buffer, buf_encoded_tables]);


        // Need to have the server deal with the observer version.
        //  Nicer to have a message for each table.
        //console.log('decode', decode);
        //throw 'stop';
        let obs_res = this.observe_send_binary_message(buf_query, decode);
        console.log('obs_res', obs_res);
        //throw 'stop';
        if (callback) {
            throw 'NYI'
        } else {
            // 
            return obs_res;

            /*
            if (decode) {
                // An observable map would help with this.???

            } else {

            }
            */
        }

    }

    'table_exists' (table_name, callback) {
        // Binary callback mechanism.
        // Inner function using a callback
        let inner = (icb) => {
            let buf_encoded = Binary_Encoding.flexi_encode_item(table_name);
            var buf_query = Buffer.concat([xas2(TABLE_EXISTS).buffer, buf_encoded]);
            this.send_binary_message(buf_query, (err, exists) => {
                if (err) {
                    icb(err);
                } else {
                    icb(null, exists);
                }
            })
        }

        if (callback) {
            inner(callback);
        } else {
            // Return a promise / observer / another resolvable.
            throw 'NYI';
        }


    }

    // Maintaining a cache, rather than memoization would be best.
    //  The cache could be deleted / invalidated at some points in time.

    'get_table_id_by_name' (table_name, callback) {
        console.log('get_table_id_by_name table_name', table_name);

        let cache = this.table_id_by_name_cache = this.table_id_by_name_cache || {};
        let cached;
        let inner = (icb) => {

            cached = cache[table_name];
            if (typeof cached === 'undefined') {
                let buf_encoded = Binary_Encoding.flexi_encode_item(table_name);
                var buf_query = Buffer.concat([xas2(TABLE_ID_BY_NAME).buffer, buf_encoded]);

                console.log('pre table id lookup');
                this.send_binary_message(buf_query, BINARY_PAGING_NONE, true, (err, table_id) => {
                    if (err) {
                        icb(err);
                    } else {
                        console.log('table_id', table_id);
                        cache[table_name] = table_id;
                        icb(null, table_id);
                    }
                })
            } else {

                process.nextTick(() => {
                    icb(null, cached);
                })


            }

        }
        if (callback) {
            inner(callback);
        } else {
            // Return a promise / observer / another resolvable.
            //throw 'NYI';
            return new Promise((resolve, reject) => {
                inner((err, res) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(res);
                    }
                })
            })
        }

    }

    // A paging definition object may help.
    //  Could help on the server side, reading the paging definition from the buffer
}


let p = LL_NextLevelDB_Client.prototype;

p.get_records_by_key_prefix = p.ll_get_records_by_key_prefix;
p.get_keys_by_key_prefix = p.ll_get_keys_by_key_prefix;

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
    var config = require('my-config').init({
        path: path.resolve('../../config/config.json') //,
        //env : process.env['NODE_ENV']
        //env : process.env
    });
    let access_token = config.nextleveldb_access.root[0];

    /*
        var app_config = require('my-config').init({
            path: path.resolve('./app-config.json') //,
            //env : process.env['NODE_ENV']
            //env : process.env
        });
    
        Object.assign(config, app_config);
        */


    // data1
    //var server_data1 = config.nextleveldb_connections.data1;
    var server_data1 = config.nextleveldb_connections.localhost;


    server_data1.access_token = access_token;


    console.log('access_token', access_token);

    var lc = new LL_NextLevelDB_Client(server_data1);

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
                        //console.log('count', count);
                        // want a higher level get all records too.

                        //  maybe not really worth having the ll version?

                        //  may look into observable transformers.


                        let decode = true;
                        let obs = lc.ll_get_all_records(new Paging.Record_Paging(64), decode);
                        // Need to be able to page the records into another instance of the DB.

                        // Called without a callback, with paging option, so it returns an observable which gives the results.

                        // Want a less low level version of it that decodes the records.

                        // Could use an observable transformer.

                        console.log('obs', obs);
                        let c = 0;
                        obs.subscribe('next', (res) => {
                            //console.log('obs next res', res);
                            c = c + res.length;
                            console.log('obs next res.length', res.length);
                        });
                        obs.subscribe('error', (err) => {
                            console.log('obs err', err);
                            console.trace();
                        });
                        obs.subscribe('complete', () => {
                            console.log('c', c);
                            console.log('obs complete');

                        });
                        // Paged get records is good for copying data from one db to another.
                    }
                })
            };
            //test_paged_get_all_records();


            var test_paged_get_all_keys = () => {
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

                        let obs = lc.ll_get_all_keys(new Paging.Record_Paging(64), decode);

                        console.log('obs', obs);
                        let c = 0;
                        obs.subscribe('next', (res) => {
                            console.log('obs next res', res);
                            c = c + res.length;
                            console.log('obs next res.length', res.length);
                        });
                        obs.subscribe('error', (err) => {
                            console.log('obs err', err);
                            console.trace();
                        });
                        obs.subscribe('complete', () => {
                            console.log('c', c);
                            console.log('obs complete');

                        });
                        // Paged get records is good for copying data from one db to another.

                    }
                })
            };
            //test_paged_get_all_keys();

            // Think this is higher level.
            test_paged_get_table_records = () => {

            }

            //test_paged_get_table_records();

            // This would be better with lots of da6ta

            test_timed_paging_count = () => {
                let obs_count = lc.ll_count_records();
                console.log('obs_count', obs_count);
                obs_count.subscribe('next', (res) => {
                    console.log('obs next res', res);
                    //c = c + res.length;
                    console.log('obs next res.length', res.length);
                });
                obs_count.subscribe('error', (err) => {
                    console.log('obs err', err);
                    console.trace();
                });
                obs_count.subscribe('complete', count => {
                    //console.log('c', c);
                    console.log('obs complete', count);
                });
            }
            //test_timed_paging_count();

            test_count_table_records = () => {

                // Would be an observer by default.

                // Could still work by counting keys in the ranges...
                //  Or keys beginning with something.

                // count_table_records seems like a convenient function to make lower level within the server.
                //  


                let obs_count = lc.count_table_records('bittrex market summary snapshots');
                console.log('obs_count', obs_count);
                obs_count.subscribe('next', (res) => {
                    console.log('obs next res', res);
                    //c = c + res.length;
                    console.log('obs next res.length', res.length);
                });
                obs_count.subscribe('error', (err) => {
                    console.log('obs err', err);
                    console.trace();
                });
                obs_count.subscribe('complete', count => {
                    //console.log('c', c);
                    console.log('obs complete', count);
                });

            }

            let test_get_table_fields_info = () => {

                console.log('test_get_table_fields_info');

                // Currently not getting the types
                //let table_name = 'bittrex market summary snapshots';
                //let table_name = 'tables';
                //let table_name = 'bittrex currencies';
                let table_name = 'bittrex markets';

                // Seems as though the type ids don't get added when ensuring tables.
                //  Type ids had been there at the start for the system tables.
                //  Don't want to always require type ids, but always want to provide them when possible.

                // Field info has not been added (seemingly) to tables (autoincrement int xas2 fields), seems OK in some cases.
                //  When a fk field refers to another, we now know its type.

                // We should know the types of the currency ids in market records because it refers to a table with a pk with its field type set.
                //  Should soon get on with adding records as appropriate to this existing system.

                // ensure bittrex currency
                // put market snapshot record.






                console.log('test_get_table_fields_info');
                lc.get_table_field_info(table_name, (err, fields_info) => {
                    if (err) {
                        throw err;
                    } else {
                        console.log('fields_info', fields_info);
                        each(fields_info, field_info => {
                            console.log('field_info', JSON.stringify(field_info));

                        });
                    }
                })
            }
            //test_get_table_fields_info();


            let test_get_records_in_ranges = () => {
                console.log('test_get_records_in_ranges');

                let buf_0 = Buffer.alloc(1);
                buf_0.writeUInt8(0, 0);
                let buf_255 = Buffer.alloc(1);
                buf_255.writeUInt8(255, 0);


                // So that would get the incrementor records.

                let range_1 = [Buffer.concat([xas2(0).buffer, buf_0]), Buffer.concat([xas2(0).buffer, buf_255])];
                let range_2 = [Buffer.concat([xas2(2).buffer, buf_0]), Buffer.concat([xas2(2).buffer, buf_255])];

                let obs = lc.ll_get_records_in_ranges([range_1, range_2]);
                // Should be unpaged on the client.


                obs.on('next', data => {
                    console.log('test_get_records_in_ranges data', data);
                });


            }
            test_get_records_in_ranges();



            /*
            lc.ll_count_records((err, count) => {
                if (err) {
                    throw err;
                } else {
                    console.log('count', count);
                }
            })
            */


        }
    });
    //console.log('pre get all');

    var all_data = [];

} else {
    //console.log('required as a module');
}


module.exports = LL_NextLevelDB_Client;