/**
 * Created by James on 15/10/2016.
 */

// Could use jsgui server capabilities to handle URL encoding and some other things.
// Level server could act as a resource, or extend jsgui server.
//  jsgui server is a resource?
//  not right now.

// Could be worth making an enchanced client that has ways of interacting with a Model.
//  Connect to a Database with a Model, and then use that model to persist records.
//   Could be a route into Active_Record technology.

// Server side indexing now looks like it will be most important to get right soon.
//  Having a GUI would greatly help admin, but take some time and effort to code.
//   Could be very useful / essential for running / testing some long-running queries.

// Seems important to get the current things working as well as possible.
//  Some more lower / mid level functionality towards batch putting records, and having them encoded in possibly different ways.

// Want to be able to put records in the DB with different types of encoding.
//  Possibly we could make new versions of some lower level functions and use them in the client.

// A sparate client ui app could be made.










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

//var fs = require('fs');

var Binary_Encoding = require('binary-encoding');
var Binary_Encoding_Record = Binary_Encoding.Record;

//console.log('encodings.poloniex.market', encodings.poloniex.market);

var Model = require('nextleveldb-model');
var Paging = Model.Paging;


// Could put this into node-client.
//  Would have more node specific features such as backup.
var fs = require('fs');

// single key / value get or put record, delete
// query to get list of records from what their keys begin with
//  ?key_begins
// queries to get the keys before or after any given key
//  ?key_before, ?key_after

// /kvs/[key]
// /kvs/?query

// get_obj_idx_poloniex_markets_by_id

//var levelup = require('levelup');
var request = require('request');
var protocol = 'http://';


// Making the client more advanced:
//  In conjunction with low level db operations, the client could carry out some functionality which had been done server-side.
//  Checking that a DB has the right tables. Ensuring tables exist.
// Client-side can also remember some index values, preventing them needing to be looked up.
//  When dealing with categorised timeseries values, the client could remember those categories, so that the client can substitute those categories for the ones on the server.
//  This would mean the queries could be made referring to string values on the client, and it automatically substitutes them for the integer values, and encodes the function
//   call as binary.

// Having the client able to 'tap into' the db modification system would help. The client could tell what version the db is at, and then put in place various modifications on
//  a lower level.
// Not so sure about huge amounts of changes, ie changing how an index works by removing all items and putting them back again.

// For the moment, getting the data about ig trades in a decent sequence seems like one of the most important tasks.

// /kvs/  JSON encoded POST data, with no specific URL
// Could make this an object that is controllable by something else.

// Handling closed or interrupted connections...
//  Would be nice if this buffered commands that are unable to be sent at present.
//  Then when the connection is reestablished, it clears the buffer.

// Non-streaming client, and streaming client.
//  For the moment, will have streaming methods.
//  If it's connected in streaming mode, it may transparently call the streaming methods.
//   May rename current methods with an http prefix.

// A collection of functions to call when reconnected?

// fns, and get them to go once the reconnection is done.
//  May be looking at approx 100MB of data a day.
//  Keeping it all within a highly optimised db sounds good in terms being able to load datasets for fast retrieval.
//   Clustering will also speed up the retrieval considerable, should the bottleneck be the speed at which the DB gets read through.

//   Also want to enable operations where the data is already considered loaded.

// TransferDB?
//  Removes all records (everything) / deletes and recreates the database


//  Being able to wipe all records, and replace with a new set of records seems like quite a feature for deployments.
//   stream a wipe_replace command from the client?
//    or ll_wipe command?
//       ll_put

//  Many put commands will be sent to the DB in a small space of time.
//   Will be useful to see how the put commands can be batched.

// A plugin system for the db would definitely be useful.
//  Possibly plugin functions.
//  Reads through the plugin functions, then can run them when they are called. This would separate out some proprietry features, making the core db js file smaller, making
//   editing easier, and more clearly defining the bounds of the nextleveldb open source project.

// Could make this extend evented class.
//  That means that subscriptions could operate with events.
//  It seems like .subscribe should be a feature.
//   And then it could specify an event name to be raised when the data comes in.
//   About integrating the near-real-time data updates with the familiar JavaScript methodology.

// This will be simplified down to core functionality.
//  Client won't use plugins for the moment, but will have versions that inherit from it.
//   Some non-core clients may make more of an effort to understand the core DB and its structure. Could associate tables with decoders.
// 04/11/2016 - This has been scaled significantly to core functionality. Other classes inherit from this, and then a Mixed Financial class mixes that functionality back in,
//  to make the more general purpose client that can access financial data.


// This is going to be made aware of the model.
//  It will communicate with the server in binary.
//  Likely to use some query encoding.

// Will create query in binary, and will interact with the server through a query function.
//  There will be a limited number of query types, and they will encode and decode as a model...
//   Decoding may be less important, as for performance, especially on the simpler queries to begin with, it may be best not to use OO queries on the server, and just to process
//    the instructions as they come in.
//   For more complex queries, it may be best to parse them using OO functionality.
//    Simpler ones, can use a few if statements to determine the code flow.

// For a large part, want to give the DB low level queries while having the client do encoding and decoding.
//  Indexing is where the DB may need an instance of the Model. This seems like a good way to do it.
//   The relevant Table object, or Active Record, would provide a way to encode any indexes for an item that is put into the db.

// -~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~- \\

const LL_COUNT_RECORDS = 0;
const LL_PUT_RECORDS = 1;

// USING PAGING OPTION
const LL_GET_ALL_KEYS = 2;
const LL_GET_KEYS_IN_RANGE = 3;
const LL_GET_RECORDS_IN_RANGE = 4;

const LL_COUNT_KEYS_IN_RANGE = 5;
const LL_GET_FIRST_LAST_KEYS_IN_RANGE = 6;

//const LL_COUNT_GET_FIRST_LAST_KEYS_IN_RANGE = 7;

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

// 



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
    'constructor'(spec) {
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
    'stop'(callback) {
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
    'start'(callback) {
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

		client.on('connectFailed', function(error) {
			// socket could be closed.

			if (error) {
				console.log('connectFailed error', error);
				//console.log('Object.keys(error)', Object.keys(error)); // [ 'code', 'errno', 'syscall', 'address', 'port' ]
				callback(error);
			}
			//attempting_reconnection = false;
			//var code = error.code;
			//var syscall = err.syscall;
			//console.log('Connect Error: ' + error.toString());
		});

		that.connected = false;
		var attempting_reconnection = false;

		var reconnection_attempts = function() {
			first_connect = false;

			if (that.connected === false && attempting_reconnection === false) {
				attempting_reconnection = true;
				client.connect(ws_address, 'echo-protocol');
				setTimeout(function() {
					attempting_reconnection = false;

					reconnection_attempts();

				}, 1000);
			}

		};

		var first_connect = true;

		//console.log('pre connect');
		client.on('connect', function(connection) {
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

			var assign_connection_events = function() {
				connection.on('error', function(error) {

					// Probably in response to attempting to write to a closed stream?


					//console.log('\nerror', error);
					//console.log('Object.keys(error)', Object.keys(error));
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
				connection.on('connectFailed', function(error) {

					console.log('connection failed, err', err);

					// Probably in response to attempting to write to a closed stream?

					//console.log('\nerror', error);
					//console.log('Object.keys(error)', Object.keys(error));
					//console.log("Connection Error: " + error.toString());
					//console.log('typeof error', typeof error);

					//attempting_reconnection = false;
					//reconnection_attempts();

				});
				connection.on('close', function() {
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
						//console.log("Received: '" + message.utf8Data + "'");
						//console.log('message', message);

						// Could have a message saying that a request has been completed, or started.
						//


						// Processing the responses to the streaming requests.
						//  tell if it is a block or the whole message.

						// assume whole message for the moment




						var obj_message = JSON.parse(message.utf8Data);



						//console.log('1) typeof obj_message', typeof obj_message);
						//console.log('obj_message', obj_message);

						// Seems like its been encoded wrong on the server.

						if (is_array(obj_message)) {

							// No, the result could be an array.
							//  Depends on if we are expecting an array to be returned or not.

							//  Though all returns should be an array, with the first item as the request key?
							//   The second as the results?



							var request_key = obj_message[0];

							//console.log('request_key', request_key);

							// and the response could be an array, ort one object.

							var res;

							if (obj_message.length === 2) {
								res = obj_message[1];
							} else {
								res = obj_message.slice(1);
							}

							//console.log('* res', res);

							// Call the response handler.

							//ws_response_handlers[]

							//console.log('ws_response_handlers', ws_response_handlers);
							//console.log('request_key', request_key);

							if (ws_response_handlers[request_key]) {
								ws_response_handlers[request_key](res);
							}

						}
						// The message may say it's a response.
						//  Then to which query

						if (obj_message.type === 'response') {
							// then see what the request key is
							var request_key = obj_message.request_key;

							// Is it the full response, or a chunk/block?
							//  May be known or estimated number of blocks, or unknown.

							if (ws_response_handlers[request_key]) {
								//console.log('pre call request handler');
								ws_response_handlers[request_key](obj_message);
							}
						}
                    }

                    // binary messages.

                    if (message.type === 'binary') {
                        //console.log('message', message);

                        that.receive_binary_message(message.binaryData);

                        //throw 'stop';
                    }

                    // Could have Message classes to interpret and encode messages.


				});
			};

			assign_connection_events();

			if (first_connect) {
				callback(null, true);
			};

			// Will send data, in general.
			//  It may be best if there is a response saying it has been received.

			// May also send queries.
			//  The client will then receive the response, labelled with a key identifying it as corresponding with the query, and also in chunks.

			//sendNumber();

		});
		// need the url without the protocol.



		var ws_address = this.server_url || 'ws://' + this.server_address + ':' + this.server_port + '/';
        //console.log('ws_address', ws_address);
        //console.log('pre client.connect');
		client.connect(ws_address, 'echo-protocol');
    }
    
    /**
     * 
     * 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    wipe(callback) {
        // send a wipe command to the server

        // just the encoded number 20
        //  anything for confirmations?

        // just encode num 20 for the moment

        var buf_command = xas2(LL_WIPE).buffer;
        this.send_binary_message(buf_command, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                //console.log('res_binary_message', res_binary_message);
                // console.log('res_binary_message.length', res_binary_message.length);
                // var arr_kv_buffers = Binary_Encoding.split_length_item_encoded_buffer_to_kv(res_binary_message);
                // console.log('arr_kv_buffers', arr_kv_buffers);
                // callback(null, arr_kv_buffers);

                //expect binary message to be BOOL_TRUE
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
                //console.log('res_binary_message', res_binary_message);
                // console.log('res_binary_message.length', res_binary_message.length);
                // var arr_kv_buffers = Binary_Encoding.split_length_item_encoded_buffer_to_kv(res_binary_message);
                // console.log('arr_kv_buffers', arr_kv_buffers);
                // callback(null, arr_kv_buffers);

                //expect binary message to be BOOL_TRUE
                callback(null, true);

            }
        });
    }

	// put timeseries value

	// just put a single timeseries value into the system.
	//  can create a new key with this when using the high level interface too.

	// can send a string formatted date over to the server.
	// can do this with an array of params

	// bid, ask, volume

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
        var idx = this.id_ws_req++, ws_response_handlers = this.ws_response_handlers;

        var buf_2 = Buffer.concat([xas2(idx).buffer, message]);

        ws_response_handlers[idx] = function (obj_message) {
            //console.log('ws response: ', idx, obj_message);

            // Maybe remove the message id/idx from the message.
            //  That gets done, I think.

            callback(null, obj_message);

            // Want something simple in the response to indicate the use of paging.
            //  Could be another byte or two in each response.

            // Not receiving paged responses for the moment.
            // Don't nullify it if paging is being used.

            ws_response_handlers[idx] = null;
        };
        //console.log('send_binary_message buf_2.length', buf_2.length);
        this.websocket_connection.sendBytes(buf_2);
    }

    // Multi callback messages / subscriptions
    //  Keeps the response handler until its closed / unsubscribed.

    send_binary_subscription_message(message, subscription_event_callback) {
        var idx = this.id_ws_req++, ws_response_handlers = this.ws_response_handlers;
        
        var buf_2 = Buffer.concat([xas2(idx).buffer, message]);
        var that = this;

        ws_response_handlers[idx] = function (obj_message) {

            console.log('obj_message', obj_message);
            // May need to decode the message (somewhat) to read the initial subscription id.
            //  will need to respond to the subscription connected event.

            subscription_event_callback(obj_message);



            // read the sub message id, then the subscription type.

            //var pos = 0;
            //xas2.read()

            // Because its a subscription.
            //ws_response_handlers[idx] = null;

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
        // table prefix number, then the rest of the pk
        // need to know the table key prefixes.
        // LL_GET_RECORDS_IN_RANGE

        // no paging right now.

        var paging = new Paging.None();
        var buf_command = xas2(LL_GET_KEYS_IN_RANGE).buffer;
        // the lengths of the buffers too...

        // Should possibly use Binary_Encoding.encode to encode the query.
        //  May be better than the ad-hoc encoding here.
        //   Query gets decoded, as-is, when it arrives on the server.
        //   Some queries would probably wind up a little more verbose, but it would be simpler code.



        var buf_query = Buffer.concat([buf_command, paging.buffer, xas2(buf_l.length).buffer, buf_l, xas2(buf_u.length).buffer, buf_u]);
        //var buf_l = 
        // Include a paging buffer too...?
        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                //console.log('res_binary_message', res_binary_message);
                //console.log('res_binary_message.length', res_binary_message.length);
                // not keys and values!
                //  just the keys



                var arr_key_buffers = Binary_Encoding.split_length_item_encoded_buffer(res_binary_message);

                //console.log('arr_kv_buffers', arr_kv_buffers);

                // still a low level version.
                //  could have a get keys in range function that decodes these buffers too.






                //throw 'stop';
                callback(null, arr_key_buffers);
            }
        });

    }


    ll_get_buf_records_in_range(buf_l, buf_u, callback) {
        // table prefix number, then the rest of the pk
        // need to know the table key prefixes.
        // LL_GET_RECORDS_IN_RANGE

        // no paging right now.

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
                //console.log('res_binary_message', res_binary_message);
                //console.log('res_binary_message.length', res_binary_message.length);
                //var arr_kv_buffers = Binary_Encoding.split_length_item_encoded_buffer_to_kv(res_binary_message);
                //console.log('arr_kv_buffers', arr_kv_buffers);
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

    // ll ll_get_records_in_range no split

    
    
    ll_get_records_in_range(buf_l, buf_u, callback) {
        // table prefix number, then the rest of the pk
        // need to know the table key prefixes.
        // LL_GET_RECORDS_IN_RANGE

        // no paging right now.

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
                //console.log('res_binary_message', res_binary_message);
                //console.log('res_binary_message.length', res_binary_message.length);

                // Seems that field names is not getting record keys...

                // Or the records are not being joined or split correctly.

                // It does not quite make sense why getting field names gets a kv array with empty keys.
                //  Could it have to do with removing key prefixes?

                var arr_kv_buffers = Binary_Encoding.split_length_item_encoded_buffer_to_kv(res_binary_message);
                //console.log('arr_kv_buffers', arr_kv_buffers);
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
                //console.log('res_binary_message', res_binary_message);
                //console.log('res_binary_message.length', res_binary_message.length);
                var arr_buffers = Binary_Encoding.split_length_item_encoded_buffer(res_binary_message);
                //console.log('arr_kv_buffers', arr_kv_buffers);
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
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_beginning, buf_0]);
        var buf_u = Buffer.concat([buf_beginning, buf_1]);

        this.ll_count_keys_in_range(buf_l, buf_u, callback);
    }

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
                //console.log('res_binary_message', res_binary_message);
                // xas2 decoding
                var count, pos;
                [count, pos] = xas2.read(res_binary_message, 0);
                //console.log('count', count);

                //var arr_kv_buffers = Binary_Encoding.split_length_item_encoded_buffer_to_kv(res_binary_message);
                callback(null, count);
            }
        });
    }

    // subscribe all
    //  would have multiple callbacks.
    //  function to end the subscription returned.


	// In nextlevel, the string keys correspond to 64 bit values, stored as integers, represented in hex


    // A paging option.
    //  That would help 

    // can give it a paging object.
    //  that would help it to put together the query.


    // If paging is just true, could use a default paging object.

    // Lower level get all keys
    //  Lower level meaning it more directly interacts with the LevelDB, including index rows an other non-record values.
    

    /**
     * 
     * 
     * @param {any} paging 
     * @param {any} callback 
     * @memberof LL_NextLevelDB_Client
     */
    'll_get_all_keys'(paging, callback) {
        var buf_query, pos = 0;
        if (!callback) {
            callback = arguments[0];
            paging = new Paging.None();
        }
        if (!paging instanceof Paging) paging = new Paging.Record_Paging(paging);

        // Write out the query...
        //console.log('paging', paging);
        //if (paging) {
        buf_query = Buffer.concat([xas2(LL_GET_ALL_KEYS).buffer, paging.buffer]);
        //} else {
            //buf_query = Buffer.concat([xas2(GET_ALL_KEYS).buffer]);
        //}

        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {

                //console.log('res_binary_message', res_binary_message);
                //console.log('res_binary_message.length', res_binary_message.length);

                // Needs to decode the response.

                // We already know the paging option.
                //  Likely no need to encode how it is paged.

                // Basically need to split up some encoded items, a bunch of keys, into an array.
                //  encoding: item length, item itself

                var arr_key_buffers = Binary_Encoding.split_length_item_encoded_buffer(res_binary_message);
                //console.log('arr_key_buffers', arr_key_buffers);

                // Then can decode them. Would need to look at the first xas2 value.

                var decoded = Model.Database.decode_keys(arr_key_buffers);

                //console.log('decoded', JSON.stringify(decoded, null, 2));
                //throw 'stop';

                ///[]



                //var count, pos = 0;

                //[count, pos] = x.read(res_binary_message, pos);

                // May receive paged results.

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
    'll_count_records'(callback) {
        // Probably best to encode a binary query.

        // could just be query 0

        // should have a message id. (this.id_ws_req)

        //var buf_query = Buffer.concat([xas2(this.id_ws_req++).buffer, xas2(COUNT_RECORDS).buffer]);

        var buf_query = Buffer.concat([xas2(LL_COUNT_RECORDS).buffer]);
        // set up the response handler...


        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                var count, pos = 0;
                //console.log('LL_COUNT_RECORDS res_binary_message', res_binary_message);
                [count, pos] = x.read(res_binary_message, pos);
                //console.log('count', count);
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
    'll_put_records_buffer'(buf_records, callback) {
        // PUT_RECORDS
        var buf_query = Buffer.concat([xas2(LL_PUT_RECORDS).buffer, buf_records]);
        console.log('buf_query', buf_query);

        this.send_binary_message(buf_query, (err, res_binary_message) => {
            if (err) {
                callback(err);
            } else {
                //var count, pos = 0;
                //[count, pos] = x.read(res_binary_message, pos);
                callback(null, true);
                // Don't know if it would be useful to get back the ids.
            }
        });
    }


    // put record
    //  would need to encode the record.




    'll_subscribe_all'(subscription_event_callback) {

        // Or two callbacks - for subscription set up, and subscription event.



        // Should maybe be able to produce error callbacks, rather than event callbacks?



        var buf_query = Buffer.concat([xas2(LL_SUBSCRIBE_ALL).buffer]);

        var unsubscribe = this.send_binary_subscription_message(buf_query, (sub_event) => {
            console.log('sub_event', sub_event);

            // still a low level function.
            //  just deals with the binary data.

            // And a higher level wrapper would process / decode these subscription events.
            subscription_event_callback(sub_event);



        });
        // Not just a normal binary message.
        //  A binary message with multiple callbacks.

        // and return unsubscribe?
        return unsubscribe;



    }

    'll_subscribe_key_prefix_puts'(buf_kp, subscription_event_callback) {
        
        // Or two callbacks - for subscription set up, and subscription event.



        // Should maybe be able to produce error callbacks, rather than event callbacks?


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
        // Not just a normal binary message.
        //  A binary message with multiple callbacks.

        // and return unsubscribe?
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

            // count all records.

            //lc.ws_count_records((err, num_records) => {
            //    console.log('num_records', num_records);
            //});


			var test_get_all_records = function() {
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

            /*
            var test_get_system_records = function () {
                // And different tests for using paging.
                // ll_get_nonindex_core   ll_get_core

                // Not a ll function.

                lc.ll_get_nonindex_core((err, core) => {
                    // Should get them as an array.

                    console.log('core', core);

                    var model_from_db = Model.Database.load(core);

                    // The table records, the table field records, and the index records seem most important.

                    // Still want it so the model is active on the server, and the records can be automatically indexed, and possibly normalized too.
                    //  May want to give string name references to some data and have the server-side db do the lookups and normaization.
                    //  Not sure how much that would slow it down, having a smart client would be very useful because it would also send over the indexing.

                    // Being able to recreate the model out of a number of records seems quite important.
                    //  Want it so that new records can have all/most of their encoding done client-side. This would make for more efficient transmission too.
                    // Server not having to decode results, because the client is able to do so, will be useful.

                    // Build up a client-side model of the core.

                });

            };
            test_get_system_records();
            */

            // Getting keys within a range, and paged keys within a range, is one of the important things.

            // Index lookups will be useful too.
            //  Being able to look at the DB from the client will better help to understand that index rows have been stored correctly.

            // Test get keys for specific table.
            //  Can do this with the table key prefix...

            // Get all keys with single key prefix gets them within a range.
            //  We can use a relatively small number of lower level queries, combined with knowledge of the database.

            // Want to use the DB to conveniently populate large in-memory data structures on the client.
            //  Should be quite fast using the binary streaming messages.
            //  Server-side implementation can be relatively stable, and probably will not require the use of plugins to be effective for the moment.
            //   Record definitions and modelling of the database will help to do some of the more complicated queries.
            //    More intelligence on the client-side, but server-side will have sufficient capabilities.
            //  Leaving the server running without changing the code is a goal.

			//test_get_all_records();

            // This may have a variety of test procedures.
            //  

            // Will be much smaller to save as binary.
            //  Would also be worth showing these binary files can be read with a UI.

            // Probably don't save records like this.

			var test_save_all_records = function() {
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
			//test_save_all_records();

			// Subscriptions...
			//  Could send the subscribe functions as normal.
			//  Would be processed differently on the server.
			//   Will have an unsubscribe method or two.

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
