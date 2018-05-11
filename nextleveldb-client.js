// Possibly makes it unusable in the browser.
//  Could possibly have node-nextleveldb-client

// Because of errors, looks like creating new instances of the server may be the right course of action?
//  Seems like new currency was not added at all, while new market was added incorrectly.

//  Probably need to do more work in the assets-client (which I have not done much on recently) to make a smooth an reliable experience adding a new Bittrex currency and its associated markets.
//  Maybe porting to CockroachDB is the right approach... but really feel the need to solve this problem to get the data running smoothly through what I have here.
//   Slowly and carefully is the approach to recovering / fixing data on the various existing servers.

// Assets client could be used to check specific functionality is working OK.
//  Have long-winded and laborious methods to resolve the problems.





const fs = require("fs");
const os = require("os");

const lang = require("lang-mini");
let each = lang.each,
    tof = lang.tof,
    Fns = lang.Fns,
    clone = lang.clone,
    get_item_sig = lang.get_item_sig;


const Evented_Class = lang.Evented_Class;
const get_a_sig = lang.get_a_sig;

const mapify = lang.mapify;

const LL_NextlevelDB_Client = require("./ll-nextleveldb-client");
const xas2 = require("xas2");
const Binary_Encoding = require("binary-encoding");
const Model = require("nextleveldb-model");
const Record_List = Model.Record_List;
const Key = Model.BB_Key;


const Model_Database = Model.Database;
const database_encoding = Model.encoding;

const Paging = Model.Paging;

const Array_Table = require("arr-table");

const path = require("path");
const resolve = path.resolve;

const Table_Subscription = require('./table-subscription');




// Maybe these shouldn't be here.
//  Could make an ll version of function and refactor it.

const SELECT_FROM_TABLE = 41;






const obs_throughput = (obs_res, obs_inner) => {
    obs_inner.on('next', data => {
        obs_res.raise('next', data);
    });
    obs_inner.on('error', err => {
        obs_res.raise('error', err);
    });
    obs_inner.on('complete', data => {
        obs_res.raise('complete', data);
    });
}

// A more advanced client would definitely help.
//  Client will be used for db replication and distribution as well.
//  The server will have a client of its own to connect to other servers.

// Want an easy way to replicate all records from one table over to the same table in a different db.
//  A GUI may prove useful for this.

// Backups and authentication seem like the best approach to keep this up online.
// Take backups from existing DBs, allow them tto be put into other dbs
//

const INSERT_TABLE_RECORD = 12;
const GET_TABLE_KEY_SUBDIVISIONS = 25;

// Could separate into browser-client and node-client.

// or node_features(client), web_features(client)

// Could have a client have a database get / initialise with a full copy of another database.
//  Streaming of rows does seem important for this.

// Streaming funtionality definitely seems more important for getting data from the db.

// Carry out replication where it streams from existing server.
//  Would likely need a little downtime to do the npm update / install.

// Can try higher level functions that deal with indexed data for the moment.
//  Still avoid use of a local Model, except maybe for temporary purposes where it is most convenient.

// Table has record...
//  Then we do index lookups on the fields to see if it's already in the table
//   Not using a local model, downloads the index field data and decodes it. ???
//   Model is specifically designed to encode / decode index and field data and connect it to definitions.

// Maybe it would be possible to obtain part of the model from the server and use that?

// Should not assume there is a client-side model for some functions?

// Still, using the model on the server side to check for records makes most sense.
//  Records would be encoded into their various fields...

const TABLE_FIELDS_TABLE_ID = 2;
const TABLE_INDEXES_TABLE_ID = 3;

const KP_CORE_UPPER = 9;

const SUB_CONNECTED = 0;
const SUB_RES_TYPE_BATCH_PUT = 1;


/*
let remove_kp = (arr_records) => {
    for (let c = 0, l = arr_records.length; c < l; c++) {
        arr_records[c][0] = arr_records[c][0].splice(1);
    }
    return arr_records;
}
*/



// 22/03/2018
//  Would be worth doing some syncing.
//  Need to be able to completely copy a remote DB.
//   Could copy the tables.

// Getting a hash of all records that are in a past time period, and then copying them over, ensuring the same hash on arrival.

// Need to replace the DB process which has been running for the longest.
//  Generally the records are formatted in the same way, interface having changed a lot.
//  Not 100% sure it will start OK.

// Work on full DB copies of the data2 db to the workstation.

// Remote -> workstation sync seems quite important for performance.

// Getting the data objects from remote seems useful too.
//  Storing / caching data in blocks looks very important / useful too.


// Could see how long it takes to download fairly large data sets.
//  Probably not all that long to read through them, but would be much quicker to send pre-made binary blobs where possible.

// Syncing all results from local to machine to workstation will be very useful indeed.
//  Then have the local workstation / server generate data sets in formats ready for analysis.






const obs_to_cb = (obs, callback) => {
    let arr_all = [];
    obs.on('next', data => arr_all.push(data));
    obs.on('error', err => callback(err));
    obs.on('complete', () => callback(null, arr_all));
}


const prom_or_cb = (inner_with_cb, opt_cb) => {
    if (typeof opt_cb !== 'undefined') {
        inner_with_cb(opt_cb);
    } else {
        return new Promise((resolve, reject) => {
            inner_with_cb((err, res) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(res);
                }
            })
        })
    }
}

const prom_opt_cb = (prom, opt_cb) => {
    if (opt_cb) {
        prom.then((res) => {
            opt_cb(null, res);
        }, err => {
            opt_cb(err);
        })
    } else {
        return prom;
    }
}








var directory_exists = function (path, callback) {
    fs.stat(resolve(path), function (err, stat) {
        if (err) {
            return callback(false);
        }
        callback(null, stat.isDirectory());
    });
};

function ensure_directory_exists(path, mask, cb) {
    if (typeof mask == "function") {
        // allow the `mask` parameter to be optional
        cb = mask;
        mask = 0777;
    }
    fs.mkdir(path, mask, function (err) {
        if (err) {
            if (err.code == "EEXIST")
                cb(null); // ignore the error if the folder already exists
            else cb(err); // something else went wrong
        } else cb(null); // successfully created folder
    });
}

var get_directories = function (dir, cb) {
    //dir = dir.split('/').join('\\');

    //console.log('* get_directories', dir);
    fs.readdir(dir, function (err, files) {
        if (err) {
            //console.log('err', err);
            //throw err;
            cb(err);
        } else {
            //console.log('files.length', files.length);
            var dirs = [],
                filePath,
                c = files.length,
                i = 0,
                d = 0;

            checkDirectory = function (err, stat) {
                //console.log('checkDirectory');
                if (stat.isDirectory()) {
                    dirs.push(files[d]);
                }
                d++;
                //console.log('i', i);
                c--;
                //console.log('c', c);
                if (c === 0) {
                    // last record
                    cb(null, dirs);
                }
            };

            for (i = 0, l = files.length; i < l; i++) {
                if (files[i][0] !== ".") {
                    // ignore hidden
                    filePath = dir + "/" + files[i];
                    fs.stat(filePath, checkDirectory);
                }
            }
            if (files.length === 0) {
                cb(null, []);
            }
        }
    });
};

var pad = (num, size) => {
    var s = num + "";
    while (s.length < size) s = "0" + s;
    return s;
};

/**
 *
 *
 * @class NextlevelDB_Client
 * @extends {LL_NextlevelDB_Client}
 */
class NextlevelDB_Client extends LL_NextlevelDB_Client {
    // Functionality to persist entire models, or system tables.
    //  Will interact with a Model.
    // Could have a local Model?
    // Initial setup of crypto database.
    // load model, including specific tables


    start(callback) {
        super.start((err, res) => {
            if (err) {
                callback(err);
            } else {
                this.load_core((err, model) => {
                    if (err) {
                        callback(err);
                    } else {
                        callback(null, model);
                    }
                })
            }
        })
    }

    /**
     *
     *
     * @param {any} callback
     * @memberof LL_NextLevelDB_Client
     */
    get_core(callback) {
        var buf_l = xas2(0).buffer;
        var buf_u = xas2(KP_CORE_UPPER).buffer;
        this.ll_get_records_in_range(buf_l, buf_u, callback);
    }

    count_core(callback) {
        var buf_l = xas2(0).buffer;
        var buf_u = xas2(9).buffer;
        this.ll_count_keys_in_range(buf_l, buf_u, callback);
    }

    /**
     *
     *
     * @param {any} callback
     * @memberof LL_NextLevelDB_Client
     */
    get_nonindex_core(callback) {
        this.get_core((err, core) => {
            if (err) {
                callback(err);
            } else {
                var filtered_core = [];
                each(core, item => {
                    var buf_key = item[0];
                    var n = Binary_Encoding.decode_first_value_xas2_from_buffer(buf_key);
                    if (n === 0 || n === 1 || n % 2 === 0) {
                        filtered_core.push(item);
                    }
                });
                callback(null, filtered_core);
            }
        });
    }


    // Now have function on the server to do this, but no interface
    get_table_max_key(table_name, callback) {
        this.get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                callback(err);
            } else {

                // do a get range in reverse, with limit of 1.
                // or the last record by key prefix, 

                let table_kp = table_id * 2 + 2;

                // being able to specify reverse and limit in get by prefix would be great.

                let reverse = true,
                    limit = 1;
                this.get_keys_by_key_prefix(table_kp, reverse, limit, (err, arr_keys) => {
                    if (err) {
                        callback(err);
                    } else {
                        console.log('arr_keys', arr_keys);
                        let res = arr_keys[0];
                        console.log('res', res);
                        throw 'stop';
                    }
                })

            }
        })
    }

    /**
     *
     *
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    load_core(callback) {
        this.get_core((err, buf_core) => {
            if (err) {
                callback(err);
            } else {
                //console.log('buf_core', buf_core);

                //throw 'stop';
                this.model = Model_Database.load_buf(buf_core);
                callback(null, this.model);
            }
        });
    }

    load_buf_core(callback) {
        var that = this;
        this.get_core((err, buf_core) => {
            if (err) {
                callback(err);
            } else {
                callback(null, buf_core);
            }
        });
    }



    // Useful for when we change a model, and then compare the changed one with the original.
    load_2_core(callback) {
        //var that = this;
        this.get_core((err, buf_core) => {
            if (err) {
                callback(err);
            } else {
                //console.log('buf_core', buf_core);
                let res = [Model_Database.load(buf_core), Model_Database.load(buf_core)];


                callback(null, res);
            }
        });
    }

    ensure_model(callback) {
        if (this.model) {
            callback(null, model);
        } else {
            this.load_core(callback);
        }
    }

    /**
     *
     *
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    load_table(table_name, callback) {
        var that = this;
        var table = that.model.map_tables[table_name];
        that.get_table_records(table_name, (err, table_records) => {
            if (err) {
                callback(err);
            } else {
                //table.clear();
                // Seems like clearing the table first makes sense as it has the same result.
                //

                // The get_table_records function won't have the id within the key.
                //table.add_records(table_records, true);
                //throw "stop";
                table.add_records(table_records);
                // It should index them.
                //  Add them to the index too?

                //table.add_records_including_table_id_in_key(table_records, true);
                callback(null, table);
            }
        });
    }

    /**
     *
     *
     * @param {any} arr_table_names
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    load_tables(arr_table_names, callback) {
        var fns = Fns();
        each(arr_table_names, table_name => {
            fns.push([this, this.get_table_records, [table_name, true]]);
        });
        fns.go((err, res_all) => {
            if (err) {
                callback(err);
            } else {
                each(res_all, (table_records, table_index) => {


                    //console.log('table_records.length', table_records.length);
                    //console.log('table_records[0]', table_records[0]);
                    //throw 'stop';


                    var table_name = arr_table_names[table_index];
                    var table = this.model.map_tables[table_name];

                    // These records should have been decoded by now.

                    //console.log('table_records', table_records);
                    //throw 'stop';
                    table.add_records(table_records);
                });
                callback(null, this.model);
            }
        });
    }

    // load table

    // get an obj_map from a table...
    //  much like an index

    // Loading to the client's model.
    /**
     *
     *
     * @param {any} arr_table_names
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    load_core_plus_tables(arr_table_names, callback) {
        // load the model from the core.
        //  then load other tables

        // can we load multiple tables at once from the server into one buffer?
        // or load the tables individually.
        var that = this;
        this.load_core((err, model) => {
            if (err) {
                callback(err);
            } else {
                that.load_tables(arr_table_names, callback);
                // Test that the core has loaded successfully?
                //throw 'stop';
                // try loading a single table from the model.
                // Could be loading these tables that creates the problem, earlier incrementor and loading issues seem to have been solved.
            }
        });
    }

    // Time-Value type data, being turned into records.
    // Defining and using record transformations (based on types, types could be defined in Model)
    /**
     *
     *
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    validate_table_index(table_name, callback) {
        // need to get the table rows, and the index rows

        // rebuild new index rows from the table rows
        // compare the rebuilt index rows to the original ones.

        // could start by doing a count.
        //  if there are 0 index rows all we need to do is build an index (in another function).
        //   return '0 index rows';

        // get the index rows
        var that = this;

        this.get_table_index_records(table_name, (err, index_records) => {
            if (err) {
                callback(err);
            } else {
                // get_table_index_records should decode the records

                if (index_records.length === 0) {
                    callback(null, "0 index rows");
                } else {
                    // load the table itself (into the model)
                    //  that would recreate the index records.

                    that.load_table(table_name, (err, table) => {
                        if (err) {
                            callback(err);
                        } else {
                            console.log("table.records.length", table.records.length);
                            // search for index that are missing.
                            // table get arr data index rows
                            var table_index_records = table.get_arr_data_index_records();
                            // then we see which are missing from remote
                            console.log("index_records", index_records);
                            console.log("index_records.length", index_records.length);
                            console.log(
                                "table.records.arr_records",
                                table.records.arr_records
                            );

                            console.log("table_index_records", table_index_records);

                            throw "stop";
                        }
                    });
                }
            }
        });
    }

    /**
     *
     *
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    maintain_table_index(table_name, callback) {
        // Would be nice to get the table index info from the db.

        this.validate_table_index(table_name, (err, validation) => {
            if (err) {
                callback(err);
            } else {
                console.log("validation", validation);

                if (validation === "0 index rows") {
                    // build the table index.
                    //  have we got that table loaded into the model?
                    //   better to get every row from the live db.
                    // should also get the index definitions for the table.
                    //  not so sure the index definitions have been properly saved to the database.
                    // may need to maintain the system core. compare it to a model.
                    //  only recently did I fix the indexes table in the model - they were not having enough records written. Did not cover added tables outside the core.
                    // Possibly exporting data as CSV is the way.
                    // Or more maintenance functions to ensure the system model is represented correctly.
                    //  Or could overwrite the system model part of the database.
                    // Maybe just overwrite the indexing table.
                }
            }
        });
    }


    // Will have validate table indexes function.
    //  Will first have it on the server.




    /**
     *
     *
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    validate_core_index_table(callback) {
        // need to have a model to validate against.

        // get the index table records from the remote db

        var model = this.model;
        if (!model) {
            throw "this.model not found";
        }

        this.get_table_records(
            "table indexes",
            (err, remote_index_table_records) => {
                if (err) {
                    callback(err);
                } else {
                    var model_indexes_table = model.map_tables["table indexes"];
                    if (
                        model_indexes_table.records.length >
                        remote_index_table_records.length
                    ) {
                        // can check for missing records.

                        callback(
                            null,
                            "number of index records in model > number of index records in remote db"
                        );
                    } else {
                        callback(null, true);
                    }
                }
            }
        );
    }

    // could have a higher level version that gets the decoded keys

    /**
     *
     *
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    replace_core_index_table(callback) {
        this.put_model_table_records("table indexes", callback);
    }

    // Will have maintain_table_indexes script.
    //

    // Server-side model functionality would make a lot of sense for dealing with indexes.

    // Should have more functions that deal with record collections.
    //


    // Have a lot of different functions for putting records at the moment.

    // Want to make use of Record_List to handle different ways the dataset can be given.
    //  It automatically backs its data as a binary buffer.


    put(records, callback) {
        let buf;
        if (records instanceof Record_List) {
            buf = records.buffer;
        } else if (records instanceof Buffer) {
            buf = records;
        } else if (Array.isArray(records)) {
            buf = new Record_List(records).buffer;
        }

        this.ll_put_records_buffer(buf, callback);
    }

    // Assuming we want to insert the table key prefix into the records.
    // Does not create index items for the records.
    /**
     *
     *
     * @param {any} table_name
     * @param {any} arr_records
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    put_table_arr_records(table_name, arr_records, callback) {
        var model = this.model;
        if (!model) {
            //throw 'this.model not found';
            callback("this.model not found");
        } else {
            // get the table records, encoded as binary

            var table = model.map_tables[table_name];
            if (!table) {
                callback("table " + table_name + " not found");
            } else {
                var arr_record_data = [];
                each(arr_records, record => {
                    //console.log('record', record);
                    //console.log('record.key', record.key);
                    //record.key.splice(0, 0, table.key_prefix);
                    arr_record_data.push(record.arr_data);
                });
                var buf_rows = Model_Database.encode_arr_rows_to_buf(
                    arr_record_data,
                    table.key_prefix
                );
                //console.log('buf_rows', buf_rows);
                //console.log('buf_rows.length', buf_rows.length);

                // put that buffer.
                this.ll_put_records_buffer(buf_rows, (err, res_put_buf) => {
                    if (err) {
                        callback(err);
                    } else {
                        callback(null, arr_record_data);
                    }
                });
            }
        }
    }

    /**
     *
     *
     * @param {any} arr_records
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */

    // Should differentiate between different put levels.
    // Lowest level could be silent, not raising events.

    // Then standard ll would raise the events

    // Just a little more until the crypto data system is working.
    //  See if we can sync between Montreal nodes.
    //  That should be fairly fast.

    // Maybe try a small test suite that creates a db and tries a variety of operations.
    // Using the client to connect to it, and using the server API directly.
    //  


    // Just a simple put function.
    //  Can put a single record.
    //  Can put a bunch of them.
    //  Eventually paged put functions, where it does it in parts.
    //   Could split it on the client into multiple puts.


    put_arr_records(arr_records, callback) {
        // encode the records into binary buffer

        //encode_arr_records_to_buffer
        //  would need to take account of index buffers, which don't have a value.



        console.log("put_arr_records arr_records", arr_records);

        throw "stop";
    }

    // put model table records
    /**
     *
     *
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    put_model_table_records(table_name, callback) {
        var model = this.model;
        if (!model) {
            //throw 'this.model not found';
            callback("this.model not found");
        } else {
            // get the table records, encoded as binary

            var table = model.map_tables[table_name];
            if (!table) {
                callback("table " + table_name + " not found");
            } else {
                // then get the binary records from the table, perform a ll put

                var arr_bufs_table_records = table.get_all_db_records_bin();
                //console.log('arr_bufs_table_records', arr_bufs_table_records);
                var encoded_buf = Model_Database.encode_model_rows(
                    arr_bufs_table_records
                );
                //console.log('encoded_buf', encoded_buf);

                // then do the ll put.
                this.ll_put_records_buffer(encoded_buf, callback);

                // then encode these records.
            }
        }
    }




    // or just put table?

    /**
     *
     *
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    maintain_core_index_table(callback) {
        // validate it,
        //  if it's not right, then replace the remote records with the new version.
        // seems a bit tricky, as we don't want to remove any records.
        // could check to see
        // var model_indexes_table = model.map_tables['table indexes'];
    }

    /**
     *
     *
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    update_core_index_table(callback) {
        // can't currently delete records in range using api
        // need to work on delete record range and delete record.
        //  would need to delete the associated index records too.
        //   so need to generate the index records in order to find them it seems.
        // just put them all for the moment.
    }





    // A version with 'limit' would be nice.
    //  Using the lower level version of the function, rather than using more advanced options for normal encoding is possible right now.

    // A client-side limit processor that calls stop would be cool too.
    //  Limiting on the server, sending the limit to the server, will be nice.

    // May be worth forming the command options sooner.


    //  would always use an observable
    // send_command(options)
    //  command name or command id
    //  communication args
    //   how the server returns the results
    //    remove kp
    //    limit
    //    (tell it to encode specifically as records, keys or just binary)???
    //   how the client processes the returned results
    //    decoding
    //     (probably best to remove KP from server side)
    //  command args
    //  and of course how to handle paging.

    // This would make it very easy to get an observable to a server side function.
    //  On the server side, there will be code that makes the processing more concise.

    // Want to expand things in the direction of a Unified Communication Protocol.
    //  The current Paging object will wind up doing plenty more to encode / decode messages.
    // Or an actual OO message object would be cool.
    //  Created on the client, reconstructed on the server.
    // Command really.
    // This will make for smaller code eventually.

    // May keep old methods and compare speed?


    /**
     *
     *
     * @param {any} i_kp
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    count_records_by_key_prefix(i_kp, limit = -1, callback) {


        let a = arguments,
            l = a.length,
            sig = get_a_sig(a);

        //console.log('count_records_by_key_prefix sig', sig);

        if (sig === '[n,n]') {

        } else if (sig === '[n,f]') {
            callback = a[1];
            limit = -1;
        } else {
            throw 'count_records_by_key_prefix unexpected sig ' + sig;
        }



        // Limit would be useful here.

        var buf_kp = xas2(i_kp).buffer;
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_kp, buf_0]);
        var buf_u = Buffer.concat([buf_kp, buf_1]);


        if (callback) {

            // Would be nice to put limit (and even stop) capability into this.
            //  An observable wrapper would handle the limit.



            this.ll_count_keys_in_range(buf_l, buf_u, limit, (err, res_count) => {
                if (err) {
                    throw err;
                } else {
                    //console.log('res_count', res_count);


                    callback(null, res_count);

                    //
                    //throw 'stop';
                }
            });
        } else {
            return this.ll_count_keys_in_range(buf_l, buf_u);
        }



    }

    // count_records_by_key_prefix_up_to
    count_records_by_key_prefix_up_to(i_kp, limit, callback) {
        console.log('count_records_by_key_prefix_up_to i_kp', i_kp);
        var buf_kp = xas2(i_kp).buffer;
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_kp, buf_0]);
        var buf_u = Buffer.concat([buf_kp, buf_1]);

        this.ll_count_keys_in_range_up_to(buf_l, buf_u, limit, (err, res_count) => {
            if (err) {
                throw err;
            } else {
                //console.log('res_count', res_count);
                callback(null, res_count);
            }
        });
    }

    /**
     *
     *
     * @param {any} buf_key_beginning
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */


    // Should be able to work as either observable or callback.
    //  and a function to use the last result of an observable as its final result.





    count_keys_beginning(buf_key_beginning, callback) {
        //console.log('count_keys_beginning');
        //console.log('buf_key_beginning', buf_key_beginning);

        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_key_beginning, buf_0]);
        var buf_u = Buffer.concat([buf_key_beginning, buf_1]);

        if (callback) {
            this.ll_count_keys_in_range(buf_l, buf_u, callback);
        } else {
            return this.ll_count_keys_in_range(buf_l, buf_u);
        }
    }

    // function to get the decoded records by key prefix

    //

    /*
    get_table_records_by_key_beginning(table_name, key_beginning, callback) {

        this.get_table_kp_by_name(table_name, (err, kp) => {
            if (err) {
                throw err;
            } else {

                // need to build up the buffer here.

                // the kp in the buffer and then they keys

                let buf_key = Model_Database.encode_key(kp, key_beginning);
                console.log('buf_key', buf_key);
                // Keys seem wrong
                //  Could get all of the buffer keys from that table to compare.

                this.get_table_keys('bittrex markets', (err, keys) => {
                    if (err) {
                        callback(err);
                    } else {
                        console.log('keys', keys);

                        this.ll_get_records_keys_beginning(buf_key, (err, encoded_records) => {
                            if (err) {
                                callback(err);
                            } else {
                                const remove_kp = 1;
                                // Not sure this will decode index records.
                                //  Could check to see if the kp is odd in this case?

                                //console.log('encoded_records', encoded_records);

                                var res = Model_Database.decode_model_rows(encoded_records, remove_kp);

                                console.log('res', res);
                                console.trace();
                                throw 'stop';

                                // While removing the key prefix.

                                callback(null, res);
                            }
                        });
                    }
                })
            }
        })
    }
    */

    // Build docoding into ll_get_records_by_key_prefix
    //  Decoding will be managed on a lower level, as we are able to do so, based on more information being in the messages, and this can be used to decode the messages as they arrive.


    // Likely best to remove this and only use the lower level version, expanding that one so that it's not just ll, it's the way it's done, and with a rather complicated interface.
    //  Would have improved automatic decoding functionality within the lower level codebase.

    /*
    get_records_by_key_prefix(key_prefix, paging, callback) {

        let a = arguments,
            sig = get_a_sig(a);
        let buf_key_prefix;

        console.log('sig', sig);

        if (sig === '[n,o]') {
            buf_key_prefix = xas2(key_prefix).buffer;
        } else if (sig === '[B,o]') {
            buf_key_prefix = key_prefix;
        }


        //throw 'stop';




        // With the key prefix as a number...
        //  Would probably be xas encoded


        /*
        if (sig === '[s]') {
            // Record paging, size 1024
            paging = new Paging.Record(1024);
        }
        /* /





        // Should maybe retire this function, expand ll_get_records_by_key_prefix to automatically do decoding, allow paging with an observable, still work through a callback function too.
        //  Server-side, may need expansion to enable paging.

        if (callback) {
            this.ll_get_records_by_key_prefix(buf_key_prefix, (err, encoded_records) => {
                if (err) {
                    callback(err);
                } else {
                    const remove_kp = 1;
                    var res = Model_Database.decode_model_rows(encoded_records, remove_kp);
                    // While removing the key prefix.
                    callback(null, res);
                }
            });
        } else {
            //throw 'NYI';

            //let obs =
        }



    }
    */

    // get_decoded_records_by_key_prefix
    /**
     *
     *
     * @param {any} key_prefix
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */

    get_records_by_key_prefix_up_to(key_prefix, limit, callback) {



        this.ll_get_records_by_key_prefix_up_to(key_prefix, limit, (err, encoded_records) => {
            if (err) {
                callback(err);
            } else {
                const remove_kp = 1;
                var res = Model_Database.decode_model_rows(encoded_records, remove_kp);

                // While removing the key prefix.

                callback(null, res);
            }
        });
    }



    // A paged version of this would be useful.
    //  May give it a paging option as its middle parameter.

    // Then it will call get_records_by_key_prefix with paging.

    // A lower level test for get_records_by_key_prefix would be useful.







    /**
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */

    // Could have lower level functions that get the table's records, sending the table name over to the server.
    //  More complex server-side functionality will allow yet more complex and useful queies to take place server-side.
    //  

    get_table_records(table_name, paging, decode = true, remove_kps = true, callback) {


        // Should possibly remove the table KPs.
        //  Doing this on a Buffer would be quite useful maybe.


        // With optional decoding too...


        //let page_size = 8192;

        let page_size = 32768;

        let a = arguments,
            sig = get_a_sig(a);

        //console.log('get_table_records sig', sig);
        //throw 'stop';

        if (sig === '[s]') {
            // Record paging, size 1024


            //let page_size = 1024;

            paging = new Paging.Record(page_size);

        } else if (sig === '[s,f]') {
            callback = a[1];
            paging = null;
        } else if (sig === '[s,b]') {
            //callback = a[1];
            decode = a[1];
            paging = new Paging.Record(page_size);
            //paging = new Paging.None();
        } else if (sig === '[s,b,f]') {
            paging = null;
            decode = a[1];
            callback = a[2];
        } else if (sig === '[s,b,b,f]') {
            paging = null;
            decode = a[1];
            remove_kps = a[2];
            callback = a[3];
        } else {
            console.trace();
            throw 'Unexpected sig to get_table_records: ' + sig;
        }

        //console.log('decode', decode);


        //console.log('sig', sig);
        //throw 'stop';

        // Paging object that is used when no callback is given.

        // API will always be promise / observable when no callback is given.






        // Making this work with paging seems useful / important.
        //  May need to upgrade server-side too.
        //  Carry out the updates on data

        // Should run using an inner observable.
        //  The server could have its own default paging, or we need to give default paging if we use an observable.

        // Using an observable should indicate we don't want all of the records at once.


        // get_table_kp_by_name


        //let obs_res;

        let obs_res = new Evented_Class();






        /*

        if (!callback) {
            // A temporary observer, because we don't have the params yet?

            // Double layer observable?

            obs_res = new Evented_Class();

            // 

        }
        */

        this.get_table_kp_by_name(table_name, (err, kp) => {
            if (err) {
                callback(err);
            } else {

                // Should also use an observable version of this, though the version with the callback would also be useful.

                if (callback) {

                    // Remove table kps from records when decoding.
                    this.get_records_by_key_prefix(kp, decode, remove_kps, callback);

                } else {
                    console.log('paging', paging);

                    //throw 'stop';

                    let obs = this.get_records_by_key_prefix(kp, paging, decode, remove_kps);
                    obs.unpaged = obs_res.unpaged;

                    //return obs;
                    //let data_pages = [];

                    //return obs_res;

                    // Need to be able to pass through these results....

                    //obs_res.




                    obs.on('next', data => {
                        //console.log('data', data);
                        //console.log('data.length', data.length);

                        //data_pages.push(data);
                        obs_res.raise('next', data);
                    });
                    obs.on('complete', data => {
                        //console.log('data', data);
                        //console.log('completed data.length', data.length);

                        // Don't get the last data again.
                        obs_res.raise('complete', data);


                        //console.log('completed data (last page)', data);



                        //let all_records = [].concat.apply([], data_pages);
                        //console.log('all_records.length', all_records.length);
                    });

                    obs_res.stop = obs.stop;


                }
            }
        });


        if (!callback) {
            return obs_res;
        }



        /*

        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                this.get_records_by_key_prefix(kp, callback);
            } else {
                callback("Table " + table_name + " not found");
            }

        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            this.get_table_kp_by_name(table_name, (err, kp) => {
                if (err) {
                    callback(err);
                } else {

                    // Should also use an observable version of this, though the version with the callback would also be useful.





                    this.get_records_by_key_prefix(kp, callback);
                }
            });
            //callback("Expected this.model, otherwise can't find table by name");
        }

        */
    }




    // get_table_records_up_to

    get_table_records_up_to(table_name, limit, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                this.get_records_by_key_prefix_up_to(kp, limit, callback);
            } else {
                callback("Table " + table_name + " not found");
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'

            this.get_table_kp_by_name(table_name, (err, kp) => {
                if (err) {
                    callback(err);
                } else {
                    this.get_records_by_key_prefix_up_to(kp, limit, callback);
                }
            })

            //callback("Expected this.model, otherwise can't find table by name");
        }
    }

    // key beginning rather than key prefix
    //  could be the full key
    get_table_records_by_key(table_name, key, callback) {
        this.get_table_kp_by_name(table_name, (err, kp) => {
            // then encode a buffer with that kp and key
            let buf_key = Model_Database.encode_key(kp, key);
            //console.log('buf_key', buf_key);

            // then search by key (prefix)
            //  it's the beginning of the key.

            this.get_records_by_key_prefix(buf_key, callback);

            //throw 'stop';
        });
    }


    // Client side remove_kp options?
    //  The subdivisions could also have KPs stripped from the beginnings.

    // Having a server-side function provide binary data by default could lead to some faster (but more difficult) processing.



    //get_table_key_subdivisions(table, decode = true, callback) {
    get_table_key_subdivisions(table, callback) {

        let a = arguments,
            sig = get_a_sig(a);

        // Would prefer to leave the table as a wildcard.



        //if (sig === '[]')

        // Maybe remove decode from the parameter.
        //  Could generally only deal with encoded data

        // making decoding an easy process will be nice.
        //  However, we are dealing with index values in this function which work matching against each other when encoded.
        //   decoding and re-encoding makes more overhead, leaving data encoded is the answer when it comes to perf.





        // Will call the server function to do this.
        //  Want to make a concise new-school server function.
        //  Want a concise way of calling it too.

        // Want a flexible way of calling a server function, that's flexible about using a callback or not.
        // default decoding.
        // Send with decoding being true?
        // obs_separate

        // OO message system will come fairly soon, but it's worth getting a few more code paths working / upgraded with what we have already.

        // obs_unpage


        //let obs_unpage_binary = 

        let obs_unpage_buffer = obs => {
            let res = new Evented_Class();
            obs.on('next', buf => {
                // could be paged binary
                // don't want to read the array.
                // better in terms of memory handling for long pages to read through it raising events?
                //console.log('obs_unpage_buffer buf', buf);

                let arr_bufs = Binary_Encoding.split_encoded_buffer(buf);


                //console.log('obs_unpage_buffer arr_bufs', arr_bufs);
                //throw 'stop';
                // need to split up a buffered array, without decoding it?
                //  binary_encoding.split_array_encoded_buffer
                //   splits it to other encoded buffers - gets the items out of the array
                //console.log('arr', arr);

                // 

                each(arr_bufs, item => res.raise('next', item))
            });
            obs.on('error', () => res.raise('error'));
            obs.on('complete', () => res.raise('complete'));
            return res;
        }

        // Could there even be 2 levels of message envelope encoding?
        //  Every item result encoded to be separate.
        //   It does seem worth having these multiple encoding levels.
        //   May want to get the individual binary results without destructuring / decoding the structure


        // Observable that does one level of decoding...?

        //  Condidering what is in the envelope, 


        let obs_decode_message_envelope = obs => {
            let res = new Evented_Class();
            obs.on('next', buf => {
                // need to split up a buffered array, without decoding it?
                //  binary_encoding.split_array_encoded_buffer
                //   splits it to other encoded buffers - gets the items out of the array
                //console.log('arr', arr);

                // Could the data have been wrongly double-encoded as an array while sending server-side?
                //  Possibly to compensate for a previous decoding problem?

                // So don't have these again encoded as arrays?
                //  We know they are arrays, so just decode them into that by default.

                // Maybe the result has been needlessly double-encoded.888

                //console.log('obs_decode_message_envelope buf', buf);
                //console.log('obs_decode_message_envelope buf.length', buf.length);

                // Buffers are encoded within the envelope.




                // Looks like a likely bug with how it was encoded on the server.


                // Standard decode of what's in the envelope
                let decoded = Binary_Encoding.decode_buffer(buf)[0];

                // Getting an error here if there is still a kp on the server?
                //  First key needs to be encoded as a buffer?



                //console.log('decoded', decoded);

                // Still need server-side kp removal from result?
                //  Then 
                let decoded2 = Binary_Encoding.decode_buffer(decoded)[0];

                //console.log('* decoded2', decoded2);

                //let decoded_3 = Binary_Encoding.decode_buffer(decoded2[0]);

                // Yes these buffers need to be available for decoding.
                //  Could the first and last key lookup be going wrong?




                //console.log('decoded_3', decoded_3);

                // Envelope should have an array of items inside.
                //  Those items may stay encoded.
                //   Items to stay encoded need to be encoded with a Buffer type


                res.raise('next', decoded2);
            });
            obs.on('error', () => res.raise('error'));
            obs.on('complete', () => res.raise('complete'));
            return res;
        }



        // An observable then for decoding the message envelope.
        //  Then we may still have the encoded data and could run it through a decode message data process.
        //  For the moment, we want to get the data back in the same format that the server gives when directly calling a function, still not decoding.
        //   Decoding the message envelope is different to decoding the message itself.



        // obs_separate

        // Then decoding messages is a different matter.

        // 

        // and could decode the key search beginning and also the internal buffers.
        //  


        let obs_send = obs_decode_message_envelope(obs_unpage_buffer(this.send(GET_TABLE_KEY_SUBDIVISIONS, [this.model.table_id(table)])));

        // could add a getter for the 'decoded' property.



        //obs.decode = function



        // Decoding option.

        //let res;
        // and then decode?


        /*
        decode = false;
        if (decode) {
            // 

            res = Model.encoding.obs_decode(obs_send);
        } else {
            res = obs_send;
        }
        */

        // Possibly could wrap the whole observable?

        // Want the page->separate observable processor.
        //  We don't decode the values here, so they are all stuck together.
        //  Look at receiving pages of data, then un-paging them as they come back to the client.



        console.log('get_table_key_subdivisions !!callback', !!callback);
        if (callback) {

            // callbackify(...)



            let res_all = [];

            obs_send.on('next', data => {
                console.log('client get_table_key_subdivisions data', data);
                // 
                res_all.push(data);
                //throw 'stop';
                // are getting pages back here
            });
            obs_send.on('complete', () => {
                callback(null, res_all);
            });
            obs_send.on('error', err => {
                callback(err);
            });

            //throw 'NYI';
        } else {
            return obs_send;
        }
    }



    // get_table_flat_records

    // does not separate the key and value.

    // getting an Arr_Table of the table records
    //  arr_table will have the keys and values in one array, together.
    //   could have functionality to output as key value pairs too.

    /**
     *
     *
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    get_at_table_records(table_name, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                this.get_table_records(table_name, (err, table_records) => {
                    if (err) {
                        callback(err);
                    } else {
                        // flatten kvps

                        var flat_records = [];
                        each(table_records, record => {
                            flat_records.push(record[0].concat(record[1]));
                        });
                        //console.log('flat_records', flat_records);
                        //console.log('table.field_names', table.field_names);

                        var res = new Array_Table([table.field_names, flat_records]);
                        callback(null, res);
                    }
                });
            } else {
                callback("Table " + table_name + " not found");
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback("Expected this.model, otherwise can't find table by name");
        }
    }

    // get table records field value map (idx field / field name)

    get_table_records_fields_value_map(table_name, field, callback) {
        //let table = this.model.map_tables[table_name];
        // could use get_table_id_by_name
        let i_field;
        if (typeof field === "number") {
            i_field = field;
        }

        let res = {};
        this.get_table_records(table_name, (err, table_records) => {
            if (err) {
                callback(err);
            } else {
                table_records.forEach(record => {
                    //console.log('record', record);
                    let flat_record = record[0].concat(record[1]);
                    console.log("flat_record", flat_record);
                    res[flat_record[i_field]] = record;
                });
                console.log("res", res);
                callback(null, res);
            }
        });
    }

    // get_table_kv_field_names

    /**
     *
     *
     * @param {any} table_name
     * @param {any} index_id
     * @param {any} value
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    table_index_lookup(table_name, index_id, value, callback) {
        // only looking up one value in the index for the moment?

        //  could also look at an array of values.

        // only will get one record.

        // will return promise if no callback is used.


        let inner = (callback) => {
            var t_value = tof(value);

            if (!this.model) {
                console.trace();
                throw "expected: this.model";
            }

            var table_kp = this.model.map_tables[table_name].key_prefix;

            if (t_value === "array") {
                throw "yet to implement";
            } else {

                var buf_idx_key = Model_Database.encode_index_key(
                    table_kp + 1,
                    index_id, [value]
                );

                this.ll_get_keys_beginning(buf_idx_key, (err, ll_res) => {
                    if (err) {
                        callback(err);
                    } else {
                        var decoded_index_key = Model_Database.decode_key(ll_res[0]);
                        var arr_pk_ref = decoded_index_key.slice(3);
                        if (arr_pk_ref.length === 1) {
                            callback(null, arr_pk_ref[0]);
                        } else {
                            callback(null, arr_pk_ref);
                        }
                    }
                });
                //Model.Database.enc
            }
        }


        /*
        if (callback) {
            inner(callback);
        } else {

        }*/

        return prom_or_cb(inner, callback);



    }

    /**
     *
     *
     * @param {any} table_name
     * @param {any} key
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    get_first_last_table_keys_in_key_selection(table_name, key, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                var buf_key = Model_Database.encode_key(kp, key);
                this.ll_get_first_last_keys_beginning(buf_key, (err, ll_res) => {
                    if (err) {
                        callback(err);
                    } else {
                        //console.log('ll_res', ll_res);
                        var res = Model_Database.decode_keys(ll_res);
                        callback(null, res);
                    }
                });

                //this.ll
            } else {
                callback("Table " + table_name + " not found");
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback("Expected this.model, otherwise can't find table by name");
        }
    }

    get_table_last_key(table_name, callback) {


        let table_id = this.model.table_id(table_name);
        var kp = table_id * 2 + 2;
        var buf_key = xas2(kp).buffer;

        //console.log('pre ll_get_last_key_beginning ', buf_key);
        this.ll_get_last_key_beginning(buf_key, (err, res_last_key) => {
            if (err) {
                callback(err);
            } else {
                // Does not decode the result here.

                //console.log('res_last_key', res_last_key);
                callback(null, res_last_key);
            }
        })
    }

    get_table_last_id(table_name, callback) {
        this.get_table_last_key(table_name, (err, last_key) => {
            if (err) {
                callback(err);
            } else {
                //console.log('last_key', last_key);

                let decoded_last_key = database_encoding.decode_key(last_key);
                //console.log('decoded_last_key', decoded_last_key);
                decoded_last_key.shift();

                if (decoded_last_key.length === 1) {
                    callback(null, decoded_last_key[0]);
                } else {
                    callback(null, decoded_last_key);
                }




            }
        })
    }

    // count_table_selection
    //  count_table_key_selection

    /**
     *
     *
     * @param {any} table_name
     * @param {any} key
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    count_table_key_selection(table_name, key, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                var buf_key = Model_Database.encode_key(kp, key);
                this.ll_count_keys_beginning(buf_key, callback);
                //this.ll
            } else {
                callback("Table " + table_name + " not found");
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback("Expected this.model, otherwise can't find table by name");
        }
    }

    // Maybe this is the LL version because the results are still binary encoded
    //  not in ll because it requires use of the model.
    /**
     *
     *
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    ll_get_table_index_records(table_name, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                //

                //console.log('kp', kp);

                // ll get records by key prefix - maybe it should do more decoding of the results buffer? 

                this.ll_get_records_by_key_prefix(kp + 1, callback);
            } else {
                callback("Table " + table_name + " not found");
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback("Expected this.model, otherwise can't find table by name");
        }
    }

    /**
     *
     *
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    get_table_index_records(table_name, callback) {
        this.ll_get_table_index_records(table_name, (err, index_records) => {
            if (err) {
                callback(err);
            } else {
                // That ll function did not split up the records.
                var row_buffers = Binary_Encoding.get_row_buffers(index_records);


                var decoded_index_records = Model_Database.decode_model_rows(row_buffers);
                //console.log('decoded_index_records', decoded_index_records);
                callback(null, decoded_index_records);
            }
        });
    }



    // cs = client_side

    cs_update_record_update_indexes(arr_record_current, arr_record_new, callback) {
        // Need to work out what the index changes to make are.
        //  will need to delete the old index records, replace them with new ones.

        // May need more low-level functions on the server to do this.

        //ll_delete_by_key
        //ll_delete_by_keys

        // Then tell it to reload the model afterwards? Only in some cases, best not to complicate this fn.

        // Need to be able to change misplaced records.
        //  Once we have changed currency records back to how they should be, we can examine if we have all the market records we should have.

        // I think this data8 problem needs strong diagnosis and fixing. Get it back in working order and get the data from it.
        //  Do need to work on some more general database operations to fix it.
        //   Most of the datasets will be fine anyway I expect.

        // Also, creating a new db and importing the old data from it seems best.
        //  A sync method that does not only sync from the source for everything, it updates the source with corrected structural information.

        // Sharding at key subdivisions makes a lot of sense.
        //  Keys which begin with x are in shard group sg(x).
        //   There would not be all that many shard groups. No more than a machine can easily have port connections. Then later on we could have sharding within a shard group.
        //    Sharding would operate according to a different formula at that level.


        // Could see if there are any records that refer to this.
        //  If there are, then in some cases changing that reference would be best.
        //   But not in others.

        // It looks like it's worth setting up data9 and data10, and using them to import data from others, such as data8.

        // Carrying out some fixes on data8 will definitely be worth it.
        //  Want it to be ready for the next bittrex currency with fixed code and data in place.




        let model = this.model;
        let model_table = this.model.map_tables_by_id[(arr_record_current[0][0] - 2) / 2];



        let indexes = model.create_index_records_by_record(arr_record_current);
        console.log('indexes', indexes);

        let new_indexes = model.create_index_records_by_record(arr_record_new);
        console.log('new_indexes', new_indexes);

        let old_keys = [arr_record_current[0]].concat(indexes);
        console.log('old_keys', old_keys);

        // then put a batch of new rows.
        //  The indexes would need to be encoded without values.

        console.log('arr_record_current', arr_record_current);

        let new_rows = [arr_record_new].concat(new_indexes);
        console.log('new_rows', new_rows);

        // would be nice to batch operations to the server.
        //  transactions would be cool, it would need to keep a reversable log.

        // Don't want this yet.
        //  Check against a map of all the bittrex supplied currency codes.
        //   Seeing which currencies are missing will be useful for retrieval of longer term data from other servers.
        //    Will attempt diagnose only on them.
        //     Will also identify uncorrupted record sets this way.

        // It looks like it will be possible to methodically work through data recovery to get mostly working data sources
        //  Update various currency indexes to what they originally would have been.
        //   Check that the market records and indexes are correct.

        // Checking for orphan records.
        // Checking for index records which contain keys that are encoded incorrectly.

        // scan_key_range_malformed_indexes
        //  could be done server-side, and only returns indexes which have got a problem.

        // Getting back access to the data that has been harvested for over a month will be extremely useful.
        //  Will do comprehensive testing, then a comprehensive upgrade that will carry out diagnosis upon start.


        //this.ll_delete_and_put(old_keys, new_rows, callback);














        //this.ll_delete_and_put(old_keys, new_rows, callback);

        // Then next when it starts up we need to check that some existing / referenced currency records are there.
        //  Finding missing currency record currency codes would be useful.

        // It would use bittrex watcher to download the currency codes.




    }


    /**
     *
     *
     * @param {any} key_prefix
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */

    // Maybe this will be retired because the ll version has got more advanced.
    //  It knows the response contains encoded keys, so it will be able to dcode automatically,

    /*

    get_keys_by_key_prefix(key_prefix, callback) {


        this.ll_get_keys_by_key_prefix(key_prefix, (err, ll_res) => {
            if (err) {
                callback(err);
            } else {
                //console.log('ll_res', ll_res);
                var res = Model_Database.decode_keys(ll_res);
                callback(null, res);
            }
        });
    }
    */

    // Will make a lower level version of this.


    /*
    get_table_id_by_name(table_name, callback) {

        // This will be moved to become a lower level server operation.
        //  Doing that in part because other server functions will need to make use of this.

        // Also, want to make more general purpose index lookup functionality within the server.

        // This is an index lookup on the tables table

        // An index lookup function would be well suited as a lower level piece of functionality
        //  Looks up the ID of the object.

        // Other index lookups could get the record.

        // get_table_id_by_name
        // idx_id_lookup
        // table_idx_id_lookup

        // With this, we need to know which index we are referring to.

        // Bringing index lookups and other indexing functionality deeper into the server db makes a lot of sense.


        // idx_lookup(tables_table_idx_kp (3), 0, [table_name])



        // need to refer to the index of tables

        const tables_table_id = 0;
        const tables_table_kp = tables_table_id * 2 + 2;
        const tables_table_idx_kp = tables_table_kp + 1;
        const idx_id = 0;
        var buf_key_beginning = Model_Database.encode_index_key(
            tables_table_idx_kp,
            idx_id, [table_name]
        );


        // Keys by key prefix, expecting possibly multiple keys
        //  Don't want paging for this.

        // Should only get one key back when looking up table name.

        // this.table_index_value_lookup

        //  can get the numbered field, could make it get the named field too.



        //this.table_index_lookup(tables_table_id, idx_id, [table_name]);



        // table_index_value_lookup
        //  would be a nice function to have on the server, then to make available through the ll api.




        // This more generic function would be a way to lower the amount of code needed for a variety of cases
        //this.table_index_value_lookup(tables_table_id, idx_id, [table_name], 3, callback);
        //  or maybe like this.table_index_value_lookup(tables_table_id, idx_id, [table_name], 'name', callback), where a string is used for the field, rather than an int index within the index.



        this.ll_get_keys_by_key_prefix(buf_key_beginning, (err, keys_beginning) => {
            if (err) {
                callback(err);
            } else {
                var key_beginning = keys_beginning[0];
                //console.log('key_beginning', key_beginning);

                console.log('table_name', table_name);
                var table_id = key_beginning[3];
                callback(null, table_id);
            }
        });


    }
    */

    get_table_kp_by_name(table_name, callback) {
        //console.log('get_table_kp_by_name table_name', table_name);
        this.get_table_id_by_name(table_name, (err, id) => {
            if (err) {
                callback(err);
            } else {
                // 

                //console.log('');
                //console.log('***id', id);
                //console.log('');
                callback(null, id * 2 + 2);
            }
        });
    }

    // [name, arr_table_def]

    // Could just encode it and pass it to the server in a query.

    /*

    ensure_table(table, callback) {

        // There will be some kind of table definition object.

        let t_table = tof(table);
        console.log('table', table);
        // Could use the definitions already made elsewhere.

        let t_sig = get_item_sig(table);
        console.log(t_sig, t_sig);

        // May need some lower level functionality in the DB to create a table.
        //  Don't want to have to use the Model on the client-side.

        // Use of the Server-side Model should be fine.
        //  Get that server-side Model to come up with the new DB rows.

        // Would need to create a bunch of new rows for things such as incrementors.

        // ensure_table seems like a decent step to take during setup.
        //  then can ensure a variety of records.

        // Check that the params are OK, then call the lower level ensure table function.


        //throw 'stop';


        if (t_table === 'array') {
            // is it length 2, containing 2 other arrays?

            if (table.length === 2) {

                // Get the current model.
                //  (do that twice?)

                // get current model twice.

                // diff the changed one with the original one.



                // Maybe better facilities to clone a Model.

                this.load_2_core((err, models) => {
                    if (err) {
                        callback(err);
                    } else {
                        let [m1, m2] = models;
                        m2.ensure_table(table);
                        let changes = m1.diff(m2);
                        console.log('changes', changes);

                    }
                })
            } else {


            }
        }

    }

    */

    // tables 2, native types 4, fields 6, indexes 8

    // Also want to get an array of the field names
    //  And map of the field names

    // get_map_table_field_names_by_id
    //  Then can use this to look up fields conveniently

    get_map_table_field_names_by_id(table_name, callback) {
        const that = this;
        that.get_table_field_names(table_name, (err, arr_field_names) => {
            if (err) {
                callback(err);
            } else {
                let res = {};
                each(arr_field_names, (v, i) => {
                    res[i] = v;
                });
                callback(null, res);
            }
        });
    }

    get_map_table_field_names_by_id_by_table_id(table_id, callback) {
        const that = this;
        that.get_table_field_names_by_table_id(table_id, (err, arr_field_names) => {
            if (err) {
                callback(err);
            } else {
                let res = {};
                each(arr_field_names, (v, i) => {
                    res[i] = v;
                });
                callback(null, res);
            }
        });
    }

    get_table_field_names_by_table_id(table_id, callback) {
        // Should know the table fields id already.
        const that = this;
        const table_fields_kp = TABLE_FIELDS_TABLE_ID * 2 + 2;
        const buf = Model_Database.encode_key(table_fields_kp, [table_id]);
        that.get_records_by_key_prefix(buf, (err, fields_records) => {
            if (err) {
                callback(err);
            } else {
                let res = [];
                each(fields_records, record => {
                    res.push(record[1][0]);
                });
                callback(null, res);
            }
        });
    }

    get_table_field_names(table_name, callback) {
        const that = this;
        const table_fields_kp = TABLE_FIELDS_TABLE_ID * 2 + 2;
        that.get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                callback(err);
            } else {
                const buf = Model_Database.encode_key(table_fields_kp, [table_id]);
                that.get_records_by_key_prefix(buf, (err, fields_records) => {
                    if (err) {
                        callback(err);
                    } else {
                        let res = [];
                        each(fields_records, record => {
                            res.push(record[1][0]);
                        });
                        callback(null, res);
                    }
                });
            }
        });
    }

    get_table_fields_records(table_name, callback) {
        const table_fields_kp = TABLE_FIELDS_TABLE_ID * 2 + 2;
        this.get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                callback(err);
            } else {
                const buf = Model_Database.encode_key(table_fields_kp, [table_id]);
                this.get_records_by_key_prefix(buf, (err, fields_records) => {
                    if (err) {
                        callback(err);
                    } else {
                        callback(null, fields_records);
                    }
                });
            }
        });
    }

    // Table initially gets set up in the crypto data model.
    //  Perhaps, some kind of active record system could persist data.


    // Maybe these fields should have types set from the beginning.
    //  That would help with record validation.

    get_table_fields_info(table_name, callback) {

        // Looking up the references, and the types, will help 

        // Getting the info on native types makes sense here.
        //  Basically get the native type table records.
        //  A map of the native types seems best.

        this.get_table_fields_records(table_name, (err, records) => {
            if (err) {
                callback(err);
            } else {
                let k = [],
                    v = [],
                    res = [k, v];

                records.forEach((item, index) => {
                    let i2 = item[1];
                    let is_pk = i2[2];
                    let name = i2[0];
                    let i_fk_to = i2[3];

                    if (is_pk) {
                        k.push(name);
                    } else {
                        v.push(name);
                    }
                })
                callback(null, res);
            }
        })
    }

    // get the table field records as arrays?

    // Want to be able to verify that records going into the DB are in the right format.

    // A function to generate sample records with the right types could be useful.

    // get them as loaded parts of the model?
    //  I think this needs to run separately from the model.
    //  The model will help with consistency, as well as more complicated operations, such as creating a new DB from scratch.

    // get_table_kv_field_names

    get_table_kv_field_names(table_name, callback) {

        // Have this work as a promise or callback.

        let inner = callback => {
            const that = this;
            const table_fields_kp = TABLE_FIELDS_TABLE_ID * 2 + 2;
            that.get_table_id_by_name(table_name, (err, table_id) => {
                if (err) {
                    callback(err);
                } else {

                    //console.log('table_id', table_id);

                    //var akp = [table_fields_id, table_id];
                    let buf = Model_Database.encode_key(table_fields_kp, [table_id]);


                    // Then need to decode the records.
                    // Would be nicer to use an observable here, as we call a function on each record we get.

                    // With decoding as a default?

                    that.get_records_by_key_prefix(buf, true, (err, fields_records) => {
                        if (err) {
                            callback(err);
                        } else {
                            //console.log('fields_records', fields_records);

                            let res_keys = [];
                            let res_values = [];
                            let res = [res_keys, res_values];
                            let is_pk;

                            // So the fields records just have values...?
                            each(fields_records, record => {
                                //console.log('record', record);

                                is_pk = record[1][2];
                                if (is_pk) {
                                    res_keys.push(record[1][0]);
                                } else {
                                    res_values.push(record[1][0]);
                                }
                            });

                            callback(null, res);
                        }
                    });
                }
            });
            //that.ll_get_t
        }


        return prom_or_cb(inner, callback);


    }

    count_table_pk_fields_by_table_id(table_id, callback) {
        const that = this;
        const table_fields_kp = TABLE_FIELDS_TABLE_ID * 2 + 2;
        let buf = Model_Database.encode_key(table_fields_kp, [table_id]);
        that.get_records_by_key_prefix(buf, (err, fields_records) => {
            if (err) {
                callback(err);
            } else {
                let res = 0;
                each(fields_records, record => {
                    is_pk = record[1][2];
                    if (is_pk) {
                        res++;
                    } else {}
                });
                callback(null, res);
            }
        });
    }

    count_table_fields_by_table_id(table_id, callback) {
        const that = this;
        const table_fields_kp = TABLE_FIELDS_TABLE_ID * 2 + 2;
        let buf = Model_Database.encode_key(table_fields_kp, [table_id]);
        //console.log('buf', buf);
        that.count_records_by_key_prefix(buf, callback);
    }

    // Something to get the fields, alongside indexes in the table that use them.
    //  One issue, are the table index definitions indexed?

    // get table names
    // does a get on the tables table
    //  just getting the names

    get_table_names(callback) {
        // Does not use the model.
        // Queries the tables table
        // Think the Table_Table has kp of 2.

        this.get_records_by_key_prefix(2, (err, res_records) => {
            if (err) {
                callback(err);
            } else {
                //console.log("res_records", res_records);
                let res = res_records.map(x => x[1][0]);
                callback(null, res);
            }
        });

        //this.get_at_table_records
    }

    check_table_records_exist(table_name, arr_arr_records, callback) {
        let fns = Fns();
        var that = this;
        arr_arr_records.forEach(arr_record => {
            fns.push([
                that,
                that.check_table_record_index_lookup, [table_name, arr_record]
            ]);
        });
        fns.go(callback);
    }

    // For the moment, want an check_record_index_lookup function.
    //  We could try it with bitcoin / whatever currency records.
    //  Want to see if we can check if a record exists.

    // The Model code helps to think about the structure and verify it.
    //  It may not always be the fastest or most efficient way of doing things.
    //  Where possible, the simpler operations should avoid using the Model, and instead operate on a lower level.

    check_table_record_index_lookup(table_name, arr_record, callback) {
        const that = this;
        let table_indexes_kp = TABLE_INDEXES_TABLE_ID * 2 + 2;

        that.get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                callback(err);
            } else {
                that.count_table_fields_by_table_id(table_id, (err, fields_count) => {
                    if (err) {
                        callback(err);
                    } else {


                        let size_diff = 0;

                        if (arr_record.length < fields_count) {
                            size_diff = fields_count - arr_record.length;
                        }
                        //console.log('size_diff', size_diff);

                        let buf_key = Model_Database.encode_key(table_indexes_kp, [
                            table_id
                        ]);
                        //console.log('buf_key', buf_key);
                        // then get all records beginning with that key
                        that.get_records_by_key_prefix(
                            buf_key,
                            (err, res_table_index_records) => {
                                if (err) {
                                    callback(err);
                                } else {
                                    //console.log("res_table_index_records.length", res_table_index_records.length);
                                    let table_indexes_kp = table_id * 2 + 2;

                                    // for each of the index records, do a search for values with the field at the set value

                                    // would need to encode index record keys
                                    let index_values = [],
                                        arr_buf_index_lookup_keys = [];

                                    each(res_table_index_records, record => {
                                        //console.log("record", record);
                                        let table_id = record[0][0];
                                        let table_index_id = record[0][1];
                                        // is it just one field that gets indexed?
                                        //  That is how the unique indexes are set up so far. They get specified with '!'.

                                        let index_field_id = record[0][2];

                                        if (record[1].length === 1) {
                                            let index_value_field_ids = record[1];
                                            let val = arr_record[index_field_id - size_diff];
                                            let encoded_index_key = Model_Database.encode_index_key(
                                                table_id * 2 + 3,
                                                table_index_id, [val]
                                            );
                                            arr_buf_index_lookup_keys.push(encoded_index_key);

                                            //Model_Database.encode_index_key(table_indexes_kp, table_index_id,
                                        } else {
                                            //console.trace();
                                            //console.log('record', record);
                                            //console.log('arr_record', arr_record);

                                            let index_value_field_ids = record[1];
                                            //console.log('index_value_field_ids', index_value_field_ids);
                                            //console.log('size_diff', size_diff);

                                            let arr_indexed_values = [];
                                            index_value_field_ids.forEach(id => {
                                                arr_indexed_values.push(arr_record[id - size_diff]);
                                            });

                                            let encoded_index_key = Model_Database.encode_index_key(
                                                table_id * 2 + 3,
                                                table_index_id,
                                                arr_indexed_values
                                            );
                                            arr_buf_index_lookup_keys.push(encoded_index_key);
                                            //console.log('encoded_index_key', encoded_index_key);
                                            //throw "NYI";
                                        }

                                        // then need to construct the index keys using this.
                                        //
                                    });
                                    var fns = Fns();
                                    each(arr_buf_index_lookup_keys, buf_key => {
                                        fns.push([that, that.ll_get_keys_beginning, [buf_key]]);
                                    });
                                    fns.go((err, res_all) => {
                                        if (err) {
                                            callback(err);
                                        } else {
                                            //console.log('res_all', res_all);

                                            if (res_all[0].length > 0) {
                                                let decoded = [];
                                                var id_pos = 3;
                                                //var record_id =

                                                // use the first record id?
                                                //  using the last, as it stands.

                                                let res = -1;

                                                each(res_all, lookup_item_res => {
                                                    let decoded_row = Model_Database.decode_key(
                                                        lookup_item_res[0]
                                                    );

                                                    let record_id = decoded_row[id_pos];
                                                    //console.log('record_id', record_id);

                                                    decoded.push(decoded_row);
                                                    res = record_id;
                                                });

                                                //console.log('decoded', decoded);

                                                callback(null, res);
                                            } else {
                                                callback(null, false);
                                            }
                                        }
                                    });
                                }
                            }
                        );
                    }
                });
            }
        });
    }

    // Having a lower level put that makes use of the index would make sense.
    //  Or call it INSERT

    // Let's have our insert record function that itself carries out the lower level put operations including the record itself, as well as the index records.

    // Then we will have plenty of bittrex records to add.
    //  Will be worth collecting from other exchanges soon too.

    query(arr_command, callback) {
        let buf_query = Binary_Encoding.encode_to_buffer(arr_query);
        console.log("buf_query", buf_query);
    }

    insert_table_record(table_name, arr_record, callback) {
        let that = this;

        that.get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                callback(err);
            } else {
                // Then get the map fields for that table.

                // compose the query

                let arr_query = [INSERT_TABLE_RECORD, table_id, arr_record];

                //console.log("table_id", table_id);

                // Then should have a convenient way for doing / encoding that kind of query.

                // Would be nicer to have more conventionally / simply encoded db queries?
                //  Or always treat the first as a key prefix.

                //

                let buf_query = Binary_Encoding.encode_to_buffer(
                    arr_query.slice(1),
                    arr_query[0]
                );

                //console.log("buf_query", buf_query);

                that.send_binary_message(buf_query, (err, res_query) => {
                    if (err) {
                        callback(err);
                    } else {
                        //console.log("res_query", res_query);
                        //throw "stop";

                        // Need to decode the res_query
                        //  res_query <Buffer fc 00 25>
                        connection.sendBytes(buf_res);
                    }
                });
            }
        });
    }

    insert_table_records(table_name, arr_arr_records, callback) {
        // An insert single record would be useful to have as well.
        // Multi-insert should make use of server-side batching.
        // Lower level insert record capability will be useful when it comes to efficiently adding records.
    }

    // Really need strong DB capabilities to store all this crypto data.
    //  Don't want to waste space, and also to have the data available quickly.

    // Will probably use these records more for the actual lookups.
    //  They are in a very concise form, so need to take care in interpreting them.

    get_native_types_records(callback) {
        this.get_table_id_by_name('native types', (err, table_id) => {
            if (err) {
                callback(err);
            } else {
                // Then get the map fields for that table.
                that.get_table_records(table_id, callback);
            }
        });
    }

    get_native_types_info(callback) {
        this.get_native_types_records((err, records) => {
            if (err) {
                callback(err);
            } else {
                console.log('records', records);
                throw 'stop';

            }
        })
    }

    get_native_types_map_info(callback) {
        this.get_native_types_info((err, info) => {
            if (err) {
                callback(err);
            } else {
                console.log('info', info);

            }
        })
    }

    // get_table_indexes_table_records
    get_table_indexes_records(table_name, callback) {
        const that = this;

        //let table_indexes_id = 3;
        let table_indexes_kp = TABLE_INDEXES_TABLE_ID * 2 + 2;

        that.get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                callback(err);
            } else {
                // Then get the map fields for that table.

                that.get_map_table_field_names_by_id_by_table_id(
                    table_id,
                    (err, map_field_names) => {
                        if (err) {
                            callback(err);
                        } else {
                            //console.log('* map_field_names', map_field_names);
                            let buf_key = Model_Database.encode_key(table_indexes_kp, [
                                table_id
                            ]);
                            //console.log('buf_key', buf_key);
                            // then get all records beginning with that key
                            that.get_records_by_key_prefix(buf_key, callback);
                        }
                    }
                );
            }
        });
    }

    get_table_indexes_info(table_name, callback) {
        // Get the table id
        //  look up the indexes table.

        // Should refer to the map of field names for the table
        // TABLE_FIELDS_TABLE_ID

        const that = this;

        //let table_indexes_id = 3;
        let table_indexes_kp = TABLE_INDEXES_TABLE_ID * 2 + 2;
        //let that = this;

        // Get the fields map for the table.

        //that.get_map_table_field_names_by_id();

        that.get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                callback(err);
            } else {
                // Then get the map fields for that table.

                that.get_map_table_field_names_by_id_by_table_id(
                    table_id,
                    (err, map_field_names) => {
                        if (err) {
                            callback(err);
                        } else {
                            //console.log('* map_field_names', map_field_names);

                            let buf_key = Model_Database.encode_key(table_indexes_kp, [
                                table_id
                            ]);
                            //console.log('buf_key', buf_key);

                            // then get all records beginning with that key
                            that.get_records_by_key_prefix(buf_key, (err, records) => {
                                if (err) {
                                    callback(err);
                                } else {
                                    //console.log('records', records);

                                    let res = [],
                                        res_record;
                                    records.forEach(record => {
                                        res_record = clone(record);

                                        if (record[0].length === 3) {
                                            res_record[0][0] = table_name;
                                            res_record[0][2] = map_field_names[res_record[0][2]];
                                            res_record[1][0] = map_field_names[res_record[1][0]];
                                            res.push(res_record);
                                        } else {
                                            console.log("record", record);
                                            throw "NYI";
                                        }
                                    });
                                    callback(null, res);
                                }
                            });
                        }
                    }
                );
                // Then [kp4, table_id]

                // Construct the key
            }
        });
    }




    // Would like to use paging in the background, but get the results back one at a time in the API.
    //  Saves having to write an each() around the results set, though for loops would be more performant. 

    // The default, simplest mode should probably default to that.

    // New version...

    get_table_keys(table_name, paging, decode = true, callback) {

        // With optional decoding too...


        //let page_size = 8192;

        let page_size = 32768;

        let a = arguments,
            sig = get_a_sig(a);

        //console.log('get_table_records sig', sig);

        if (sig === '[s]') {
            paging = new Paging.Key(page_size);
        } else if (sig === '[s,f]') {
            callback = a[1];
            paging = null;
        } else if (sig === '[s,b]') {
            //callback = a[1];
            decode = a[1];
            paging = new Paging.Key(page_size);
            //paging = new Paging.None();
        } else if (sig === '[s,b,f]') {
            paging = null;
            decode = a[1];
            callback = a[2];
        } else {
            console.trace();
            throw 'Unexpected sig to get_table_records: ' + sig;
        }

        let obs_res;
        if (!callback) {
            obs_res = new Evented_Class();
        }
        this.get_table_kp_by_name(table_name, (err, kp) => {
            if (err) {

                if (callback) {
                    callback(err);
                } else {
                    obs_res.raise('error', err);
                }


            } else {
                // Should also use an observable version of this, though the version with the callback would also be useful.
                if (callback) {
                    // Remove table kps from records when decoding.
                    this.get_keys_by_key_prefix(kp, decode, callback);
                } else {
                    //console.log('paging', paging);

                    // Would like it so that we get the keys individually on the client.
                    //  will make for more fn calls with a callback for each result.




                    let obs = this.get_keys_by_key_prefix(kp, paging, decode, true);


                    let data_pages = [];
                    obs.on('next', data => {
                        obs_res.raise('next', data);
                    });
                    obs.on('complete', data => {
                        obs_res.raise('complete', data);
                    });
                }
            }
        });
        if (!callback) {
            return obs_res;
        }
    }

    // Should probably retire this and upgrade the ll version, which will do decoding by default
    /*
    /**
     *
     *
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     * /
    get_table_keys(table_name, callback) {


        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                this.get_keys_by_key_prefix(kp, callback);
            } else {
                callback("Table " + table_name + " not found");
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback("Expected this.model, otherwise can't find table by name");
        }
    }
    */

    /**
     *
     *
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    get_table_index_keys(table_name, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                this.get_keys_by_key_prefix(kp + 1, callback);
            } else {
                callback("Table " + table_name + " not found");
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback("Expected this.model, otherwise can't find table by name");
        }
    }

    new_backup_path(name, callback) {
        //console.log('new_backup_path');
        new_backup_path(name, callback);
    }

    /**
     *
     *
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */

    // This would be nice with a limit param.
    //  Time limit / record count limit.

    count_table_records(table_name, limit = -1, callback) {


        let a = arguments,
            l = a.length;
        if (l === 2) {
            callback = a[1];
            limit = -1;
        }

        // use a get table id promise

        //let prom_table_id = get_table_id_by_name(table_name);
        //prom_table_id.then(table_id)

        let obs_res = new Evented_Class();

        this.get_table_id_by_name(table_name).then(table_id => {
            let kp = table_id * 2 + 2;

            //this.count_records_by_key_prefix(kp, callback);


            // But the counts should not be as an array

            // A version with a limit would be nice.
            let obs_count_records = this.count_records_by_key_prefix(kp, limit);

            let t_obs_count_records = tof(obs_count_records);
            //console.log('t_obs_count_records', t_obs_count_records);

            // Would help to detect an Evented Class or Observable.



            // Pass it an O observable or E evented class?
            //  Detecting Observables and Promises in tof would be useful.

            // That way we could pass one observable returning function another observable to send results to, rather than a callback.

            console.log('obs_res, obs_count_records', obs_res, obs_count_records);

            obs_throughput(obs_res, obs_count_records);


        });


        return obs_res;

        /*

        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                this.count_records_by_key_prefix(kp, callback);
            } else {
                callback("Table " + table_name + " not found");
            }
        } else {

            // We could look up the key prefix in the database.
            //  Will make more advanced functionality that does not require having the model loaded on the client - but making use of the client-side model will be available for some more complex features, as well as a
            //  way to guarantee consistency.

            if (callback) {
                this.get_table_id_by_name(table_name, (err, table_id) => {
                    if (err) {
                        callback(err);
                    } else {
                        let kp = table_id * 2 + 2;
                        this.count_records_by_key_prefix(kp, callback);
    
                    }
                });
            } else {

            }

            
        }
        */
    }

    count_table_records_up_to(table_name, limit, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                this.count_records_by_key_prefix_up_to(kp, limit, callback);
            } else {
                callback("Table " + table_name + " not found");
            }
        } else {

            // We could look up the key prefix in the database.
            //  Will make more advanced functionality that does not require having the model loaded on the client - but making use of the client-side model will be available for some more complex features, as well as a
            //  way to guarantee consistency.

            this.get_table_id_by_name(table_name, (err, table_id) => {
                if (err) {
                    callback(err);
                } else {
                    let kp = table_id * 2 + 2;
                    this.count_records_by_key_prefix_up_to(kp, limit, callback);

                }
            });
        }
    }

    /**
     *
     *
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    count_table_index_records(table_name, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                this.count_records_by_key_prefix(kp + 1, callback);
            } else {
                callback("Table " + table_name + " not found");
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback("Expected this.model, otherwise can't find table by name");
        }
    }

    count_each_table_records(callback) {
        this.get_table_names((err, table_names) => {
            if (err) {
                callback(err);
            } else {
                let fns = Fns();
                each(table_names, table_name => {
                    fns.push([this, this.count_table_records, [table_name]]);
                })
                fns.go((err, res_all) => {
                    if (err) {
                        callback(err);
                    } else {
                        let res = [];
                        res_all.forEach((v, i) => {
                            res.push([table_names[i], v]);
                        })
                        callback(null, res);
                    }
                })
            }
        })
    }

    count_each_table_records_up_to(limit, callback) {
        //console.log('count_each_table_records');
        this.get_table_names((err, table_names) => {
            if (err) {
                callback(err);
            } else {
                //console.log('table_names', table_names);
                let fns = Fns();
                each(table_names, table_name => {
                    fns.push([this, this.count_table_records_up_to, [table_name, limit]]);
                })
                fns.go((err, res_all) => {
                    if (err) {
                        callback(err);
                    } else {
                        let res = [];
                        res_all.forEach((v, i) => {
                            res.push([table_names[i], v]);
                        })
                        callback(null, res);
                    }
                })
            }
        })
    }



    get_table_selection_records(table_name, arr_key_selection, callback) {

        // This would be nicer if it uses an observable to get a number of records.
        //  If it has some observable util functions where it normally processes as an observable, but then can be run as a callback.

        // May be worth putting into lang-mini

        //let res = new Evented_Class;


        let table_id = this.model.table_id(table_name);
        //console.log('table_id', table_id);
        //console.log('arr_key_selection', arr_key_selection);

        var buf = Model_Database.encode_key(
            table_id * 2 + 2,
            arr_key_selection
        );

        // And does not decode the records.

        let res = this.get_records_by_key_prefix(buf);

        if (callback) {
            // obs_to_cb(res);
            //console.log('using obs_to_cb');
            obs_to_cb(res, callback);

        } else {
            return res;
        }
    }

    // Selecting from the index...
    //  should use key prefix plus one
    /**
     *
     *
     * @param {any} table_name
     * @param {any} arr_index_selection
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    count_table_selection_records(table_name, arr_index_selection, callback) {

        // accept callback
        //  observable would be better, getting counts as it progresses.

        // can rely on there being a Model with correct core now.

        //console.log('count_table_selection_records table_name', table_name);



        if (callback) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                var encoded = Binary_Encoding.encode_to_buffer(arr_index_selection, kp);
                this.count_keys_beginning(encoded, callback);
            } else {
                callback("Table " + table_name + " not found");
            }
        } else {
            // an observable will be the result.

            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                var encoded = Binary_Encoding.encode_to_buffer(arr_index_selection, kp);
                return this.count_keys_beginning(encoded);
            } else {
                throw new Error("Table " + table_name + " not found");
            }

        }




        /*
        if (this.model) {
            
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback("Expected this.model, otherwise can't find table by name");
        }
        */
    }

    /**
     * @param {any} table_name
     * @param {any} arr_index_selection
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    get_table_index_selection_records(table_name, arr_index_selection, callback) {

        throw 'get_table_index_selection_records NYI';
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix + 1;
                var encoded = Binary_Encoding.encode_to_buffer(arr_index_selection, kp);
            } else {
                callback("Table " + table_name + " not found");
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback("Expected this.model, otherwise can't find table by name");
        }
    }


    // Want a matching function.
    //  Won't search through index.
    //  Get table records, apply matching function to these table records.

    // hard to do this efficiently client-side.
    //  hard to get a matching function over to the server.

    // an OO result match could be ok. will match against the field names / indexes
    //  Then the matching info will be sent from the client to the server.
    //   Matching info could be part of extended options, as it operates on the results set.



    // matching records against values for retrieval seems important
    //  seems a lot like a 'where' clause in SQL.






    select_from_table(table, arr_fields, paging, decode = true, callback) {


        // want to track the specified paging too.
        //  We may use one type of paging to communicate with the server, and another type of paging for the results.
        //  Individual results seem more useful.

        // results as 'single' or 'page'

        let result_grouping = 'single';





        // Could automatically encode the array of fields as their IDs, use the model for this?
        //  Then only the field IDs would be sent to the server, save bandwidth and some server-side processing (not much).


        let a = arguments,
            sig = get_a_sig(a);

        // Want to specify record paging
        //  Will get back binary buffers.
        //  The non-decode version will be called on the server, decoding here is an option on the client.

        // Then very soon need to move to more work on syncing.
        //  Making the syncing process very quick on startup if there is not much to sync.
        //   Would involve looking into the subdivisions of records both on the local and remote.
        //    select_from_table is one of the underlying functions which would enable this in a less clunky way.



        //  Showing progress indications.


        let table_id;


        console.log('select_from_table sig', sig);

        if (sig === '[s,a]') {
            table_id = this.model.table_id(table);
            paging = new Paging.Record(1024);
            // by default lets get 1024 records at once.
        }

        // Paging by default will be useful in many situations.
        //  


        // Though we specify paging on observe_send_binary_message, we also want to be able to split up results that come in.



        let res = this.observe_send_binary_message(SELECT_FROM_TABLE, [table_id, arr_fields], paging, result_grouping);




        if (callback) {
            throw 'NYI';
        } else {
            return res;
        }



        //throw 'stop';

        // basically call the observable function.



        // Use inner observable?
        //  That seems best, while we build up the results on the client-side, still using paging to get the data from the server.
        //   Paged data retrieval is much kinder on the server's resources.






    }

    // get table record by index lookup
    // get table record where, full table scan

    // And what about getting an OO record class?
    //  These records here are kv arrays.

    // Could make more flexibility on return types.


    get_table_record(table_name, arr_key, callback) {
        this.get_table_kp_by_name(table_name, (err, kp) => {
            if (err) {
                callback(err);
            } else {
                let buf_key = Model_Database.encode_key(kp, arr_key);
                this.ll_get_record(buf_key, (err, ll_res) => {
                    if (err) {
                        callback(err);
                    } else {

                        if (ll_res && ll_res.length > 0) {
                            let arr_bufs_kv = Binary_Encoding.split_length_item_encoded_buffer_to_kv(ll_res);
                            let remove_kp = true;
                            let arr_decoded = Model_Database.decode_model_row(arr_bufs_kv[0], remove_kp);
                            callback(null, arr_decoded);
                        } else {
                            callback(null, undefined);
                        }

                        //let num_xas2_prefixes = 0;

                    }
                })
            }
        })
    }

    get_by_arr_key(arr_key, callback) {
        // Would perhaps get an index record back.
        //  The encoding of this is a bit trickier.
        //   The index records are just stored in keys.
        //    Its the last value of the index record which is the id for the item.

        throw 'NYI';

        // Seems maybe tricky for operations from the DB that return both index and normal records together?


    }


    // Incrementors make putting data a lot more difficult, because 

    put_model_record(model_record, callback) {

        // This has a problem where it ignores the incrementors.
        //  Don't think incrementation should be here.
        //  Need to update incrementor values in some cases though.

        // and the pk incrementor record if there is one.

        let bufs;

        /*
        if (model_record.table.pk_incrementor) {
            // get that incrementor row.

            let inc_bin = model_record.table.pk_incrementor.get_record_bin();

            bufs = Buffer.concat([inc_bin, model_record.to_arr_buffer_with_indexes()]);
        } else {
            bufs = model_record.to_arr_buffer_with_indexes();
        }
        */

        bufs = model_record.to_arr_buffer_with_indexes();
        //let buf = model_record.to_buffer_with_indexes();



        //console.log('bufs', bufs);
        //console.log('model_record', model_record);




        //let row_buffers = Binary_Encoding.get_row_buffers(buf);
        //console.log('row_buffers', row_buffers);
        // Decode that buffer?
        //console.log('bufs', bufs);

        //let buf2 = Buffer.concat(flatten(bufs));
        //console.log('buf2', buf2);
        //console.log('buf2.length', buf2.length);

        // Think we need to encode these differently.
        let buf3;
        if (model_record.table.pk_incrementor) {
            //console.log('[Model_Database.encode_model_rows(bufs), model_record.table.pk_incrementor.get_record_bin()]', [Model_Database.encode_model_rows(bufs), Model_Database.encode_model_rows(model_record.table.pk_incrementor.get_record_bin())]);

            //let bufs2 = Array.concat()

            // This part does not seem quite right.

            //console.log('bufs', bufs);
            let inc_record_bin = model_record.table.pk_incrementor.get_record_bin();
            //console.log('inc_record_bin', inc_record_bin);

            bufs.push(inc_record_bin);
            //throw 'stop';
            buf3 = Model_Database.encode_model_rows(bufs);



            //buf3 = Buffer.concat([Model_Database.encode_model_rows(bufs), Model_Database.encode_model_rows(model_record.table.pk_incrementor.get_record_bin())]);
        } else {
            buf3 = Model_Database.encode_model_rows(bufs);
        }


        //console.log('buf3', buf3);
        //console.log('buf2.length', buf2.length);
        //console.log('buf3.length', buf3.length);


        //throw 'stop';

        this.ll_put_records_buffer(buf3, callback);
    }

    // get table index records
    //  (all of them)

    subscribe_all(subscription_event_handler) {
        var unsubscribe = this.ll_subscribe_all(ll_subscription_event => {
            var pos = 0,
                i_num,
                i_sub_evt_type;

            [i_num, pos] = xas2.read(ll_subscription_event, pos);
            [i_sub_evt_type, pos] = xas2.read(ll_subscription_event, pos);

            var buf_the_rest = Buffer.alloc(ll_subscription_event.length - pos);
            ll_subscription_event.copy(buf_the_rest, 0, pos);

            var res = {};
            if (i_sub_evt_type === SUB_CONNECTED) {
                console.log("Connected!");

                // When it is connected, we return the subscription id.
                //console.log('i_num', i_num);
                // May need the subscription number to unsubscribe.

                res.type = "connected";
                res.client_subscription_id = i_num;
                res.id = i_num;
                subscription_event_handler(res);
            }

            if (i_sub_evt_type === SUB_RES_TYPE_BATCH_PUT) {
                //console.log('SUB_RES_TYPE_BATCH_PUT', SUB_RES_TYPE_BATCH_PUT);

                //console.log('buf_the_rest', buf_the_rest);

                res.type = "batch_put";

                // need to decode the buffer.
                var row_buffers = Binary_Encoding.get_row_buffers(buf_the_rest);
                //console.log('row_buffers', row_buffers);

                var decoded_row_buffers = Model_Database.decode_model_rows(row_buffers);

                //console.log('decoded_row_buffers', decoded_row_buffers);

                res.records = decoded_row_buffers;

                subscription_event_handler(res);
            }
        });
        return unsubscribe;
    }

    subscribe_key_prefix_puts(buf_kp, subscription_event_handler, remove_kp) {
        var unsubscribe = this.ll_subscribe_key_prefix_puts(
            buf_kp,
            ll_subscription_event => {
                var pos = 0,
                    i_num,
                    i_sub_evt_type;
                [i_num, pos] = xas2.read(ll_subscription_event, pos);
                [i_sub_evt_type, pos] = xas2.read(ll_subscription_event, pos);
                //console.log('[i_num, i_sub_evt_type]', [i_num, i_sub_evt_type]);

                var buf_the_rest = Buffer.alloc(ll_subscription_event.length - pos);
                ll_subscription_event.copy(buf_the_rest, 0, pos);
                //console.log('ll_subscription_event.length', ll_subscription_event.length);

                var res = {};
                if (i_sub_evt_type === SUB_CONNECTED) {
                    console.log("Connected");

                    // When it is connected, we return the subscription id.
                    //console.log('i_num', i_num);
                    // May need the subscription number to unsubscribe.

                    res.type = "connected";
                    res.client_subscription_id = i_num;
                    res.id = i_num;
                    subscription_event_handler(res);
                }

                if (i_sub_evt_type === SUB_RES_TYPE_BATCH_PUT) {
                    //console.log("SUB_RES_TYPE_BATCH_PUT", SUB_RES_TYPE_BATCH_PUT);

                    //console.log('buf_the_rest', buf_the_rest);

                    res.type = "batch_put";

                    // need to decode the buffer.
                    var row_buffers = Binary_Encoding.get_row_buffers(buf_the_rest);
                    //console.log('row_buffers', row_buffers);

                    each(row_buffers, (rb, i) => {
                        var d = Model_Database.decode_model_row(rb, remove_kp);
                    });
                    var decoded_row_buffers = Model_Database.decode_model_rows(
                        row_buffers, remove_kp
                    );
                    //console.log('decoded_row_buffers', decoded_row_buffers);
                    res.records = decoded_row_buffers;
                    subscription_event_handler(res);
                }
            }
        );
        return unsubscribe;
    }
    // a version that removes the table kp from the records...

    subscribe_table_puts(table_name, subscription_event_handler, remove_kp = true) {
        //var that = this;
        this.get_table_kp_by_name(table_name, (err, kp) => {
            if (err) {
                subscription_event_handler({
                    error: err
                });
            } else {
                let buf_kp = xas2(kp).buffer;
                let unsubscribe = this.subscribe_key_prefix_puts(buf_kp, subscription_event_handler, remove_kp);
            }
        });
    }

    get_table_subscription(table_name) {
        // Then within the table subscription, we can subscribe to a filter or possibly fast key lookup.
        //  Would respond with events for actions that happen to that table.
        // 

        // Subscribe here with the subscribe_table_puts function above, then have the events go through a Table_Subscription object.

        let res = new Table_Subscription();

        // Getting closer to the raw data back. It's decoded, but still has the table key prefix.
        //  Maybe this will be ll subscribe table puts.

        this.subscribe_table_puts(table_name, (table_event) => {
            //console.log('table_event', table_event);

            let type = table_event.type;
            if (type === 'batch_put') {
                res.raise('batch_put', table_event.records);
            }
            if (type === 'put') {
                res.raise('put', table_event.record);
            }

            //throw 'stop';
        });
        return res;
    }




    get_table_record_pk_by_index_lookup(table_name, index_field_name, index_field_value, callback) {


        // inner function, could be async.

        /*

        (async () => {

            



            //throw 'stop';

        })();

        */

        let table = this.model.map_tables[table_name];

        let table_id = table.id;
        let table_kp = table_id * 2 + 2;
        let table_ikp = table_kp + 1;

        //console.log('index_field_name', index_field_name);

        let field_id = table.map_fields[index_field_name].id;
        //console.log('***field_id', field_id);

        let index_id = table.get_index_id_by_field_id(field_id);
        //console.log('index_id', index_id);

        // then do the lookup on the index with that id.

        // could use the Buffer-Backed Index-Key to do this.
        //  Like a normal key, but put together differently.

        // Or the normal key would function as an index key.

        let idx_beginning = new Key([table_ikp, index_id, index_field_value]);

        //console.log('idx_beginning', idx_beginning);
        //console.log('idx_beginning.buffer', idx_beginning.buffer);

        // then do the lookup

        // get a single record by the key prefix


        let res = new Promise((resolve, reject) => {
            this.get_records_by_key_prefix(idx_beginning.buffer, (err, records) => {
                if (err) {
                    //throw err;
                    reject(err);
                } else {
                    //console.log('records', records);

                    let rl = new Record_List(records);
                    //console.log('rl', rl);

                    //console.log('rl.decoded', rl.decoded);

                    // but want to select the 1st / ith item from the record list.

                    // get_nth
                    let first_record = rl.get_nth(0);
                    //console.log('first_record', first_record);

                    //throw 'stop';




                    // should just be 1 record.

                    let l2 = first_record.length - idx_beginning.buffer.length;
                    let b2 = Buffer.alloc(l2);

                    first_record.copy(b2, 0, idx_beginning.buffer.length);
                    //console.log('b2', b2);

                    let decoded_2 = Binary_Encoding.decode_buffer(b2);
                    //console.log('decoded_2', decoded_2);


                    resolve(decoded_2);



                }
            })
        })

        return prom_opt_cb(res, callback);



    }


    // get the record itself by an index field lookup

    get_table_record_field_by_index_lookup(
        table_name,
        field_name,
        index_field_name,
        index_field_value,
        callback
    ) {
        //var that = this;


        // get the primary key for it.

        (async () => {
            // But need to look up on the model which index can get the id by which field.

            // Put the index key together



            let pk = await this.get_table_record_pk_by_index_lookup(table_name, index_field_name, index_field_value);

            console.log('pk', pk);



        })();



        //throw 'NYI';

        // Possibly there should be further functionality for this on the server.
        //  A server maintaining its own core model would be useful.
        //   That means the model would not hold non-core records.
        //    It would know how to do the indexing.

        // A local copy of the Model would help scan the indexes to see which fields are there.
        //  Maintaining the local copy of the Model seems very useful for a lot of functionality.
        //   Nevertheless, it will be useful to be able to operate without a local copy of the model.

        // Having a copy of the Model on both the client and the server seems very useful.
        //  The server could load its model automatically on load.
        //  There could be ws functions made available to the client to read from the server-side model.

        // There could also be server-side index verification and fixing.
        //  Getting the Model running on the server means the server could properly index rows.


        // want to do this as a promise if we were not given the callback

        /*

        let inner = (callback) => {
            let table = this.model.map_tables[table_name];

            let table_id = table.id;
            let table_kp = table_id * 2 + 2;

            console.log('field_name', field_name);

            console.log('Object.keys(table.map_fields)', Object.keys(table.map_fields));

            let field_id = table.map_fields[field_name];


            console.log('field_id', field_id);

            // get the field id of what we are looking for,




            (async () => {
                // But need to look up on the model which index can get the id by which field.

                // Put the index key together



                //let index_key =



            })();
        }

        return prom_or_cb(inner, callback);

        */



    }

    iterate_backup_files(path, cb_iteration, cb_done) {
        fs.readdir(path, (err, files) => {
            var fns = Fns();
            files.forEach(file => {
                fns.push([
                    fs.readFile, [path + "/" + file],
                    (err, res) => {
                        console.log("file", file);
                        cb_iteration(res, file);
                    }
                ]);
            });
            fns.go(cb_done);
        });
    }

    validate_last_backup(callback) {
        // Needs to ensure the model is loaded first.
        //  Load it from the server if its not already.

        var that = this;

        that.ensure_model((err, model) => {
            if (err) {
                callback(err);
            } else {
                last_backup_path((err, lbp) => {
                    if (err) {
                        callback(err);
                    } else {
                        //console.log('lbp', lbp);

                        // need a map of table kps.

                        var map_kps = this.model.map_table_kps;
                        var table, kp;

                        // Check the row against the table

                        // and stop function?s

                        var res = true;

                        // Need to indicate when its complete

                        // better iterator needed. want file name too.

                        // iterate backup files, in parallel...
                        //  meaning different in the event loop.

                        var decoded, still_buf_encoded_rows, rows;
                        that.iterate_backup_files(
                            lbp,
                            (file, file_name) => {
                                // Could wrap older forms of encoding all within an encoding type.
                                //  Need to get in the habit of always specifying an encoding type.

                                //console.log('2) file.length', file.length);
                                // decode it

                                // Internal records have got xas2 prefix of 1.
                                //  Others don't have any xas2 prefix.

                                // The internal array gets encoded using XAS2 prefixes.
                                //  It should be possible to label some part of the encoded data as using xas2 prefixes.
                                //   This would be done during the backup process.
                                //    During array encoding.

                                decoded = Binary_Encoding.decode_buffer(file)[0];
                                //console.log('decoded.length', decoded.length);

                                // Decode rows from within Binary_Encoding.

                                still_buf_encoded_rows = Binary_Encoding.get_row_buffers(
                                    decoded
                                );
                                //console.log('still_buf_encoded_rows.length', still_buf_encoded_rows.length);

                                rows = Model_Database.decode_model_rows(still_buf_encoded_rows);

                                //console.log('rows', rows);

                                // possibly could do this in a web worker.

                                each(rows, row => {
                                    kp = row[0][0];
                                    table = map_kps[kp];
                                    //var row_is_valid = validate_row(row);
                                    //console.log('row_is_valid', row_is_valid);
                                    if (!table.validate_row(row)) {
                                        res = false;
                                    }
                                });
                                // Then decode with xas2

                                //var decoded_2 = Binary_Encoding.decode_buffer(decoded, 0);
                                //console.log('decoded_2.length', decoded_2.length);
                            },
                            (err, res_complete) => {
                                console.log("files iteration complete");
                                callback(null, res);
                            }
                        );
                    }
                });
            }
        });
    }



    // scan table records
    error_scan_table(table_name) {
        //let table_id = this.model.table_id(table);

        //console.log('');
        //console.log('pre get_table_records');
        let obs_records = this.get_table_records(table_name, false);
        obs_records.unpaged = true;
        //obs_records.
        // 

        //console.log('2) obs_records.unpaged', obs_records.unpaged);


        //console.log('3) obs_records.unpaged', obs_records.unpaged);
        // get_table_records should automatically unpage




        let res = new Evented_Class();
        let error_records = [];
        obs_records.on('next', record => {
            //console.log('record', record);



            try {

                let decoded = Model_Database.decode_model_row(record);


                //let decoded = Model_Database.decode_model_row(record);


            } catch (err) {
                error_records.push(record);
            }
        });

        obs_records.on('complete', () => {
            //console.log('error_records', error_records);

            // And can try to decode the values of each of them.

            each(error_records, error_record => {
                let d_value = Binary_Encoding.decode_buffer(error_record[1]);
                //console.log('d_value', d_value);
                //console.log('decoded', decoded);

                res.raise('next', error_record);
            });

            res.raise('complete');

        });

        return res;
        //return obs_records;
    }

}

var last_backup_path = callback => {
    var user_dir = os.homedir();
    //console.log('user_dir', user_dir);
    //var docs_dir =

    var path_backups = user_dir + "/NextLevelDB/backups";
    path_backups = path_backups.split("\\").join("/");

    //exists(path_backups, )

    directory_exists(path_backups, (err, exists) => {
        if (!exists) {
            callback(
                new Error("No backup path found, expected it at:", path_backups)
            );
        } else {
            get_directories(path_backups, (err, dirs) => {
                if (err) {
                    callback(err);
                } else {
                    if (dirs.length === 0) {
                        callback(null, path_backups + "/0000 " + name);
                    } else {
                        dirs.sort();
                        var last = dirs[dirs.length - 1];
                        callback(null, path_backups + "/" + last);
                    }
                }
            });
        }
    });
};

var new_backup_path = (name, callback) => {
    //console.log('new_backup_path');
    var user_dir = os.homedir();
    //console.log('user_dir', user_dir);
    //var docs_dir =

    var path_backups = user_dir + "/NextLevelDB/backups";
    path_backups = path_backups.split("\\").join("/");
    //console.log('path_backups', path_backups);

    // ensure that directory exists.

    ensure_directory_exists(path_backups, (err, res) => {
        //console.log('res', res);

        if (err) {
            callback(err);
        } else {
            get_directories(path_backups, (err, dirs) => {
                if (err) {
                    callback(err);
                } else {
                    //console.log('dirs', dirs);

                    //console.log('2* path_backups', path_backups);

                    if (dirs.length === 0) {
                        //console.log()
                        callback(null, path_backups + "/0000 " + name);
                    } else {
                        dirs.sort();
                        //console.log('dirs', dirs);
                        var last = dirs[dirs.length - 1];
                        var str_num = last.split(" ")[0];
                        //console.log('str_num', str_num);

                        var i_num = parseInt(str_num, 10);
                        i_num++;

                        var s_num = pad(i_num, 4);
                        //console.log('s_num', s_num);

                        var res_path = path_backups + "/" + s_num + " " + name;
                        //console.log('res_path', res_path);
                        //throw 'stop';
                        callback(null, res_path);
                    }
                }
            });
        }
    });
};

NextlevelDB_Client.new_backup_path = new_backup_path;
NextlevelDB_Client.last_backup_path = last_backup_path;

// count_each_table_records


module.exports = NextlevelDB_Client;

// to the xeon?
//  192.168.1.159


if (require.main === module) {

    var config = require('my-config').init({
        path: path.resolve('../../config/config.json') //,
        //env : process.env['NODE_ENV']
        //env : process.env
    });

    let access_token = config.nextleveldb_access.root[0];



    var local_info = {
        'server_address': 'localhost',
        //'server_address': 'localhost',
        //'db_path': 'localhost',
        'server_port': 420,
        'access_token': access_token
    }

    var local_xeon = {
        'server_address': '192.168.1.159',
        'server_port': 420
    }


    console.log('access_token', access_token);
    var server_data8 = config.nextleveldb_connections.data8;
    server_data8.access_token = access_token;



    var lc = new NextlevelDB_Client(server_data8);

    // Looks like the level client keeps itself open.
    //  console.log('pre start');

    lc.start((err, res_start) => {
        if (err) {
            console.trace();
            throw err;
        } else {

            console.log('Client started');

            console.log('icpt', lc.model.index_count_per_table);



            // Automatically loading the core on start makes sense.



            // count_each_table_records_up_to
            // count_each_table_records

            let test_markets_info = () => {
                lc.count_each_table_records_up_to(1000, (err, res_count) => {
                    if (err) {
                        console.trace();
                        throw err;
                    } else {
                        console.log('res_count', res_count);

                        // limited version

                        lc.get_table_records_up_to('bittrex markets', 10, (err, records) => {
                            if (err) {
                                console.trace();
                                throw err;
                            } else {
                                //console.log('bittrex markets');
                                //console.log('records', records);
                                // get the field items as 

                                lc.get_table_fields_records('bittrex markets', (err, table_fields_records) => {
                                    if (err) {
                                        throw err;
                                    } else {
                                        console.log('table_fields_records', table_fields_records);
                                        // get_table_fields_info

                                        lc.get_table_fields_info('bittrex markets', (err, table_fields_info) => {
                                            if (err) {
                                                throw err;
                                            } else {
                                                console.log('table_fields_info', table_fields_info);
                                                // get_table_fields_info

                                            }
                                        });
                                    }
                                });
                            }
                        })
                    }
                })
            }

            let test_table_subscription = (table_name) => {
                table_subscription = lc.get_table_subscription(table_name);
                console.log('table_subscription', table_subscription);

                table_subscription.on('batch_put', (records) => {
                    //console.log('records', JSON.stringify(records));

                    console.log('records JSON length', JSON.stringify(records).length);

                    // Could strip the table kp.
                })
            }
            //test_table_subscription('bittrex market summary snapshots');


            let test_paged_get_table_records = table_name => {
                // Would use default paging when using an observable.

                // What about with no decoding.
                //  Can not handle it client-side fast enough

                let obs_table_records = lc.get_table_records(table_name, true);
                obs_table_records.unpaged = true;

                obs_table_records.on('next', data => {

                    console.log('data', data);
                    console.log('data.length', data.length);

                    //data_pages.push(data);
                    //obs_res.raise('next', data);
                });
                obs_table_records.on('complete', data => {
                    //console.log('data', data);
                    console.log('completed data.length', data.length);

                    // Don't get the last data again.
                    //obs_res.raise('complete', data);


                    //console.log('completed data (last page)', data);



                    //let all_records = [].concat.apply([], data_pages);
                    //console.log('all_records.length', all_records.length);
                });

            }
            //test_paged_get_table_records('bittrex market summary snapshots');
            //test_paged_get_table_records('bittrex currencies');

            let test_paged_get_table_keys = table_name => {

                let obs_table_records = lc.get_table_keys(table_name, true);

                // Auto data amalgamation, or use the observable as a stream.

                obs_table_records.on('next', data => {
                    console.log('data', data);
                    console.log('data.length', data.length);
                });
                obs_table_records.on('complete', last_data => {
                    //console.log('data', data);
                    console.log('completed last_data.length', last_data.length);
                });

            }
            //test_paged_get_table_keys('bittrex market summary snapshots');

            let test_select_from_table = () => {

                // Want single results back by default.

                // Select all table keys
                // get_table_keys
                //  





                //let obs_select = lc.select_from_table('bittrex currencies', ['id', 'Currency', 'CurrencyLong']);
                let obs_select = lc.select_from_table('bittrex markets', [0, 1, 3]);

                // Without paging specified here, it's better to get the records individually.
                //  Or can specify we give back a page in the results.
                //   An observable callback for each record would be quite a nice programming model, may not be the most performant.
                //    Still should be OK at processing many records per second.



                obs_select.on('next', data => {

                    // We still want them back individually as we have not specified paging.




                    console.log('obs_select data', data);
                })
                obs_select.on('complete', () => {
                    console.log('complete');
                })

            }
            //test_select_from_table();


            let test_get_table_keys = () => {

                // should remove KPs by default.

                let obs = lc.get_table_keys('bittrex markets');
                obs.on('next', data => {
                    console.log('data', data);
                });
                obs.on('complete', () => {
                    console.log('complete');
                })
            }
            //test_get_table_keys();

            let test_get_table_index_records = () => {
                let table_name = 'bittrex currencies';
                lc.get_table_index_records(table_name, (err, index_records) => {
                    if (err) {
                        throw err;
                    } else {
                        console.log('*35 ' + table_name + ' index_records');
                        each(index_records, index_record => {
                            console.log('index_record', index_record);
                        })
                    }
                })
            }
            //test_get_table_index_records();


            let scan = () => {
                //let obs_scan = lc.error_scan_table('bittrex currencies');

                // do this without decoding.
                //  error scan table needs to get the rows without decoding.
                //  seems best to use this as an option on the result, save param complexity.



                let obs_scan = lc.error_scan_table('bittrex markets');
                obs_scan.on('next', data => {
                    console.log('scan data', data);

                    let decoded_data_value = Binary_Encoding.decode_buffer(data[1]);
                    console.log('decoded_data_value', decoded_data_value);


                })
            }
            //scan();


            let test_get_table_key_subdivisions = () => {

                /*

                let obs = lc.get_table_key_subdivisions('bittrex market summary snapshots');



                obs.on('next', data => console.log('2) data', data));

                */

                lc.get_table_key_subdivisions('bittrex market summary snapshots', (err, subdivisions) => {
                    if (err) {
                        throw err;
                    } else {
                        //console.log('subdivisions', subdivisions);
                        console.log('Subdivisions');
                        console.log('------------');
                        each(subdivisions, subdivision => console.log(subdivision));
                    }
                });

            }
            //test_get_table_key_subdivisions();

            // Check the number of indexes for each table.



        }
    });
    var all_data = [];
} else {
    //console.log('required as a module');
}