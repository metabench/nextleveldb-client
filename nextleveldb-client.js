// Possibly makes it unusable in the browser.
//  Could possibly have node-nextleveldb-client

const fs = require("fs");
const os = require("os");

const lang = require("lang-mini");
let each = lang.each,
    tof = lang.tof,
    Fns = lang.Fns,
    clone = lang.clone;

const mapify = lang.mapify;

const LL_NextlevelDB_Client = require("./ll-nextleveldb-client");
const xas2 = require("xas2");
const Binary_Encoding = require("binary-encoding");
const Model = require("nextleveldb-model");
const Model_Database = Model.Database;

const Array_Table = require("arr-table");

const path = require("path");
const resolve = path.resolve;

const Table_Subscription = require('./table-subscription');

// A more advanced client would definitely help.
//  Client will be used for db replication and distribution as well.
//  The server will have a client of its own to connect to other servers.

// Want an easy way to replicate all records from one table over to the same table in a different db.
//  A GUI may prove useful for this.

// Backups and authentication seem like the best approach to keep this up online.
// Take backups from existing DBs, allow them tto be put into other dbs
//

const INSERT_TABLE_RECORD = 12;

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

    /**
     *
     *
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    load_core(callback) {
        var that = this;
        this.get_core((err, buf_core) => {
            if (err) {
                callback(err);
            } else {
                //console.log('buf_core', buf_core);
                that.model = Model_Database.load(buf_core);
                callback(null, that.model);
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
                throw "stop";
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
            fns.push([this, this.get_table_records, [table_name]]);
        });
        fns.go((err, res_all) => {
            if (err) {
                callback(err);
            } else {
                each(res_all, (table_records, table_index) => {
                    var table_name = arr_table_names[table_index];
                    var table = this.model.map_tables[table_name];
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
    put_arr_records(arr_records, callback) {
        // encode the records into binary buffer

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

    /**
     *
     *
     * @param {any} i_kp
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    count_records_by_key_prefix(i_kp, callback) {
        var buf_kp = xas2(i_kp).buffer;
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_kp, buf_0]);
        var buf_u = Buffer.concat([buf_kp, buf_1]);

        this.ll_count_keys_in_range(buf_l, buf_u, (err, res_count) => {
            if (err) {
                throw err;
            } else {
                //console.log('res_count', res_count);

                callback(null, res_count);

                //
                //throw 'stop';
            }
        });
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
    count_keys_beginning(buf_key_beginning, callback) {
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_key_beginning, buf_0]);
        var buf_u = Buffer.concat([buf_key_beginning, buf_1]);

        this.ll_count_keys_in_range(buf_l, buf_u, callback);
    }

    // function to get the decoded records by key prefix

    //

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


    get_records_by_key_prefix(key_prefix, callback) {
        this.ll_get_records_by_key_prefix(key_prefix, (err, encoded_records) => {
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


    /**
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    get_table_records(table_name, callback) {
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
                    this.get_records_by_key_prefix(kp, callback);
                }
            })

            //callback("Expected this.model, otherwise can't find table by name");
        }
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
            console.log('buf_key', buf_key);

            // then search by key (prefix)
            //  it's the beginning of the key.

            this.get_records_by_key_prefix(buf_key, callback);

            //throw 'stop';
        });
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
                var decoded_index_records = Model_Database.decode_model_rows(
                    index_records
                );
                //console.log('decoded_index_records', decoded_index_records);
                callback(null, decoded_index_records);
            }
        });
    }

    /**
     *
     *
     * @param {any} key_prefix
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
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

    get_table_id_by_name(table_name, callback) {
        // need to refer to the index of tables

        const tables_table_id = 0;
        const tables_table_kp = tables_table_id * 2 + 2;
        const tables_table_idx_kp = tables_table_kp + 1;
        const idx_id = 0;
        var buf_key_beginning = Model_Database.encode_index_key(
            tables_table_idx_kp,
            idx_id, [table_name]
        );
        this.get_keys_by_key_prefix(buf_key_beginning, (err, keys_beginning) => {
            if (err) {
                callback(err);
            } else {
                var key_beginning = keys_beginning[0];
                //console.log('key_beginning', key_beginning);
                var table_id = key_beginning[3];
                callback(null, table_id);
            }
        });
    }

    get_table_kp_by_name(table_name, callback) {
        this.get_table_id_by_name(table_name, (err, id) => {
            if (err) {
                callback(err);
            } else {
                callback(null, id * 2 + 2);
            }
        });
    }

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

    // then a version to get it by table id.

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

    // get_table_field_names

    get_table_field_names(table_name, callback) {
        // Should know the table fields id already.

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
        // look up the table id
        const that = this;

        const table_fields_kp = TABLE_FIELDS_TABLE_ID * 2 + 2;
        // Get the field records.

        that.get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                callback(err);
            } else {
                //var akp = [table_fields_id, table_id];
                let buf = Model_Database.encode_key(table_fields_kp, [table_id]);

                that.get_records_by_key_prefix(buf, (err, fields_records) => {
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
        // Check to see if the record can be found according to the indexes in the table.

        // This record maybe lacks the ID field.
        //

        // Compare the record with the number of fields the table has.
        //  May want to get the number of PK fields as well.
        //  Could give the record in a format that says its missing its PK?

        // Want the array of field names too / KV fields
        //  Or to count the missing fields...

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
                                            //console.log("val", val);

                                            //index_values.push(val);

                                            // We do want it to return the record id if it finds it.

                                            // these two values can be used for that index lookup.

                                            let encoded_index_key = Model_Database.encode_index_key(
                                                table_id * 2 + 3,
                                                table_index_id, [val]
                                            );
                                            arr_buf_index_lookup_keys.push(encoded_index_key);

                                            //Model_Database.encode_index_key(table_indexes_kp, table_index_id,
                                        } else {
                                            //console.trace();
                                            console.log('record', record);
                                            console.log('arr_record', arr_record);

                                            let index_value_field_ids = record[1];
                                            console.log('index_value_field_ids', index_value_field_ids);
                                            console.log('size_diff', size_diff);

                                            let arr_indexed_values = [];
                                            index_value_field_ids.forEach(id => {
                                                arr_indexed_values.push(arr_record[id - size_diff]);
                                            })

                                            let encoded_index_key = Model_Database.encode_index_key(
                                                table_id * 2 + 3,
                                                table_index_id,
                                                arr_indexed_values
                                            );
                                            arr_buf_index_lookup_keys.push(encoded_index_key);
                                            console.log('encoded_index_key', encoded_index_key);
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

    /**
     *
     *
     * @param {any} table_name
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
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
    count_table_records(table_name, callback) {
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

            this.get_table_id_by_name(table_name, (err, table_id) => {
                if (err) {
                    callback(err);
                } else {
                    let kp = table_id * 2 + 2;
                    this.count_records_by_key_prefix(kp, callback);

                }
            });
        }
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
        var that = this;
        that.get_table_id_by_name(table_name, (err, table_id) => {
            if (err) {
                callback(err);
            } else {
                var buf = Model_Database.encode_key(
                    table_id * 2 + 2,
                    arr_key_selection
                );
                that.get_records_by_key_prefix(buf, callback);
            }
        });
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
    get_table_selection_record_count(table_name, arr_index_selection, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                var encoded = Binary_Encoding.encode_to_buffer(arr_index_selection, kp);
                this.count_keys_beginning(encoded, callback);
            } else {
                callback("Table " + table_name + " not found");
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback("Expected this.model, otherwise can't find table by name");
        }
    }

    /**
     * @param {any} table_name
     * @param {any} arr_index_selection
     * @param {any} callback
     * @memberof NextlevelDB_Client
     */
    get_table_index_selection_records(table_name, arr_index_selection, callback) {
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
                        let num_xas2_prefixes = 0;
                        let arr_bufs_kv = Binary_Encoding.split_length_item_encoded_buffer_to_kv(ll_res);
                        let remove_kp = true;
                        let arr_decoded = Model_Database.decode_model_row(arr_bufs_kv[0], remove_kp);
                        callback(null, arr_decoded);
                    }
                })
            }
        })
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

    get_table_record_field_by_index_lookup(
        table_name,
        field_name,
        index_field_name,
        index_field_value,
        callback
    ) {
        //var that = this;

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

        this.get_table_kp_by_name(table_name, (err, kp) => {
            if (err) {
                callback(err);
            } else {
                //var buf_kp = xas2(kp).buffer;
                //that.subscribe_key_prefix_puts(buf_kp, subscription_event_handler);

                var idx_kp = kp + 1;
                //  then
            }
        });
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

var local_info = {
    'server_address': 'localhost',
    //'server_address': 'localhost',
    //'db_path': 'localhost',
    'server_port': 420
}

var local_xeon = {
    'server_address': '192.168.1.159',
    'server_port': 420
}


if (require.main === module) {
    var lc = new NextlevelDB_Client(local_xeon);

    // Looks like the level client keeps itself open.
    //  console.log('pre start');

    lc.start((err, res_start) => {
        if (err) {
            throw err;
        } else {

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
                                console.log('bittrex markets');
                                console.log('records', records);
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
            test_table_subscription('bittrex market summary snapshots');

        }
    });

    var all_data = [];

} else {
    //console.log('required as a module');
}