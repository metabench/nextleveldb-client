var jsgui = require('jsgui3');
var each = jsgui.each, tof = jsgui.tof, Fns = jsgui.Fns;

var LL_NextlevelDB_Client = require('./ll-nextleveldb-client');
var xas2 = require('xas2');
var Binary_Encoding = require('binary-encoding');
var Model = require('nextleveldb-model');
var Model_Database = Model.Database;

var Array_Table = require('arr-table');

class NextlevelDB_Client extends LL_NextlevelDB_Client {

    // Functionality to persist entire models, or system tables.
    //  Will interact with a Model.

    // Could have a local Model?

    // Initial setup of crypto database.

    // load model, including specific tables


    'load_core'(callback) {
        var that = this;
        this.ll_get_core((err, buf_core) => {
            if (err) { callback(err); } else {
                //console.log('buf_core', buf_core);
                that.model = Model_Database.load(buf_core);
                callback(null, that.model);

            }
        });
    }

    'load_table'(table_name, callback) {
        var that = this;
        var table = that.model.map_tables[table_name];
        that.get_table_records(table_name, (err, table_records) => {
            if (err) { callback(err); } else {
                //throw 'stop';

                /*
                table.add_records_including_table_id_in_key(table_records, true, (err, res) => {
                    if (err) { callback(err); } else {
                        throw 'stop';
                        callback(null, table);
                    }
                });
                */

                // The get_table_records function won't have the id within the key.

                
                table.add_records(table_records, true);
                //table.add_records_including_table_id_in_key(table_records, true);
                callback(null, table);
            }
        });
    }


    'load_tables'(arr_table_names, callback) {
        var fns = Fns();
        //fns.push([fn, arr_params]);

        // get table records should decode the records
        var that = this;

        each(arr_table_names, (table_name) => { fns.push([that, that.get_table_records, [table_name]]); });
        fns.go((err, res_all) => {
            if (err) { callback(err); } else {
                //console.log('res_all', res_all);
                //console.log('res_all', JSON.stringify(res_all));
                //console.log('res_all.length', res_all.length);

                

                each(res_all, (table_records, table_index) => {
                    var table_name = arr_table_names[table_index];
                    //console.log('table_name', table_name);
                    //console.log('');
                    //console.log('table_records', table_records);

                    // then for each table in the model, load the records.

                    // each of the records contains the table id.

                    // add_records_including_table_id_in_key

                    var table = that.model.map_tables[table_name];


                    //table.add_records_including_table_id_in_key(table_records, true);
                    table.add_records(table_records, true);
                    
                    // but does this set up the indexing correctly?

                    // a way to add the records while verifying the indexing?
                    //  add records without indexing?

                    // Could reindex and see where the problem is.
                });

                callback(null, true);

                //throw 'stop';
            }
        });
    }

    // load table

    // get an obj_map from a table...
    //  much like an index

    // Loading to the client's model.

    'load_core_plus_tables'(arr_table_names, callback) {

        // load the model from the core.
        //  then load other tables

        // can we load multiple tables at once from the server into one buffer?

        // or load the tables individually.

        var that = this;
        this.load_core((err, model) => {
            if (err) { callback(err); } else {

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

    'validate_table_index'(table_name, callback) {
        // need to get the table rows, and the index rows

        // rebuild new index rows from the table rows
        // compare the rebuilt index rows to the original ones.

        // could start by doing a count.
        //  if there are 0 index rows all we need to do is build an index (in another function).
        //   return '0 index rows';

        // get the index rows
        var that = this;

        this.get_table_index_records(table_name, (err, index_records) => {
            if (err) { callback(err); } else {

                // get_table_index_records should decode the records

                

                if (index_records.length === 0) {
                    callback(null, '0 index rows');
                } else {

                    // load the table itself (into the model)
                    //  that would recreate the index records.

                    that.load_table(table_name, (err, table) => {
                        if (err) { callback(err); } else {
                            console.log('table.records.length', table.records.length);

                            // search for index that are missing.

                            // table get arr data index rows

                            var table_index_records = table.get_arr_data_index_records();
                            

                            // then we see which are missing from remote

                            console.log('index_records', index_records);
                            console.log('index_records.length', index_records.length);
                            console.log('table.records.arr_records', table.records.arr_records);

                            console.log('table_index_records', table_index_records);


                            throw 'stop';



                        }
                    });


                }
            }
        });
    }

    'maintain_table_index'(table_name, callback) {

        // Would be nice to get the table index info from the db.


        this.validate_table_index(table_name, (err, validation) => {
            if (err) { callback(err); } else {
                console.log('validation', validation);

                if (validation === '0 index rows') {
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

    'validate_core_index_table'(callback) {
        // need to have a model to validate against.

        // get the index table records from the remote db

        var model = this.model;
        if (!model) {
            throw 'this.model not found';
        }

        this.get_table_records('table indexes', (err, remote_index_table_records) => {
            if (err) { callback(err); } else {
                //console.log('remote_index_table_records', remote_index_table_records);
                //console.log('remote_index_table_records.length', remote_index_table_records.length);

                //if (index_records.length === 0) {
                //    callback(null, '0 index rows');
                //}

                // get the core index table from the model

                var model_indexes_table = model.map_tables['table indexes'];
                //console.log('model_indexes_table', model_indexes_table);
                //console.log('model_indexes_table.records.length', model_indexes_table.records.length);

                // two arrays
                //  missing from first, missing from second.
                //   if we only find items missing from the second, we are good to do the update.

                if (model_indexes_table.records.length > remote_index_table_records.length) {
                    // can check for missing records.

                    callback(null, 'number of index records in model > number of index records in remote db');
                } else {
                    callback(null, true);
                }

            }
        });
    }

    // could have a higher level version that gets the decoded keys



    'replace_core_index_table'(callback) {
        this.put_model_table_records('table indexes', callback);
    }


    // Will have maintain_table_indexes script.
    //  

    // Server-side model functionality would make a lot of sense for dealing with indexes.



    // Should have more functions that deal with record collections.
    // 

    // Assuming we want to insert the table key prefix into the records.
    // Does not create index items for the records.
    'put_table_arr_records'(table_name, arr_records, callback) {
        var model = this.model;
        if (!model) {
            //throw 'this.model not found';
            callback('this.model not found');
        } else {
            // get the table records, encoded as binary

            var table = model.map_tables[table_name];
            if (!table) {
                callback('table ' + table_name + ' not found');
            } else {
                // then get the binary records from the table, perform a ll put

                // splice the table kp into the records.

                
                var arr_record_data = [];
                each(arr_records, (record) => {
                    //console.log('record', record);
                    //console.log('record.key', record.key);
                    //record.key.splice(0, 0, table.key_prefix);
                    arr_record_data.push(record.arr_data);
                });

                

                // The records

                //console.log('arr_records', arr_records);
                //throw 'stop';
                //console.log('arr_record_data', arr_record_data);
                //throw 'stop';

                // Want to be able to encode arr_record_data as binary, easily, using the row encoding.

                // 
                
                var buf_rows = Model_Database.encode_arr_rows_to_buf(arr_record_data, table.key_prefix);
                //console.log('buf_rows', buf_rows);
                //console.log('buf_rows.length', buf_rows.length);

                // put that buffer.
                this.ll_put_records_buffer(buf_rows, (err, res_put_buf) => {
                    if (err) { callback(err); } else {
                        callback(null, arr_record_data);
                    }
                });

                // Should do some validation in the future.
                //  Go through the db, validating structures, finding rows that are malformed, deleting them.


                // Just contains the records to be sent to the database.
                //  Does not do the indexing for them.

                // Possibility of temporarily loading up model functionality on the server?

                // Maybe it needs a Local_NextlevelDB_Client.
                //  Basically acts as the DBMS, but acts with the same API as the web socket client.

                // May be important to create the indexes on the client too.

                // Could also do some client-side index validation or generation.
                //  With many functions, need to break things down and keep them organised.

                // then encode these records.

            }

        }
    }

    'put_arr_records'(arr_records, callback) {
        // encode the records into binary buffer

        console.log('put_arr_records arr_records', arr_records);

        throw 'stop';
    }


    // put model table records

    'put_model_table_records'(table_name, callback) {
        var model = this.model;
        if (!model) {
            //throw 'this.model not found';
            callback('this.model not found');
        } else {
            // get the table records, encoded as binary

            var table = model.map_tables[table_name];
            if (!table) {
                callback('table ' + table_name + ' not found');
            } else {
                // then get the binary records from the table, perform a ll put

                var arr_bufs_table_records = table.get_all_db_records_bin();
                //console.log('arr_bufs_table_records', arr_bufs_table_records);

                // Could use some encoding that's part of the Model.



                var encoded_buf = Model_Database.encode_model_rows(arr_bufs_table_records);
                //console.log('encoded_buf', encoded_buf);

                // then do the ll put.
                this.ll_put_records_buffer(encoded_buf, callback);

                // then encode these records.

            }

        }
    }


    'maintain_core_index_table'(callback) {
        // validate it,
        //  if it's not right, then replace the remote records with the new version.

        // seems a bit tricky, as we don't want to remove any records.

        // could check to see

        // var model_indexes_table = model.map_tables['table indexes'];


    }

    'update_core_index_table'(callback) {
        // can't currently delete records in range using api

        // need to work on delete record range and delete record.
        //  would need to delete the associated index records too.
        //   so need to generate the index records in order to find them it seems.




        // just put them all for the moment.

        



    }

    // could possibly import data from another database too.





    // Have the Crypto_Collector interact with this NextlevelDB_Client.
    //  Will be nice to have the Crypto_Collector actually collect assets in the future.
    //  Crypto_Collector will listen to a Bittrex_Watcher, and connect to this.

    // Crypto_Collector for the moment will focus on its info pipe.

    'count_records_by_key_prefix'(i_kp, callback) {
        var buf_kp = xas2(i_kp).buffer;
        var buf_0 = Buffer.alloc(1);
        buf_0.writeUInt8(0, 0);
        var buf_1 = Buffer.alloc(1);
        buf_1.writeUInt8(255, 0);
        // and another 0 byte...?

        var buf_l = Buffer.concat([buf_kp, buf_0]);
        var buf_u = Buffer.concat([buf_kp, buf_1]);

        this.ll_count_keys_in_range(buf_l, buf_u, (err, res_count) => {
            if (err) { throw err; } else {
                //console.log('res_count', res_count);

                callback(null, res_count);

                // 
                //throw 'stop';
            }
        })
    }

    'count_keys_beginning'(buf_key_beginning, callback) {
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
    // get_decoded_records_by_key_prefix
    'get_records_by_key_prefix'(key_prefix, callback) {
        this.ll_get_records_by_key_prefix(key_prefix, (err, encoded_records) => {
            if (err) { callback(err); } else {
                const remove_kp = 1;
                var res = Model_Database.decode_model_rows(encoded_records, remove_kp);

                // While removing the key prefix.


                callback(null, res);
            }
        });
    }

    'get_table_records'(table_name, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                this.get_records_by_key_prefix(kp, callback);
            } else {
                callback('Table ' + table_name + ' not found');
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback('Expected this.model, otherwise can\'t find table by name');
        }
    }

    // getting an Arr_Table of the table records
    //  arr_table will have the keys and values in one array, together.
    //   could have functionality to output as key value pairs too.

    'get_at_table_records'(table_name, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                this.get_table_records(table_name, (err, table_records) => {
                    if (err) { callback(err); } else {
                        // flatten kvps
        
                        var flat_records = [];
                        each(table_records, (record) => {
                            flat_records.push(record[0].concat(record[1]));
                        });
                        //console.log('flat_records', flat_records);
                        //console.log('table.field_names', table.field_names);

                        var res = new Array_Table([table.field_names, flat_records]);
                        callback(null, res);

        
        
                    }
                });
            } else {
                callback('Table ' + table_name + ' not found');
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback('Expected this.model, otherwise can\'t find table by name');
        }
        
    }



    'table_index_lookup'(table_name, index_id, value, callback) {
        // only looking up one value in the index for the moment?

        //  could also look at an array of values.

        var t_value = tof(value);

        var table_kp = this.model.map_tables[table_name].key_prefix;

        if (t_value === 'array') {
            throw 'yet to implement'
        } else {

            // also need to put the index part together

            // the indexes have got 2 xas2 key prefixes, followed by a binary encoded array


            // encode_key function would be helpful
            //  encode_index_key

            var buf_idx_key = Model_Database.encode_index_key(table_kp + 1, index_id, [value]);
            //console.log('buf_idx_key', buf_idx_key);

            //throw 'stop';

            // all values matching.

            this.ll_get_keys_beginning(buf_idx_key, (err, ll_res) => {
                if (err) { callback(err); } else {
                    console.log('ll_res', ll_res);

                    var decoded_index_key = Model_Database.decode_key(ll_res[0]);
                    console.log('decoded_index_key', decoded_index_key);

                    var arr_pk_ref = decoded_index_key.slice(3);
                    console.log('arr_pk_ref', arr_pk_ref);

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

    'get_first_last_table_keys_in_key_selection'(table_name, key, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;

                // Need consistent rules for this.

                // key in an array?
                //  or in an array if it's got length more than 1

                var buf_key = Model_Database.encode_key(kp, key);

                //var buf_key = Model_Database.encode_key(kp, key);
                //console.log('buf_key', buf_key);
                //console.log('key', key);



                // then do the count from that key beginning

                // ll_get_first_last_keys_beginning
                //  ll_get_first_last_keys_in_range

                //this.ll_count_keys_beginning(buf_key, callback);

                this.ll_get_first_last_keys_beginning(buf_key, (err, ll_res) => {
                    if (err) { callback(err); } else {
                        var res = Model_Database.decode_keys(ll_res);
                        callback(null, res);
                    }
                });

                //this.ll

            } else {
                callback('Table ' + table_name + ' not found');
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback('Expected this.model, otherwise can\'t find table by name');
        }
    }


    // count_table_selection
    //  count_table_key_selection

    'count_table_key_selection'(table_name, key, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;

                // Need consistent rules for this.

                // key in an array?
                //  or in an array if it's got length more than 1

                var buf_key = Model_Database.encode_key(kp, key);

                //var buf_key = Model_Database.encode_key(kp, key);
                //console.log('buf_key', buf_key);
                //console.log('key', key);

                // then do the count from that key beginning

                this.ll_count_keys_beginning(buf_key, callback);

                //this.ll

            } else {
                callback('Table ' + table_name + ' not found');
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback('Expected this.model, otherwise can\'t find table by name');
        }
    }


    // Maybe this is the LL version because the results are still binary encoded
    //  not in ll because it requires use of the model.
    'll_get_table_index_records'(table_name, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                // 

                this.ll_get_records_by_key_prefix(kp + 1, callback);
            } else {
                callback('Table ' + table_name + ' not found');
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback('Expected this.model, otherwise can\'t find table by name');
        }
    }

    'get_table_index_records'(table_name, callback) {
        this.ll_get_table_index_records(table_name, (err, index_records) => {
            if (err) { callback(err); } else {
                var decoded_index_records = Model_Database.decode_model_rows(index_records);
                console.log('decoded_index_records', decoded_index_records);
                callback(null, decoded_index_records);
            }
        })
    }



    // 

    // Should probably decode them if its not a LL function.

    'get_keys_by_key_prefix'(key_prefix, callback) {
        this.ll_get_keys_by_key_prefix(key_prefix, (err, ll_res) => {
            if (err) { callback(err); } else {
                var res = Model_Database.decode_keys(ll_res);
                callback(null, res);
            }
        });
    }

    'get_table_keys'(table_name, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                this.get_keys_by_key_prefix(kp, callback);
            } else {
                callback('Table ' + table_name + ' not found');
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback('Expected this.model, otherwise can\'t find table by name');
        }
    }

    'get_table_index_keys'(table_name, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                this.get_keys_by_key_prefix(kp + 1, callback);
            } else {
                callback('Table ' + table_name + ' not found');
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback('Expected this.model, otherwise can\'t find table by name');
        }
    }

    // get all tables, then get the counts and index counts for all of them

    // all_tables_records_and_index_records_counts
    //  could also do this while iterating through all keys.

    // Then need to narrow down on available times / timestamps

    // The encoded data in the buffers is relatively efficient.
    //  Would take a bit more work on the server to have a model that can efficiently do some index lookups.



    // The full A&A has a fair bit of complexity.
    //  Allows for actions to be defined and assigned to users and their groups

    // Would be worth integrating authentication authentication into the db sooner rather than later.
    //  Also, users, groups, user groups, permission consumer (maybe just an id), permission based actions, permission based action consumers
    //   roles, user roles
    //    being like groups?
    //  auth tokens

    // There is quite a lot of complexity in securing the database and allowing granular permission setting.


    // For the moment, exporting data seems most important.
    //  When a more advanced database with A&A is set up, its import feature can be used along with the more basic db's export feature.


    // Want a db function that will take a snapshot and export all rows from it.
    //  Could call that at the same time as subscribing to updates.


    // general purpose batching commands for the server?
    //  That would definitely help extensibility, and quicker development of more features on the client.

    // In the very near term, substantially improve the server functionality to make sure it's good enough for the longer term.
    //  See about putting everything in apart from authentication to start with.
    //  An authentication / access middleware module would work well.








    'count_table_records'(table_name, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                this.count_records_by_key_prefix(kp, callback);
            } else {
                callback('Table ' + table_name + ' not found');
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback('Expected this.model, otherwise can\'t find table by name');
        }
    }

    'count_table_index_records'(table_name, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                this.count_records_by_key_prefix(kp + 1, callback);
            } else {
                callback('Table ' + table_name + ' not found');
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback('Expected this.model, otherwise can\'t find table by name');
        }
    }


    // Call it key selection
    //  or index (key) selection?


    // We need to encode part of the key, and get record count for keys beginning with that.




    // Selecting from the index...
    //  should use key prefix plus one
    'get_table_selection_record_count'(table_name, arr_index_selection, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix;
                //this.ll_get_records_by_key_prefix(kp, callback);

                // compose the selection key

                //var selection_key = [kp];
                //each(arr_index_selection, (v) => {
                //    selection_key.push(v);
                //});

                console.log('kp', kp);

                var encoded = Binary_Encoding.encode_to_buffer(arr_index_selection, kp);
                console.log('encoded', encoded);
                throw 'stop';



                this.count_keys_beginning(encoded, callback);

                // then get count with that key prefix
                //  count keys starting


                // encode the key

                //var encoded = Binary_Encoding.encode_to_buffer(arr_index_selection, kp);
                //console.log('encoded', encoded);

                //this.ll_count_keys_in_range(buf_l, buf_u, callback);

            } else {
                callback('Table ' + table_name + ' not found');
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback('Expected this.model, otherwise can\'t find table by name');
        }
    }

    'get_table_index_selection_records'(table_name, arr_index_selection, callback) {
        if (this.model) {
            var table = this.model.map_tables[table_name];
            if (table) {
                var kp = table.key_prefix + 1;
                //this.ll_get_records_by_key_prefix(kp, callback);

                // compose the selection key

                //var selection_key = [kp];
                //each(arr_index_selection, (v) => {
                //    selection_key.push(v);
                //});

                var encoded = Binary_Encoding.encode_to_buffer(arr_index_selection, kp);
                console.log('encoded', encoded);


                // then encode the selection key

                



            } else {
                callback('Table ' + table_name + ' not found');
            }
        } else {
            //throw 'Expected this.model, otherwise can\'t find table by name'
            callback('Expected this.model, otherwise can\'t find table by name');
        }
    }


}

module.exports = NextlevelDB_Client;