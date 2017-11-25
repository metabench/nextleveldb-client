
// Could validate it against an existing table structure.

// Supply the table name
const Client = require('../nextleveldb-client');
const Binary_Encoding = require('binary-encoding');

// read the backup path.

// could look at the table's fields to check each record.

// Go through the .be files, loading them, and decoding the values.

// iterate_latest_backup_files

// also want to iterate the latest backup records.
//  could count them, and ensure they fit in with the fields, as defined.


const lang = require('lang-mini');
const each = lang.each;
const Fns = lang.Fns;
const arrayify = lang.arrayify;
const path = require('path');

//console.log(path.resolve('../../../config/config.json'));

var config_path = path.resolve('../../../config/config.json');
var config = require('my-config').init({
	path : config_path//,
	//env : process.env['NODE_ENV']
	//env : process.env
});


if (require.main === module) {
    //setTimeout(() => {
//var db = new Database();

var server_data1 = config.nextleveldb_connections.data1;
//var server_data1 = config.nextleveldb_connections.localhost;

// The table field (for info on the fields themselves) rows are wrong on the remote database which has got approx 12 days of data.
//  Can still extract the data, I expect.
// Don't want to replace the code on the server quite yet.

// May be possible to edit the fields, possibly validate the fields?

var client = new Client(server_data1);

client.start((err, res_start) => {
    if (err) {
        throw err;
    } else {
        //console.log('Assets Client connected to', server_data1);

        client.validate_last_backup((err, validation) => {

            // Will get into more row checking.
            //  Will say which files get validated as they pass.

            if (err) {
                throw err;
            } else {
                console.log('validation', validation);
            }
        });
    }
});
} else {
//console.log('required as a module');
}

