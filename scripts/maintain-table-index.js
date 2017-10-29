
const NextLevelDB_Client = require('../nextleveldb-client');


if (require.main === module) {
    // use bittrex watcher to get all bittrex currencies

    var local_info = {
        'server_address': 'localhost',
        //'server_address': 'localhost',
        //'db_path': 'localhost',
        'server_port': 420
    }

    // ensure all of these currencies, and get back info that contains their IDs.

    //var bw = new Bittrex_Watcher();
    var client = new NextLevelDB_Client(local_info);

    // Could automatically load the core upon start.

    client.start((err) => {
        if (err) { throw err; } else {

            client.load_core((err) => {
                // Seems like it has not set up pk_incrementor of the table.

                if (err) { throw err; } else {
                    client.maintain_table_index('bittrex currencies', (err, res_maintain) => {
                        if (err) {
                            throw err;
                        } else {
                            console.log('res_maintain', res_maintain);
                        }
                    });
                }
            })

            

            /*

            client.load_core_plus_tables(['bittrex markets', 'bittrex currencies'], (err) => {
                // Seems like it has not set up pk_incrementor of the table.

                if (err) { throw err; } else {
                    bw.get_at_all_currencies_info((err, at_all_currencies_info) => {
                        if (err) { throw err; } else {
                            console.log('at_all_currencies_info', at_all_currencies_info);
                            
                            var arr_currencies = at_all_currencies_info.values;
                            console.log('arr_currencies', arr_currencies);
                            
                            console.log('at_all_currencies_info.keys', at_all_currencies_info.keys);
                            client.ensure_bittrex_currencies(arr_currencies, (err, res_ensured) => {
                                if (err) {
                                    throw err;
                                } else {
                                    console.log('res_ensured', res_ensured);
                                }
                            });
                
                
                        }
                    });
                }
            })

            */



            
        }
    });


    
    

} else {
    throw 'Run this as a main js file.';
}

