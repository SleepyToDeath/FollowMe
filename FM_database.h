#ifndef FM_DATABASE
#define FM_DATABASE

#include<vector>
#include<fstream>

#define index_t long long

class db_meta
{

};

class key_meta
{

};

class data_meta 
{
	index_t entry_len;
	index_t entry_sum;
	index_t free_head;
	index_t key_sum;
	vector<index_t> key_len;
	index_t max_order;
};

class entry
{
	index_t index;
	index_t order;
	string value;
	vector<string> keys;
};

class key
{
	index_t index;
	index_t order;
	string value;
}

class cache_entry
{
	entry e;
	index_t pos;
}

class table
{
	fstream dataio;
	
	vector<fstream*> keyios;

	vector<entry> cache;

	vector<index_t> hash;

	data_meta data_meta_0;

	vector<key_meta> key_metas;

};

/*
 * this database is designed for data with limited length
 * on disk everything is a string
 * new_db creates a new data base with a meta & a data file
 * new_db returns a handler>=0 if success ,-1 if fail
 * index ( in fact is position ) is the only key to identify an entry
 * add_key will add a key to the data specified by handler, if success, a key handler will
 * be returned, a meta & an index file will be created
 * a B tree will be created for each key, and stored in index file
 * keys are compared by string ( this should be ok if integers are converted to strings )
 * entries are stored in cache by central key ( that is, when writen back, entries with 
 * same central key will be writen back together, and when read from disk, a constant
 * sequential of entries are read together so that if entries with the same central key
 * are often used together, less read would be used )
 * TODO central key could be set only once right now
 * TODO all keys must be set before using right now
 */

class database
{

	public:

	database();
	~database();

	index_t new_table( index_t len );
	index_t init_table( index_t handler );

	index_t add_key( index_t handler , index_t len , bool central );
	string get_key( index_t handler , index_t key_handler , index_t index );
	void ins_key( index_t handler , index_t key_handler , index_t index , index_t order , string value );
	void get_index( index_t handler , index_t 
	index_t search();

	index_t del( index_t handler , index_t index );
	index_t add( index_t handler , char* value , vector<string> keys=vector() );
	string get( index handler , index_t index );

	private:

	void init_hash();

	/* constant */

	index_t cache_size;
	index_t prime_0;
	char init_file_name[]="database.ini";

	/* variable */


	vector<table*> tables;


};


#endif

















